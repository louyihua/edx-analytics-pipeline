import csv
import datetime
import hashlib
import json
import logging
from itertools import groupby
from operator import itemgetter
import re
import sys
import time

import luigi
import luigi.task
from luigi import date_interval

try:
    from elasticsearch import Elasticsearch
    from elasticsearch.client import IndicesClient
    from elasticsearch.exceptions import NotFoundError
    from elasticsearch import helpers
except ImportError:
    pass

from edx.analytics.tasks.calendar_task import CalendarTableTask
from edx.analytics.tasks.database_imports import (
    ImportAuthUserTask, ImportAuthUserProfileTask, ImportCourseUserGroupTask, ImportCourseUserGroupUsersTask)
from edx.analytics.tasks.enrollments import CourseEnrollmentTableTask
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join, IgnoredTarget
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.mysql_load import MysqlInsertTask

from edx.analytics.tasks.util.hive import WarehouseMixin, BareHiveTableTask, HivePartitionTask, HivePartition

log = logging.getLogger(__name__)


class EngagementTableTask(BareHiveTableTask):

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'engagement'

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('username', 'STRING'),
            ('entity_type', 'STRING'),
            ('entity_id', 'STRING'),
            ('count', 'INT')
        ]


class EngagementTask(EventLogSelectionMixin, WarehouseMixin, MapReduceJobTask):

    # Required parameters
    date = luigi.DateParameter()

    # Optional parameters
    output_root = luigi.Parameter(default=None)

    # Override superclass to disable these parameters
    interval = None

    def __init__(self, *args, **kwargs):
        super(EngagementTask, self).__init__(*args, **kwargs)

        self.interval = date_interval.Date.from_date(self.date)
        if not self.output_root:
            self.output_root = url_path_join(self.warehouse_path, 'engagement', 'dt=' + self.date.isoformat())

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        username = event.get('username', '').strip()
        if not username:
            return

        event_type = event.get('event_type')
        if event_type is None:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        event_source = event.get('event_source')

        entity_id = None
        entity_type = None
        if event_type == 'problem_check':
            if event_source != 'server':
                return

            entity_type = 'problem'
            entity_id = event_data.get('problem_id')
        elif event_type == 'play_video':
            entity_type = 'video'
            entity_id = event_data.get('id')
        elif event_type.startswith('edx.forum.'):
            entity_type = 'forum'
            entity_id = event_data.get('commentable_id')

        if not entity_id or not entity_type:
            return

        key = tuple([k.encode('utf8') for k in (date_string, course_id, username, entity_type, entity_id)])

        yield (key, 1)

    def reducer(self, key, values):
        yield ('\t'.join(key),)

    def output(self):
        return get_target_from_url(self.output_root)


class EngagementPartitionTask(EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin, HivePartitionTask):

    # Required Parameters
    date = luigi.DateParameter()

    # Optional parameters
    output_root = luigi.Parameter(default=None)

    # Override superclass to disable these parameters
    interval = None

    @property
    def partition_value(self):
        return self.date.isoformat()

    @property
    def hive_table_task(self):
        return EngagementTableTask(
            warehouse_path=self.warehouse_path
        )

    def requires(self):
        yield EngagementTask(
            date=self.date,
            output_root=self.output_root,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path
        )
        yield self.hive_table_task


class EngagementIntervalTask(EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin, WarehouseMixin, luigi.WrapperTask):

    # Optional parameters
    output_root = luigi.Parameter(default=None)

    def requires(self):
        for date in self.interval:
            yield EngagementPartitionTask(
                date=date,
                output_root=self.output_root,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path
            )

    def output(self):
        return [task.output() for task in self.requires()]
