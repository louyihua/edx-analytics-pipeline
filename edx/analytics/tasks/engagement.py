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
from luigi.configuration import get_config
from luigi.hive import HiveQueryTask, HivePartitionTarget

try:
    from elasticsearch import Elasticsearch
    from elasticsearch.client import IndicesClient
    from elasticsearch.exceptions import NotFoundError
    from elasticsearch import helpers
except ImportError:
    pass

try:
    import mysql.connector
    from mysql.connector.errors import ProgrammingError
    from mysql.connector import errorcode
    mysql_client_available = True
except ImportError:
    log.warn('Unable to import mysql client libraries')
    # On hadoop slave nodes we don't have mysql client libraries installed so it is pointless to ship this package to
    # them, instead just fail noisily if we attempt to use these libraries.
    mysql_client_available = False

from edx.analytics.tasks.calendar_task import CalendarTableTask
from edx.analytics.tasks.database_imports import (
    ImportAuthUserTask, ImportAuthUserProfileTask, ImportCourseUserGroupTask, ImportCourseUserGroupUsersTask)
from edx.analytics.tasks.enrollments import CourseEnrollmentTableTask
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.mysql_load import MysqlInsertTask

from edx.analytics.tasks.util.hive import WarehouseMixin, BareHiveTableTask, HivePartitionTask, HivePartition, HiveTableFromQueryTask

log = logging.getLogger(__name__)


class EngagementTask(EventLogSelectionMixin, OverwriteOutputMixin, WarehouseMixin, MapReduceJobTask):

    # Required parameters
    date = luigi.DateParameter()

    # Override superclass to disable these parameters
    interval = None

    def __init__(self, *args, **kwargs):
        super(EngagementTask, self).__init__(*args, **kwargs)

        self.interval = date_interval.Date.from_date(self.date)

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
        actions = []
        if event_type == 'problem_check':
            if event_source != 'server':
                return

            entity_type = 'problem'
            if event_data.get('success', 'incorrect').lower() == 'correct':
                actions.append('completed')

            actions.append('attempted')
            entity_id = event_data.get('problem_id')
        elif event_type == 'play_video':
            entity_type = 'video'
            actions.append('played')
            entity_id = event_data.get('id')
        elif event_type.startswith('edx.forum.'):
            entity_type = 'forum'
            if event_type == 'edx.forum.comment.created':
                actions.append('commented')
            elif event_type == 'edx.forum.response.created':
                actions.append('responded')
            elif event_type == 'edx.forum.thread.created':
                actions.append('created')
            entity_id = event_data.get('commentable_id')

        if not entity_id or not entity_type:
            return

        for action in actions:
            key = tuple([k.encode('utf8') for k in (course_id, username, date_string, entity_type, entity_id, action)])
            yield (key, 1)

    def reducer(self, key, values):
        yield ('\t'.join(key), sum(values))

    def output(self):
        return get_target_from_url(url_path_join(self.warehouse_path, 'engagement', 'dt=' + self.date.isoformat()) + '/')

    def run(self):
        self.remove_output_on_overwrite()
        return super(EngagementTask, self).run()


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
            ('course_id', 'STRING'),
            ('username', 'STRING'),
            ('date', 'STRING'),
            ('entity_type', 'STRING'),
            ('entity_id', 'STRING'),
            ('action', 'STRING'),
            ('count', 'INT')
        ]


class EngagementPartitionTask(EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin, OverwriteOutputMixin, HivePartitionTask):

    # Required Parameters
    date = luigi.DateParameter()

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
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )
        yield self.hive_table_task


class EngagementMysqlTask(EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin, WarehouseMixin, MysqlInsertTask):

    # Required Parameters
    date = luigi.DateParameter()

    # Override superclass to disable these parameters
    interval = None

    @property
    def table(self):
        return "engagement"

    def init_copy(self, connection):
        # clear only the data for this date
        self.attempted_removal = True
        if self.overwrite:
            # first clear the appropriate rows from the luigi mysql marker table
            marker_table = self.output().marker_table  # side-effect: sets self.output_target if it's None
            try:
                query = "DELETE FROM {marker_table} where `update_id`='{update_id}'".format(
                    marker_table=marker_table,
                    update_id=self.update_id,
                )
                connection.cursor().execute(query)
            except mysql.connector.Error as excp:  # handle the case where the marker_table has yet to be created
                if excp.errno == errorcode.ER_NO_SUCH_TABLE:
                    pass
                else:
                    raise

            # Use "DELETE" instead of TRUNCATE since TRUNCATE forces an implicit commit before it executes which would
            # commit the currently open transaction before continuing with the copy.
            query = "DELETE FROM {table} WHERE date='{date}'".format(table=self.table, date=self.date.isoformat())
            connection.cursor().execute(query)

    @property
    def auto_primary_key(self):
        return None

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('username', 'VARCHAR(30) NOT NULL'),
            ('date', 'DATE NOT NULL'),
            ('entity_type', 'VARCHAR(10) NOT NULL'),
            ('entity_id', 'VARCHAR(255) NOT NULL'),
            ('action', 'VARCHAR(30) NOT NULL'),
            ('count', 'INTEGER NOT NULL')
        ]

    @property
    def default_columns(self):
        # Use a primary key since we will always be pulling these records by course_id, username ordered by date
        # This dramatically speeds up access times at the cost of write speed.

        # From: http://dev.mysql.com/doc/refman/5.6/en/innodb-restrictions.html

        # The InnoDB internal maximum key length is 3500 bytes, but MySQL itself restricts this to 3072 bytes. This
        # limit applies to the length of the combined index key in a multi-column index.

        # The total for this key is:
        #   course_id(255 characters * 3 bytes per utf8 char)
        #   username(30 characters * 3 bytes per utf8 char)
        #   date(3 bytes per DATE)
        #   entity_type(10 characters * 3 bytes per utf8 char)
        #   entity_id(255 characters * 3 bytes per utf8 char)
        #   action(30 characters * 3 bytes per utf8 char)
        #   count(4 bytes per INTEGER)

        # Total = 1747
        return [
            ('PRIMARY KEY', '(course_id, username, date, entity_type, entity_id, action)')
        ]

    @property
    def insert_source_task(self):
        return EngagementTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )


class EngagementVerticaTask(
        EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin, WarehouseMixin, VerticaCopyTask):

    # Required Parameters
    date = luigi.DateParameter()

    # Override superclass to disable these parameters
    interval = None

    @property
    def insert_source_task(self):
        return EngagementTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def table(self):
        return 'f_engagement'

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255)'),
            ('username', 'VARCHAR(30)'),
            ('date', 'DATE'),
            ('entity_type', 'VARCHAR(10)'),
            ('entity_id', 'VARCHAR(255)'),
            ('action', 'VARCHAR(30)'),
            ('count', 'INT'),
        ]


class OptionalVerticaMixin(object):

    vertica_schema = luigi.Parameter(default=None)
    vertica_credentials = luigi.Parameter(default=None)

    def __init__(self, *args, **kwargs):
        super(OptionalVerticaMixin, self).__init__(*args, **kwargs)

        if not self.vertica_credentials:
            self.vertica_credentials = get_config().get('vertica-export', 'credentials', None)

        if not self.vertica_schema:
            self.vertica_schema = get_config().get('vertica-export', 'schema', None)

        self.vertica_enabled = self.vertica_credentials and self.vertica_schema


class EngagementIntervalTask(EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin, WarehouseMixin, OverwriteOutputMixin, OptionalVerticaMixin, luigi.WrapperTask):

    def requires(self):
        for date in self.interval:
            yield EngagementPartitionTask(
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
            )
            yield EngagementMysqlTask(
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
            )
            if self.vertica_enabled:
                yield EngagementVerticaTask(
                    date=date,
                    n_reduce_tasks=self.n_reduce_tasks,
                    warehouse_path=self.warehouse_path,
                    overwrite=self.overwrite,
                    schema=self.vertica_schema,
                    credentials=self.vertica_credentials,
                )

    def output(self):
        return [task.output() for task in self.requires()]


class WeeklyStudentCourseEngagementTableTask(BareHiveTableTask):

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'course_engagement_weekly'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('username', 'STRING'),
            ('date', 'STRING'),
            ('email', 'STRING'),
            ('name', 'STRING'),
            ('enrollment_mode', 'STRING'),
            ('cohort', 'STRING'),
            ('problem_attempts', 'INT'),
            ('problems_attempted', 'INT'),
            ('problems_completed', 'INT'),
            ('videos_played', 'INT'),
            ('discussion_activity', 'INT'),
        ]


class WeeklyStudentCourseEngagementPartitionTask(HivePartitionTask):

    # Required Parameters
    date = luigi.DateParameter()

    @property
    def partition_value(self):
        return self.date.isoformat()

    @property
    def hive_table_task(self):
        return WeeklyStudentCourseEngagementTableTask(
            warehouse_path=self.warehouse_path
        )


class WeeklyStudentCourseEngagementTask(EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin, WarehouseMixin, OverwriteOutputMixin, OptionalVerticaMixin, HiveQueryTask):

    date = luigi.DateParameter()
    interval = None

    def __init__(self, *args, **kwargs):
        super(WeeklyStudentCourseEngagementTask, self).__init__(*args, **kwargs)

        start_date = self.date - datetime.timedelta(weeks=1)
        self.interval = date_interval.Custom(start_date, self.date)

    @property
    def query(self):
        # Join with calendar data only if calculating weekly engagement.
        last_complete_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member
        iso_weekday = last_complete_date.isoweekday()

        return """
        INSERT OVERWRITE TABLE student_course_engagement_weekly PARTITION {partition.query_spec} {if_not_exists}
        SELECT
            ce.course_id,
            au.username,
            ce.date,
            au.email,
            regexp_replace(regexp_replace(aup.name, '\\\\t|\\\\n|\\\\r', ' '), '\\\\\\\\', ''),
            ce.mode,
            cohort.name,
            eng.problem_attempts,
            eng.problems_attempted,
            eng.problems_completed,
            eng.videos_played,
            eng.discussion_activity
        FROM course_enrollment ce
        INNER JOIN calendar cal ON (ce.date = cal.date)
        INNER JOIN auth_user au
            ON (ce.user_id = au.id)
        INNER JOIN auth_userprofile aup
            ON (au.id = aup.user_id)
        LEFT OUTER JOIN (
            SELECT
                cugu.user_id,
                cug.course_id,
                cug.name
            FROM course_groups_courseusergroup_users cugu
            INNER JOIN course_groups_courseusergroup cug
                ON (cugu.courseusergroup_id = cug.id)
        ) cohort
            ON (au.id = cohort.user_id AND ce.course_id = cohort.course_id)
        LEFT OUTER JOIN (
            SELECT
                course_id,
                username,
                DATE_ADD('{start}', ((DATEDIFF('{end}', date) / 7) + 1) * 7) as end_date,
                SUM(
                    CASE
                        WHEN entity_type = "problem" AND action = "attempted"
                        THEN count
                        ELSE 0
                    END
                ) as problem_attempts,
                SUM(
                    CASE
                        WHEN entity_type = "problem" AND action = "attempted"
                        THEN 1
                        ELSE 0
                    END
                ) as problems_attempted,
                SUM(
                    CASE
                        WHEN entity_type = "problem" AND action = "completed"
                        THEN 1
                        ELSE 0
                    END
                ) as problems_completed,
                SUM(
                    CASE
                        WHEN entity_type = "video" AND action = "played"
                        THEN 1
                        ELSE 0
                    END
                ) as videos_played,
                SUM(
                    CASE
                        WHEN entity_type = "forum"
                        THEN count
                        ELSE 0
                    END
                ) as discussion_activity,
            FROM engagement
            GROUP BY
                course_id,
                username,
                DATE_ADD('{start}', ((DATEDIFF('{end}', date) / 7) + 1) * 7)
        ) eng
            ON (ce.course_id = eng.course_id AND au.username = eng.username AND ce.date = eng.end_date)
        WHERE
            ce.date >= '{start}'
            AND ce.date < '{end}'
            AND cal.iso_weekday = {iso_weekday}
        """.format(
            start=self.interval.date_a.isoformat(),  # pylint: disable=no-member
            end=self.interval.date_b.isoformat(),  # pylint: disable=no-member
            iso_weekday=iso_weekday,
            partition=HivePartition('dt', self.date.isoformat()),
            if_not_exists='' if self.overwrite else 'IF NOT EXISTS'
        )

    def requires(self):
        kwargs_for_db_import = {
            'overwrite': self.overwrite,
            'import_date': self.date
        }
        yield (
            WeeklyStudentCourseEngagementPartitionTask(
                warehouse_path=self.warehouse_path,
                date=self.date,
            ),
            EngagementIntervalTask(
                interval=self.interval,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
            ),
            CourseEnrollmentTableTask(
                interval_end=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
            ),
            CalendarTableTask(
                warehouse_path=self.warehouse_path,
            ),
            ImportAuthUserTask(**kwargs_for_db_import),
            ImportCourseUserGroupTask(**kwargs_for_db_import),
            ImportCourseUserGroupUsersTask(**kwargs_for_db_import),
            ImportAuthUserProfileTask(**kwargs_for_db_import),
        )
