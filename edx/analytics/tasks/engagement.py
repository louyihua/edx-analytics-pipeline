import csv
from collections import namedtuple
import datetime
import hashlib
import json
import logging
from itertools import groupby
from operator import itemgetter
import re
import sys
import time

log = logging.getLogger(__name__)

import luigi
import luigi.task
from luigi import date_interval
from luigi.configuration import get_config

try:
    from elasticsearch import Elasticsearch
    from elasticsearch import helpers
    from elasticsearch.connection import Urllib3HttpConnection, RequestsHttpConnection
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
from edx.analytics.tasks.url import get_target_from_url, url_path_join, IgnoredTarget
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.elastic_search import BotoHttpConnection
from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.mysql_load import MysqlInsertTask

from edx.analytics.tasks.util.hive import (
    WarehouseMixin, BareHiveTableTask, HivePartitionTask, hive_database_name, tsv_to_named_tuple, named_tuple_to_tsv
)


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
        events = []
        if event_type == 'problem_check':
            if event_source != 'server':
                return

            entity_type = 'problem'
            if event_data.get('success', 'incorrect').lower() == 'correct':
                events.append('completed')

            events.append('attempted')
            entity_id = event_data.get('problem_id')
        elif event_type == 'play_video':
            entity_type = 'video'
            events.append('played')
            entity_id = event_data.get('id')
        elif event_type.startswith('edx.forum.'):
            entity_type = 'forum'
            if event_type == 'edx.forum.comment.created':
                events.append('commented')
            elif event_type == 'edx.forum.response.created':
                events.append('responded')
            elif event_type == 'edx.forum.thread.created':
                events.append('created')
            entity_id = event_data.get('commentable_id')

        if not entity_id or not entity_type:
            return

        for event in events:
            key = tuple([k.encode('utf8') for k in (course_id, username, date_string, entity_type, entity_id, event)])
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
            ('event', 'STRING'),
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
            ('event', 'VARCHAR(30) NOT NULL'),
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
        #   event(30 characters * 3 bytes per utf8 char)
        #   count(4 bytes per INTEGER)

        # Total = 1747
        return [
            ('PRIMARY KEY', '(course_id, username, date, entity_type, entity_id, event)')
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
            ('event', 'VARCHAR(30)'),
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
        requirements = {
            'hive': [],
            'mysql': [],
            'vertica': []
        }
        for date in self.interval:
            requirements['hive'].append(
                EngagementPartitionTask(
                    date=date,
                    n_reduce_tasks=self.n_reduce_tasks,
                    warehouse_path=self.warehouse_path,
                    overwrite=self.overwrite,
                )
            )
            requirements['mysql'].append(
                EngagementMysqlTask(
                    date=date,
                    n_reduce_tasks=self.n_reduce_tasks,
                    warehouse_path=self.warehouse_path,
                    overwrite=self.overwrite,
                )
            )
            if self.vertica_enabled:
                requirements['vertica'].append(
                    EngagementVerticaTask(
                        date=date,
                        n_reduce_tasks=self.n_reduce_tasks,
                        warehouse_path=self.warehouse_path,
                        overwrite=self.overwrite,
                        schema=self.vertica_schema,
                        credentials=self.vertica_credentials,
                    )
                )

        return requirements

    def output(self):
        return [task.output() for task in self.requires().values()]


EngagementRecord = namedtuple(
    'EngagementRecord',
    [
        'course_id',
        'username',
        'date',
        'entity_type',
        'entity_id',
        'event',
        'count'
    ]
)


class SparseWeeklyStudentCourseEngagementTableTask(BareHiveTableTask):

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'sparse_course_engagement_weekly'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('username', 'STRING'),
            ('date', 'STRING'),
            ('problem_attempts', 'INT'),
            ('problems_attempted', 'INT'),
            ('problems_completed', 'INT'),
            ('videos_played', 'INT'),
            ('discussion_activity', 'INT'),
            ('days_active', 'INT')
        ]


class SparseWeeklyStudentCourseEngagementPartitionTask(EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin, OverwriteOutputMixin, OptionalVerticaMixin, HivePartitionTask):

    # Required Parameters
    date = luigi.DateParameter()

    # Override superclass to disable these parameters
    interval = None

    @property
    def partition_value(self):
        return self.date.isoformat()

    @property
    def hive_table_task(self):
        return SparseWeeklyStudentCourseEngagementTableTask(
            warehouse_path=self.warehouse_path
        )

    def requires(self):
        yield SparseWeeklyStudentCourseEngagementTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )
        yield self.hive_table_task


class SparseWeeklyStudentCourseEngagementTask(EventLogSelectionDownstreamMixin, OverwriteOutputMixin, WarehouseMixin, OptionalVerticaMixin, MapReduceJobTask):

    date = luigi.DateParameter()
    interval = None

    def __init__(self, *args, **kwargs):
        super(SparseWeeklyStudentCourseEngagementTask, self).__init__(*args, **kwargs)

        start_date = self.date - datetime.timedelta(weeks=1)
        self.interval = date_interval.Custom(start_date, self.date)

    def requires_hadoop(self):
        return self.requires().requires()['hive']

    def mapper(self, line):
        record = tsv_to_named_tuple(EngagementRecord, line)
        yield ((record.course_id, record.username), line.strip())

    def reducer(self, key, lines):
        """Calculate counts for events corresponding to user and course in a given time period."""
        course_id, username = key

        output_record = SparseWeeklyCourseEngagementRecord()
        for line in lines:
            record = tsv_to_named_tuple(EngagementRecord, line)

            output_record.days_active.add(record.date)

            count = int(record.count)
            if record.entity_type == 'problem':
                if record.event == 'attempted':
                    output_record.problem_attempts += count
                    output_record.problems_attempted.add(record.entity_id)
                elif record.event == 'completed':
                    output_record.problems_completed.add(record.entity_id)
            elif record.entity_type == 'video':
                if record.event == 'played':
                    output_record.videos_played.add(record.entity_id)
            elif record.entity_type == 'forum':
                output_record.discussion_activity += count
            else:
                log.warn('Unrecognized entity type: %s', record.entity_type)

        yield (
            course_id.encode('utf-8'),
            username.encode('utf-8'),
            self.date,
            output_record.problem_attempts,
            len(output_record.problems_attempted),
            len(output_record.problems_completed),
            len(output_record.videos_played),
            output_record.discussion_activity,
            len(output_record.days_active)
        )

    def output(self):
        return get_target_from_url(url_path_join(self.warehouse_path, 'sparse_course_engagement_weekly', 'dt=' + self.date.isoformat()) + '/')

    def requires(self):
        return EngagementIntervalTask(
            interval=self.interval,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )


class SparseWeeklyCourseEngagementRecord(object):

    def __init__(self):
        self.problem_attempts = 0
        self.problems_attempted = set()
        self.problems_completed = set()
        self.videos_played = set()
        self.discussion_activity = 0
        self.days_active = set()


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
            ('segments', 'STRING')
        ]


class WeeklyStudentCourseEngagementTask(EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin, OverwriteOutputMixin, OptionalVerticaMixin, HivePartitionTask):

    date = luigi.DateParameter()
    interval = None
    partition_value = None

    def __init__(self, *args, **kwargs):
        super(WeeklyStudentCourseEngagementTask, self).__init__(*args, **kwargs)

        start_date = self.date - datetime.timedelta(weeks=1)
        self.interval = date_interval.Custom(start_date, self.date)
        self.partition_value = self.date.isoformat()

    def query(self):
        # Join with calendar data only if calculating weekly engagement.
        last_complete_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member
        iso_weekday = last_complete_date.isoweekday()

        query = """
        USE {database_name};
        INSERT OVERWRITE TABLE {table} PARTITION ({partition.query_spec}) {if_not_exists}
        SELECT
            ce.course_id,
            au.username,
            ce.date,
            au.email,
            regexp_replace(regexp_replace(aup.name, '\\\\t|\\\\n|\\\\r', ' '), '\\\\\\\\', ''),
            ce.mode,
            cohort.name,
            COALESCE(eng.problem_attempts, 0),
            COALESCE(eng.problems_attempted, 0),
            COALESCE(eng.problems_completed, 0),
            COALESCE(eng.videos_played, 0),
            COALESCE(eng.discussion_activity, 0),
            CONCAT_WS(
                ",",
                IF(ce.at_end = 0, "unenrolled", NULL),
                IF(eng.days_active = 0, "inactive", NULL),
                IF(old_eng.days_active > 0 AND eng.days_active = 0, "disengaging", NULL),
                IF((CAST(eng.problems_attempted AS DOUBLE) / CAST(eng.problems_completed AS DOUBLE)) > 1.25, "struggling", NULL)
            )
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
        LEFT OUTER JOIN sparse_course_engagement_weekly eng
            ON (ce.course_id = eng.course_id AND au.username = eng.username AND eng.date = '{end}')
        LEFT OUTER JOIN sparse_course_engagement_weekly old_eng
            ON (ce.course_id = old_eng.course_id AND au.username = old_eng.username AND old_eng.date = DATE_SUB('{end}', 7))
        WHERE
            ce.date >= '{start}'
            AND ce.date < '{end}'
            AND cal.iso_weekday = {iso_weekday}
        """.format(
            start=self.interval.date_a.isoformat(),  # pylint: disable=no-member
            end=self.interval.date_b.isoformat(),  # pylint: disable=no-member
            iso_weekday=iso_weekday,
            partition=self.partition,
            if_not_exists='' if self.overwrite else 'IF NOT EXISTS',
            database_name=hive_database_name(),
            table=self.hive_table_task.table,
        )
        log.info(query)
        return query

    @property
    def hive_table_task(self):
        return WeeklyStudentCourseEngagementTableTask(
            warehouse_path=self.warehouse_path,
        )

    def requires(self):
        kwargs_for_db_import = {
            'overwrite': self.overwrite,
            'import_date': self.date
        }
        yield (
            self.hive_table_task,
            SparseWeeklyStudentCourseEngagementPartitionTask(
                date=self.date,
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


WeeklyCourseEngagementRecordBase = namedtuple(
    'WeeklyCourseEngagementRecord',
    [
        'course_id',
        'username',
        'date',
        'email',
        'name',
        'enrollment_mode',
        'cohort',
        'problem_attempts',
        'problems_attempted',
        'problems_completed',
        'videos_played',
        'discussion_activity',
        'segments'
    ]
)


class WeeklyCourseEngagementRecord(WeeklyCourseEngagementRecordBase):

    def __new__(cls, *args, **kwargs):
        result = super(WeeklyCourseEngagementRecord, cls).__new__(cls, *args, **kwargs)
        result = result._replace(  # pylint: disable=no-member,protected-access
            problems_attempted=int(result.problems_attempted),  # pylint: disable=no-member
            problem_attempts=int(result.problem_attempts),  # pylint: disable=no-member
            problems_completed=int(result.problems_completed),  # pylint: disable=no-member
            videos_played=int(result.videos_played),  # pylint: disable=no-member
            discussion_activity=int(result.discussion_activity),  # pylint: disable=no-member
            segments=result.segments.split(',')  # pylint: disable=no-member
        )

        return result


class WeeklyStudentCourseEngagementIndexTask(
        EventLogSelectionDownstreamMixin,
        OverwriteOutputMixin,
        OptionalVerticaMixin,
        MapReduceJobTask):

    date = luigi.DateParameter()
    interval = None

    elasticsearch_host = luigi.Parameter(
        is_list=True,
        config_path={'section': 'student-engagement', 'name': 'elasticsearch_host'}
    )
    elasticsearch_index = luigi.Parameter(
        config_path={'section': 'student-engagement', 'name': 'elasticsearch_index'}
    )
    elasticsearch_number_of_shards = luigi.Parameter(
        config_path={'section': 'student-engagement', 'name': 'elasticsearch_number_of_shards'}
    )
    elasticsearch_connection_type = luigi.Parameter(
        config_path={'section': 'student-engagement', 'name': 'elasticsearch_connection_type'}
    )
    scale_factor = luigi.IntParameter(default=1)
    throttle = luigi.FloatParameter(default=0.25)
    batch_size = luigi.IntParameter(default=500)
    indexing_tasks = luigi.IntParameter(default=None)

    def __init__(self, *args, **kwargs):
        super(WeeklyStudentCourseEngagementIndexTask, self).__init__(*args, **kwargs)

        self.other_reduce_tasks = self.n_reduce_tasks
        if self.indexing_tasks is not None:
            self.n_reduce_tasks = self.indexing_tasks

    def requires(self):
        return WeeklyStudentCourseEngagementTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.other_reduce_tasks,
            overwrite=self.overwrite,
            date=self.date,
        )

    def init_local(self):
        super(WeeklyStudentCourseEngagementIndexTask, self).init_local()

        es = self.create_elasticsearch_client()
        if not es.indices.exists(index=self.elasticsearch_index):
            es.indices.create(index=self.elasticsearch_index, body={
                'settings': {
                    'number_of_shards': self.elasticsearch_number_of_shards,
                    'refresh_interval': -1
                },
                'mappings': {
                    'roster_entry': {
                        'properties': {
                            'course_id': {'type': 'string', 'index': 'not_analyzed'},
                            'username': {'type': 'string', 'index': 'not_analyzed'},
                            'email': {'type': 'string', 'index': 'not_analyzed', 'doc_values': True},
                            'name': {'type': 'string'},
                            'enrollment_mode': {'type': 'string', 'index': 'not_analyzed', 'doc_values': True},
                            'cohort': {'type': 'string', 'index': 'not_analyzed', 'doc_values': True},
                            'problems_attempted': {'type': 'integer', 'doc_values': True},
                            'discussion_activity': {'type': 'integer', 'doc_values': True},
                            'problems_completed': {'type': 'integer', 'doc_values': True},
                            'attempts_per_problem_completed': {'type': 'float', 'doc_values': True},
                            'videos_watched': {'type': 'integer', 'doc_values': True},
                            'segments': {'type': 'string'},
                            'name_suggest': {
                                'type': 'completion',
                                'index_analyzer': 'simple',
                                'search_analyzer': 'simple',
                                'payloads': True,
                                "context": {
                                    "course_id": {
                                        "type": "category",
                                        "default": "unknown",
                                        "path": "course_id"
                                    }
                                }
                            }
                        }
                    }
                }
            })

    def create_elasticsearch_client(self):
        if self.elasticsearch_connection_type == 'urllib3':
            connection_class = Urllib3HttpConnection
        elif self.elasticsearch_connection_type == 'boto':
            connection_class = BotoHttpConnection
        elif self.elasticsearch_connection_type == 'requests':
            connection_class = RequestsHttpConnection
        else:
            raise ValueError('Unrecognized connection type: {}'.format(self.elasticsearch_connection_type))

        return Elasticsearch(
            hosts=self.elasticsearch_host,
            timeout=60,
            retry_on_status=(408, 504),
            retry_on_timeout=True,
            connection_class=connection_class
        )

    def mapper(self, line):
        record = tsv_to_named_tuple(WeeklyCourseEngagementRecord, line)
        yield (record.course_id.encode('utf8'), line)

    def reducer(self, _key, lines):
        es = self.create_elasticsearch_client()

        self.batch_index = 0

        def record_generator():
            for line in lines:
                record = tsv_to_named_tuple(WeeklyCourseEngagementRecord, line)

                document = {
                    '_type': 'roster_entry',
                    '_id': '|'.join([record.course_id, record.username]),
                    '_source': {
                        'course_id': record.course_id,
                        'username': record.username,
                        'email': record.email,
                        'name': record.name,
                        'enrollment_mode': record.enrollment_mode,
                        'problems_attempted': record.problems_attempted,
                        'discussion_activity': record.discussion_activity,
                        'problems_completed': record.problems_completed,
                        'videos_watched': record.videos_played,
                        'segments': record.segments,
                        'name_suggest': {
                            'input': [record.name, record.username, record.email],
                            'output': record.name,
                            'payload': {'username': record.username},
                            'context': {
                                'course_id': record.course_id
                            }
                        }
                    }
                }

                if record.cohort is not None:
                    document['_source']['cohort'] = record.cohort

                if record.problems_completed > 0:
                    document['_source']['attempts_per_problem_completed'] = (
                        float(record.problem_attempts) / float(record.problems_completed)
                    )

                original_id = document['_id']
                for i in range(self.scale_factor):
                    if i > 0:
                        document['_id'] = original_id + '|' + str(i)
                    yield document

                    self.batch_index += 1
                    if self.batch_size is not None and self.batch_index >= self.batch_size:
                        self.incr_counter('Elasticsearch', 'Records Indexed', self.batch_index)
                        self.batch_index = 0
                        if self.throttle:
                            time.sleep(self.throttle)

        num_indexed, errors = helpers.bulk(
            es,
            record_generator(),
            index=self.elasticsearch_index,
            chunk_size=self.batch_size,
            raise_on_error=False
        )
        self.incr_counter('Elasticsearch', 'Records Indexed', self.batch_index)
        num_errors = len(errors)
        self.incr_counter('Elasticsearch', 'Indexing Errors', num_errors)
        sys.stderr.write('Number of errors: {0}\n'.format(num_errors))
        for error in errors:
            sys.stderr.write(str(error))
            sys.stderr.write('\n')

        yield ('', '')

    def extra_modules(self):
        import urllib3
        import elasticsearch
        return [urllib3, elasticsearch]

    def jobconfs(self):
        jcs = super(WeeklyStudentCourseEngagementIndexTask, self).jobconfs()
        jcs.append('mapred.reduce.tasks.speculative.execution=false')
        return jcs

    def output(self):
        return IgnoredTarget()
