"""Test enrollment validation."""

import datetime
import gzip
import json
import logging
from collections import defaultdict
import StringIO

from luigi.s3 import S3Target

from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase


log = logging.getLogger(__name__)


class EnrollmentValidationAcceptanceTest(AcceptanceTestCase):
    """Test enrollment validation."""

    INPUT_FILE = 'enrollment_trends_tracking.log'
    END_DATE = datetime.datetime.utcnow().date()
    START_DATE = datetime.date(2014, 8, 1)
    # Define an interval that ends with today, so that a dump is triggered.
    DATE_INTERVAL = "{}-{}".format(START_DATE, END_DATE)
    # Create a wider interval that will include today's dump.
    WIDER_DATE_INTERVAL = "{}-{}".format(START_DATE, END_DATE + datetime.timedelta(days=1))
    SQL_FIXTURE = 'load_student_courseenrollment_for_enrollment_validation.sql'

    def test_enrollment_validation(self):
        # Initial setup.
        self.upload_tracking_log(self.INPUT_FILE, self.START_DATE)
        self.execute_sql_fixture_file(self.SQL_FIXTURE)
        self.test_validate = url_path_join(self.test_root, 'validate')

        # Run once.  This will generate the new validation events, but
        # will not include them in the validation run (because the
        # requirements for the validation run are computed before any
        # validation events are generated).
        self.test_first_run = url_path_join(self.test_out, 'first_run')
        self.launch_task(self.test_first_run, run_with_validation_events=False)

        # Check that validation took place.
        self.check_validation_events()

        # Run again, with the validation events generated by the first run.
        self.test_second_run = url_path_join(self.test_out, 'second_run')
        self.launch_task(self.test_second_run)

        # Check that synthetic events were created.
        self.check_synthetic_events(self.test_second_run)

        # Run again, with the synthetic events generated by the second run.
        self.test_third_run = url_path_join(self.test_out, 'third_run')
        self.launch_task(self.test_third_run, extra_source=self.test_second_run)

        # Check that no events are output.
        self.check_no_synthetic_events(self.test_third_run)

    def launch_task(self, output_root, extra_source=None, run_with_validation_events=True):
        """Run the enrollment validation workflow."""

        # Widen the interval to include the latest validation events.
        interval = self.WIDER_DATE_INTERVAL if run_with_validation_events else self.DATE_INTERVAL
        source_pattern = r'".*?\.log-(?P<date>\d{8}).*\.gz"'
        validation_pattern = r'".*?enroll_validated_(?P<date>\d{8})\.log\.gz"'
        launch_args = [
            'EnrollmentValidationWorkflow',
            '--interval', interval,
            '--validation-root', self.test_validate,
            '--validation-pattern', validation_pattern,
            '--credentials', self.import_db.credentials_file_url,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--source', self.test_src,
            '--pattern', source_pattern,
            '--output-root', output_root,
        ]
        # An extra source means we're using synthetic events, so we
        # don't want to generate outside the interval in that case.
        if extra_source:
            launch_args.extend(['--source', extra_source])
        else:
            launch_args.extend(['--generate-before'])
        if run_with_validation_events:
            launch_args.extend(['--expected-validation', "{}T00".format(self.END_DATE)])

        self.task.launch(launch_args)

    def check_validation_events(self):
        """Confirm that validation data was properly created."""
        validate_output_dir = url_path_join(self.test_validate, str(self.END_DATE))
        outputs = self.s3_client.list(validate_output_dir)
        outputs = [url_path_join(validate_output_dir, p) for p in outputs]

        # There are 2 courses in the test data.
        self.assertEqual(len(outputs), 2)

    def get_synthetic_event_urls(self, output_dir):
        """Helper to get URLs for synthetic event files."""
        outputs = self.s3_client.list(output_dir)
        outputs = [url_path_join(output_dir, p) for p in outputs if p.startswith("synthetic_enroll")]
        return outputs

    def check_synthetic_events(self, output_dir):
        """Confirm that some data was output."""
        outputs = self.get_synthetic_event_urls(output_dir)
        self.assertTrue(len(outputs) > 0)
        histogram = defaultdict(int)  # int() returns 0
        for output in outputs:
            # Read S3 file into a buffer, since the S3 file doesn't support seek() and tell().
            gzip_output = StringIO.StringIO()
            with S3Target(output).open('r') as event_file:
                gzip_output.write(event_file.read())
            gzip_output.seek(0)
            with gzip.GzipFile(fileobj=gzip_output) as input_file:
                for line in input_file:
                    event = json.loads(line)
                    event_type = event.get('event_type')
                    reason = event.get('synthesized', {}).get('reason')
                    key = (event_type, reason)
                    histogram[key] += 1
        expected_histogram = {
            ("edx.course.enrollment.activated", "start => validate(active)"): 4,
            ("edx.course.enrollment.mode_changed", "activate => deactivate (audit=>honor)"): 1,
            ("edx.course.enrollment.deactivated", "activate => missing"): 2,
            ("edx.course.enrollment.activated", "deactivate => validate(active)"): 2,
            ("edx.course.enrollment.mode_changed", "deactivate => validate(active) (honor=>verified)"): 1,
        }
        self.assertEquals(histogram, expected_histogram)

    def check_no_synthetic_events(self, output_dir):
        """Confirm that no data was output."""
        outputs = self.get_synthetic_event_urls(output_dir)
        self.assertEqual(len(outputs), 0)
