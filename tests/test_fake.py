import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import json
import os
import unittest
from functools import reduce

class GoogleAnalyticsBookmarks(unittest.TestCase):
    def setUp(self):
        # TODO: Pulling credentials from authing harrison+test@stitchdata.com, instead of Brian's. If we want to use a dedicated account, this needs data populated somehow.
        missing_envs = [x for x in ['TAP_GOOGLE_ANALYTICS_CLIENT_ID',
                                    'TAP_GOOGLE_ANALYTICS_CLIENT_SECRET',
                                    'TAP_GOOGLE_ANALYTICS_REFRESH_TOKEN',
                                    # TODO: Might not be required to add report_definitions? Maybe that's another test
                                    'TAP_GOOGLE_ANALYTICS_REPORT_DEFINITIONS',
                                    'TAP_GOOGLE_ANALYTICS_VIEW_ID'] if os.getenv(x) == None]
        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    def name(self):
        return "tap_tester_google_analytics_bookmarks"

    def get_type(self):
        return "platform.google-analytics"

    def get_credentials(self):
        return {
            'client_id': os.getenv('TAP_GOOGLE_ANALYTICS_CLIENT_ID'),
            'client_secret': os.getenv('TAP_GOOGLE_ANALYTICS_CLIENT_SECRET'),
            'refresh_token': os.getenv('TAP_GOOGLE_ANALYTICS_REFRESH_TOKEN'),
        }

    def expected_check_streams(self):
        return {
            'report',
            'report_2',
        }

    def expected_sync_streams(self):
        return {
            'report',
            'report_2',
        }

    def tap_name(self):
        return "tap-google-analytics"

    def expected_pks(self):
        return {
            "report":                 {}, # TODO: What are our PKs?
            "report_2":               {},
        }

    def get_properties(self):
        return {
            'start_date' : '2019-03-16T00:00:00Z',
            'view_id': os.getenv('TAP_GOOGLE_ANALYTICS_VIEW_ID'),
            'report_definitions': json.loads(os.getenv('TAP_GOOGLE_ANALYTICS_REPORT_DEFINITIONS')),
        }

    def test_run(self):
        # TODO: Add STITCH_API_TOKEN to environment, I think?
        # Here's the current command that works: (removed --orchestrator since venv path is weird in dev and changed client_id to 3)
        # ./bin/run-test --tap=tap-google-analytics --target=target-stitch --email=blackhole+pipeline+admin@rjmetrics.com --api-host=http://10.10.10.4:8443 --password=$SANDBOX_PASSWORD --client-id=3 /opt/code/tap-google-analytics/tests
        
        conn_id = connections.ensure_connection(self)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)
