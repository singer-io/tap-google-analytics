import os
import unittest
from datetime import datetime as dt
from datetime import timedelta
from functools import reduce

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner


class TestGoogleAnalyticsBasicSync(unittest.TestCase):
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"

    def setUp(self):
        missing_envs = [x for x in ['TAP_GOOGLE_ANALYTICS_CLIENT_ID',
                                    'TAP_GOOGLE_ANALYTICS_CLIENT_SECRET',
                                    'TAP_GOOGLE_ANALYTICS_REFRESH_TOKEN',
                                    'TAP_GOOGLE_ANALYTICS_VIEW_ID'] if os.getenv(x) == None]
        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    def name(self):
        return "tap_tester_google_analytics_basic_sync"

    def get_type(self):
        return "platform.google-analytics"

    def get_credentials(self):
        return {
            'client_id': os.getenv('TAP_GOOGLE_ANALYTICS_CLIENT_ID'),
            'client_secret': os.getenv('TAP_GOOGLE_ANALYTICS_CLIENT_SECRET'),
            'refresh_token': os.getenv('TAP_GOOGLE_ANALYTICS_REFRESH_TOKEN')
        }

    def expected_check_streams(self):
        return {
            'Audience Overview',
            'Audience Technology',
            'Acquisition Overview',
            'Ecommerce Overview',
            'Audience Geo Location',
            'Behavior Overview',
            'a665732c-d18b-445c-89b2-5ca8928a7305'
        }

    def expected_sync_streams(self):
        return {
            'Audience Overview',
            'Audience Technology',
            'Acquisition Overview',
            'Ecommerce Overview',
            'Audience Geo Location',
            'Behavior Overview',
            "report 1"
        }

    def tap_name(self):
        return "tap-google-analytics"

    def expected_pks(self):
        return {
            "report 1" : {"_sdc_record_hash"},
            'Audience Overview': {"_sdc_record_hash"},
            'Audience Technology': {"_sdc_record_hash"},
            'Acquisition Overview': {"_sdc_record_hash"},
            'Ecommerce Overview': {"_sdc_record_hash"},
            'Audience Geo Location': {"_sdc_record_hash"},
            'Behavior Overview': {"_sdc_record_hash"},
        }

    def expected_automatic_fields(self):
        return_value = self.expected_pks()
        for stream in return_value.keys():
            return_value[stream] = {"_sdc_record_hash", "start_date", "end_date", "account_id", "web_property_id", "profile_id"}

        return return_value

    def expected_default_fields(self):
        return {
            "report 1" : {},
            "Audience Overview": {
                "ga:users","ga:newUsers","ga:sessions","ga:sessionsPerUser","ga:pageviews","ga:pageviewsPerSession",
                "ga:avgSessionDuration","ga:bounceRate","ga:date"
            },
            "Audience Geo Location": {
                "ga:users","ga:newUsers","ga:sessions","ga:pageviewsPerSession","ga:avgSessionDuration","ga:bounceRate",
                "ga:date","ga:country","ga:city","ga:continent","ga:subContinent"
            },
            "Audience Technology": {
                "ga:users","ga:newUsers","ga:sessions","ga:pageviewsPerSession","ga:avgSessionDuration","ga:bounceRate",
                "ga:date","ga:browser","ga:operatingSystem"
            },
            "Acquisition Overview": {
                "ga:sessions","ga:pageviewsPerSession","ga:avgSessionDuration","ga:bounceRate","ga:acquisitionTrafficChannel"
                ,"ga:acquisitionSource","ga:acquisitionSourceMedium","ga:acquisitionMedium"
            },
            "Behavior Overview": {
                "ga:pageviews","ga:uniquePageviews","ga:avgTimeOnPage","ga:bounceRate","ga:exitRate","ga:exits","ga:date"
                ,"ga:pagePath","ga:pageTitle"
            },
            "Ecommerce Overview": {
                "ga:transactions","ga:transactionId","ga:campaign","ga:source","ga:medium","ga:keyword","ga:socialNetwork"
            }
        }

    def get_properties(self):
        return {
            'start_date' : dt.strftime(dt.utcnow() - timedelta(days=30), self.START_DATE_FORMAT),  # 'start_date' : '2020-03-01T00:00:00Z',
            'view_id': os.getenv('TAP_GOOGLE_ANALYTICS_VIEW_ID'),
            'report_definitions': [{"id": "a665732c-d18b-445c-89b2-5ca8928a7305", "name": "report 1"}]
        }

    def get_field_selection(self):
        return set()

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = self.expected_check_streams().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are kosher")

        # select all catalogs
        for c in found_catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, c['stream_id'])

            for k in self.expected_automatic_fields()[c['stream_name']]:
                mdata = next((m for m in catalog_entry['metadata']
                              if len(m['breadcrumb']) == 2 and m['breadcrumb'][1] == k), None)
                print("Validating inclusion on {}: {}".format(c['stream_name'], mdata))
                self.assertTrue(mdata and mdata['metadata']['inclusion'] == 'automatic')

            keys = set(catalog_entry['annotated-schema']['properties'].keys())
            non_selected_fields = keys - self.get_field_selection()
            connections.select_catalog_via_metadata(conn_id,
                                                    c,
                                                    catalog_entry)

        # clear state
        menagerie.set_state(conn_id, {})

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # This should be validating the the PKs are written in each record
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values(), 0)
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        synced_records = runner.get_records_from_target_output()
        for stream_name, data in synced_records.items():
            record_messages = [set(row['data'].keys()) for row in data['messages']]
            for record_keys in record_messages:
                self.assertEqual(record_keys, (self.expected_automatic_fields().get(stream_name, set()) |
                                               set(self.expected_default_fields()[stream_name])))
