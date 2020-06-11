import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

from functools import reduce

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner


class TestGoogleAnalyticsSelectionLimitations(unittest.TestCase):
    """
    Test the limitations for the number of metrics and dimensions that can be selected
    • Verify that a synced report using maximum number of metrics and dimensions
      (10 & 7 respectively) results in records with 23 fields.
        10 metrics + 7 dimensions + 6 default fields
          default fields = {"_sdc_record_hash", "start_date", "end_date",
                            "account_id", "web_property_id", "profile_id"}

    Test reports that reflect different compatibility states
    • Verify that data is not replicated for a report when incompatible metrics and
      dimensions are selected
    """
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"

    def setUp(self):
        missing_envs = [x for x in ['TAP_GOOGLE_ANALYTICS_CLIENT_ID',
                                    'TAP_GOOGLE_ANALYTICS_CLIENT_SECRET',
                                    'TAP_GOOGLE_ANALYTICS_REFRESH_TOKEN',
                                    'TAP_GOOGLE_ANALYTICS_VIEW_ID'] if os.getenv(x) == None]

        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    def name(self):
        return "tap_tester_google_analytics_selection_limits"

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
            'test-report-1'
        }

    def expected_sync_streams(self):
        return {
            'Audience Overview',
            'Audience Technology',
            'Acquisition Overview',
            'Ecommerce Overview',
            'Audience Geo Location',
            'Behavior Overview',
            'Test Report 1'
        }

    def tap_name(self):
        return "tap-google-analytics"

    def expected_pks(self):
        return {
            'Test Report 1': {"_sdc_record_hash"},
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
            "Audience Overview": {"ga:avgSessionDuration","ga:bounceRate","ga:browser","ga:city","ga:country","ga:date",
                                  "ga:hour","ga:language","ga:month","ga:newUsers","ga:operatingSystem","ga:pageviews",
                                  "ga:pageviewsPerSession","ga:sessions","ga:sessionsPerUser","ga:users","ga:year"},
            "Audience Geo Location": {"ga:users","ga:newUsers","ga:sessions","ga:pageviewsPerSession","ga:avgSessionDuration",
                                      "ga:bounceRate","ga:date","ga:country","ga:city","ga:continent","ga:subContinent"},
            "Audience Technology": {"ga:users","ga:newUsers","ga:sessions","ga:pageviewsPerSession","ga:avgSessionDuration",
                                    "ga:bounceRate","ga:date","ga:browser","ga:operatingSystem"},
            "Acquisition Overview": {"ga:sessions","ga:pageviewsPerSession","ga:avgSessionDuration","ga:bounceRate",
                                     "ga:acquisitionTrafficChannel","ga:acquisitionSource","ga:acquisitionSourceMedium",
                                     "ga:acquisitionMedium"},
            "Behavior Overview": {"ga:pageviews","ga:uniquePageviews","ga:avgTimeOnPage","ga:bounceRate","ga:exitRate",
                                  "ga:exits","ga:date","ga:pagePath","ga:pageTitle"},
            "Ecommerce Overview": {"ga:transactions","ga:transactionId","ga:campaign","ga:source","ga:medium",
                                   "ga:keyword","ga:socialNetwork"},
            "Test Report 1": set()
        }

    def get_properties(self):
        return {
            'start_date' : dt.strftime(dt.utcnow() - timedelta(days=7), self.START_DATE_FORMAT),
            'view_id': os.getenv('TAP_GOOGLE_ANALYTICS_VIEW_ID'),
            'report_definitions': [{"id": "test-report-1", "name": "Test Report 1"}]
        }

    def select_streams_and_fields(self, conn_id, catalogs, default_values: bool = True,
                                  select_all_fields: bool = False,
                                  additional_selections: bool = True):
        """Select specific fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            stream = catalog['stream_name']
            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                default_properties = self.expected_default_fields().get(stream, set()) if default_values else set()
                selected = self.get_field_selection().get(stream, set()) if additional_selections else set()
                all_properties = schema.get('annotated-schema', {}).get('properties', {}).keys()

                non_selected_properties = [prop for prop in all_properties
                                           if prop not in selected.union(default_properties)]

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, non_selected_fields=non_selected_properties)

    def get_field_selection(self):
        return {
            "Audience Overview": set(),  # 10 dimensions 7 metrics MAX both for a standard report
            "Audience Geo Location": {"ga:year", "ga:month"}, # 7 metrics MAX
            "Audience Technology": set(), 
            "Acquisition Overview": set(),
            "Behavior Overview": set(),
            "Ecommerce Overview": set(),
            "Test Report 1":  # 10 metrics, 5 dim (max for custom report)
            {
                "ga:sessions","ga:users","ga:bounces","ga:hits","ga:newUsers",\
                "ga:avgSessionDuration","ga:pagesPerSession","ga:bounceRate",\
                "ga:avgTimeOnPage","ga:sessionDuration","ga:deviceCategory",\
                "ga:eventAction","ga:date","ga:eventLabel","ga:eventCategory"
            }
        }

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

        # select specific streams and fields
        self.select_streams_and_fields(conn_id, found_catalogs)

        # clear state
        menagerie.set_state(conn_id, {})

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # This should be validating the the PKs are written in each record
        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values(), 0)
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(replicated_row_count))
        print("total replicated row count: {}".format(replicated_row_count))

        # Verify we got data for expected streams
        # BUG | 'Test Report 1' | https://stitchdata.atlassian.net/browse/SRCE-3374
        # BUG | 'Ecommerce Overview' | https://stitchdata.atlassian.net/browse/SRCE-3375
        for stream in self.expected_sync_streams().difference({'Ecommerce Overview', 'Test Report 1'}):
            self.assertGreater(record_count_by_stream.get(stream, 0), 0,
                               msg="Did not replicate any data for {}".format(stream))

        # Verify we replicated the expected records at the max selection limit for metircs & dimensions
        synced_records = runner.get_records_from_target_output()
        for stream_name, data in synced_records.items():
            record_messages = [set(row['data'].keys()) for row in data['messages']]
            for record_keys in record_messages:
                expected_number_of_fields = len(self.expected_automatic_fields().get(stream_name, set())) + \
                    len(set(self.expected_default_fields()[stream_name])) + len(self.get_field_selection().get(stream_name, set()))

                # Verify the number of fields (metrics, dimensions, and default values) match expectations for each stream
                self.assertEqual(expected_number_of_fields, len(record_keys),
                                 msg="Got an unexpected number of fields for {}".format(stream_name)
                )

                # Verify the expected field names match what was replciated
                self.assertEqual(record_keys, (self.expected_automatic_fields().get(stream_name, set()) |
                                               set(self.expected_default_fields()[stream_name]) |
                                               self.get_field_selection().get(stream_name, set())))

                # TODO Verify that data is not replicated for a report when incompatible metrics and dimensions are selected
