import os
import unittest
from datetime import datetime as dt
from datetime import timedelta
from functools import reduce

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner


class TestGoogleAnalyticsStartDate(unittest.TestCase):
    """
    Test that the replication start date is respected

    • verify all streams have exactly 1 record per day starting with the replication
      start date and ending with today
    • verify for any given stream that a sync with an earlier replication start date has
      exactly x records more than the 1st sync with a later replication start date, where 
          x = difference in days between the two start dates
    TODO Determine if this ^ is necessary, first bullet true would imply second bullet is true...
    • verify all records in a given stream have a unique 'start_date' value in that sync
    • verify that 'start_date' == 'end_date' is True for all records of a given stream 
    """

    START_DATE = ""
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    
    def setUp(self):
        missing_envs = [x for x in ['TAP_GOOGLE_ANALYTICS_CLIENT_ID',
                                    'TAP_GOOGLE_ANALYTICS_CLIENT_SECRET',
                                    'TAP_GOOGLE_ANALYTICS_REFRESH_TOKEN',
                                    'TAP_GOOGLE_ANALYTICS_VIEW_ID'] if os.getenv(x) == None]

        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    def name(self):
        return "tap_tester_google_analytics_start_date"

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
            "Audience Overview": {"ga:users","ga:newUsers","ga:sessions","ga:sessionsPerUser","ga:pageviews","ga:pageviewsPerSession","ga:avgSessionDuration","ga:bounceRate","ga:date"},
            "Audience Geo Location": {"ga:users","ga:newUsers","ga:sessions","ga:pageviewsPerSession","ga:avgSessionDuration","ga:bounceRate","ga:date","ga:country","ga:city","ga:continent","ga:subContinent"},
            "Audience Technology": {"ga:users","ga:newUsers","ga:sessions","ga:pageviewsPerSession","ga:avgSessionDuration","ga:bounceRate","ga:date","ga:browser","ga:operatingSystem"},
            "Acquisition Overview": {"ga:sessions","ga:pageviewsPerSession","ga:avgSessionDuration","ga:bounceRate","ga:acquisitionTrafficChannel","ga:acquisitionSource","ga:acquisitionSourceMedium","ga:acquisitionMedium"},
            "Behavior Overview": {"ga:pageviews","ga:uniquePageviews","ga:avgTimeOnPage","ga:bounceRate","ga:exitRate","ga:exits","ga:date","ga:pagePath","ga:pageTitle","ga:searchKeyword"},
            "Ecommerce Overview": {"ga:transactions","ga:transactionId","ga:campaign","ga:source","ga:medium","ga:keyword","ga:socialNetwork"}
        }

    def get_properties(self, original: bool = True):
        return_value = {
            'start_date' : dt.strftime(dt.utcnow() - timedelta(days=30), self.START_DATE_FORMAT),
            'view_id': os.getenv('TAP_GOOGLE_ANALYTICS_VIEW_ID'),
            'report_definitions': [{"id": "a665732c-d18b-445c-89b2-5ca8928a7305", "name": "report 1"}]
        }
        if original:
            return return_value
        
        # Start Date test needs the new connections start date to be prior to the default
        assert self.START_DATE < return_value["start_date"]

        # Assign start date to be the default 
        return_value["start_date"] = self.START_DATE
        return return_value

    def get_field_selection(self):
        return set()

    def select_all_catalogs(self, conn_id, found_catalogs):
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

    def test_run(self):
        # Initialize start date prior to first sync
        self.START_DATE = self.get_properties().get('start_date')

        ##########################################################################
        ### First Sync
        ##########################################################################
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

        self.select_all_catalogs(conn_id, found_catalogs)

        # clear state
        menagerie.set_state(conn_id, {})

        # Run sync 1
        sync_job_1 = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status_1 = menagerie.get_exit_status(conn_id, sync_job_1)
        menagerie.verify_sync_exit_status(self, exit_status_1, sync_job_1)

        # This should be validating the the PKs are written in each record
        record_count_by_stream_1 = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count_1 =  reduce(lambda accum,c : accum + c, record_count_by_stream_1.values(), 0)
        self.assertGreater(replicated_row_count_1, 0, msg="failed to replicate any data: {}".format(record_count_by_stream_1))
        print("total replicated row count: {}".format(replicated_row_count_1))

        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        start_date_1 = self.get_properties()['start_date']
        self.START_DATE = dt.strftime(dt.strptime(self.START_DATE, self.START_DATE_FORMAT) \
                                      - timedelta(days=10), self.START_DATE_FORMAT)
        start_date_2 = self.START_DATE
        print("REPLAICATION START DATE CHANGE: {} ===>>> {} ".format(start_date_1, start_date_2))

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id = connections.ensure_connection(self, original_properties=False)

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

        self.select_all_catalogs(conn_id, found_catalogs)

        # clear state
        menagerie.set_state(conn_id, {})

        # Run sync 2
        sync_job_2 = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status_2 = menagerie.get_exit_status(conn_id, sync_job_2)
        menagerie.verify_sync_exit_status(self, exit_status_2, sync_job_2)

        # This should be validating the the PKs are written in each record
        record_count_by_stream_2 = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count_2 =  reduce(lambda accum,c : accum + c, record_count_by_stream_2.values(), 0)
        self.assertGreater(replicated_row_count_2, 0, msg="failed to replicate any data: {}".format(record_count_by_stream_2))
        print("total replicated row count: {}".format(replicated_row_count_2))

        synced_records_2 = runner.get_records_from_target_output()

        ##########################################################################
        ### Start Date Tests
        ##########################################################################

        # Verify the 1st sync replicated less data than the 2nd
        self.assertGreater(replicated_row_count_2, replicated_row_count_1,
                           msg="we replicated less data with an older start date " +
                           "------------------------------" +
                           "Start Date 1: {} ".format(start_date_1) +
                           "Row Count 1: {} ".format(replicated_row_count_1) +
                           "------------------------------" +
                           "Start Date 1: {} ".format(start_date_1) +
                           "Row Count 2: {} ".format(replicated_row_count_2))

        # Test by sync
        for synced_records, start_date in [(synced_records_1.items(), start_date_1),
                                           (synced_records_2.items(), start_date_2)]:
            for stream_name, data in synced_records:
                data_set = [row['data'] for row in data['messages']]
                expected_start_dates, actual_start_dates = set(), set()
                start_date_unformatted = dt.strptime(start_date, self.START_DATE_FORMAT)
                number_of_days_in_sync = (dt.today() -
                                          start_date_unformatted).days + 1  # +1 includes today

                # THIS IS ONLY VALID FOR 1 DIMENSION
                # Verify the number of records is equal to the number of days covered by the sync
                # self.assertEqual(number_of_days_in_sync, len(record_messages),
                #                  msg="Number of records does not match the number of days " +
                #                  "between the start date and today.")

                # Verify that each record spans exactly 1 day.
                for row in data_set:
                    self.assertEqual(row['start_date'], row['end_date'],
                                     msg="Records should not span multiple days.") 

                    actual_start_dates.add(row['start_date'])

                # Verify the stream contains at least 1 record for every day in the sync
                for day in range(number_of_days_in_sync): # Create a set of expected days
                    expected_start_dates.add(dt.strftime(start_date_unformatted + timedelta(days=day), "%Y-%m-%dT00:00:00.000000Z"))
                self.assertEqual(expected_start_dates.difference(actual_start_dates),
                                 set(),
                                 msg="Missing at least one record for a day in the sync range.")
                self.assertEqual(actual_start_dates.difference(expected_start_dates),
                                 set(),
                                 msg="Got an extra record for a day outside of the sync range")

                # TODO Verify the stream does not have duplicate records for any given day

        # Test by stream
        missing_records_streams = {'Behavior Overview', 'Ecommerce Overview'}
        for stream in self.expected_sync_streams().difference(missing_records_streams):
            # Verify the 2nd sync got more records per stream than the 1st
            self.assertGreater(record_count_by_stream_2.get(stream, 0),
                               record_count_by_stream_1.get(stream, 0),
                               msg="Stream '{}' ".format(stream) +
                               "Expected sync with start date {} to have more records ".format(start_date_2) +
                               "than sync with start date {}. It does not.".format(start_date_1))

        if missing_records_streams:
            print("\n\n THE FOLLOWING STREAMS DID NOT REPLICATE DATA, FIELD SELECTION MUST BE INVALID:\n")
            print("{}\n\n".format(missing_records_streams))
