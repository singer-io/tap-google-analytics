##########################################################################
### TODO WAITING ON https://stitchdata.atlassian.net/browse/SRCE-5065
##########################################################################

import os

from datetime import timedelta
from datetime import datetime as dt

from  tap_tester import connections, runner

from base import GoogleAnalyticsBaseTest


class GoogleAnalyticsPaginationTest(GoogleAnalyticsBaseTest):
    """Test that we are paginating for streams when exceeding the API record limit of a single query"""

    API_LIMIT = 1
    
    @staticmethod
    def name():
        return "tap_tester_google_analytics_pagination_test"


    def test_run(self):
        """
        Verify that for each report you can get multiple pages of data for a given date.

        PREREQUISITE
        For EACH report, a dimension must be selected in which the number of distinct values is
        greater than the default value of maxResults (page size).
        """
        self.start_date = (dt.utcnow() - timedelta(days=4)).strftime(self.START_DATE_FORMAT) # Needs to be prior to isGolden..
        
        expected_streams = {'Acquisition Overview', 'Audience Geo Location', 'Audience Overview'}

        # Create connection but do not use default start date
        conn_id = connections.ensure_connection(self, original_properties=False)

        # run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # TODO remove once all streams covered
        found_catalogs = [catalog for catalog in found_catalogs
                          if catalog.get('stream_name') in expected_streams]

        # TODO select specific streams
        # table and field selection
        self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs, #select_all_fields=True,
            select_default_fields=False, select_pagination_fields=True
        )


        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]

                # TODO Verify we are paginating for testable synced streams
                self.assertGreater(record_count_by_stream.get(stream, -1), self.API_LIMIT,
                                   msg="We didn't guarantee pagination. The number of records should exceed the api limit.")

                data = synced_records.get(stream, [])
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
                auto_fields = self.expected_automatic_fields().get(stream)

                for actual_keys in record_messages_keys:

                    # Verify that the automatic fields are sent to the target for paginated streams
                    self.assertTrue(auto_fields.issubset(actual_keys),
                                    msg="A paginated synced stream has a record that is missing automatic fields.")

                    # Verify we have more fields sent to the target than just automatic fields (this is set above)
                    self.assertEqual(set(), auto_fields.difference(actual_keys),
                                     msg="A paginated synced stream has a record that is missing expected fields.")

                
                messages = synced_records[stream]['messages']
                start_date_messages = [m for m in messages if m['data']['start_date'] == self.start_date ]
                primary_keys_list = [tuple([message.get('data').get(expected_pk) for expected_pk in expected_primary_keys])
                                        for message in start_date_messages
                                        if message.get('action') == 'upsert']

                primary_keys_list_1 = primary_keys_list[:self.API_LIMIT]
                primary_keys_list_2 = primary_keys_list[self.API_LIMIT:2*self.API_LIMIT]

                primary_keys_page_1 = set(primary_keys_list_1)
                primary_keys_page_2 = set(primary_keys_list_2)

                # Verify by primary keys that data is unique for page
                self.assertTrue(
                    primary_keys_page_1.isdisjoint(primary_keys_page_2))