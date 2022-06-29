##########################################################################
### TODO WAITING ON https://stitchdata.atlassian.net/browse/SRCE-5065
##########################################################################


from math import ceil
from datetime import timedelta
from datetime import datetime as dt

from  tap_tester import connections, runner

from base import GoogleAnalyticsBaseTest


class GoogleAnalyticsPaginationTest(GoogleAnalyticsBaseTest):
    """Test that we are paginating for streams when exceeding the API record limit of a single query"""

    # TODO https://stitchdata.atlassian.net/browse/SRCE-5084
    SKIP_STREAMS = {'Ecommerce Overview',}

    API_LIMIT = 2
    
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
        
        expected_streams = self.expected_sync_streams() - self.SKIP_STREAMS

        # Reduce page_size to pass the pagination test case.
        self.PAGE_SIZE = self.API_LIMIT
        
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

        # Revert back page size to 1000 for other test cases.
        self.PAGE_SIZE = 1000

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
                
                # Retrieve records for the start_date window only because, the tap calls the API on day-wise.
                # So, we are verifying pagination for the start_date API call.
                start_date_messages = [m for m in messages if m['data']['start_date'] == self.start_date ]
                primary_keys_list = [tuple([message.get('data').get(expected_pk) for expected_pk in expected_primary_keys])
                                        for message in start_date_messages
                                        if message.get('action') == 'upsert']

                # Chunk the replicated records (just primary keys) into expected pages
                pages = []
                page_count = ceil(len(primary_keys_list) / self.API_LIMIT)
                page_size = self.API_LIMIT
                for page_index in range(page_count):
                    page_start = page_index * page_size
                    page_end = (page_index + 1) * page_size
                    pages.append(set(primary_keys_list[page_start:page_end]))

                # Verify by primary keys that data is unique for each page
                for current_index, current_page in enumerate(pages):
                    with self.subTest(current_page_primary_keys=current_page):

                        for other_index, other_page in enumerate(pages):
                            if current_index == other_index:
                                continue  # don't compare the page to itself

                            self.assertTrue(
                                current_page.isdisjoint(other_page), msg=f'other_page_primary_keys={other_page}'
                            )