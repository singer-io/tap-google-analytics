from tap_tester import connections, runner

from base import GoogleAnalyticsBaseTest


class GoogleAnalyticsStartDateTest(GoogleAnalyticsBaseTest):
    # TODO https://stitchdata.atlassian.net/browse/SRCE-5084
    SKIP_STREAMS = {'Ecommerce Overview',}

    start_date_1 = ""
    start_date_2 = ""

    @staticmethod
    def name():
        return "tap_tester_google_analytics_start_date_test"

    def test_run(self):
        """Instantiate start date according to the desired data set and run the test"""

        self.start_date_1 = self.get_properties().get('start_date')
        self.start_date_2 = self.timedelta_formatted(self.start_date_1, days=5)

        self.start_date = self.start_date_1

        expected_streams = self.expected_sync_streams() - self.SKIP_STREAMS

        ##########################################################################
        ### First Sync
        ##########################################################################

        # instantiate connection
        conn_id_1 = connections.ensure_connection(self)

        # run check mode
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        # table and field selection
        test_catalogs_1_all_fields = [catalog for catalog in found_catalogs_1
                                      if catalog.get('stream_name') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_1, test_catalogs_1_all_fields)

        # run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        print("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(self.start_date, self.start_date_2))
        self.start_date = self.start_date_2

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id_2 = connections.ensure_connection(self, original_properties=False)

        # run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs_2
                                      if catalog.get('stream_name') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_2, test_catalogs_2_all_fields)

        # run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)
        synced_records_2 = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_start_date_1 = self.start_date_1
                expected_start_date_2 = self.start_date_2

                # collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = record_count_by_stream_2.get(stream, 0)
                primary_keys_list_1 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_1.get(stream).get('messages')
                                       if message.get('action') == 'upsert']
                primary_keys_list_2 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_2.get(stream).get('messages')
                                       if message.get('action') == 'upsert']
                primary_keys_sync_1 = set(primary_keys_list_1)
                primary_keys_sync_2 = set(primary_keys_list_2)


                # collect information specific to incremental streams from syncs 1 & 2
                expected_replication_key = next(iter(self.expected_replication_keys().get(stream)))
                replication_dates_1 =[row.get('data').get(expected_replication_key) for row in
                                      synced_records_1.get(stream, {'messages': []}).get('messages', [])
                                      if row.get('data')]
                replication_dates_2 =[row.get('data').get(expected_replication_key) for row in
                                      synced_records_2.get(stream, {'messages': []}).get('messages', [])
                                      if row.get('data')]

                # # Verify replication key is greater or equal to start_date for sync 1
                for replication_date in replication_dates_1:
                    self.assertGreaterEqual(
                        self.parse_date(replication_date), self.parse_date(expected_start_date_1),
                            msg="Report pertains to a date prior to our start date.\n" +
                            "Sync start_date: {}\n".format(expected_start_date_1) +
                            "Record date: {} ".format(replication_date)
                    )

                # Verify replication key is greater or equal to start_date for sync 2
                for replication_date in replication_dates_2:
                    self.assertGreaterEqual(
                        self.parse_date(replication_date), self.parse_date(expected_start_date_2),
                            msg="Report pertains to a date prior to our start date.\n" +
                            "Sync start_date: {}\n".format(expected_start_date_2) +
                            "Record date: {} ".format(replication_date)
                    )

                # Verify the number of records replicated in sync 1 is greater than the number
                # of records replicated in sync 2
                self.assertGreater(record_count_sync_1, record_count_sync_2)

                # Verify the records replicated in sync 2 were also replicated in sync 1
                self.assertTrue(primary_keys_sync_2.issubset(primary_keys_sync_1))
