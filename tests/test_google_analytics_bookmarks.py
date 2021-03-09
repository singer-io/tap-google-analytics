import datetime
import dateutil.parser

from tap_tester import runner, menagerie, connections

from base import GoogleAnalyticsBaseTest


class GoogleAnalyticsBookmarksTest(GoogleAnalyticsBaseTest):
    # TODO https://stitchdata.atlassian.net/browse/SRCE-5084
    SKIP_STREAMS = {'Ecommerce Overview',}

    @staticmethod
    def name():
        return "tap_tester_google_analytics_bookmarks"

    @staticmethod
    def state_comparison_format(date_str):
        """
        Convert a saved bookmark value of the form '2020-08-25' to
        a string formatted datetime, in order to compare agianst json formatted datetime values.
        """
        date_object = dateutil.parser.parse(date_str)
        return datetime.datetime.strftime(date_object, "%Y-%m-%dT%H:%M:%SZ")

    def calculated_states_by_stream(self, current_state):
        """
        Look at the bookmarks from a previous sync and set a new bookmark
        value based off timedelta expectations. This ensures the subsequent sync will replicate
        at least 1 record but, fewer records than the previous sync.

        Sufficient test data is required for this test to cover a given stream.
        An incrmeental replication stream must have at least two records with
        replication keys that differ by more than the lookback window.

        If the test data is changed in the future this will break expectations for this test.

        """
        expected_streams = self.expected_sync_streams() - self.SKIP_STREAMS
        custom_streams_name_to_id = self.custom_reports_names_to_ids()
        for stream in expected_streams:  # the tap saves state based on tap_stream_id
            if stream in custom_streams_name_to_id.keys():
                expected_streams.remove(stream)
                expected_streams.add(custom_streams_name_to_id[stream])

        timedelta_by_stream = {stream: 15  # {stream_name: number_of_days, ...}
                               for stream in expected_streams}

        stream_to_calculated_state = {stream: "" for stream in current_state['bookmarks'].keys()}
        for stream, account_to_state in current_state['bookmarks'].items():
            state = account_to_state[self.account_id]
            state_key, state_value = next(iter(state.keys())), next(iter(state.values()))
            state_as_datetime = dateutil.parser.parse(state_value)

            days = timedelta_by_stream[stream]
            calculated_state_as_datetime = state_as_datetime - datetime.timedelta(days=days)

            state_format = '%Y-%m-%d'
            calculated_state_formatted = datetime.datetime.strftime(calculated_state_as_datetime, state_format)

            stream_to_calculated_state[stream] = {self.account_id: {state_key: calculated_state_formatted}}

        return stream_to_calculated_state


    def test_run(self):

        expected_streams = self.expected_sync_streams() - self.SKIP_STREAMS
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        today = datetime.datetime.utcnow().strftime(self.BOOKMARK_COMPARISON_FORMAT)
        self.account_id = self.get_properties()['view_id']
        custom_streams_name_to_id = self.custom_reports_names_to_ids()

        ##########################################################################
        ### First Sync
        ##########################################################################

        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        catalog_entries = [ce for ce in found_catalogs if ce['stream_name'] in expected_streams]
        self.perform_and_verify_table_and_field_selection(
            conn_id, catalog_entries, select_default_fields=True
        )

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        ### Update State Between Syncs
        ##########################################################################

        first_sync_currently_syncing = first_sync_bookmarks.get('currently_syncing')
        new_states = {'bookmarks': dict(),
                      'currently_syncing': first_sync_currently_syncing}
        simulated_states = self.calculated_states_by_stream(first_sync_bookmarks)
        for stream, new_state in simulated_states.items():
            new_states['bookmarks'][stream] = new_state
        menagerie.set_state(conn_id, new_states)

        ##########################################################################
        ### Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        ### Test By Stream
        ##########################################################################

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_replication_method = expected_replication_methods[stream]
                # tap saves state as stream_id for custom reports
                stream_id = custom_streams_name_to_id.get(stream, stream)

                # collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [record.get('data') for record in
                                       first_sync_records.get(stream).get('messages')
                                       if record.get('action') == 'upsert']
                second_sync_messages = [record.get('data') for record in
                                        second_sync_records.get(stream).get('messages')
                                        if record.get('action') == 'upsert']
                first_sync_sequences = [record.get('sequence') for record in
                                        first_sync_records.get(stream).get('messages')
                                        if record.get('action') == 'upsert']
                second_sync_sequences = [record.get('sequence') for record in
                                         second_sync_records.get(stream).get('messages')
                                         if record.get('action') == 'upsert']
                first_bookmark_key_value = first_sync_bookmarks['bookmarks'][stream_id][self.account_id]
                second_bookmark_key_value = second_sync_bookmarks['bookmarks'][stream_id][self.account_id]

                if expected_replication_method == self.INCREMENTAL:

                    # collect information specific to incremental streams from syncs 1 & 2
                    replication_key = next(iter(expected_replication_keys[stream]))
                    bookmark_key = "last_report_date"
                    first_bookmark_value_unformatted = first_bookmark_key_value.get(bookmark_key)
                    second_bookmark_value_unformatted = second_bookmark_key_value.get(bookmark_key)
                    first_bookmark_value = self.state_comparison_format(first_bookmark_value_unformatted)
                    second_bookmark_value = self.state_comparison_format(second_bookmark_value_unformatted)
                    simulated_bookmark_value = new_states['bookmarks'][stream_id][self.account_id][bookmark_key]


                    # Verify a sequence number is always present for a given record
                    self.assertEqual(len(first_sync_sequences), len(first_sync_messages))
                    self.assertEqual(len(second_sync_sequences), len(second_sync_messages))

                    # Verify sequence numbers are unique
                    first_sync_sequences_set = set(first_sync_sequences)
                    second_sync_sequences_set = set(second_sync_sequences)
                    self.assertEqual(len(first_sync_sequences), len(first_sync_sequences_set))
                    self.assertEqual(len(second_sync_sequences), len(second_sync_sequences_set))

                    # Verify the sequence numbers are always increasing
                    self.assertEqual(first_sync_sequences, sorted(first_sync_sequences))
                    self.assertEqual(second_sync_sequences, sorted(second_sync_sequences))


                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_key_value)
                    self.assertIsNotNone(first_bookmark_value)

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_key_value)
                    self.assertIsNotNone(second_bookmark_value)


                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    self.assertEqual(second_bookmark_value, first_bookmark_value) # assumes no changes to data during test

                    # Verify the second sync records respect the previous (simulated) bookmark value
                    self.assertTrue(all([record[replication_key] >= simulated_bookmark_value
                                         for record in second_sync_messages]))

                    # Verify today's date is the max replication-key value for the second sync
                    self.assertTrue(all([record[replication_key] <= today
                                         for record in second_sync_messages]))
                    self.assertEqual(second_sync_messages[-1][replication_key], today)

                    # Verify today's date is the max replication-key value for the first sync
                    self.assertTrue(all([record[replication_key] <= today
                                         for record in first_sync_messages]))
                    self.assertEqual(first_sync_messages[-1][replication_key], today)

                    # NB: We bookmark based on the last date where data "is golden" (unchanging), but will
                    #     replicate data through the date when the sync is ran, in this case today.
                    #     According to Gooogle Analytics Docs, is_golden should only be false for the last 24-48
                    #     hours of data.

                    # Verify the first sync bookmark falls within the date window of today - 48 hours
                    today_minus_golden_window = self.timedelta_formatted(today, days=-2)
                    self.assertGreaterEqual(
                        first_bookmark_value, today_minus_golden_window,
                        msg="First sync bookmark was set prior to the expected date."
                    )


                    # Verify the number of records in the 2nd sync is less then the first
                    self.assertLess(second_sync_count, first_sync_count)

                    # Verify at least 1 record was replicated in the second sync
                    self.assertGreater(second_sync_count, 0, msg="We are not fully testing bookmarking for {}".format(stream))


                else:

                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(stream, expected_replication_method)
                    )
