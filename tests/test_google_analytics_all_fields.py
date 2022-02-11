import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
from base import GoogleAnalyticsBaseTest


class GoogleAnalyticsAllFieldsTest(GoogleAnalyticsBaseTest):
    """Ensure running the tap with all streams and fields selected results in the replication of all fields."""
    
    # Skipped below streams due to following issues/limitations
    # Ecommerce Overview - TODO https://stitchdata.atlassian.net/browse/SRCE-5084
    # Test Report 1, Audience Technology - As per this doc (https://www.stitchdata.com/docs/integrations/saas/google-analytics#track-data)
    # we can only select max 10 metrics and 7 dimensions. So skipped these streams.
    # Behavior Overview - Some combination of metric and dimensions are not allowed. So skipped from all fields test.
    SKIP_STREAMS = {'Ecommerce Overview','Test Report 1', 'Audience Technology', 'Behavior Overview',}

    def name(self):
        return "tap_tester_google_analytics_all_fields_test"

    def test_run(self):
        """
        • Verify no unexpected streams were replicated
        • Verify that more than just the automatic fields are replicated for each stream. 
        • verify all fields for each stream are replicated
        """

        # Streams to verify all fields tests
        expected_streams = self.expected_sync_streams() - self.SKIP_STREAMS

        expected_automatic_fields = self.expected_automatic_fields()
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in expected_streams]
        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields, select_default_fields=True
        )

        # grab metadata after performing table-and-field selection to set expectations
        # used for asserting all fields are replicated
        stream_to_all_catalog_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(
                fields_from_field_level_md)

        self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)


        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_all_keys = stream_to_all_catalog_fields[stream]
                expected_automatic_keys = expected_automatic_fields.get(
                    stream, set())

                # Verify that more than just the automatic fields are replicated for each stream.
                self.assertTrue(expected_automatic_keys.issubset(
                    expected_all_keys), msg='{} is not in "expected_all_keys"'.format(expected_automatic_keys-expected_all_keys))

                messages = synced_records.get(stream)
                # collect actual values
                actual_all_keys = set()
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                # verify all fields for each stream are replicated
                self.assertSetEqual(expected_all_keys, actual_all_keys)