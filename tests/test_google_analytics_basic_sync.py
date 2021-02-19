import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest
from datetime import timedelta
from singer import utils
from functools import reduce

from base import GoogleAnalyticsBaseTest


class TestGoogleAnalyticsBasicSync(GoogleAnalyticsBaseTest):
    def name(self):
        return "tap_tester_google_analytics_basic_sync"

    def tap_name(self):
        return "tap-google-analytics"

    def get_field_selection(self):
        return set()

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

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

        # clear state and run a sync
        menagerie.set_state(conn_id, {})
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Test by stream
        for stream in synced_records.keys():
            with self.subTest(stream=stream):
                expected_standard_automatic_fields = self.expected_automatic_fields()[stream]
                expected_default_fields = self.expected_default_fields().get(stream, set())
                expected_automatic_fields = expected_standard_automatic_fields | expected_default_fields

                record_messages = [set(row['data'].keys()) for row in synced_records[stream]['messages']]
                for record_keys in record_messages:

                    self.assertEqual(record_keys, expected_automatic_fields)
