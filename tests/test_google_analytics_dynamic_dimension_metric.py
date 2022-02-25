import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
from base import GoogleAnalyticsBaseTest
import random

NO_OF_DIMENSIION_ALLOWED = 9
NO_OF_METRIC_ALLOWED = 10

class GoogleAnalyticsDynamicFieldselectionTest(GoogleAnalyticsBaseTest):
    """Ensure running the tap with streams and fields selected results in the replication of more than automatic fields."""

    stream_dimension = {
        # Provide the dimension for streams for the random selection
        "Audience Technology": {
            "ga:browser",
            "ga:operatingSystem",
            "ga:flashVersion",
            "ga:javaEnabled",
            "ga:screenColors",
            "ga:screenResolution",
            "ga:hostname",
            "ga:date",
            "ga:year",
            "ga:month",
            "ga:hour"
            }
        }

    conflicting_dimension = {
        # Provide the conflicting dimension and metric 
        # Eg: "ga:searchKeyword": {"ga:eventCategory"}
    }

    stream_metric = {
        # Provide the metrics for streams  for the random selection
        "Audience Technology": {
            "ga:users",
            "ga:newUsers",
            "ga:sessions",
            "ga:bounceRate",
            "ga:avgSessionDuration",
            "ga:pageviewsPerSession"
            }
    }

    # Ecommerce Overview - TODO https://stitchdata.atlassian.net/browse/SRCE-5084
    # Test Report 1 - Currently, skipping as it has 200+ dimension and exclusion logic
    SKIP_STREAMS = {'Ecommerce Overview', 'Test Report 1'}

    
    @staticmethod
    def name():
        return "tt_google_analytics_dynamic_metric_dimension"

    def get_non_selected_fields(self, streams):
        """
        This will return the non_selected_fields for each streams
        """     
        non_selected_fields = {}
        
        for stream in streams:
            # For below streams all fields needs to be selected
            if stream in ["Audience Overview", "Audience Geo Location", "Acquisition Overview"]:
                non_selected_fields[stream] = set()

            # For Behavior Overview stream, except "ga:searchKeyword", "ga:eventCategory" all fields needs to be selected
            elif stream == "Behavior Overview":
                non_selected_fields[stream] = {"ga:searchKeyword", "ga:eventCategory"}
            
            # This will randomly select the dimension and metric for the stream
            else:
                print(f'Randomly selecting metrics and dimensions for stream: {stream}')
                dimension = self.stream_dimension.get(stream) # Getting the dimension
                metric = self.stream_metric.get(stream) # Getting the metrics
                # Randomly select the dimension
                selected_dimension_fields = self.random_field_selection(dimension, NO_OF_DIMENSIION_ALLOWED) 
                print(f'Random Combination of dimension for {stream} : {selected_dimension_fields}')
                # Randomly select the metrics
                selected_metrics_fields = self.random_field_selection(metric, NO_OF_METRIC_ALLOWED)
                print(f'Random Combination of metrics for {stream} : {selected_metrics_fields} ')
                non_selected_fields[stream] = dimension | metric - selected_dimension_fields - selected_metrics_fields 
        return non_selected_fields

    def validate_selected_fields(self, selected_fields):
        """
        This function will validate the combination of randomly selected dimension and metrics
        """
        for field, conflict_fields in self.conflicting_dimension.items():
            # If any field in selected_fields, then discard conflict_field from the selected_fields
            if field in selected_fields:
                for conflict_field in conflict_fields:
                    selected_fields.discard(conflict_field)

    def random_field_selection(self, fields, no_of_fields):
        """
        This function will randomly select the dimensions and metrics
        """
        if len(fields) > no_of_fields:
            fields = set(random.sample(fields, no_of_fields))
        self.validate_selected_fields(fields)
        return fields

    def verify_field_selection(self, expected_streams):
        """
        • Verify no unexpected streams were replicated.
        • Verify that more than just the automatic fields are replicated for each stream. 
        • verify the selected fields for each streams are replicated.
        """

        # Grabbing the non_selected_props for the streams
        non_selected_properties_by_stream = self.get_non_selected_fields(expected_streams)

        expected_automatic_fields = self.expected_automatic_fields()
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in expected_streams]
        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields, select_default_fields=False, non_selected_props=non_selected_properties_by_stream
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
                fields_from_field_level_md) - non_selected_properties_by_stream.get(catalog['stream_name'])

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

                # verify all the selected fields for each streams are replicated
                self.assertSetEqual(expected_all_keys, actual_all_keys)

    def test_run(self):

        # As Audience Technology stream has random field selection, running that stream 5 times
        for i in range(5):
            expected_streams = {"Audience Technology"}
            self.verify_field_selection(expected_streams)

        # For other streams, running the test case once
        expected_streams = self.expected_sync_streams() - self.SKIP_STREAMS
        self.verify_field_selection(expected_streams)
