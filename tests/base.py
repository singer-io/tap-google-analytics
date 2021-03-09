"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import unittest
import os
from datetime import timedelta
from datetime import datetime as dt

from tap_tester import connections, menagerie, runner

##########################################################################
### TODO https://stitchdata.atlassian.net/browse/SRCE-5083
##########################################################################


class GoogleAnalyticsBaseTest(unittest.TestCase):
    """
    Setup expectations for test sub classes.
    Metadata describing streams.

    A bunch of shared methods that are used in tap-tester tests.
    Shared tap-specific methods (as needed).
    """
    AUTOMATIC_FIELDS = "automatic"
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    HASHED_KEYS = "default-hashed-keys"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    BOOKMARK_COMPARISON_FORMAT = "%Y-%m-%dT00:00:00.000000Z"

    start_date = ""

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "tap-google-analytics"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.google-analytics"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        return_value = {
            'start_date' : (dt.utcnow() - timedelta(days=30)).strftime(self.START_DATE_FORMAT),
            'view_id': os.getenv('TAP_GOOGLE_ANALYTICS_VIEW_ID'),
            'report_definitions': [{"id": "a665732c-d18b-445c-89b2-5ca8928a7305", "name": "Test Report 1"}]
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    @staticmethod
    def get_credentials():
        return {
            'client_id': os.getenv('TAP_GOOGLE_ANALYTICS_CLIENT_ID'),
            'client_secret': os.getenv('TAP_GOOGLE_ANALYTICS_CLIENT_SECRET'),
            'refresh_token': os.getenv('TAP_GOOGLE_ANALYTICS_REFRESH_TOKEN')
        }

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        default_hashed_keys = {
            'web_property_id',
            'account_id',
            'profile_id',
            'end_date'
        }

        return {
            "Test Report 1": {
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"start_date"},
                self.HASHED_KEYS: default_hashed_keys,
            },
            'Audience Overview': {
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"start_date"},
                self.HASHED_KEYS: default_hashed_keys,
            },
            'Audience Technology': {
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"start_date"},
                self.HASHED_KEYS: default_hashed_keys,
            },
            'Acquisition Overview': {
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"start_date"},
                self.HASHED_KEYS: default_hashed_keys,
            },
            'Ecommerce Overview': {
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"start_date"},
                self.HASHED_KEYS: default_hashed_keys,
            },
            'Audience Geo Location': {
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"start_date"},
                self.HASHED_KEYS: default_hashed_keys,
            },
            'Behavior Overview': {
                self.PRIMARY_KEYS: {"_sdc_record_hash"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"start_date"},
                self.HASHED_KEYS: default_hashed_keys,
            }
        }


    def expected_sync_streams(self):
        """A set of expected stream names"""
        return set(self.expected_metadata().keys())

    def child_streams(self):
        """
        Return a set of streams that are child streams
        based on having foreign key metadata
        """
        return {stream for stream, metadata in self.expected_metadata().items()
                if metadata.get(self.FOREIGN_KEYS)}

    def expected_primary_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of primary key fields
        """
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of replication key fields
        """
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_hashed_keys(self):
        """
        return a dictionary with key of table name
        and value as a set of hashed key fields used to form the primary key
        """
        return {table: properties.get(self.HASHED_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_automatic_fields(self):
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set()) \
                | v.get(self.HASHED_KEYS, set())

        return auto_fields

    def expected_replication_method(self):
        """return a dictionary with key of table name nd value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_GOOGLE_ANALYTICS_CLIENT_ID'),
                                    os.getenv('TAP_GOOGLE_ANALYTICS_CLIENT_SECRET'),
                                    os.getenv('TAP_GOOGLE_ANALYTICS_REFRESH_TOKEN'),
                                    os.getenv('TAP_GOOGLE_ANALYTICS_VIEW_ID')] if x is None]
        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))



    #########################
    #   Helper Methods      #
    #########################

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.

        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['stream_name'], found_catalogs))

        self.assertSetEqual(self.expected_sync_streams(), found_catalog_names, msg="discovered schemas do not match")
        print("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_primary_keys())
        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        print("total replicated row count: {}".format(sum(sync_record_count.values())))

        return sync_record_count

    def perform_and_verify_table_and_field_selection(self, conn_id, test_catalogs,
                                                     select_default_fields: bool = True,
                                                     select_pagination_fields: bool = False):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters. Note that selecting all fields is not
        possible for this tap due to dimension/metric conflicts set by Google and
        enforced by the Stitch UI.

        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self._select_streams_and_fields(
            conn_id=conn_id, catalogs=test_catalogs,
            select_default_fields=select_default_fields,
            select_pagination_fields=select_pagination_fields
        )

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected_streams = [tc.get('stream_name') for tc in test_catalogs]
        expected_default_fields = self.expected_default_fields()
        expected_pagination_fields = self.expected_pagination_fields()
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify all intended streams are selected
            selected = catalog_entry['metadata'][0]['metadata'].get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_selected_streams:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            # collect field selection expecationas
            expected_automatic_fields = self.expected_automatic_fields()[cat['stream_name']]
            selected_default_fields = expected_default_fields[cat['stream_name']] if select_default_fields else set()
            selected_pagination_fields = expected_pagination_fields[cat['stream_name']] if select_pagination_fields else set()

            # Verify all intended fields within the stream are selected
            expected_selected_fields = expected_automatic_fields | selected_default_fields | selected_pagination_fields
            selected_fields = self._get_selected_fields_from_metadata(catalog_entry['metadata'])
            for field in expected_selected_fields:
                field_selected = field in selected_fields
                print("\tValidating field selection on {}.{}: {}".format(cat['stream_name'], field, field_selected))
            self.assertSetEqual(expected_selected_fields, selected_fields)

    @staticmethod
    def _get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (
                field['metadata']['selected'] is True or \
                field['metadata']['inclusion'] == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields

    def _select_streams_and_fields(self, conn_id, catalogs, select_default_fields, select_pagination_fields):
        """Select all streams and all fields within streams"""

        for catalog in catalogs:

            schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
            metadata = schema_and_metadata['metadata']

            properties = set(md['breadcrumb'][-1] for md in metadata
                             if len(md['breadcrumb']) > 0 and md['breadcrumb'][0] == 'properties')

            # get a list of all properties so that none are selected
            if select_default_fields:
                non_selected_properties = properties.difference(
                    self.expected_default_fields()[catalog['stream_name']]
                )
            elif select_pagination_fields:
                non_selected_properties = properties.difference(
                    self.expected_pagination_fields()[catalog['stream_name']]
                )
            else:
                non_selected_properties = properties

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema_and_metadata, [], non_selected_properties)

    @staticmethod
    def parse_date(date_value):
        """
        Pass in string-formatted-datetime, parse the value, and return it as an unformatted datetime object.
        """
        date_formats = {
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f+00:00",
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%d"
        }
        for date_format in date_formats:
            try:
                date_stripped = dt.strptime(date_value, date_format)
                return date_stripped
            except ValueError:
                continue

        raise NotImplementedError("Tests do not account for dates of this format: {}".format(date_value))

    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)

            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
            try:
                date_stripped = dt.strptime(dtime, self.BOOKMARK_COMPARISON_FORMAT)
                return_date = date_stripped + timedelta(days=days)

                return dt.strftime(return_date, self.BOOKMARK_COMPARISON_FORMAT)

            except ValueError:
                return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))

    ##########################################################################
    ### Tap Specific Methods
    ##########################################################################

    @staticmethod
    def expected_default_fields():
        return {
            "Test Report 1" : {
                #"ga:sessions",  # Metric
                "ga:avgSessionDuration",  # Metric
                "ga:bounceRate",  # Metric
                "ga:users",  # Metric
                #"ga:pagesPerSession",  # Metric
                "ga:avgTimeOnPage",  # Metric
                "ga:bounces",  # Metric
                "ga:hits",  # Metric
                "ga:sessionDuration",  # Metric
                "ga:newUsers",  # Metric
                "ga:deviceCategory",  # Dimension
                # "ga:eventAction",  # Dimension
                "ga:date",  # Dimension
                # "ga:eventLabel",  # Dimension
                # "ga:eventCategory"  # Dimension
            },
            "Audience Overview": {
                "ga:users", "ga:newUsers", "ga:sessions", "ga:sessionsPerUser", "ga:pageviews",
                "ga:pageviewsPerSession", "ga:avgSessionDuration", "ga:bounceRate", "ga:date",
            },
            "Audience Geo Location": {
                "ga:users", "ga:newUsers", "ga:sessions", "ga:pageviewsPerSession",
                "ga:avgSessionDuration", "ga:bounceRate", "ga:date", "ga:country", "ga:city",
                "ga:continent", "ga:subContinent"
            },
            "Audience Technology": {
                "ga:users", "ga:newUsers", "ga:sessions", "ga:pageviewsPerSession",
                "ga:avgSessionDuration", "ga:bounceRate", "ga:date", "ga:browser", "ga:operatingSystem"
            },
            "Acquisition Overview": {
                "ga:sessions", "ga:pageviewsPerSession", "ga:avgSessionDuration", "ga:bounceRate",
                "ga:acquisitionTrafficChannel", "ga:acquisitionSource", "ga:acquisitionSourceMedium",
                "ga:acquisitionMedium"
            },
            "Behavior Overview": {
                "ga:pageviews", "ga:uniquePageviews", "ga:avgTimeOnPage", "ga:bounceRate",
                "ga:exitRate", "ga:exits", "ga:date", "ga:pagePath", "ga:pageTitle"
            },
            "Ecommerce Overview": {  # TODO https://stitchdata.atlassian.net/browse/SRCE-5084
                "ga:transactions", "ga:transactionId", "ga:campaign", "ga:source", "ga:medium",
                "ga:keyword", "ga:socialNetwork"
            }
        }

    @staticmethod
    def expected_pagination_fields():
        return {
            "Test Report 1" : set(),
            "Audience Overview": {
                "ga:users", "ga:newUsers", "ga:sessions", "ga:sessionsPerUser", "ga:pageviews",
                "ga:pageviewsPerSession", "ga:sessionDuration", "ga:bounceRate", "ga:date",
                # "ga:pageviews",
            },
            "Audience Geo Location": set(),
            "Audience Technology": set(),
            "Acquisition Overview": set(),
            "Behavior Overview": set(),
            "Ecommerce Overview": set(),
        }

    def custom_reports_names_to_ids(self):
        report_definitions =self.get_properties()['report_definitions']
        name_to_id_map = {
            definition.get('name'): definition.get('id')
            for definition in report_definitions
        }

        return name_to_id_map

    @staticmethod
    def is_custom_report(stream):
        standard_reports = {
            "Audience Overview",
            "Audience Geo Location",
            "Audience Technology",
            "Acquisition Overview",
            "Behavior Overview",
            "Ecommerce Overview",
        }
        return stream not in standard_reports

    @staticmethod
    def custom_report_minimum_valid_field_selection():
        """
        The uncommented dimensions and metrics are sufficient for the current test suite.
        In the future consider mixing up the selection to increase test covereage.
        See TODO header at top of file.
        """
        return {
            'Test Report 1': {
                #"ga:sessions",  # Metric
                "ga:avgSessionDuration",  # Metric
                "ga:bounceRate",  # Metric
                "ga:users",  # Metric
                # "ga:pagesPerSession",  # Metric
                "ga:avgTimeOnPage",  # Metric
                "ga:bounces",  # Metric
                "ga:hits",  # Metric
                "ga:sessionDuration",  # Metric
                "ga:newUsers",  # Metric
                "ga:deviceCategory",  # Dimension
                # "ga:eventAction",  # Dimension
                "ga:date",  # Dimension
                # "ga:eventLabel",  # Dimension
                # "ga:eventCategory"  # Dimension
            },
        }
