"""
Setup expectations for test sub classes
Run discovery for as a prerequisite for most tests
"""
import unittest
import os
from datetime import timedelta
from datetime import datetime as dt

import singer
from tap_tester import connections, menagerie, runner

##########################################################################
### TODO Test cases to think about Carding  Out
##########################################################################
# Verify the tap is handling is_golden data as expected
# Verify the tap fails when invalid dimension/metric selections are made
# Verify the tap can pick up from a terminated sync using `currently_syncing`
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

    LOGGER = singer.get_logger()
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

    @staticmethod
    def _select_streams_and_fields(conn_id, catalogs, select_default_fields, select_pagination_fields):
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
            "Ecommerce Overview": {  # TODO there's no data for this report b/c we don't have any purchases
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
        TODO So the when we uncomment the other dimensions we get no data...
             but we are able to select them???
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

    @staticmethod
    def custom_report_fields():  # TODO do we need this? Could grab from discovery in test
        return {
            'Test Report 1': {
                'ga:14dayUsers',
                'ga:1dayUsers',
                'ga:28dayUsers',
                'ga:30dayUsers',
                'ga:7dayUsers',
                'ga:acquisitionCampaign',
                'ga:acquisitionMedium',
                'ga:acquisitionSource',
                'ga:acquisitionSourceMedium',
                'ga:acquisitionTrafficChannel',
                'ga:adClicks',
                'ga:adContent',
                'ga:adCost',
                'ga:adDestinationUrl',
                'ga:adDisplayUrl',
                'ga:adDistributionNetwork',
                'ga:adFormat',
                'ga:adGroup',
                'ga:adKeywordMatchType',
                'ga:adMatchedQuery',
                'ga:adMatchType',
                'ga:adPlacementDomain',
                'ga:adPlacementUrl',
                'ga:adQueryWordCount',
                'ga:adsenseAdsClicks',
                'ga:adsenseAdsViewed',
                'ga:adsenseAdUnitsViewed',
                'ga:adsenseCoverage',
                'ga:adsenseCTR',
                'ga:adsenseECPM',
                'ga:adsenseExits',
                'ga:adsensePageImpressions',
                'ga:adsenseRevenue',
                'ga:adsenseViewableImpressionPercent',
                'ga:adSlot',
                'ga:adTargetingOption',
                'ga:adTargetingType',
                'ga:adwordsAdGroupID',
                'ga:adwordsCampaignID',
                'ga:adwordsCreativeID',
                'ga:adwordsCriteriaID',
                'ga:adwordsCustomerID',
                'ga:adxClicks',
                'ga:adxCoverage',
                'ga:adxCTR',
                'ga:adxECPM',
                'ga:adxImpressions',
                'ga:adxImpressionsPerSession',
                'ga:adxMonetizedPageviews',
                'ga:adxRevenue',
                'ga:adxRevenuePer1000Sessions',
                'ga:adxViewableImpressionsPercent',
                'ga:affiliation',
                'ga:appId',
                'ga:appInstallerId',
                'ga:appName',
                'ga:appVersion',
                'ga:avgDomainLookupTime',
                'ga:avgDomContentLoadedTime',
                'ga:avgDomInteractiveTime',
                'ga:avgEventValue',
                'ga:avgPageDownloadTime',
                'ga:avgPageLoadTime',
                'ga:avgRedirectionTime',
                'ga:avgScreenviewDuration',
                'ga:avgSearchDepth',
                'ga:avgSearchDuration',
                'ga:avgSearchResultViews',
                'ga:avgServerConnectionTime',
                'ga:avgServerResponseTime',
                'ga:avgSessionDuration',
                'ga:avgTimeOnPage',
                'ga:avgUserTimingValue',
                'ga:backfillClicks',
                'ga:backfillCoverage',
                'ga:backfillCTR',
                'ga:backfillECPM',
                'ga:backfillImpressions',
                'ga:backfillImpressionsPer','ga:backfillMonetizedPageviews',
                'ga:backfillRevenue',
                'ga:backfillRevenuePer1000Sessions',
                'ga:backfillViewableImpressionsPercent',
                'ga:bounceRate',
                'ga:bounces',
                'ga:browser',
                'ga:browserSize',
                'ga:browserVersion',
                'ga:buyToDetailRate',
                'ga:campaign',
                'ga:campaignCode',
                'ga:cartToDetailRate',
                'ga:channelGrouping',
                'ga:checkoutOptions',
                'ga:city',
                'ga:cityId',
                'ga:cohort',
                'ga:cohortActiveUsers',
                'ga:cohortAppviewsPer',
                'ga:cohortAppviewsPerUserWithLifetimeCriteria',
                'ga:cohortGoalCompletionsPer',
                'ga:cohortGoalCompletionsPerUserWithLifetimeCriteria',
                'ga:cohortNthDay',
                'ga:cohortNthMonth',
                'ga:cohortNthWeek',
                'ga:cohortPageviewsPer',
                'ga:cohortPageviewsPerUserWithLifetimeCriteria',
                'ga:cohortRetentionRate',
                'ga:cohortRevenuePer',
                'ga:cohortRevenuePerUserWithLifetimeCriteria',
                'ga:cohortSessionDurationPer',
                'ga:cohortSessionDurationPerUserWithLifetimeCriteria',
                'ga:cohortSessionsPer',
                'ga:cohortSessionsPerUserWithLifetimeCriteria',
                'ga:cohortTotalUsers',
                'ga:cohortTotalUsersWithLifetimeCriteria',
                'ga:contentGroup1',
                'ga:contentGroup2',
                'ga:contentGroup3',
                'ga:contentGroup4',
                'ga:contentGroup5',
                'ga:contentGroupUniqueViews1',
                'ga:contentGroupUniqueViews2',
                'ga:contentGroupUniqueViews3',
                'ga:contentGroupUniqueViews4',
                'ga:contentGroupUniqueViews5',
                'ga:continent',
                'ga:continentId',
                'ga:costPerConversion',
                'ga:costPerGoalConversion',
                'ga:costPerTransaction',
                'ga:country',
                'ga:countryIsoCode',
                'ga:CPC',
                'ga:CPM',
                'ga:CTR',
                'ga:currencyCode',
                'ga:dataSource',
                'ga:date',
                'ga:dateHour',
                'ga:dateHourMinute',
                'ga:day',
                'ga:dayOfWeek',
                'ga:dayOfWeekName',
                'ga:daysSinceLast',
                'ga:daysToTransaction',
                'ga:dbmClickAdvertiser',
                'ga:dbmClickAdvertiserId',
                'ga:dbmClickCreativeId',
                'ga:dbmClickExchange',
                'ga:dbmClickExchangeId',
                'ga:dbmClickInsertionOrder',
                'ga:dbmClickInsertionOrderId',
                'ga:dbmClickLineItem',
                'ga:dbmClickLineItemId',
                'ga:dbmClicks',
                'ga:dbmClickSite',
                'ga:dbmClickSiteId',
                'ga:dbmConversions',
                'ga:dbmCost',
                'ga:dbmCPA',
                'ga:dbmCPC',
                'ga:dbmCPM',
                'ga:dbmCTR',
                'ga:dbmImpressions',
                'ga:dbmLastEventAdvertiser',
                'ga:dbmLastEventAdvertiserId',
                'ga:dbmLastEventCreativeId',
                'ga:dbmLastEventExchange',
                'ga:dbmLastEventExchangeId',
                'ga:dbmLastEventInsertionOrder',
                'ga:dbmLastEventInsertionOrderId',
                'ga:dbmLastEventLineItem',
                'ga:dbmLastEventLineItemId',
                'ga:dbmLastEventSite',
                'ga:dbmLastEventSiteId',
                'ga:dbmROAS',
                'ga:dcmClickAd',
                'ga:dcmClickAdId',
                'ga:dcmClickAdType',
                'ga:dcmClickAdTypeId',
                'ga:dcmClickAdvertiser',
                'ga:dcmClickAdvertiserId',
                'ga:dcmClickCampaign',
                'ga:dcmClickCampaignId',
                'ga:dcmClickCreative',
                'ga:dcmClickCreativeId',
                'ga:dcmClickCreativeType',
                'ga:dcmClickCreativeTypeId',
                'ga:dcmClickCreativeVersion',
                'ga:dcmClickRenderingId',
                'ga:dcmClicks',
                'ga:dcmClickSite',
                'ga:dcmClickSiteId',
                'ga:dcmClickSitePlacement',
                'ga:dcmClickSitePlacementId',
                'ga:dcmClickSpotId',
                'ga:dcmCost',
                'ga:dcmCPC',
                'ga:dcmCTR',
                'ga:dcmFloodlightActivity',
                'ga:dcmFloodlightActivityAndGroup',
                'ga:dcmFloodlightActivityGroup',
                'ga:dcmFloodlightActivityGroupId',
                'ga:dcmFloodlightActivityId',
                'ga:dcmFloodlightAdvertiserId',
                'ga:dcmFloodlightQuantity',
                'ga:dcmFloodlightRevenue',
                'ga:dcmFloodlightSpotId',
                'ga:dcmImpressions',
                'ga:dcmLastEventAd',
                'ga:dcmLastEventAdId',
                'ga:dcmLastEventAdType',
                'ga:dcmLastEventAdTypeId',
                'ga:dcmLastEventAdvertiser',
                'ga:dcmLastEventAdvertiserId',
                'ga:dcmLastEventAttributionType',
                'ga:dcmLastEventCampaign',
                'ga:dcmLastEventCampaignId',
                'ga:dcmLastEventCreative',
                'ga:dcmLastEventCreativeId',
                'ga:dcmLastEventCreativeType',
                'ga:dcmLastEventCreativeTypeId',
                'ga:dcmLastEventCreativeVersion',
                'ga:dcmLastEventRenderingId',
                'ga:dcmLastEventSite',
                'ga:dcmLastEventSiteId',
                'ga:dcmLastEventSitePlacement',
                'ga:dcmLastEventSitePlacementId',
                'ga:dcmLastEventSpotId',
                'ga:dcmROAS',
                'ga:dcmRPC',
                'ga:deviceCategory',
                'ga:dfpClicks',
                'ga:dfpCoverage',
                'ga:dfpCTR',
                'ga:dfpECPM',
                'ga:dfpImpressions',
                'ga:dfpImpressionsPer',
                'ga:dfpLineItemId',
                'ga:dfpLineItemName',
                'ga:dfpMonetizedPageviews',
                'ga:dfpRevenue',
                'ga:dfpRevenuePer1000Sessions',
                'ga:dfpViewableImpressionsPercent',
                'ga:domainLookupTime',
                'ga:domContentLoadedTime',
                'ga:domInteractiveTime',
                'ga:domLatencyMetricsSample',
                'ga:dsAdGroup',
                'ga:dsAdGroupId',
                'ga:dsAdvertiser',
                'ga:dsAdvertiserId',
                'ga:dsAgency',
                'ga:dsAgencyId',
                'ga:dsCampaign',
                'ga:dsCampaignId',
                'ga:dsClicks',
                'ga:dsCost',
                'ga:dsCPC',
                'ga:dsCTR',
                'ga:dsEngineAccount',
                'ga:dsEngineAccountId',
                'ga:dsImpressions',
                'ga:dsKeyword',
                'ga:dsKeywordId',
                'ga:dsProfit',
                'ga:dsReturnOnAdSpend',
                'ga:dsRevenuePerClick',
                'ga:entranceRate',
                'ga:entrances',
                'ga:eventAction',
                'ga:eventCategory',
                'ga:eventLabel',
                'ga:eventsPerSessionWithEvent',
                'ga:eventValue',
                'ga:exceptionDescription',
                'ga:exceptions',
                'ga:exceptionsPerScreenview',
                'ga:exitPagePath',
                'ga:exitRate',
                'ga:exits',
                'ga:exitScreenName',
                'ga:experimentCombination',
                'ga:experimentId',
                'ga:experimentName',
                'ga:experimentVariant',
                'ga:fatal',
                'ga:fatalExceptionsPerScreenview',
                'ga:flashVersion',
                'ga:fullReferrer',
                'ga:goalAbandonRateAll',
                'ga:goalAbandonsAll',
                'ga:goalCompletionLocation',
                'ga:goalCompletionsAll',
                'ga:goalConversionRateAll',
                'ga:goalPreviousStep1',
                'ga:goalPreviousStep2',
                'ga:goalPreviousStep3',
                'ga:goalStartsAll',
                'ga:goalValueAll',
                'ga:goalValueAllPerSearch',
                'ga:goalValuePer',
                'ga:hasSocialSourceReferral',
                'ga:hits',
                'ga:hostname',
                'ga:hour',
                'ga:impressions',
                'ga:interestAffinityCategory',
                'ga:interestInMarketCategory',
                'ga:interestOtherCategory',
                'ga:internalPromotionClicks',
                'ga:internalPromotionCreative',
                'ga:internalPromotionCTR',
                'ga:internalPromotionId',
                'ga:internalPromotionName',
                'ga:internalPromotionPosition',
                'ga:internalPromotionViews',
                'ga:isoWeek',
                'ga:isoYear',
                'ga:isoYearIsoWeek',
                'ga:isTrueViewVideoAd',
                'ga:itemQuantity',
                'ga:itemRevenue',
                'ga:itemsPerPurchase',
                'ga:javaEnabled',
                'ga:keyword',
                'ga:landingContentGroup1',
                'ga:landingContentGroup2',
                'ga:landingContentGroup3',
                'ga:landingContentGroup4',
                'ga:landingContentGroup5',
                'ga:landingPagePath',
                'ga:landingScreenName',
                'ga:language',
                'ga:latitude',
                'ga:localItemRevenue',
                'ga:localProductRefundAmount',
                'ga:localRefundAmount',
                'ga:localTransactionRevenue',
                'ga:localTransactionShipping',
                'ga:localTransactionTax',
                'ga:longitude',
                'ga:medium',
                'ga:metro',
                'ga:metroId',
                'ga:minute',
                'ga:mobileDeviceBranding',
                'ga:mobileDeviceInfo',
                'ga:mobileDeviceMarketingName',
                'ga:mobileDeviceModel',
                'ga:mobileInputSelector',
                'ga:month',
                'ga:networkDomain',
                'ga:networkLocation',
                'ga:newUsers',
                'ga:nthDay',
                'ga:nthHour',
                'ga:nthMinute',
                'ga:nthMonth',
                'ga:nthWeek',
                'ga:operatingSystem',
                'ga:operatingSystemVersion',
                'ga:orderCouponCode',
                'ga:organicSearches',
                'ga:pageDepth',
                'ga:pageDownloadTime',
                'ga:pageLoadSample',
                'ga:pageLoadTime',
                'ga:pagePath',
                'ga:pagePathLevel1',
                'ga:pagePathLevel2',
                'ga:pagePathLevel3',
                'ga:pagePathLevel4',
                'ga:pageTitle',
                'ga:pageValue',
                'ga:pageviews',
                'ga:pageviewsPer',
                'ga:percentNewSessions',
                'ga:percentSearchRefinements',
                'ga:percentSessionsWithSearch',
                'ga:previousContentGroup1',
                'ga:previousContentGroup2',
                'ga:previousContentGroup3',
                'ga:previousContentGroup4',
                'ga:previousContentGroup5',
                'ga:previousPagePath',
                'ga:productAddsToCart',
                'ga:productBrand',
                'ga:productCategory',
                'ga:productCategoryHierarchy',
                'ga:productCategoryLevel1',
                'ga:productCategoryLevel2',
                'ga:productCategoryLevel3',
                'ga:productCategoryLevel4',
                'ga:productCategoryLevel5',
                'ga:productCheckouts',
                'ga:productCouponCode',
                'ga:productDetailViews',
                'ga:productListClicks',
                'ga:productListCTR',
                'ga:productListName',
                'ga:productListPosition',
                'ga:productListViews',
                'ga:productName',
                'ga:productRefundAmount',
                'ga:productRefunds',
                'ga:productRemovesFromCart',
                'ga:productRevenuePerPurchase',
                'ga:productSku',
                'ga:productVariant',
                'ga:quantityAddedToCart',
                'ga:quantityCheckedOut',
                'ga:quantityRefunded',
                'ga:quantityRemovedFromCart',
                'ga:redirectionTime',
                'ga:referralPath',
                'ga:refundAmount',
                'ga:region',
                'ga:regionId',
                'ga:regionIsoCode',
                'ga:revenuePerItem',
                'ga:revenuePerTransaction',
                'ga:revenuePer',
                'ga:ROAS',
                'ga:RPC',
                'ga:screenColors',
                'ga:screenDepth',
                'ga:screenName',
                'ga:screenResolution',
                'ga:screenviews',
                'ga:screenviewsPer',
                'ga:searchAfterDestinationPage',
                'ga:searchCategory',
                'ga:searchDepth',
                'ga:searchDestinationPage',
                'ga:searchDuration',
                'ga:searchExitRate',
                'ga:searchExits',
                'ga:searchGoalConversionRateAll',
                'ga:searchKeyword',
                'ga:searchKeywordRefinement',
                'ga:searchRefinements',
                'ga:searchResultViews',
                'ga:searchSessions',
                'ga:searchStartPage',
                'ga:searchUniques',
                'ga:searchUsed',
                'ga:secondPagePath',
                'ga:serverConnectionTime',
                'ga:serverResponseTime',
                'ga:sessionCount',
                'ga:sessionDuration',
                'ga:sessionDurationBucket',
                'ga:sessions',
                'ga:sessionsPer',
                'ga:sessionsToTransaction',
                'ga:sessionsWithEvent',
                'ga:shoppingStage',
                'ga:socialEngagementType',
                'ga:socialInteractionAction',
                'ga:socialInteractionNetwork',
                'ga:socialInteractionNetworkAction',
                'ga:socialInteractions',
                'ga:socialInteractionsPer',
                'ga:socialInteractionTarget',
                'ga:socialNetwork',
                'ga:source',
                'ga:sourceMedium',
                'ga:sourcePropertyDisplayName',
                'ga:sourcePropertyTrackingId',
                'ga:speedMetricsSample',
                'ga:subContinent',
                'ga:subContinentCode',
                'ga:timeOnPage',
                'ga:timeOnScreen',
                'ga:totalEvents',
                'ga:totalPublisherClicks',
                'ga:totalPublisherCoverage',
                'ga:totalPublisherCTR',
                'ga:totalPublisherECPM',
                'ga:totalPublisherImpressions',
                'ga:totalPublisherImpressionsPer',
                'ga:totalPublisherMonetizedPageviews',
                'ga:totalPublisherRevenue',
                'ga:totalPublisherRevenuePer1000Sessions',
                'ga:totalPublisherViewableImpressionsPercent',
                'ga:totalRefunds',
                'ga:totalValue',
                'ga:transactionId',
                'ga:transactionRevenue',
                'ga:transactionRevenuePer',
                'ga:transactions',
                'ga:transactionShipping',
                'ga:transactionsPer',
                'ga:transactionsPer',
                'ga:transactionTax',
                'ga:uniqueDimensionCombinations',
                'ga:uniqueEvents',
                'ga:uniquePageviews',
                'ga:uniquePurchases',
                'ga:uniqueScreenviews',
                'ga:uniqueSocialInteractions',
                'ga:userAgeBracket',
                'ga:userBucket',
                'ga:userDefinedValue',
                'ga:userGender',
                'ga:users',
                'ga:userTimingCategory',
                'ga:userTimingLabel',
                'ga:userTimingSample',
                'ga:userTimingValue',
                'ga:userTimingVariable',
                'ga:userType',
                'ga:week',
                'ga:year',
                'ga:yearMonth',
                'ga:yearWeek',
            }
        }
