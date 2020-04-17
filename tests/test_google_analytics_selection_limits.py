import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest
from functools import reduce

class TestGoogleAnalyticsSelectionLimitations(unittest.TestCase):
    """
    Test the limitations for the number of metrics and dimensions that can be selected
    • Verify that a synced report using maximum number of metrics and dimensions
      (10 & 7 respectively) results in records with 23 fields.
        10 metrics + 7 dimensions + 6 default fields
          default fields = {"_sdc_record_hash", "start_date", "end_date",
                            "account_id", "web_property_id", "profile_id"}

    Test reports that reflect different compatibility states
    • Verify that data is not replicated for a report when incompatible metrics and
      dimensions are selected
    • Verify that data is replicated for compatible metrics and dimensions???
    """
    def setUp(self):
        missing_envs = [x for x in ['TAP_GOOGLE_ANALYTICS_CLIENT_ID',
                                    'TAP_GOOGLE_ANALYTICS_CLIENT_SECRET',
                                    'TAP_GOOGLE_ANALYTICS_REFRESH_TOKEN',
                                    'TAP_GOOGLE_ANALYTICS_VIEW_ID'] if os.getenv(x) == None]
                                    #'TAP_GA_MERCH_VIEW_ID'] if os.getenv(x) == None]
        if len(missing_envs) != 0:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    def name(self):
        return "tap_tester_google_analytics_basic_sync"

    def get_type(self):
        return "platform.google-analytics"

    def get_credentials(self):
        return {
            'client_id': os.getenv('TAP_GOOGLE_ANALYTICS_CLIENT_ID'),
            'client_secret': os.getenv('TAP_GOOGLE_ANALYTICS_CLIENT_SECRET'),
            'refresh_token': os.getenv('TAP_GOOGLE_ANALYTICS_REFRESH_TOKEN')
        }

    def get_properties(self):
        return {
            'start_date' : '2020-03-01T00:00:00Z',
            'view_id': os.getenv('TAP_GOOGLE_ANALYTICS_VIEW_ID'),
            #'view_id': os.getenv('TAP_GA_MERCH_VIEW_ID'),
            'report_definitions': [{"id": "a665732c-d18b-445c-89b2-5ca8928a7305", "name": "report 1"}]
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

    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, non_selected_fields=non_selected_properties)

        # Select specific metrics and dimensions
        #self.select_all_streams_and_fields(conn_id, found_catalogs, select_all_fields=False)

