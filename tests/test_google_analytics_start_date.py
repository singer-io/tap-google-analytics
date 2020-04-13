import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest
from functools import reduce

class TestGoogleAnalyticsStartDate(unittest.TestCase):
    """
    Test that the replication start date is respected

    • verify all streams have exactly 1 record per day starting with the replication
      start date and ending with today
    • verify for any given stream that a sync with an earlier replication start date has
      exactly x records more than the 1st sync with a later replication start date, where 
          x = difference in days between the two start dates
    TODO Determine if this ^ is necessary, first bullet true would imply second bullet is true...
    • verify all records in a given stream have a unique 'start_date' value in that sync
    • verify that 'start_date' == 'end_date' is True for all records of a given stream 
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
