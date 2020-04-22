import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest
from functools import reduce

class TestGoogleAnalyticsDataIncomplete(unittest.TestCase):
    """
    Google Analytics report data is aggregated per day, meaning that the 
    day’s data isn’t complete until the next day begins. For this reason,
    data for the current day may be re-replicated until the day is ‘complete’. 

    Note: The attribution modeling of your account may also affect when data is considered complete.

    • Verify that data with a 'start_date' of today, is re-replicated if 
      multiple replications are run in a single day
    • Verify if data is not complete it is repreplicated if a sync is ran at a later time
      (This most likely )
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
