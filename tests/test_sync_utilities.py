import unittest
from unittest.mock import Mock, MagicMock, patch
from singer import utils

import tap_google_analytics.sync
from tap_google_analytics.sync import sync_report

reports = {
    utils.strptime_to_utc("2019-11-01"): [{"reports": [{"data": {"isDataGolden": True}}]}],
    utils.strptime_to_utc("2019-11-02"): [{"reports": [{"data": {"isDataGolden": True}}]}],
    utils.strptime_to_utc("2019-11-03"): [{"reports": [{"data": {"isDataGolden": False}}]}],
    utils.strptime_to_utc("2019-11-04"): [{"reports": [{"data": {"isDataGolden": True}}]}],
}

def get_mock_report(profile_id, report_date, metrics, dimensions):
    return reports[report_date]

class TestIsDataGoldenBookmarking(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()        
        self.client.get_report = MagicMock(side_effect=get_mock_report)

    @patch("tap_google_analytics.sync.report_to_records")
    @patch("singer.write_record")
    @patch("singer.write_state")
    def test_bookmarking_stops_at_first_false(self, *args):
        state = {}
        sync_report(self.client,
             {"name":"test_report", "profile_id": "12345", "metrics":[], "dimensions":[]},
             utils.strptime_to_utc("2019-11-01"),
             utils.strptime_to_utc("2019-11-04"),
             state)
        # Ensure we stopped bookmarking at third day
        self.assertEqual({'bookmarks': {'test_report': {'last_report_date': '2019-11-03'}}}, state)
        # Ensure we paginated through all 4 days, not stopping at third
        self.assertEqual(self.client.get_report.call_count, 4)

    @patch("tap_google_analytics.sync.report_to_records")
    @patch("singer.write_record")
    @patch("singer.write_state")
    def test_bookmark_is_saved_if_first_is_false(self, *args):
        state = {}
        sync_report(self.client,
             {"name":"test_report", "profile_id": "12345", "metrics":[], "dimensions":[]},
             utils.strptime_to_utc("2019-11-03"),
             utils.strptime_to_utc("2019-11-03"),
             state)
        self.assertEqual({'bookmarks': {'test_report': {'last_report_date': '2019-11-03'}}}, state)
        self.assertEqual(self.client.get_report.call_count, 1)
