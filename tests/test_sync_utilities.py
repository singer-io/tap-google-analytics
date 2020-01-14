import unittest
from unittest.mock import Mock, MagicMock, patch
from singer import utils

import tap_google_analytics.sync
from tap_google_analytics.sync import sync_report, generate_sdc_record_hash

reports = {
    utils.strptime_to_utc("2019-11-01"): [{"reports": [{"data": {"isDataGolden": True}}]}],
    utils.strptime_to_utc("2019-11-02"): [{"reports": [{"data": {"isDataGolden": True}}]}],
    utils.strptime_to_utc("2019-11-03"): [{"reports": [{"data": {}}]}],
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
                    {},
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
                    {},
                    {"name":"test_report", "profile_id": "12345", "metrics":[], "dimensions":[]},
                    utils.strptime_to_utc("2019-11-03"),
                    utils.strptime_to_utc("2019-11-03"),
                    state)
        self.assertEqual({'bookmarks': {'test_report': {'last_report_date': '2019-11-03'}}}, state)
        self.assertEqual(self.client.get_report.call_count, 1)

class TestRecordHashing(unittest.TestCase):
    """
    Canary test with a constant hash, if this value ever changes, it
    indicates that the primary key has been invalidated by changes.
    """
    def test_record_hash_canary(self):
        test_report = {"accountId": "12345",
                       "webPropertyId": "AA-TESTID",
                       "profileId": "67890",
                       "reports": [{
                           "columnHeader": {
                               "dimensions": ["ga:dim1", "ga:dim2", "ga:apples", "ga:visitDateThing"]
                           }
                       }]}
        row = {"dimensions": [5.23, "a string value", 123, "2019-04-03T00:11:40.04836Z"]}
        report_start = utils.strptime_to_utc("2019-11-20")
        report_end = utils.strptime_to_utc("2019-11-25")

        expected_hash = 'f107fb927002d0cbf257bd53c1a5d88bcb80e4e796f1812cf501107cf1f1544b'
        self.assertEqual(expected_hash, generate_sdc_record_hash(test_report, row, report_start, report_end))
