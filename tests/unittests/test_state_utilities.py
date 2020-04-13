import datetime
import pytz
import unittest
from unittest.mock import Mock, MagicMock, patch

from tap_google_analytics import clean_state_for_report, get_start_date

class TestCleanStateForReport(unittest.TestCase):

    def test_single_profile_old_state(self):
        config = {
            'view_id': '12345',
            'start_date': '2020-03-15'
        }

        state = {
            'bookmarks': {
                'report1': {
                    'last_report_date': '2020-04-01'
                }
            }
        }

        actual = clean_state_for_report(config, state, 'report1')

        expected = {
            'bookmarks': {
                'report1': {
                    '12345': {
                        'last_report_date': '2020-04-01'
                    }
                }
            }
        }
        self.assertEqual(expected, actual)

    def test_multiple_profiles_old_state(self):
        config = {
            'view_ids': ['12345', '67890'],
            'start_date': '2020-03-15'
        }

        state = {
            'bookmarks': {
                'report1': {
                    'last_report_date': '2020-04-01'
                }
            }
        }

        actual = clean_state_for_report(config, state, 'report1')

        expected = {
            'bookmarks': {
                'report1': {
                    '12345': {
                        'last_report_date': '2020-04-01'
                    },
                    '67890': {
                        'last_report_date': '2020-04-01'
                    }
                }
            }
        }
        self.assertEqual(expected, actual)

    def test_converted_state_does_nothing(self):
        config = {
            'view_id': '12345',
            'start_date': '2020-03-15'
        }

        state = {
            'bookmarks': {
                'report1': {
                    '12345': {
                        'last_report_date': '2020-04-01'
                    }
                }
            }
        }

        actual = clean_state_for_report(config, state, 'report1')

        expected = {
            'bookmarks': {
                'report1': {
                    '12345': {
                        'last_report_date': '2020-04-01'
                    }
                }
            }
        }
        self.assertEqual(expected, actual)

    def test_empty_state_does_nothing(self):
        config = {
            'view_id': '12345',
            'start_date': '2020-03-15'
        }

        state = {}

        actual = clean_state_for_report(config, state, 'report1')

        expected = {}

        self.assertEqual(expected, actual)

class TestGetStartDate(unittest.TestCase):

    def test_new_view_id_returns_start_date(self):
        config = {'start_date': '2020-03-15'}

        view_id = '67890'

        state = {
            'bookmarks': {
                'report1': {
                    '12345': {
                        'last_report_date': '2020-04-01'
                    }
                }
            }
        }

        actual = get_start_date(config, view_id, state, 'report1')

        expected = datetime.datetime(2020, 3, 15, tzinfo=pytz.utc)

        self.assertEqual(expected, actual)

    def test_has_view_id_returns_bookmark(self):
        config = {'start_date': '2020-03-15'}

        view_id = '12345'

        state = {
            'bookmarks': {
                'report1': {
                    '12345': {
                        'last_report_date': '2020-04-01'
                    }
                }
            }
        }

        actual = get_start_date(config, view_id, state, 'report1')

        expected = datetime.datetime(2020, 4, 1, tzinfo=pytz.utc)

        self.assertEqual(expected, actual)


    def test_empty_state_returns_start_date(self):
        config = {'start_date': '2020-03-15'}

        view_id = '12345'

        state = {}

        actual = get_start_date(config, view_id, state, 'report1')

        expected = datetime.datetime(2020, 3, 15, tzinfo=pytz.utc)

        self.assertEqual(expected, actual)
