import datetime
import pytz
import unittest
from unittest.mock import Mock, MagicMock, patch

from tap_google_analytics.discover import calculate_custom_fields_support, \
    get_custom_fields_supertypes, types_to_schema

class TestCalculateCustomFieldsSupport(unittest.TestCase):

    def test_single_profile_id_full_support(self):

        custom_fields = {
            '12345': [{'id': 'metric1',
                       'profiles': ['12345']},
                      {'id': 'metric2',
                       'profiles': ['12345']},
                      {'id': 'dimension1',
                       'profiles': ['12345']}]
        }

        actual = calculate_custom_fields_support(custom_fields)

        expected = {
            'metric1': set(),
            'metric2': set(),
            'dimension1': set()
        }

        self.assertEquals(expected, actual)

    def test_multiple_profiles_same_property_id_full_support(self):
        custom_fields = {
            '12345': [{'id': 'metric1',
                       'profiles': ['12345', '67890']},
                      {'id': 'metric2',
                       'profiles': ['12345', '67890']},
                      {'id': 'dimension1',
                       'profiles': ['12345', '67890']}],
            '67890': [{'id': 'metric1',
                       'profiles': ['12345', '67890']},
                      {'id': 'metric2',
                       'profiles': ['12345', '67890']},
                      {'id': 'dimension1',
                       'profiles': ['12345', '67890']}]
        }

        actual = calculate_custom_fields_support(custom_fields)

        expected = {
            'metric1': set(),
            'metric2': set(),
            'dimension1': set()
        }

        self.assertEquals(expected, actual)

    def test_multiple_profiles_different_property_id_full_support(self):
        custom_fields = {
            '12345': [{'id': 'metric1',
                       'profiles': ['12345']},
                      {'id': 'metric2',
                       'profiles': ['12345']},
                      {'id': 'dimension1',
                       'profiles': ['12345']}],
            '67890': [{'id': 'metric1',
                       'profiles': ['67890']},
                      {'id': 'metric2',
                       'profiles': ['67890']},
                      {'id': 'dimension1',
                       'profiles': ['67890']}]
        }

        actual = calculate_custom_fields_support(custom_fields)

        expected = {
            'metric1': set(),
            'metric2': set(),
            'dimension1': set()
        }

        self.assertEquals(expected, actual)


    def test_multiple_profile_id_partial_support(self):
        custom_fields = {
            '12345': [{'id': 'metric1',
                       'profiles': ['12345']},
                      {'id': 'metric2',
                       'profiles': ['12345']},
                      {'id': 'dimension1',
                       'profiles': ['12345']}],
            '67890': [{'id': 'metric1',
                       'profiles': ['67890']},
                      {'id': 'metric3',
                       'profiles': ['67890']},
                      {'id': 'dimension1',
                       'profiles': ['67890']}]
        }

        actual = calculate_custom_fields_support(custom_fields)

        expected = {
            'metric1': set(),
            'metric2': {'67890'},
            'metric3': {'12345'},
            'dimension1': set()
        }

        self.assertEquals(expected, actual)

class TestGetCustomFieldsSupertypes(unittest.TestCase):

    def test_single_profile_works(self):
        custom_fields = {
            '12345': [{'id': 'metric1',
                       'kind': 'analytics#customMetric',
                       'dataType': 'STRING',
                       'type': 'METRIC',
                       'group': 'Custom Fields'}]
        }

        actual = get_custom_fields_supertypes(custom_fields)

        expected = [
            {'id': 'metric1',
             'kind': 'analytics#customMetric',
             'dataTypes': {'STRING'},
             'type': 'METRIC',
             'group': 'Custom Fields'}
        ]

        self.assertEqual(expected, actual)

    def test_multiple_profiles_same_datatypes_works(self):
        custom_fields = {
            '12345': [{'id': 'metric1',
                       'kind': 'analytics#customMetric',
                       'dataType': 'STRING',
                       'type': 'METRIC',
                       'group': 'Custom Fields'}],
            '67890': [{'id': 'metric1',
                       'kind': 'analytics#customMetric',
                       'dataType': 'STRING',
                       'type': 'METRIC',
                       'group': 'Custom Fields'}]
        }

        actual = get_custom_fields_supertypes(custom_fields)

        expected = [
            {'id': 'metric1',
             'kind': 'analytics#customMetric',
             'dataTypes': {'STRING'},
             'type': 'METRIC',
             'group': 'Custom Fields'}
        ]

        self.assertEqual(expected, actual)

    def test_multiple_profiles_different_datatypes_works(self):
        custom_fields = {
            '12345': [{'id': 'metric1',
                       'kind': 'analytics#customMetric',
                       'dataType': 'STRING',
                       'type': 'METRIC',
                       'group': 'Custom Fields'}],
            '67890': [{'id': 'metric1',
                       'kind': 'analytics#customMetric',
                       'dataType': 'CURRENCY',
                       'type': 'METRIC',
                       'group': 'Custom Fields'}]
        }

        actual = get_custom_fields_supertypes(custom_fields)

        expected = [
            {'id': 'metric1',
             'kind': 'analytics#customMetric',
             'dataTypes': {'STRING', 'CURRENCY'},
             'type': 'METRIC',
             'group': 'Custom Fields'}
        ]

        self.assertEqual(expected, actual)

class TestTypesToSchema(unittest.TestCase):

    def test_single_type_returns_no_anyof(self):
        actual = types_to_schema(['FLOAT'], 'ga:testField')

        expected = {'type': ['number', 'null']}

        self.assertEqual(expected, actual)

    def test_multiple_types_no_duplicates_returns_sorted(self):
        # NB: The order of the input for this one is important.
        # CURRENCY = number, TIME = string, INTEGER = integer
        actual = types_to_schema(['CURRENCY', 'TIME', 'INTEGER'], 'ga:testField')

        expected = {'anyOf': [{'type': ['integer', 'null']},
                              {'type': ['number', 'null']},
                              {'type': ['string', 'null']}]}

        self.assertEqual(expected, actual)

    def test_multiple_types_with_duplicates_returns_no_duplicates(self):
        actual = types_to_schema(['CURRENCY', 'PERCENT', 'INTEGER'], 'ga:testField')

        expected = {'anyOf': [{'type': ['integer', 'null']},
                              {'type': ['number', 'null']}]}

        self.assertEqual(expected, actual)
