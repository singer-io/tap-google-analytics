import unittest
import requests
import re
from unittest.mock import patch

from tap_google_analytics.client import Client


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        if self.json_data is None:
            raise Exception("json data missing")

        return self.json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.RequestException("Raise for status", response=self)
        else:
            return None

def mocked_post_with_self(self, url, headers=None, params=None, json=None):
    return mocked_post(url, headers=headers, params=params, json=json)

URL_PATTERN = re.compile(r'^(?P<status_code>\d+)-(?P<status_title>[A-Z_]+)$')
def mocked_post(url, headers=None, params=None, json=None, data=None):
    if url == "https://oauth2.googleapis.com/token":
        return MockResponse(
            {
                "access_token": "access_token",
                "expires_in": 9999999999,
            },
            200)
    elif url == "NoJsonData":
        return MockResponse(None, 401)

    matches = URL_PATTERN.match(url)
    if matches:
        status_code = int(matches.group("status_code"))
        status_title = matches.group("status_title")
        return MockResponse(
            {
                "error": {
                    "code": status_code,
                    "message": "error message",
                    "status": status_title
                }
            },
            status_code)

    raise NotImplementedError(
        "Unexpected mocked post called with "
        "[url={}][headers={}][params={}][json={}][data={}]".format(url, headers, params, json, data)
    )

def mocked_request(self, method, url, headers=None, params=None):
    if method == 'GET' and url == "https://www.googleapis.com/analytics/v3/management/accountSummaries":
        return MockResponse(
            {
                'items': []
            },
            200)

    raise NotImplementedError(
        "Unexpected mocked get called with "
        "[method={}][url={}][headers={}][params={}]".format(method, url, headers, params)
    )

@patch("requests.post", side_effect=mocked_post, autospec=True)
@patch.object(requests.Session, 'request', autospec=True, side_effect=mocked_request)
@patch.object(requests.Session, 'post', autospec=True, side_effect=mocked_post_with_self)
@patch('time.sleep', return_value=None)
class TestClientRetries(unittest.TestCase):
    """
    Ensure certain status code responses trigger retries
    See documentation at https://developers.google.com/analytics/devguides/reporting/core/v4/errors
    """

    def setUp(self):
        self.config = {
            'auth_method': 'oauth2',
            'refresh_token': 'refresh_token',
            'client_id': 'client_id',
            'client_secret': 'client_secret',
            'quota_user': 'quota_user',
            'user_agent': 'user_agent',
            'view_id': 'view_id',
        }
        self.config_path = '/tmp/fake-config-path'

    def test_non_json_response_triggers_retry_backoff(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        with self.assertRaisesRegex(requests.exceptions.RequestException, 'Raise for status'):
            client.post("NoJsonData")

        # Assert we retried 3 times until failing the last one
        # max tries = 4
        self.assertEqual(mocked_session_post.call_count, 4)

    def test_429_triggers_retry_backoff(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        with self.assertRaisesRegex(requests.exceptions.RequestException, 'Raise for status'):
            client.post("429-RESOURCE_EXHAUSTED")

        # Assert we retried 3 times until failing the last one
        # max tries = 4
        self.assertEqual(mocked_session_post.call_count, 4)

    def test_503_unavailable_triggers_retry_backoff(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        with self.assertRaisesRegex(requests.exceptions.RequestException, 'Raise for status'):
            client.post("503-UNAVAILABLE")

        # Assert we retried 3 times until failing the last one
        # max tries = 4
        self.assertEqual(mocked_session_post.call_count, 4)

    def test_503_backend_triggers_no_retry(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        with self.assertRaisesRegex(requests.exceptions.RequestException, 'Raise for status'):
            client.post("503-BACKENDERROR")

        # Assert we gave up only after the first try
        self.assertEqual(mocked_session_post.call_count, 1)

    def test_500_backend_triggers_no_retry(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        with self.assertRaisesRegex(requests.exceptions.RequestException, 'Raise for status'):
            client.post("500-INTERNAL")

        # Assert we gave up only after the first try
        self.assertEqual(mocked_session_post.call_count, 1)


    def test_400_backend_triggers_no_retry(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        with self.assertRaisesRegex(Exception, 'Client Error, error message'):
            client.post("400-INVALID_ARGUMENT")

        # Assert we gave up only after the first try
        self.assertEqual(mocked_session_post.call_count, 1)

    def test_401_backend_triggers_no_retry(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        with self.assertRaisesRegex(Exception, 'Client Error, error message'):
            client.post("401-UNAUTHENTICATED")

        # Assert we gave up only after the first try
        self.assertEqual(mocked_session_post.call_count, 1)

    def test_403_backend_triggers_no_retry(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        with self.assertRaisesRegex(Exception, 'Client Error, error message'):
            client.post("403-PERMISSION_DENIED")

        # Assert we gave up only after the first try
        self.assertEqual(mocked_session_post.call_count, 1)
