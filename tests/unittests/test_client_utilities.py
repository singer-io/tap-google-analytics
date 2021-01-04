import unittest
import requests
from unittest.mock import Mock, MagicMock, patch

from tap_google_analytics.client import Client


class MockResponse:
    def __init__(self, json_data, status_code, do_raise_for_status=False):
        self.json_data = json_data
        self.status_code = status_code
        self.do_raise_for_status = do_raise_for_status

    def json(self):
        return self.json_data

    def raise_for_status(self):
        if self.do_raise_for_status:
            raise requests.exceptions.RequestException("Raise for status")
        else:
            return None

def mocked_post_with_self(self, url, headers=None, params=None, json=None):
    return mocked_post(url, headers=headers, params=params, json=json)

def mocked_post(url, headers=None, params=None, json=None, data=None):
    if url == "429-error-do-raise-for-status":
        return MockResponse(
            {
                "error": {
                    "message": "retry error message"
                }
            },
            429,
            do_raise_for_status=True)
    elif url == "503-error-do-raise-for-status":
        return MockResponse(
            {
                "error": {
                    "message": "retry error message"
                }
            },
            503,
            do_raise_for_status=True)
    elif url == "https://oauth2.googleapis.com/token":
        return MockResponse(
            {
                "access_token": "access_token",
                "expires_in": 9999999999,
            },
            200)

    raise NotImplementedError(
        "Unexpected mocked post called with "
        "[url={}][headers={}][params={}][json={}][data={}]".format(url, headers, params, json, data)
    )

def mocked_request(self, method, url, headers=None, params=None):
    if method != 'GET':
        raise NotImplementedError(
            "Unexpected mocked get called with "
            "[url={}][headers={}][params={}]".format(url, headers, params)
        )

    if url == "https://www.googleapis.com/analytics/v3/management/accountSummaries":
        return MockResponse({
            'items': []
        }, 200)

    raise NotImplementedError(
        "Unexpected mocked get called with "
        "[url={}][headers={}][params={}]".format(url, headers, params)
    )


class MockSession(requests.Session):
    # Without this the call to update headers fails since
    # headers attribute is not instantiated by default
    headers = {}

class TestClientRetries(unittest.TestCase):
    """
    Ensure certain status code responses trigger retries
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

    @patch("requests.post", side_effect=mocked_post, autospec=True)
    @patch.object(requests.Session, 'request', autospec=True, side_effect=mocked_request)
    @patch.object(requests.Session, 'post', autospec=True, side_effect=mocked_post_with_self)
    @patch('time.sleep', return_value=None)
    def test_429_triggers_retry_backoff(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        with self.assertRaisesRegex(requests.exceptions.RequestException, 'Raise for status') as context_manager:
            client.post("429-error-do-raise-for-status")

        self.assertIsNotNone(context_manager)
        # Assert we retried 3 times until failing the last one
        # max tries = 4
        self.assertEqual(mocked_session_post.call_count, 4)

    @patch("requests.post", side_effect=mocked_post, autospec=True)
    @patch.object(requests.Session, 'request', autospec=True, side_effect=mocked_request)
    @patch.object(requests.Session, 'post', autospec=True, side_effect=mocked_post_with_self)
    @patch('time.sleep', return_value=None)
    def test_503_triggers_retry_backoff(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        with self.assertRaisesRegex(requests.exceptions.RequestException, 'Raise for status') as context_manager:
            client.post("503-error-do-raise-for-status")

        self.assertIsNotNone(context_manager)
        # Assert we retried 3 times until failing the last one
        # max tries = 4
        self.assertEqual(mocked_session_post.call_count, 4)
