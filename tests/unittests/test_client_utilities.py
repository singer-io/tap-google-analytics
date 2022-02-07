import unittest
import requests
import re
import tap_google_analytics.client as GoogleAnalyticsClient
from unittest.mock import patch
from unittest.mock import MagicMock

from tap_google_analytics.client import Client

import singer

LOGGER = singer.get_logger()

DEFAULT_TIMEOUT = 300

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

def mocked_post_with_self(self, url, headers=None, params=None, json=None, timeout=DEFAULT_TIMEOUT):
    return mocked_post(url, headers=headers, params=params, json=json)

URL_PATTERN = re.compile(r'^(?P<status_code>\d+)-(?P<status_title>[A-Z_]+)$')
def mocked_post(url, headers=None, params=None, json=None, data=None, timeout=DEFAULT_TIMEOUT):
    if url == "https://oauth2.googleapis.com/token":
        return MockResponse(
            {
                "access_token": "access_token",
                "expires_in": 9999999999,
            },
            200)
    elif url == "NoJsonData":
        return MockResponse(None, 401)
    elif url == "TimeoutError": # Raise timeout error when URL is passed TimeoutError
        raise requests.exceptions.Timeout

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

def mocked_request(self, method, url, headers=None, params=None, timeout=DEFAULT_TIMEOUT):
    if method == 'GET' and url == "https://www.googleapis.com/analytics/v3/management/accountSummaries":
        return MockResponse(
            {
                'items': []
            },
            200)
    
    elif url == "TimeoutError": # Raise timeout error when URL is passed TimeoutError
        raise requests.exceptions.Timeout

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
        with self.assertRaises(GoogleAnalyticsClient.GoogleAnalyticsClientError):
            client.post("NoJsonData")
        # Assert we retried 3 times until failing the last one
        # max tries = 4
        self.assertEqual(mocked_session_post.call_count, 5)

    def test_429_triggers_retry_backoff(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        expected_error_msg = "HTTP-error-code: 429, Error: {'code': 429, 'message': 'error message', 'status': 'RESOURCE_EXHAUSTED'}, Message: API rate limit exceeded, please retry after some time."
        with self.assertRaises(GoogleAnalyticsClient.GoogleAnalyticsResourceExhaustedError) as e:
            client.post("429-RESOURCE_EXHAUSTED")
        # Assert the message raise in the exceptions is as expected
        self.assertEqual(e.exception.message, expected_error_msg)
        # Assert we retried 3 times until failing the last one
        # max tries = 4
        self.assertEqual(mocked_session_post.call_count, 5)

    def test_503_unavailable_triggers_retry_backoff(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        expected_error_msg = "HTTP-error-code: 503, Error: {'code': 503, 'message': 'error message', 'status': 'UNAVAILABLE'}, Message: The service was unable to process the request."
        with self.assertRaises(GoogleAnalyticsClient.GoogleAnalyticsClientError) as e:
            client.post("503-UNAVAILABLE")
        # Assert the message raise in the exceptions is as expected
        self.assertEqual(e.exception.message, expected_error_msg)
        # Assert we retried 3 times until failing the last one
        # max tries = 4
        self.assertEqual(mocked_session_post.call_count, 5)

    def test_timeout_error_post_retry(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        '''
            Verify that timeout error is retrying for 4 times for post call
        '''
        client = Client(self.config, self.config_path)
        with self.assertRaises(requests.exceptions.Timeout):
            client.post("TimeoutError")

        # Post request(Session.post) called 5 times (4 for backoff + 1 for _ensure_access_token())
        self.assertEqual(mocked_session_post.call_count, 5)
    
    def test_timeout_error_get_retry(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        '''
            Verify that timeout error is retrying for 4 times for get call
        '''
        client = Client(self.config, self.config_path)
        with self.assertRaises(requests.exceptions.Timeout):
            client.get("TimeoutError")

        # Get request(Session.request) called 5 times (4 for backoff + 1 for _populate_profile_lookup())
        self.assertEqual(mocked_session_request.call_count, 5)

    def test_503_backend_triggers_no_retry(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        with self.assertRaises(GoogleAnalyticsClient.GoogleAnalyticsBackendError):
            client.post("503-BACKENDERROR")

        # Assert we gave up only after the first try
        self.assertEqual(mocked_session_post.call_count, 2)

    def test_500_backend_triggers_no_retry(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        expected_error_msg = "HTTP-error-code: 500, Error: {'code': 500, 'message': 'error message', 'status': 'INTERNAL'}, Message: An internal error has occurred at GoogleAnalytics's end."
        with self.assertRaises(GoogleAnalyticsClient.GoogleAnalyticsInternalServerError) as e:
            client.post("500-INTERNAL")
        # Assert the message raise in the exceptions is as expected
        self.assertEqual(e.exception.message, expected_error_msg)
        # Assert we gave up only after the first try
        self.assertEqual(mocked_session_post.call_count, 2)

    def test_400_backend_triggers_no_retry(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        expected_error_msg = "HTTP-error-code: 400, Error: {'code': 400, 'message': 'error message', 'status': 'INVALID_ARGUMENT'}, Message: The request is missing or has a bad parameter."
        with self.assertRaises(GoogleAnalyticsClient.GoogleAnalyticsInvalidArgumentError) as e:
            client.post("400-INVALID_ARGUMENT")
        # Assert the message raise in the exceptions is as expected
        self.assertEqual(e.exception.message, expected_error_msg)
        # Assert we gave up only after the first try
        self.assertEqual(mocked_session_post.call_count, 2)

    def test_401_backend_triggers_no_retry(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        expected_error_msg = "HTTP-error-code: 401, Error: {'code': 401, 'message': 'error message', 'status': 'UNAUTHENTICATED'}, Message: Invalid authorization credentials."
        with self.assertRaises(GoogleAnalyticsClient.GoogleAnalyticsUnauthenticatedError) as e:
            client.post("401-UNAUTHENTICATED")
        # Assert the message raise in the exceptions is as expected
        self.assertEqual(e.exception.message, expected_error_msg)
        # Assert we gave up only after the first try
        self.assertEqual(mocked_session_post.call_count, 2)

    def test_403_backend_triggers_no_retry(self, mocked_time_sleep, mocked_session_post, mocked_session_request, mocked_request_post):
        client = Client(self.config, self.config_path)
        expected_error_msg = "HTTP-error-code: 403, Error: {'code': 403, 'message': 'error message', 'status': 'PERMISSION_DENIED'}, Message: User does not have permission to access the resource."
        with self.assertRaises(GoogleAnalyticsClient.GoogleAnalyticsPermissionDeniedError) as e:
            client.post("403-PERMISSION_DENIED")
        # Assert the message raise in the exceptions is as expected
        self.assertEqual(e.exception.message, expected_error_msg)
        # Assert we gave up only after the first try
        self.assertEqual(mocked_session_post.call_count, 2)

class TestClientTCPKeepalive(unittest.TestCase):
    def setUp(self):
        self.request_spy = MagicMock()
        requests.models.PreparedRequest.prepare = self.request_spy

        mock_adaptor = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_adaptor.send = MagicMock(return_value=mock_response)
        requests.sessions.Session.get_adapter = MagicMock(return_value=mock_adaptor)

        requests.sessions.Session.resolve_redirects = MagicMock()

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

    def test_keepalive_on_session_request(self):
        client = Client(self.config, self.config_path)
        self.assertEqual(self.request_spy.call_args[1].get('headers', {}).get('Connection'), 'keep-alive')
