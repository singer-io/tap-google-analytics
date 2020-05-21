from datetime import timedelta
import json
import math
import os
from jwt import (
    JWT,
    jwk_from_pem,
)
import requests
import singer
from singer import utils
import backoff

LOGGER = singer.get_logger()

def is_retryable_403(e):
    response = e.response
    if not _is_json(response):
        return False

    retryable_errors = ["userRateLimitExceeded", "rateLimitExceeded", "quotaExceeded"]
    error_reasons = [error.get('reason') for error in response.json().get('errors',[])]
    for retryable_error in retryable_errors:
        if retryable_error in error_reasons:
            return True
    return False

def should_giveup(e):
    should_retry = e.response.status_code == 429 or is_retryable_403(e)

    if should_retry and is_json(e.response):
        error_message = e.response.json().get("error", {}).get("message")
        if error_message:
            LOGGER.info("Encountered retryable %s, backing off exponentially. Details: %s",
                        e.response.status_code,
                        error_message)

    return not should_retry

def _is_json(response):
    try:
        response.json()
        return True
    except Exception:
        return False


# pylint: disable=too-many-instance-attributes
class Client():
    def __init__(self, config):
        self.auth_method = config['auth_method']
        if self.auth_method == "oauth2":
            self.refresh_token = config["refresh_token"]
            self.client_id = config["client_id"]
            self.client_secret = config["client_secret"]
        elif self.auth_method == "service_account":
            self.client_email = config["client_email"]
            self.private_key = config["private_key"].encode()

        self.__access_token = None
        self.expires_in = 0
        self.last_refreshed = None

        self.quota_user = config.get("quota_user")
        self.user_agent = config.get("user_agent")

        self.session = requests.Session()
        if self.user_agent:
            self.session.headers.update({"User-Agent": self.user_agent})

        self.profile_lookup = {}
        self.__populate_profile_lookup()

    def __populate_profile_lookup(self):
        """
        Get all profiles available and associate them with their web property
        and account IDs to be looked up later during discovery.
        """
        for account_id in self.get_accounts_for_token():
            for web_property_id in self.get_web_properties_for_account(account_id):
                for profile_id in self.get_profiles_for_property(account_id, web_property_id):
                    self.profile_lookup[profile_id] = {"web_property_id": web_property_id,
                                                       "account_id": account_id}

    # Authentication and refresh
    def _ensure_access_token(self):
        if self.last_refreshed is not None and \
           (utils.now() - self.last_refreshed).total_seconds() < self.expires_in:
            return

        LOGGER.info("Refreshing access token.")
        self.last_refreshed = utils.now()

        if self.auth_method == "oauth2":
            payload = {
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "refresh_token"
            }
        else:
            message = {
                "iss": self.client_email,
                "scope": "https://www.googleapis.com/auth/analytics.readonly",
                "aud":"https://oauth2.googleapis.com/token",
                "exp": math.floor((self.last_refreshed + timedelta(hours=1)).timestamp()),
                "iat": math.floor(self.last_refreshed.timestamp())
            }
            signing_key = jwk_from_pem(self.private_key)
            payload = {
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": JWT().encode(message, signing_key, 'RS256')
            }

        token_response = requests.post("https://oauth2.googleapis.com/token", data=payload)

        token_response.raise_for_status()

        token_json = token_response.json()
        self.__access_token = token_json['access_token']
        self.expires_in = token_json['expires_in']


    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.RequestException),
                          max_tries=10,
                          giveup=should_giveup,
                          factor=4,
                          jitter=None)
    def _make_request(self, method, url, params=None, data=None):
        params = params or {}
        data = data or {}

        self._ensure_access_token()

        headers = {"Authorization" : "Bearer " + self.__access_token}
        if self.quota_user:
            params["quotaUser"] = self.quota_user

        if method == 'POST':
            response = self.session.post(url, headers=headers, params=params, json=data)
        else:
            response = self.session.request(method, url, headers=headers, params=params)

        response_is_json = _is_json(response)
        error_message = response_is_json and response.json().get("error", {}).get("message")
        if response.status_code in [400, 403] and error_message:
            raise Exception("{} Error from Google - Details: {}".format(response.status_code, error_message))

        response.raise_for_status()

        return response

    def get(self, url, params=None):
        return self._make_request("GET", url, params=params)

    def post(self, url, data=None):
        return self._make_request("POST", url, data=data)

    # Discovery requests

    def get_field_metadata(self):
        metadata_response = self.get("https://www.googleapis.com/analytics/v3/metadata/{reportType}/columns".format(reportType="ga"))
        return metadata_response.json()

    def get_raw_cubes(self):
        try:
            cubes_response = self.get("https://ga-dev-tools.appspot.com/ga_cubes.json")
            cubes_response.raise_for_status()
            cubes_json = cubes_response.json()
        except Exception as ex:
            LOGGER.warning("Error fetching raw cubes, falling back to local copy. Exception message: %s", ex)
            local_cubes_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "ga_cubes.json")
            with open(local_cubes_path, "r") as f:
                cubes_json = json.load(f)
        return cubes_json

    def get_accounts_for_token(self):
        """ Return a list of account IDs available to hte associated token. """
        accounts_response = self.get('https://www.googleapis.com/analytics/v3/management/accounts')
        account_ids = [i['id'] for i in accounts_response.json()['items']]
        return account_ids

    def get_web_properties_for_account(self, account_id):
        """ Return a list of webproperty IDs for the account specified. """
        webprops_response = self.get('https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties'.format(accountId=account_id))
        webprops_ids = [w['id'] for w in webprops_response.json()['items']]
        return webprops_ids

    def get_profiles_for_property(self, account_id, web_property_id):
        """
        Gets all profiles for property to associate with custom metrics and dimensions.
        """
        profiles_url = 'https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/profiles'
        profiles_response = self.get(profiles_url.format(accountId=account_id,
                                                         webPropertyId=web_property_id))
        return [p["id"] for p in profiles_response.json()['items']]

    def get_goals_for_profile(self, profile_id):
        """
        Gets all goals for a profile_id.
        """
        return self.get_goals(self.profile_lookup[profile_id]["account_id"],
                              self.profile_lookup[profile_id]["web_property_id"],
                              profile_id)

    def get_goals(self, account_id, web_property_id, profile_id):
        """
        Gets all goal IDs for property and account to name custom metrics and dimensions.
        """
        profiles_url = 'https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/profiles/{profileId}/goals'
        goals_response = self.get(profiles_url.format(accountId=account_id,
                                                      webPropertyId=web_property_id,
                                                      profileId=profile_id))
        return [g["id"] for g in goals_response.json()['items']]

    def get_custom_metrics_for_profile(self, profile_id):
        """
        Get all custom metrics associated with the given profile ID.
        """
        return self.get_custom_metrics(self.profile_lookup[profile_id]["account_id"],
                                       self.profile_lookup[profile_id]["web_property_id"])

    def get_custom_metrics(self, account_id, web_property_id):
        """
        Gets all metrics for the specified web_property_id.

        """
        metrics_url = 'https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/customMetrics'

        custom_metrics_response = self.get(metrics_url.format(accountId=account_id,
                                                              webPropertyId=web_property_id))
        return custom_metrics_response.json()

    def get_custom_dimensions_for_profile(self, profile_id):
        """
        Get all custom dimensions associated with the given profile ID.
        """
        return self.get_custom_dimensions(self.profile_lookup[profile_id]["account_id"],
                                          self.profile_lookup[profile_id]["web_property_id"])

    def get_custom_dimensions(self, account_id, web_property_id):
        """
        Gets all dimensions for the specified web_property_id
        """
        url = 'https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/customDimensions'
        custom_dimensions_response = self.get(url.format(accountId=account_id,
                                                         webPropertyId=web_property_id))

        # NOTE: Assuming that all custom dimensions are STRING, since there's no type information
        return custom_dimensions_response.json()

    # Sync Requests w/ Pagination and token refresh
    # Docs for more info: https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet
    def get_report(self, name, profile_id, report_date, metrics, dimensions):
        """
        Parameters:
        - name - the tap_stream_id of the report being run
        - profile_id - the profile for which this report is being run
        - report_date - the day to retrieve data for, as a Python datetime object, to limit report data
        - metrics - list of metrics, of the form ["ga:metric1", "ga:metric2", ...]
        - dimensions - list of dimensions, of the form ["ga:dim1", "ga:dim2", ...]

        Returns:
        - A generator of a sequence of reports w/ associated metadata (metrics/dims/report_date/profile)
        """
        nextPageToken = None
        # TODO: Optimization, if speed is an issue, up to 5 requests can be placed per HTTP batch
        # - This will require changes to all parsing code to account for multiple report responses coming back
        while True:
            report_date_string = report_date.strftime("%Y-%m-%d")
            LOGGER.info("Making report request for profile ID %s and date %s (nextPageToken: %s)",
                        profile_id,
                        report_date_string,
                        nextPageToken)
            body = {"reportRequests":
                    [{"viewId": profile_id,
                      "dateRanges": [{"startDate": report_date_string,
                                      "endDate": report_date_string}],
                      "metrics": [{"expression": m} for m in metrics],
                      "dimensions": [{"name": d} for d in dimensions]}]}
            if nextPageToken:
                body["reportRequests"][0]["pageToken"] = nextPageToken
            with singer.metrics.http_request_timer(name):
                report_response = self.post("https://analyticsreporting.googleapis.com/v4/reports:batchGet", body)
            report = report_response.json()

            # Assoc in the request data to be used by the caller
            report.update({"profileId": profile_id,
                           "webPropertyId": self.profile_lookup[profile_id]["web_property_id"],
                           "accountId": self.profile_lookup[profile_id]["account_id"],
                           "reportDate": report_date,
                           "metrics": metrics,
                           "dimensions": dimensions})

            yield report

            # NB: Assumes only one report at a time
            nextPageToken = report["reports"][0].get("nextPageToken")

            if not nextPageToken:
                break
