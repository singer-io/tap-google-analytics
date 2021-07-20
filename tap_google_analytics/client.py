from datetime import timedelta
import json
import pkgutil
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


def is_retryable_403(response):
    """
    The Google Analytics Management API and Metadata API define three types of 403s that are retryable due to quota limits.

    Docs:
    https://developers.google.com/analytics/devguides/config/mgmt/v3/errors
    https://developers.google.com/analytics/devguides/reporting/metadata/v3/errors
    """
    retryable_errors = {"userRateLimitExceeded", "rateLimitExceeded", "quotaExceeded"}
    error_reasons = get_error_reasons(response)

    if any(error_reasons.intersection(retryable_errors)):
        return True

    return False


def get_error_reasons(response):
    """
    The google apis don't document the way the errors appear in their reponse json in the same way across different api endpoints and versions. This method defensively tries to grab the error reason(s) in all the ways response have shown to have them. Lastly if all those ways fail the error message just shows the full response json.
    """
    response_json = response.json()

    error_reasons = set()
    if 'error' in response_json:
        error = response_json.get('error')
        if isinstance(error, dict) and 'errors' in error:
            errors = error.get('errors')
            for sub_error in errors:
                if 'reason' in sub_error:
                    error_reasons.add(sub_error.get('reason'))
                elif 'error_description' in sub_error:
                    error_reasons.add(sub_error.get('error_description'))
                else:
                    error_reasons.add('reason or error_description missing from error, see full response {}'.format(response_json))
        elif 'reason' in response_json:
            error_reasons.add(response_json.get('reason'))
        elif 'error_description' in response_json:
            error_reasons.add(response_json.get('error_description'))
        elif isinstance(error, str):
            error_reasons.add(error)
        else:
            error_reasons.add('reason or error_description missing from error, see full response {}'.format(response_json))

    return error_reasons


def should_giveup(e):
    """
    Note: Due to `backoff` expecting a `giveup` parameter, this function returns:

    True - if the exception is NOT retryable
    False - if the exception IS retryable
    """
    response = e.response
    if not _is_json(response):
        # Most retryable errors require a JSON response body
        # If the response is not a json assume it's transient and should retry
        return False

    do_retry = should_retry(response)

    if do_retry:
        error_message = response.json().get("error", {}).get("message")
        if error_message:
            LOGGER.info("Encountered retryable %s, backing off exponentially. Details: %s",
                        response.status_code,
                        error_message)

    return not do_retry

def should_retry(response):
    """
    Ensure certain status code responses trigger retries
    See documentation at https://developers.google.com/analytics/devguides/reporting/core/v4/errors
    """
    if not _is_json(response):
        # Most retryable errors require a JSON response body
        # If the response is not a json assume it's transient and should retry
        return True

    response_error = response.json().get("error", {})

    if isinstance(response_error, dict):
        error_status_title = response_error.get("status", "")
    else:
        # Some responses put a string in the error instead, such as 401
        error_status_title = ""

    return (response.status_code == 429 or
            is_retryable_403(response) or
            (response.status_code == 503 and error_status_title == 'UNAVAILABLE'))

def _is_json(response):
    try:
        response.json()
        return True
    except Exception:
        return False

def _update_config_file(config, config_path):
    with open(config_path, 'w') as config_file:
        json.dump(config, config_file, indent=2)


def is_cached_profile_lookup_valid(config):
    # When cached_profile_lookup is not in config, the cache is invalid
    if "cached_profile_lookup" not in config:
        return False

    view_ids = set(config.get("view_ids") or [config.get("view_id")])
    cached_profile_lookup = json.loads(config["cached_profile_lookup"] or '{}')
    # When view_ids are not all in cached_profile_lookup's top level keys, the cache is invalid
    if len(view_ids - set(cached_profile_lookup.keys())) != 0:
        return False

    # cached_profile_lookup is valid
    return True


# pylint: disable=too-many-instance-attributes
class Client():
    def __init__(self, config, config_path):
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
        self._populate_profile_lookup(config, config_path)


    def _populate_profile_lookup(self, config, config_path):
        """
        Get all profiles available and associate them with their web property
        and account IDs to be looked up later during discovery.
        """
        if is_cached_profile_lookup_valid(config):
            LOGGER.info("Using cached profile_lookup. Will not check Account Summaries API.")
            self.profile_lookup = json.loads(config["cached_profile_lookup"])
            return

        LOGGER.info("Cached profile_lookup does not exist or is invalid. Rebuilding.")
        account_summaries = self.get_account_summaries_for_token()
        view_ids = set(config.get("view_ids") or [config.get("view_id")])
        for account in account_summaries:
            for web_property in account.get('webProperties', []):
                for profile in web_property.get('profiles', []):
                    # Only cache profile ids that are in our chosen view_ids
                    if profile['id'] not in view_ids:
                        continue
                    self.profile_lookup[profile['id']] = {"web_property_id": web_property['id'],
                                                          "account_id": account['id']}

        # After rebuilding the cache, write it back to config so it can be persisted
        config['cached_profile_lookup'] = json.dumps(self.profile_lookup)
        _update_config_file(config, config_path)

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

        token_response = self.session.post("https://oauth2.googleapis.com/token", json=payload)

        token_response.raise_for_status()

        token_json = token_response.json()
        self.__access_token = token_json['access_token']
        self.expires_in = token_json['expires_in']


    # For fewer requests, and reliability. This backoff tries less hard.
    # Backoff Max Time: try 1 (wait 10) 2 (wait 100) 3 (wait 1000) 4
    # Gives us waits of: (10 * 10 ^ 0), (10 * 10 ^ 1), (10 * 10 ^ 2)
    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.RequestException),
                          max_tries=4,
                          base=10,
                          giveup=should_giveup,
                          factor=10,
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

        error_message = _is_json(response) and response.json().get("error", {}).get("message")
        if 400 <= response.status_code < 500 and error_message and not should_retry(response):
            raise Exception("{} Client Error, error message: {}".format(response.status_code, error_message))

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
            cubes_json = json.loads(pkgutil.get_data(__package__, "ga_cubes.json"))
        return cubes_json

    def get_account_summaries_for_token(self):
        """
        Return a list of accountSummaries (full account hierarchy that token
        user has access to) to discover Goals and custom metrics/dimensions.
        """
        account_summaries_response = self.get('https://www.googleapis.com/analytics/v3/management/accountSummaries')
        return account_summaries_response.json()['items']

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
