# Google Analytics Making GA API Requests

##############################################################
# Goals:                                                     #
# Initial Authentication (Refresh_Token to get Access_Token) #
# Identify All Requests That Will Be Made (Discovery)        #
# Consolidate them into a single "Client" class              #
# Understand How Sync Queries Will Work                      #
# - Pagination, Token Refresh                                #
#                                                            #
# Expected Result:                                           #
# A draft of a Client class that can be used for the tap     #
##############################################################

import os
import requests
import singer
from singer import utils

LOGGER = singer.get_logger()

# For your viewing pleasure
from pprint import pprint

class Client():
    def __init__(self, config):
        self.refresh_token = config["refresh_token"]
        self.client_id = config["client_id"]
        self.client_secret = config["client_secret"]

        self.__access_token = None
        self.expires_in = 0
        self.last_refreshed = None

        self.quota_user = config.get("quota_user")
        self.user_agent = config.get("user_agent")

    # Authentication and refresh
    def _ensure_access_token(self):
        if self.last_refreshed is not None and \
           (utils.now() - self.last_refreshed).total_seconds() < self.expires_in:
            return

        LOGGER.info("Refreshing access token.")
        self.last_refreshed = utils.now()

        payload = {
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token"
        }
        token_response = requests.post("https://oauth2.googleapis.com/token", data=payload)

        token_response.raise_for_status()

        token_json = token_response.json()
        self.__access_token = token_json['access_token']
        self.expires_in = token_json['expires_in']

    def _is_json(self, response):
        try:
            response.json()
            return True
        except:
            return False

    def _make_request(self, method, url, params=None, data=None):
        params = params or {}
        data = data or {}

        self._ensure_access_token()

        headers = {"Authorization" : "Bearer " + self.__access_token}
        if self.quota_user:
            params["quotaUser"] = self.quota_user
        if self.user_agent:
            headers["User-Agent"] = self.user_agent

        if method == 'POST':
            response = requests.post(url, headers=headers, params=params, json=data)
        else:
            response = requests.request(method, url, headers=headers, params=params)

        error_message = self._is_json(response) and response.json().get("error", {}).get("message")
        if response.status_code == 400 and error_message:
            raise Exception("400 Client Error: Bad Request, details: {}".format(error_message))

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

    def get_raw_field_exclusions(self):
        cubes_response = self.get("https://ga-dev-tools.appspot.com/ga_cubes.json")
        return cubes_response.json()

    def get_accounts_for_token(self):
        """ Return a list of account IDs available to hte associated token. """
        accounts_response = self.get('https://www.googleapis.com/analytics/v3/management/accounts')
        account_ids = [i['id'] for i in accounts_response.json()['items']]
        return account_ids

    def get_web_properties_for_account(self, account_id):
        """ Return a list of webproperty IDs for the account specified. """
        webprops_response = self.get('https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties'.format(accountId=account_id))
        webprops_ids = [w['id'] for w in webprops_response.json()['items']]
        #TODO: should we add logic to skip deactivated webprops?
        return webprops_ids

    def get_profiles_for_property(self,  account_id, web_property_id):
        """
        Gets all profiles for property to associate with custom metrics and dimensions.
        """
        profiles_url = 'https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/profiles'
        profiles_response = self.get(profiles_url.format(accountId=account_id,
                                                             webPropertyId=web_property_id))
        return [p["id"] for p in profiles_response.json()['items']]

    def get_goals_for_profile(self, account_id, web_property_id, profile_id):
        """
        Gets all profiles for property to associate with custom metrics and dimensions.
        """
        profiles_url = 'https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/profiles/{profileId}/goals'
        goals_response = self.get(profiles_url.format(accountId=account_id,
                                                          webPropertyId=web_property_id,
                                                          profileId=profile_id))
        return [g["id"] for g in goals_response.json()['items']]

    def get_custom_metrics(self, account_id, web_property_id):
        """
        Gets all metrics for the specified web_property_id.
        """
        metrics_fields = {"id", "name", "kind", "active", "min_value", "max_value"}
        metrics_url = 'https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/customMetrics'

        custom_metrics_response = self.get(metrics_url.format(accountId=account_id,
                                                                  webPropertyId=web_property_id))
        profiles = self.get_profiles_for_property(account_id, web_property_id)
        metrics_field_infos = [{"account_id": account_id,
                                "web_property_id": web_property_id,
                                "profiles": profiles,
                                "type": "METRIC",
                                "dataType": item["type"],
                                **{k:v for k,v in item.items() if k in metrics_fields}}
                               for item in custom_metrics_response.json()['items']]

        return metrics_field_infos

    def get_custom_dimensions(self, account_id, web_property_id):
        """
        Gets all dimensions for the specified web_property_id
        """
        dimensions_fields = {"id", "name", "kind", "active"}
        url = 'https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/customDimensions'
        custom_dimensions_response = self.get(url.format(accountId=account_id,
                                                             webPropertyId=web_property_id))
        profiles = self.get_profiles_for_property(account_id, web_property_id)
        # NOTE: Assuming that all custom dimensions are STRING, since there's no type information
        dimensions_field_infos = [{"dataType": "STRING",
                                   "account_id": account_id,
                                   "web_property_id": web_property_id,
                                   "profiles": profiles,
                                   "type": "DIMENSION",
                                   **{k:v for k,v in item.items() if k in dimensions_fields}}
                                  for item in custom_dimensions_response.json()['items']]

        return dimensions_field_infos


    # Sync Requests w/ Pagination and token refresh
    # Docs for more info: https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet
    def get_report(self, profile_id, report_date, metrics, dimensions):
        """
        Parameters:
        - profile_id - the profile for which this report is being run
        - report_date - the day to retrieve data for, in YYYY-MM-DD format, to limit report data
        - metrics - list of metrics, of the form ["ga:metric1", "ga:metric2", ...]
        - dimensions - list of dimensions, of the form ["ga:dim1", "ga:dim2", ...]

        Returns:
        - A generator of a sequence of reports w/ associated metadata (metrics/dims/report_date/profile)
        """
        nextPageToken = None
        while True:
            body = {"reportRequests":
                    [{"viewId": profile_id,
                      "dateRanges": [{"startDate": report_date,
                                      "endDate": report_date}],
                      "metrics": [{"expression": m} for m in metrics],
                      "dimensions": [{"name": d} for d in dimensions]}]}
            if nextPageToken:
                body["reportRequests"][0]["pageToken"] = nextPageToken
            report_response = self.post('https://analyticsreporting.googleapis.com/v4/reports:batchGet', body)
            report = report_response.json()

            # Assoc in the request data to be used by the caller
            report.update({"profileId": profile_id,
                           "reportDate": report_date,
                           "metrics": metrics,
                           "dimensions": dimensions})

            yield report

            # NB: Assumes only one report at a time
            nextPageToken = report["reports"][0].get("nextPageToken")

            if not nextPageToken:
                break

config = {
    "refresh_token": os.getenv("GA_SPIKE_REFRESH_TOKEN"),
    "client_id": os.getenv("GA_SPIKE_CLIENT_ID"),
    "client_secret": os.getenv("GA_SPIKE_CLIENT_SECRET"),
    "user_agent": "Stitch Tap Spike (+support@stitchdata.com)",
    "quota_user": "spike_user"
}

client = Client(config)
accounts_response = client.get('https://www.googleapis.com/analytics/v3/management/accounts')
try:
    report = client.get_report(os.getenv("GA_SPIKE_VIEW_ID"), "2019-11-08", ["ga:users", "ga:newUsers", "ga:sessionsPerUser"], ["ga:userType", "ga:userBucket", "ga:sessionDurationBucket", "ga:hostname", "ga:pagePath", "ga:source", "ga:browser", "ga:latitude", "ga:minute"])
except Exception as ex:
    import ipdb; ipdb.set_trace()
    1+1
