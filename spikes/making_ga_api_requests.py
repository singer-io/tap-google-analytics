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

    def _make_request(self, method, url, params=None, data=None):
        params = params or {}
        data = data or {}
        
        self._ensure_access_token()

        headers = {"Authorization" : "Bearer " + self.__access_token}
        if self.quota_user:
            params["quotaUser"] = self.quota_user
        if self.user_agent:
            headers["User-Agent"] = self.user_agent
            
        response = requests.request(method, url, headers=headers, params=params, data=data)
        response.raise_for_status()

        return response

    def get(self, url, params=None):
        return self._make_request("GET", url, params=params)

    def post(self, url, data=None):
        return self._make_request("POST", url, data=data)

    # Discovery requests
    


    # Sync Requests w/ Pagination and token refresh

    
config = {
    "refresh_token": os.getenv("GA_SPIKE_REFRESH_TOKEN"),
    "client_id": os.getenv("GA_SPIKE_CLIENT_ID"),
    "client_secret": os.getenv("GA_SPIKE_CLIENT_SECRET"),
    "user_agent": "Stitch Tap Spike (+support@stitchdata.com)",
    "quota_user": "spike_user"
}

client = Client(config)
accounts_response = client.get('https://www.googleapis.com/analytics/v3/management/accounts')
