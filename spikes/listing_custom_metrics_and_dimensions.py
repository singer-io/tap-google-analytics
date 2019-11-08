from pprint import pprint
import requests
import json
import os
#########################################################################
# Outstanding result of discovery spike. Can we discover custom metrics #
# and dimensions and provide them in the catalog?                       #
#########################################################################

# 1. Get all accounts
# https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/accounts/list
refresh_token = os.getenv("GA_SPIKE_REFRESH_TOKEN")
client_id = os.getenv("GA_SPIKE_CLIENT_ID")
client_secret = os.getenv("GA_SPIKE_CLIENT_SECRET")
payload = {
    "refresh_token": refresh_token,
    "client_id": client_id,
    "client_secret": client_secret,
    "grant_type": "refresh_token"
}
token_response = requests.post("https://oauth2.googleapis.com/token", data=payload)

token_json = token_response.json()

access_token = token_json['access_token']

headers = {'Authorization' : 'Bearer ' + access_token}

quota_user = "my_spike_user"

accounts_response = requests.get('https://www.googleapis.com/analytics/v3/management/accounts',
                        headers=headers,
                        params={"quotaUser": quota_user})

# To get the view id, grab accounts_response['items'][0]['id']

# However, for the tap, the account(s) will be provided under `view_ids` in the config
view_ids = [os.getenv("GA_SPIKE_VIEW_ID")]

# 1a. Get all web properties per account
# https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/webproperties/list

webprops_response = requests.get('https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties'.format(accountId=view_ids[0]),
                                 headers=headers,
                                 params={"quotaUser": quota_user})

webprops_ids = [w['id'] for w in webprops_response.json()['items']]

example_account = {'accountId': '1234567',
                   'childLink': {'href': 'https://www.googleapis.com/analytics/v3/management/accounts/1234567/webproperties/UA-1234567-16/profiles',
                                 'type': 'analytics#profiles'},
                   'created': '2017-09-22T19:15:07.883Z',
                   'dataRetentionResetOnNewActivity': True,
                   'dataRetentionTtl': 'MONTHS_26',
                   'defaultProfileId': '76543212',
                   'id': 'UA-1234567-16',
                   'industryVertical': 'REFERENCE',
                   'internalWebPropertyId': '09876543',
                   'kind': 'analytics#webproperty',
                   'level': 'STANDARD',
                   'name': 'MyWebProperty',
                   'parentLink': {'href': 'https://www.googleapis.com/analytics/v3/management/accounts/1234567',
                                  'type': 'analytics#account'},
                   'permissions': {'effective': ['COLLABORATE',
                                                 'EDIT',
                                                 'READ_AND_ANALYZE']}}




# 2. Get all custom metrics for account
# https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/customMetrics/list

# 3. Get all custom dimensions for account
# https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/customDimensions/list

custom_dimensions_response = requests.get('https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/customDimensions'.format(accountId=view_ids[0],
                                                                                                                                                                          webPropertyId=webprops_ids[1]),
                                          headers=headers,
                                          params={"quotaUser": quota_user})

                   
# 4. Profit! Build out catalog values for custom metrics/dimensions
