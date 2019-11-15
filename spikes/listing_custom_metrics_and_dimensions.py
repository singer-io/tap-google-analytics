from pprint import pprint
import requests
import json
import os

#########################################################################
# Outstanding result of discovery spike. Can we discover custom metrics #
# and dimensions and provide them in the catalog?                       #
#########################################################################

# Pre Setup: Get an access token
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

# 1. Get all accounts
# https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/accounts/list
headers = {'Authorization' : 'Bearer ' + access_token}

quota_user = "my_spike_user"

accounts_response = requests.get('https://www.googleapis.com/analytics/v3/management/accounts',
                        headers=headers,
                        params={"quotaUser": quota_user})

# To get the account id, grab accounts_response.json()['items'][0]['id']

# Me:
#   So... account_id != view_id?

# Them:
#   correct
#   there is:
#   account > property > view

account_ids = [a['id'] for a in accounts_response.json()['items']]

# 1a. Get all web properties per account
# https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/webproperties/list

webprops_response = requests.get('https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties'.format(accountId=account_ids[0]),
                                 headers=headers,
                                 params={"quotaUser": quota_user})

example_webprop = {'accountId': '1234567',
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

# NOTE: This can fail if the user doesn't have enough permissions.
# We should probably log a clear warning and just move on to skip
# discovering custom things for this view_id

# >>> webprops_response.json()

webprops_auth_error = {'error': {'errors': [{'message': 'User does not have sufficient permissions for this account.', 'reason': 'insufficientPermissions', 'domain': 'global'}], 'code': 403, 'message': 'User does not have sufficient permissions for this account.'}}

def has_insufficient_permission(response):
    return any([e for e in response.json().get("error", {}).get("errors", [])
                if e.get("reason") == "insufficientPermissions"])

if webprops_response.status_code == 403 and has_insufficient_permission(webprops_response):
    print("User has insufficient permissions to list WebProps for AccountID {}. Not discovering custom Metrics and Dimensions for this Account.".format(account_ids[0]))

webprops_ids = [w['id'] for w in webprops_response.json()['items']]


# 2. Get all custom metrics for account
# https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/customMetrics/list

custom_metrics_response = requests.get('https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/customMetrics'.format(accountId=account_ids[0],
                                                                                                                                                                    webPropertyId=webprops_ids[1]), # The second webprop has our custom dims and mets
                                       headers=headers,
                                       params={"quotaUser": quota_user})

# NOTE: These probably can also fail based on permissions, this should be a soft warning

# >>> pprint(custom_metrics_response.json())
example_custom_metrics_response = {'items': [{'accountId': '1234567',
                                              'active': True,
                                              'created': '2019-11-08T20:05:22.072Z',
                                              'id': 'ga:metric1',
                                              'index': 1,
                                              'kind': 'analytics#customMetric',
                                              'max_value': '25',
                                              'min_value': '0',
                                              'name': 'custom_hit_met',
                                              'parentLink': {'href': 'https://www.googleapis.com/analytics/v3/management/accounts/1234567/webproperties/UA-1234567-7',
                                                             'type': 'analytics#webproperty'},
                                              'scope': 'HIT',
                                              'selfLink': 'https://www.googleapis.com/analytics/v3/management/accounts/1234567/webproperties/UA-1234567-7/customMetrics/ga:metric1',
                                              'type': 'CURRENCY',
                                              'updated': '2019-11-08T20:05:22.072Z',
                                              'webPropertyId': 'UA-1234567-7'}],
                                   'itemsPerPage': 1000,
                                   'kind': 'analytics#customMetrics',
                                   'startIndex': 1,
                                   'totalResults': 1,
                                   'username': 'foo@thing.com'}


# 3. Get all custom dimensions for account
# https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/customDimensions/list

custom_dimensions_response = requests.get('https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/customDimensions'.format(accountId=account_ids[0],
                                                                                                                                                                          webPropertyId=webprops_ids[1]), # The second webprop has our custom dims and mets
                                          headers=headers,
                                          params={"quotaUser": quota_user})

# NOTE: These probably can also fail based on permissions, this should be a soft warning

# >>> pprint(custom_dimensions_response.json())
example_custom_dim_response = {'items': [{'accountId': '1234567',
                                          'active': True,
                                          'created': '2019-11-08T20:03:32.052Z',
                                          'id': 'ga:dimension1',
                                          'index': 1,
                                          'kind': 'analytics#customDimension',
                                          'name': 'custom_hit_dim',
                                          'parentLink': {'href': 'https://www.googleapis.com/analytics/v3/management/accounts/1234567/webproperties/UA-1234567-7',
                                                         'type': 'analytics#webproperty'},
                                          'scope': 'HIT',
                                          'selfLink': 'https://www.googleapis.com/analytics/v3/management/accounts/1234567/webproperties/UA-1234567-7/customDimensions/ga:dimension1',
                                          'updated': '2019-11-08T20:03:32.052Z',
                                          'webPropertyId': 'UA-1234567-7'}],
                               'itemsPerPage': 1000,
                               'kind': 'analytics#customDimensions',
                               'startIndex': 1,
                               'totalResults': 1,
                               'username': 'foo@thing.com'}

# 3a. Investigate how these other types under "Custom Variables or Columns" get created? They will also need discovered
custom_variable_types = ['ga:customVarValueXX', # TODO: This looks like a legacy/deprecated feature of ga.js, need to figure out how to describe. Manual Input?
                         'ga:calcMetric_<NAME>', # TODO: This is a beta feature, cannot find an endpoint to discover. May need to exclude or provide a manual input.
                         'ga:metricXX', # See 2. above for info on this request
                         'ga:customVarNameXX',
                         'ga:dimensionXX' # See 3. above for info on this request
]

# CUSTOM VARIABLES
# According to this documentation: https://developers.google.com/analytics/devguides/collection/gajs/gaTrackingCustomVariables
# It seems like these are declared and managed from Javascript. https://developers.google.com/analytics/devguides/collection/gajs/gaTrackingCustomVariables#setup

# RESULT: If we want to include the fields marked TODO above, we should simply provide a text input to generate metadata to include them, that way a user can specify them. For lack of a discovery endpoint, this seems to be the only way to get them if they are useful.

# 4. Profit! Build out catalog values for custom metrics/dimensions

# CUSTOM METRICS
# In order to build out metadata for a custom metric, we have a bit more available than dims.
# These should give us
# active - Whether or not to build out the schema (TODO: May not want to skip inactive fields, in case they get reactivated? Could cause field selection issues)
# id - The "Google name" of the metric
# kind - Verifying that it is in fact a metric ("analytics#customMetric")
# min/maxValue - In case we want to be strict of value verification (TODO: This might not be desirable, since it's likely to cause issues)
# type - The "Google datatype" of the field (one of: "CURRENCY", "TIME", "INTEGER")
interesting_custom_met_properties = {"id", "name", "kind", "active", "minValue", "maxValue", "type"}

# CUSTOM DIMENSIONS
# In order to build out the metadata for a custom dimension, we can use the keys below where "id" is the `ga:` form, "name" is a user friendly name, "kind" is `analytics#customDimension`
# NOTE: It LOOKS like all dimensions are thought of as strings in GA's eyes. Likely due to their nature as an OLAP-y thing. Hard code string schema.
interesting_custom_dim_properties = {"id", "name", "kind", "active"}

# TODO: Not sure what value, if any, "scope" has in the response
# TODO: Maybe connecting back to the webproperty somehow might be useful, but that's a feature add, if so.
# TODO: Should we skip all non-active dimensions?

def get_access_token():
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

    return token_json['access_token']

def get_accounts_for_token(access_token):
    """ Return a list of account IDs available to hte associated token. """
    accounts_response = requests.get('https://www.googleapis.com/analytics/v3/management/accounts',
                                     headers={'Authorization' : 'Bearer ' + access_token},
                                     params={"quotaUser": quota_user})
    account_ids = [i['id'] for i in accounts_response.json()['items']]
    return account_ids


def get_web_properties_for_account(access_token, account_id):
    """ Return a list of webproperty IDs for the account specified. """
    webprops_response = requests.get('https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties'.format(accountId=account_id),
                                     headers={'Authorization' : 'Bearer ' + access_token},
                                     params={"quotaUser": quota_user})
    webprops_ids = [w['id'] for w in webprops_response.json()['items']]
    #TODO: should we add logic to skip deactivated webprops?
    return webprops_ids

def get_profiles_for_property(access_token, account_id, web_property_id):
    """
    Gets all profiles for property to associate with custom metrics and dimensions.
    """
    profiles_url = 'https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/profiles'
    profiles_response = requests.get(profiles_url.format(accountId=account_id,
                                                         webPropertyId=web_property_id),
                                              headers={'Authorization' : 'Bearer ' + access_token},
                                              params={"quotaUser": quota_user})
    return [p["id"] for p in profiles_response.json()['items']]

def get_goals_for_profile(access_token, account_id, web_property_id, profile_id):
    """
    Gets all profiles for property to associate with custom metrics and dimensions.
    """
    profiles_url = 'https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/profiles/{profileId}/goals'
    goals_response = requests.get(profiles_url.format(accountId=account_id,
                                                         webPropertyId=web_property_id,
                                                         profileId=profile_id),
                                              headers={'Authorization' : 'Bearer ' + access_token},
                                              params={"quotaUser": quota_user})
    return [g["id"] for g in goals_response.json()['items']]

def get_custom_metrics(access_token, account_id, web_property_id):
    """
    Gets all metrics for the specified web_property_id.
    """
    metrics_fields = {"id", "name", "kind", "active", "min_value", "max_value"}
    metrics_url = 'https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/customMetrics'

    custom_metrics_response = requests.get(metrics_url.format(accountId=account_id,
                                                              webPropertyId=web_property_id),
                                           headers={'Authorization' : 'Bearer ' + access_token},
                                           params={"quotaUser": quota_user})
    profiles = get_profiles_for_property(access_token, account_id, web_property_id)
    metrics_field_infos = [{"account_id": account_id,
                            "web_property_id": web_property_id,
                            "profiles": profiles,
                            "type": "METRIC",
                            "dataType": item["type"],
                            **{k:v for k,v in item.items() if k in metrics_fields}}
                           for item in custom_metrics_response.json()['items']]

    return metrics_field_infos

def get_custom_dimensions(access_token, account_id, web_property_id):
    """
    Gets all dimensions for the specified web_property_id
    """
    dimensions_fields = {"id", "name", "kind", "active"}
    url = 'https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/customDimensions'
    custom_dimensions_response = requests.get(url.format(accountId=account_id,
                                                         webPropertyId=web_property_id),
                                              headers={'Authorization' : 'Bearer ' + access_token},
                                              params={"quotaUser": quota_user})
    profiles = get_profiles_for_property(access_token, account_id, web_property_id)
    # NOTE: Assuming that all custom dimensions are STRING, since there's no type information
    dimensions_field_infos = [{"dataType": "STRING",
                               "account_id": account_id,
                               "web_property_id": web_property_id,
                               "profiles": profiles,
                               "type": "DIMENSION",
                               **{k:v for k,v in item.items() if k in dimensions_fields}}
                              for item in custom_dimensions_response.json()['items']]

    return dimensions_field_infos

custom_metrics_and_dimensions = []
for account_id in get_accounts_for_token(access_token):
    for web_property_id in get_web_properties_for_account(access_token, account_id):
        custom_metrics_and_dimensions.extend(get_custom_dimensions(access_token, account_id, web_property_id))
        custom_metrics_and_dimensions.extend(get_custom_metrics(access_token, account_id, web_property_id))

# Now custom_metrics_and_dimensions has all customs that are defined

# ================

# TODO: TO DESIGN - Metrics and Dimensions are defined per view (profile).
# If we allow multiple profile selection, then the field selection
# (catalog entries) need to be generated in a way that allows users to
# only select custom metrics and dimensions that are associated with the
# specific view that they're reporting on.

# Hierarchy: account -> webproperty -> view (profile)

# PROPOSED SOLUTION: Track view where custom dimension/metric is defined
# along with the field using custom metadata. Allow the user to select all
# dims/mets that they want, but at runtime, check if the view that you're
# running for has that dimension/metric defined and, if not, log a WARNING
# level message and remove it from the report.
