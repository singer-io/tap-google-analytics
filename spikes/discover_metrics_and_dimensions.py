# Google Analytics Metrics and Dimensions Discovery

############################################################################################
# Research:                                                                                #
# - Looks like the Metadata API is the source of this information                          #
#                                                                                          #
# Here is the metadata API reference documentation, it appears to only be                  #
# available in v3, no indication of either deprecating it from v3 (they                    #
# actually explicitly state they will maintain it for now):                                #
#                                                                                          #
# Overview -                                                                               #
# https://developers.google.com/analytics/devguides/reporting/metadata/v3/                 #
# Developer Guide -                                                                        #
# https://developers.google.com/analytics/devguides/reporting/metadata/v3/devguide         #
# API Reference -                                                                          #
# https://developers.google.com/analytics/devguides/reporting/metadata/v3/reference        #
#                                                                                          #
# Questions:                                                                               #
# - Can we discover field exclusion rules without running a report?                        #
#    - This metrics and dimensions explorer uses this endpoint to discover exclusion rules #
#        - Explorer - https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/        #
#        - XHR Request To - https://ga-dev-tools.appspot.com/ga_cubes.json                 #
# - Shape of the response of metadata?                                                     #
# - Data typing of fields?                                                                 #
#     - What is the `TIME` datatype? HH:MM:ss.mmmmmmm?                                     #
############################################################################################


# Sample Code (Python)
# - Run using `python -i spikes/discover_metrics_and_dimensions.py`
#```

import requests
import os
import re

# For your viewing pleasure
from pprint import pprint

#######################
# Get an access token #
#######################

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

access_token = token_json["access_token"]
expiration = token_json["expires_in"] # Remaining lifetime of this token in seconds

###################################
# Make request to v3 API Metadata #
###################################

# We want to separate requests per user for proper quota tracking
quota_user = "my_spike_user"

# `reportType` is documented to only have the value of ga. Looks like
# gold-plating, but to communicate, it's included as a parameter.
# https://developers.google.com/analytics/devguides/reporting/metadata/v3/reference/metadata/columns/list
metadata_response = requests.get("https://www.googleapis.com/analytics/v3/metadata/{reportType}/columns".format(reportType="ga"),
                                 params={"quotaUser": quota_user})

metadata_json = metadata_response.json()

total_fields = metadata_json["totalResults"]
fields = metadata_json["items"]
example_field = {'attributes': {'addedInApiVersion': '3',
                                'allowedInSegments': 'true',
                                'dataType': 'STRING',
                                'description': 'A boolean, either New Visitor or Returning '
                                'Visitor, indicating if the users are new or '
                                'returning.',
                                'group': 'User',
                                'status': 'PUBLIC', # This can also be 'DEPRECATED', should be removed from discovery if so
                                'type': 'DIMENSION',
                                'uiName': 'User Type'},
                 'id': 'ga:userType',
                 'kind': 'analytics#column'}

#####################################
# Parse output and answer questions #
#####################################

def transform_field(field):
    interesting_attributes = {k: v for k, v in field["attributes"].items()
                              if k in {"dataType", "group", "status", "type", "uiName"}}
    # TODO: It seems like some dimensions can be typed as an integer or float, so we might want to do that when generating schemas.
    # - They may come through here as "string". This should be validated when building discovery
    return {"id": field["id"], **interesting_attributes}

def get_field_infos():
    metadata_response = requests.get("https://www.googleapis.com/analytics/v3/metadata/{reportType}/columns".format(reportType="ga"),
                                     params={"quotaUser": quota_user})
    # TODO: Error handling
    return [transform_field(f) for f in metadata_response.json()["items"]]

field_infos = get_field_infos()

# >>> len([v for v in field_infos if v["type"] == "DIMENSION"])
# 273
# >>> len([v for v in field_infos if v["type"] == "METRIC"])
# 262
# >>> len(field_infos)
# 535

# >>> pprint(set([v["group"] for _, v in field_infos.items() ]))
all_groups = {'Ad Exchange',
              'Adsense',
              'Adwords',
              'App Tracking',
              'Audience',
              'Channel Grouping',
              'Content Experiments',
              'Content Grouping',
              'Custom Variables or Columns',
              'DoubleClick Bid Manager',
              'DoubleClick Campaign Manager',
              'DoubleClick Search',
              'DoubleClick for Publishers',
              'DoubleClick for Publishers Backfill',
              'Ecommerce',
              'Event Tracking',
              'Exceptions',
              'Geo Network',
              'Goal Conversions',
              'Internal Search',
              'Lifetime Value and Cohorts',
              'Page Tracking',
              'Platform or Device',
              'Publisher',
              'Session',
              'Site Speed',
              'Social Activities',
              'Social Interactions',
              'System',
              'Time',
              'Traffic Sources',
              'User',
              'User Timings'}

# >>> pprint(set([v["status"] for _, v in field_infos.items() ]))
possible_statuses = {'DEPRECATED', 'PUBLIC'}

# ?????????? Custom Fields?
# >>> [k for k, v in field_infos.items() if v["group"] == "Custom Variables or Columns"]
custom_ELLIPSIS_variables_QMARK = ['ga:customVarValueXX', 'ga:calcMetric_<NAME>', 'ga:metricXX', 'ga:customVarNameXX', 'ga:dimensionXX']

##################################################################
# Grab list of field exclusions and transform them into metadata #
##################################################################

# This file appears to be organized by some kind of functional grouping,
# with fields that CAN occur together under each grouping. The fields that
# a particular field CAN'T be selected with are under the set difference.
cubes_response = requests.get("https://ga-dev-tools.appspot.com/ga_cubes.json")
cubes = cubes_response.json()

# Processing this file for exclusion rules

# cubes' shape
# key = 'per_active_visitors_day_active_visitors_30'
# value = {'ga:30dayUsers': 1,
#          'ga:CTR': 1,
#          'ga:RPC': 1,
#          'ga:adClicks': 1,
#          'ga:adsenseAdUnitsViewed': 1,
#          'ga:adsenseAdsClicks': 1,
#          'ga:adsenseAdsViewed': 1,
#          'ga:adsenseCTR': 1,
#          'ga:adsenseCoverage': 1,
#          ... }

# Get all fields
# >>> set_of_fields = set([e for value in cubes.values() for e in value.keys()])

# Get all fields that occur with `ga:transactionId`
# >>> set_of_fields_with_transaction_id = set([e for values in cubes.values() for e in values.keys() if "ga:transactionId" in values.keys()])

# This should be the set of fields that can't be selected with `ga:transactionId`
# >>> set_of_fields - set_of_fields_with_transaction_id

# NOTE: Every value in this file currently is `1`, but it might be a
# future proofing feature that it's structured like `{"ga:name": 1}` in
# order to mark fields as incompatible after the fact.
# >>> set([e for value in cubes.values() for e in value.values()])
# {1}

def get_all_fields_available(cubes):
    """
    Converts the ga_cubes.json response into a set of all available fields.
    """
    return {e for value in cubes.values() for e in value.keys()}

def get_field_exclusions_for(field, cubes, all_fields):
    """
    Returns the set of all fields that never occur with the specified
    `field` in the "ga_cubes" dataset.
    """
    fields_available_with_field = {e for values in cubes.values() for e in values.keys() if field in values.keys()}
    return all_fields - fields_available_with_field

all_fields = get_all_fields_available(cubes)
all_exclusions = {f: get_field_exclusions_for(f, cubes, all_fields) for f in all_fields}

def generate_exclusions_lookup():
    """
    Generates a map of {field_id: exclusions_list} for use in generating
    tap metadata for the catalog.
    """
    # TODO: When applying this map, you need to expand non-custom grouped
    # GA metadata fields into their numeric counterparts from the
    # `ga_cubes` data set
    # - Some `XX` fields (goals and custom dims/mets) are in their `XX` form in this file
    cubes = requests.get("https://ga-dev-tools.appspot.com/ga_cubes.json").json()
    all_fields = get_all_fields_available(cubes)
    return {f: get_field_exclusions_for(f, cubes, all_fields) for f in all_fields}


#######################################################################
# Validation                                                          #
# Checking that the fields that exist in the `ga_cubes` set match the #
# fields that exist in the `Metadata API`                             #
#######################################################################
symm_diff = {fi["id"] for fi in field_infos}.symmetric_difference(all_fields)

# Spoiler Alert: It does not match, on either side. Reasons:

in_cube_not_in_metadata = {'ga:productCategoryLevel5', 'ga:previousContentGroup1', 'ga:contentGroup1', 'ga:adwordsCustomerName', 'ga:chanceToBeatOriginal', 'ga:nextContentGroup1', 'ga:landingContentGroup4', 'ga:contentGroupUniqueViews5', 'ga:externalActivityId', 'ga:experimentOutcomes', 'ga:contentGroupUniqueViews1', 'ga:dbmLastEventCreativeName', 'ga:contentGroupUniqueViews3', 'ga:experimentStarts', 'ga:contentGroup5', 'ga:landingContentGroup5', 'ga:nextContentGroup5', 'ga:contentGroup4', 'ga:nextPagePath', 'ga:entranceBounceRate', 'ga:adSlotPosition', 'ga:dcmROI', 'ga:landingContentGroup1', 'ga:contentGroup3', 'ga:isMobile', 'ga:trafficType', 'ga:isTablet', 'ga:previousContentGroup2', 'ga:compareToOriginal', 'ga:nextContentGroup3', 'ga:nextContentGroup2', 'ga:experimentOutcomeType', 'ga:dbmClickCreativeName', 'ga:contentGroupUniqueViews2', 'ga:socialInteractionNetworkActionSession', 'ga:previousContentGroup5', 'ga:nextContentGroup4', 'ga:productCategoryLevel3', 'ga:productCategoryLevel1', 'ga:previousContentGroup3', 'ga:productCategoryLevel4', 'ga:contentGroupUniqueViews4', 'ga:productCategoryLevel2', 'ga:previousPageLinkId', 'ga:landingContentGroup3', 'ga:previousContentGroup4', 'ga:landingContentGroup2', 'ga:clientId', 'ga:contentGroup2'}

in_metadata_not_in_cube = {'ga:landingContentGroupXX', 'ga:uniqueAppviews', 'ga:percentNewVisits', 'ga:newVisits', 'ga:socialInteractionsPerVisit', 'ga:visitorType', 'ga:contentGroupXX', 'ga:visitBounceRate', 'ga:visitorGender', 'ga:transactionRevenuePerVisit', 'ga:visitsToTransaction', 'ga:transactionsPerVisit', 'ga:pageviewsPerVisit', 'ga:visitCount', 'ga:goalValuePerVisit', 'ga:previousContentGroupXX', 'ga:visits', 'ga:visitLength', 'ga:visitorAgeBracket', 'ga:productCategoryLevelXX', 'ga:percentVisitsWithSearch', 'ga:searchVisits', 'ga:visitsWithEvent', 'ga:contentGroupUniqueViewsXX', 'ga:eventsPerVisitWithEvent', 'ga:visitors'}

# FIELDS THAT ARE IN METADATA AND NOT IN CUBE
# 1. Some of these have numeric endings, where in the metadata, they are named with a suffix of `XX`
#    - These will need to be handled in the fieldExclusion metadata generation

# 2. Of those that do not have numeric endings, ALL are marked as "DEPRECATED"
# >>> set(filter(lambda f: not re.search("XX$", f), set(field_infos.keys()) - all_fields))
# {'ga:visitsToTransaction', 'ga:visitBounceRate', 'ga:transactionsPerVisit', 'ga:visitorGender', 'ga:visits', 'ga:eventsPerVisitWithEvent', 'ga:visitLength', 'ga:visitorAgeBracket', 'ga:pageviewsPerVisit', 'ga:searchVisits', 'ga:percentVisitsWithSearch', 'ga:uniqueAppviews', 'ga:visitCount', 'ga:percentNewVisits', 'ga:transactionRevenuePerVisit', 'ga:goalValuePerVisit', 'ga:visitsWithEvent', 'ga:newVisits', 'ga:socialInteractionsPerVisit', 'ga:visitorType', 'ga:visitors'}
no_numeric_end = set(filter(lambda f: not re.search("XX$", f), in_metadata_not_in_cube))
{f["attributes"]["status"] for f in fields if f["id"] in no_numeric_end}
# {'DEPRECATED'}

# FIELDS THAT ARE IN CUBE BUT NOT IN METADATA
# Filter out numeric
#>>> set(filter(lambda f: not re.search("\d+$", f), all_fields - set(field_infos.keys())))
no_numeric_end_in_cube = {'ga:experimentOutcomes', 'ga:adwordsCustomerName', 'ga:isMobile', 'ga:trafficType', 'ga:isTablet', 'ga:chanceToBeatOriginal', 'ga:dbmLastEventCreativeName', 'ga:socialInteractionNetworkActionSession', 'ga:compareToOriginal', 'ga:experimentStarts', 'ga:previousPageLinkId', 'ga:nextPagePath', 'ga:experimentOutcomeType', 'ga:entranceBounceRate', 'ga:clientId', 'ga:dbmClickCreativeName', 'ga:externalActivityId', 'ga:adSlotPosition', 'ga:dcmROI'}

# We can still generate a metadata lookup for these fields, and not worry
# about if they exist in the GA Metadata API, if they are requested or are
# returned in the future, they would just work

##############################################################################
# How to Handle 'XX' in Field Names                                          #
# - Field names may contain 'XX' in the metadata API response to mean        #
#   `\d+` at any point in their actual field name, how do we generate these? #
##############################################################################

# We think that those that are in the `ga_cubes` dataset are standard
# fields that just always have a number. We should provide these for field
# selection based on their "\d+$" name and generate exclusions based on
# their "XX$" name

# CUSTOM VARIABLE OR COLUMNS
# These are the fields that would need to be designed, returned from Metadata API under the group "Custom Variables or Columns"
custom_ELLIPSIS_variables_QMARK = ['ga:customVarValueXX', 'ga:calcMetric_<NAME>', 'ga:metricXX', 'ga:customVarNameXX', 'ga:dimensionXX']

# TODO: To Design - Dynamic discovery of these custom fields. Anything
# with an `XX` in it appears to be a custom metric/dimension that will
# have an actual numeric value depending on the one the user is referring
# to.
# - Research on discovering them through the API in `spikes/listing_cutsom_metrics_and_dimensions.py`


# ============

# POTENTIAL FEATURE: It looks like the ga_cubes.json file is organized
# into groups of metrics/dimensions that work with each other. These might
# be useful to get as a set of default reports? Could be a good QoL
# improvement for people just wanting to get quick insights.

# Example:
  # "per_cost_data_import": {
  #   "ga:adGroup": 1,
  #   "ga:referralPath": 1,
  #   "ga:adwordsCampaignID": 1,
  #   "ga:CPC": 1,
  #   "ga:adDisplayUrl": 1,
  #   "ga:adwordsCriteriaID": 1,
  #   "ga:impressions": 1,
  #   "ga:adSlotPosition": 1,
  #   "ga:adContent": 1,
  #   "ga:source": 1,
  #   "ga:campaign": 1,
  #   "ga:adSlot": 1,
  #   "ga:CTR": 1,
  #   "ga:CPM": 1,
  #   "ga:medium": 1,
  #   "ga:adDestinationUrl": 1,
  #   "ga:keyword": 1,
  #   "ga:adCost": 1,
  #   "ga:adClicks": 1
  # }

#```
