# Google Analytics Json Schema Generation

############################################################################################
# Goals:                                                                                   #
# Research into the specific data types that will be needed for the json schema            #
# Generate metadata from 'field_infos' (field_exclusions, profile_id, metric or dimension) #
#                                                                                          #
# Expected Result:                                                                         #
# Have rough draft of code that generates a catalog, given field_infos                     #
############################################################################################

import re
import listing_custom_metrics_and_dimensions as listing
import discover_metrics_and_dimensions as discover

standard_fields = discover.field_infos
custom_fields = listing.custom_metrics_and_dimensions

# What data types exist?
# ipdb> {i['dataType'] for i in standard_fields}
standard_fields_data_types = {'CURRENCY', 'STRING', 'PERCENT', 'TIME', 'INTEGER', 'FLOAT'}

# ipdb> {i['type'] for i in custom_fields}
custom_fields_data_types = {'CURRENCY', 'INTEGER', 'STRING', 'TIME'}

# How should we translate them?

def type_to_schema(ga_type):
    if ga_type == 'CURRENCY':
        # https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters#ecomm
        return {"type": ["number", "null"]}
    elif ga_type == 'STRING':
        # TODO: What about date-time fields?
        return {"type": ["string", "null"]}
    elif ga_type == 'PERCENT':
        # TODO: Unclear whether these come back as "0.25%" or just "0.25"
        return {"type": ["number", "null"]}
    elif ga_type == 'TIME':
        return {"type": ["string", "null"]}
    elif ga_type == 'INTEGER':
        return {"type": ["integer", "null"]}
    elif ga_type == 'FLOAT':
        return {"type": ["number", "null"]}
    else:
        raise Exception("Unknown Google Analytics type: {}".format(ga_type))

# TODO: Trim the `ga:` here?
# TODO: Do we need to generate the `XX` fields schemas here somehow? e.g., 'ga:productCategoryLevel5' vs. 'ga:productCategoryLevelXX'
# - The numeric versions are in `ga_cubes.json`
field_schemas = {**{f["id"]: type_to_schema(f["dataType"]) for f in standard_fields},
                 **{f["id"]: type_to_schema(f["type"]) for f in custom_fields}}

# These are data types that we traditionally have used, compare them with those discovered
known_dimension_types = {"ga:start-date": "DATETIME",
                         "ga:end-date": "DATETIME",
                         "ga:cohortNthWeek": "INTEGER",
                         "ga:nthDay": "INTEGER",
                         "ga:cohortNthDay": "INTEGER",
                         "ga:screenDepth": "INTEGER",
                         "ga:latitude": "FLOAT",
                         "ga:visitCount": "INTEGER",
                         "ga:visitsToTransaction": "INTEGER",
                         "ga:daysSinceLastSession": "INTEGER",
                         "ga:sessionsToTransaction": "INTEGER",
                         "ga:longitude": "FLOAT",
                         "ga:nthMonth": "INTEGER",
                         "ga:nthHour": "INTEGER",
                         "ga:subContinentCode": "INTEGER",
                         "ga:pageDepth": "INTEGER",
                         "ga:nthWeek": "INTEGER",
                         "ga:daysSinceLastVisit": "INTEGER",
                         "ga:sessionCount": "INTEGER",
                         "ga:nthMinute": "INTEGER",
                         "ga:dateHour": "DATETIME",
                         "ga:date": "DATETIME",
                         "ga:daysToTransaction": "INTEGER",
                         "ga:cohortNthMonth": "INTEGER",
                         "ga:visitLength": "INTEGER"}

known_metric_types = {"ga:transactionsPerSession": "FLOAT",
                      "ga:localTransactionRevenue": "FLOAT",
                      "ga:internalPromotionViews": "INTEGER",
                      "ga:uniqueSocialInteractions": "INTEGER",
                      "ga:domainLookupTime": "INTEGER",
                      "ga:adxImpressions": "INTEGER",
                      "ga:socialInteractionsPerSession": "FLOAT",
                      "ga:adxCoverage": "FLOAT",
                      "ga:goalValuePerSession": "FLOAT",
                      "ga:eventsPerSessionWithEvent": "FLOAT",
                      "ga:visitsWithEvent": "INTEGER",
                      "ga:productRemovesFromCart": "INTEGER",
                      "ga:percentSearchRefinements": "FLOAT",
                      "ga:serverResponseTime": "INTEGER",
                      "ga:newVisits": "INTEGER",
                      "ga:organicSearches": "INTEGER",
                      "ga:goalValuePerVisit": "FLOAT",
                      "ga:14dayUsers": "INTEGER",
                      "ga:adsenseAdUnitsViewed": "INTEGER",
                      "ga:totalValue": "FLOAT",
                      "ga:quantityAddedToCart": "INTEGER",
                      "ga:cohortRevenuePerUser": "FLOAT",
                      "ga:cohortActiveUsers": "INTEGER",
                      "ga:CTR": "FLOAT",
                      "ga:adsenseCoverage": "FLOAT",
                      "ga:searchUniques": "INTEGER",
                      "ga:percentVisitsWithSearch": "FLOAT",
                      "ga:avgPageDownloadTime": "FLOAT",
                      "ga:quantityRemovedFromCart": "INTEGER",
                      "ga:avgRedirectionTime": "FLOAT",
                      "ga:avgSessionDuration": "FLOAT",
                      "ga:searchGoalConversionRateAll": "FLOAT",
                      "ga:revenuePerItem": "FLOAT",
                      "ga:localTransactionTax": "FLOAT",
                      "ga:goalXXCompletions": "INTEGER",
                      "ga:cohortPageviewsPerUser": "FLOAT",
                      "ga:productDetailViews": "INTEGER",
                      "ga:pageviewsPerVisit": "FLOAT",
                      "ga:domContentLoadedTime": "INTEGER",
                      "ga:avgDomContentLoadedTime": "FLOAT",
                      "ga:adClicks": "INTEGER",
                      "ga:pageLoadTime": "INTEGER",
                      "ga:uniqueScreenviews": "INTEGER",
                      "ga:visitBounceRate": "FLOAT",
                      "ga:revenuePerTransaction": "FLOAT",
                      "ga:fatalExceptionsPerScreenview": "FLOAT",
                      "ga:goalXXAbandons": "INTEGER",
                      "ga:cohortSessionsPerUserWithLifetimeCriteria": "FLOAT",
                      "ga:cohortRetentionRate": "FLOAT",
                      "ga:hits": "INTEGER",
                      "ga:dcmROI": "FLOAT",
                      "ga:dcmFloodlightRevenue": "FLOAT",
                      "ga:sessionDuration": "FLOAT",
                      "ga:entrances": "INTEGER",
                      "ga:CPM": "FLOAT",
                      "ga:fatalExceptions": "INTEGER",
                      "ga:adsenseRevenue": "FLOAT",
                      "ga:cohortGoalCompletionsPerUserWithLifetimeCriteria": "FLOAT",
                      "ga:productRefunds": "INTEGER",
                      "ga:costPerTransaction": "FLOAT",
                      "ga:queryProductQuantity": "INTEGER",
                      "ga:contentGroupUniqueViewsXX": "INTEGER",
                      "ga:quantityCheckedOut": "INTEGER",
                      "ga:goalValueAll": "FLOAT",
                      "ga:uniqueAppviews": "INTEGER",
                      "ga:avgServerResponseTime": "FLOAT",
                      "ga:cohortSessionDurationPerUser": "FLOAT",
                      "ga:pageLoadSample": "INTEGER",
                      "ga:exceptions": "INTEGER",
                      "ga:bounceRate": "FLOAT",
                      "ga:searchExitRate": "FLOAT",
                      "ga:visits": "INTEGER",
                      "ga:redirectionTime": "INTEGER",
                      "ga:adxRevenuePer1000Sessions": "FLOAT",
                      "ga:adxClicks": "INTEGER",
                      "ga:impressions": "INTEGER",
                      "ga:avgSearchResultViews": "FLOAT",
                      "ga:cohortPageviewsPerUserWithLifetimeCriteria": "FLOAT",
                      "ga:timeOnPage": "FLOAT",
                      "ga:cohortSessionsPerUser": "FLOAT",
                      "ga:RPC": "FLOAT",
                      "ga:adsenseECPM": "FLOAT",
                      "ga:productListClicks": "INTEGER",
                      "ga:buyToDetailRate": "FLOAT",
                      "ga:dcmClicks": "INTEGER",
                      "ga:dcmMargin": "FLOAT",
                      "ga:exits": "INTEGER",
                      "ga:uniqueEvents": "INTEGER",
                      "ga:adsenseAdsViewed": "INTEGER",
                      "ga:productRevenuePerPurchase": "FLOAT",
                      "ga:avgSearchDepth": "FLOAT",
                      "ga:timeOnScreen": "FLOAT",
                      "ga:metricXX": "INTEGER",
                      "ga:adsensePageImpressions": "INTEGER",
                      "ga:pageviewsPerSession": "FLOAT",
                      "ga:goalXXValue": "FLOAT",
                      "ga:dcmCPC": "FLOAT",
                      "ga:speedMetricsSample": "INTEGER",
                      "ga:dcmROAS": "FLOAT",
                      "ga:sessionsWithEvent": "INTEGER",
                      "ga:bounces": "INTEGER",
                      "ga:dcmImpressions": "INTEGER",
                      "ga:adsenseViewableImpressionPercent": "FLOAT",
                      "ga:dcmCTR": "FLOAT",
                      "ga:cohortSessionDurationPerUserWithLifetimeCriteria": "FLOAT",
                      "ga:userTimingSample": "INTEGER",
                      "ga:adxViewableImpressionsPercent": "FLOAT",
                      "ga:avgTimeOnPage": "FLOAT",
                      "ga:adxImpressionsPerSession": "FLOAT",
                      "ga:transactionTax": "FLOAT",
                      "ga:uniquePurchases": "INTEGER",
                      "ga:uniquePageviews": "INTEGER",
                      "ga:eventsPerVisitWithEvent": "FLOAT",
                      "ga:cohortTotalUsers": "INTEGER",
                      "ga:domInteractiveTime": "INTEGER",
                      "ga:relatedProductQuantity": "INTEGER",
                      "ga:adxRevenue": "FLOAT",
                      "ga:goalXXStarts": "INTEGER",
                      "ga:transactionsPerVisit": "FLOAT",
                      "ga:productCheckouts": "INTEGER",
                      "ga:productAddsToCart": "INTEGER",
                      "ga:appviews": "INTEGER",
                      "ga:pageviews": "INTEGER",
                      "ga:timeOnSite": "FLOAT",
                      "ga:adxECPM": "FLOAT",
                      "ga:avgPageLoadTime": "FLOAT",
                      "ga:users": "INTEGER",
                      "ga:avgSearchDuration": "FLOAT",
                      "ga:pageValue": "FLOAT",
                      "ga:newUsers": "INTEGER",
                      "ga:goalAbandonsAll": "INTEGER",
                      "ga:7dayUsers": "INTEGER",
                      "ga:cohortGoalCompletionsPerUser": "FLOAT",
                      "ga:costPerGoalConversion": "FLOAT",
                      "ga:percentNewVisits": "FLOAT",
                      "ga:totalEvents": "INTEGER",
                      "ga:transactionRevenue": "FLOAT",
                      "ga:30dayUsers": "INTEGER",
                      "ga:ROI": "FLOAT",
                      "ga:margin": "FLOAT",
                      "ga:cohortRevenuePerUserWithLifetimeCriteria": "FLOAT",
                      "ga:screenviews": "INTEGER",
                      "ga:adsenseCTR": "FLOAT",
                      "ga:searchExits": "INTEGER",
                      "ga:transactionShipping": "FLOAT",
                      "ga:domLatencyMetricsSample": "INTEGER",
                      "ga:localItemRevenue": "FLOAT",
                      "ga:dcmFloodlightQuantity": "INTEGER",
                      "ga:percentNewSessions": "FLOAT",
                      "ga:itemsPerPurchase": "FLOAT",
                      "ga:socialActivities": "INTEGER",
                      "ga:dcmRPC": "FLOAT",
                      "ga:productListCTR": "FLOAT",
                      "ga:internalPromotionClicks": "INTEGER",
                      "ga:entranceRate": "FLOAT",
                      "ga:cohortTotalUsersWithLifetimeCriteria": "INTEGER",
                      "ga:adsenseExits": "INTEGER",
                      "ga:localProductRefundAmount": "FLOAT",
                      "ga:searchResultViews": "INTEGER",
                      "ga:exceptionsPerScreenview": "FLOAT",
                      "ga:productRefundAmount": "FLOAT",
                      "ga:avgEventValue": "FLOAT",
                      "ga:searchGoalXXConversionRate": "FLOAT",
                      "ga:totalRefunds": "INTEGER",
                      "ga:adxCTR": "FLOAT",
                      "ga:avgServerConnectionTime": "FLOAT",
                      "ga:correlationScore": "FLOAT",
                      "ga:sessions": "INTEGER",
                      "ga:transactionRevenuePerSession": "FLOAT",
                      "ga:itemQuantity": "INTEGER",
                      "ga:avgUserTimingValue": "FLOAT",
                      "ga:avgDomainLookupTime": "FLOAT",
                      "ga:entranceBounceRate": "FLOAT",
                      "ga:goalCompletionsAll": "INTEGER",
                      "ga:socialInteractionsPerVisit": "FLOAT",
                      "ga:goalValueAllPerSearch": "FLOAT",
                      "ga:adCost": "FLOAT",
                      "ga:goalXXAbandonRate": "FLOAT",
                      "ga:ROAS": "FLOAT",
                      "ga:percentSessionsWithSearch": "FLOAT",
                      "ga:appviewsPerVisit": "FLOAT",
                      "ga:screenviewsPerSession": "FLOAT",
                      "ga:goalAbandonRateAll": "FLOAT",
                      "ga:cartToDetailRate": "FLOAT",
                      "ga:revenuePerUser": "FLOAT",
                      "ga:socialInteractions": "INTEGER",
                      "ga:adxMonetizedPageviews": "INTEGER",
                      "ga:avgDomInteractiveTime": "FLOAT",
                      "ga:visitors": "INTEGER",
                      "ga:quantityRefunded": "INTEGER",
                      "ga:searchRefinements": "INTEGER",
                      "ga:localTransactionShipping": "FLOAT",
                      "ga:searchDuration": "FLOAT",
                      "ga:searchDepth": "INTEGER",
                      "ga:goalConversionRateAll": "FLOAT",
                      "ga:cohortAppviewsPerUserWithLifetimeCriteria": "FLOAT",
                      "ga:exitRate": "FLOAT",
                      "ga:transactions": "INTEGER",
                      "ga:goalXXConversionRate": "FLOAT",
                      "ga:serverConnectionTime": "INTEGER",
                      "ga:costPerConversion": "FLOAT",
                      "ga:internalPromotionCTR": "FLOAT",
                      "ga:goalStartsAll": "INTEGER",
                      "ga:pageDownloadTime": "INTEGER",
                      "ga:localRefundAmount": "FLOAT",
                      "ga:avgTimeOnSite": "FLOAT",
                      "ga:dcmCost": "FLOAT",
                      "ga:itemRevenue": "FLOAT",
                      "ga:transactionsPerUser": "FLOAT",
                      "ga:transactionRevenuePerVisit": "FLOAT",
                      "ga:searchVisits": "INTEGER",
                      "ga:avgScreenviewDuration": "FLOAT",
                      "ga:productListViews": "INTEGER",
                      "ga:searchSessions": "INTEGER",
                      "ga:sessionsPerUser": "FLOAT",
                      "ga:1dayUsers": "INTEGER",
                      "ga:eventValue": "INTEGER",
                      "ga:refundAmount": "FLOAT",
                      "ga:cohortAppviewsPerUser": "FLOAT",
                      "ga:adsenseAdsClicks": "INTEGER",
                      "ga:userTimingValue": "INTEGER",
                      "ga:CPC": "FLOAT"}

# These are the fields described as STRING that are known to us to not be strings
# TODO: There are other discrepancies, but they seem to just be things like CURRENCY, etc.
all_datatype_discrepancies = {f["id"]: {**known_dimension_types, **known_metric_types}[f["id"]]
                              for f in standard_fields
                              if f["id"] in {**known_dimension_types, **known_metric_types}
                              and f["dataType"] != {**known_dimension_types, **known_metric_types}[f["id"]]}

# ipdb> pp {f["id"]: {**known_dimension_types, **known_metric_types}[f["id"]] for f in standard_fields if f["id"] in {**known_dimension_types, **known_metric_types} and f["dataType"] != {**known_dimension_types, **known_metric_types}[f["id"]] and f["dataType"] == "STRING"}
fields_that_are_not_actually_strings = {'ga:cohortNthDay': 'INTEGER',
                                        'ga:cohortNthMonth': 'INTEGER',
                                        'ga:cohortNthWeek': 'INTEGER',
                                        'ga:date': 'DATETIME',
                                        'ga:dateHour': 'DATETIME',
                                        'ga:daysSinceLastSession': 'INTEGER',
                                        'ga:daysToTransaction': 'INTEGER',
                                        'ga:latitude': 'FLOAT',
                                        'ga:longitude': 'FLOAT',
                                        'ga:nthDay': 'INTEGER',
                                        'ga:nthHour': 'INTEGER',
                                        'ga:nthMinute': 'INTEGER',
                                        'ga:nthMonth': 'INTEGER',
                                        'ga:nthWeek': 'INTEGER',
                                        'ga:pageDepth': 'INTEGER',
                                        'ga:screenDepth': 'INTEGER',
                                        'ga:sessionCount': 'INTEGER',
                                        'ga:sessionsToTransaction': 'INTEGER',
                                        'ga:subContinentCode': 'INTEGER',
                                        'ga:visitCount': 'INTEGER',
                                        'ga:visitLength': 'INTEGER',
                                        'ga:visitsToTransaction': 'INTEGER'}

integer_field_overrides = {'ga:cohortNthDay',
                           'ga:cohortNthMonth',
                           'ga:cohortNthWeek',
                           'ga:daysSinceLastSession',
                           'ga:daysToTransaction',
                           'ga:nthDay',
                           'ga:nthHour',
                           'ga:nthMinute',
                           'ga:nthMonth',
                           'ga:nthWeek',
                           'ga:pageDepth',
                           'ga:screenDepth',
                           'ga:sessionCount',
                           'ga:sessionsToTransaction',
                           'ga:subContinentCode',
                           'ga:visitCount',
                           'ga:visitLength',
                           'ga:visitsToTransaction'}

datetime_field_overrides = {'ga:date',
                            'ga:dateHour'}

float_field_overrides = {'ga:latitude',
                           'ga:longitude'}

import ipdb; ipdb.set_trace()
1+1

def revised_type_to_schema(ga_type, field_id):
    if field_id in datetime_field_overrides:
        return {"type": ["string", "null"], "format": "date-time"}
    elif ga_type == 'CURRENCY':
        # https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters#ecomm
        return {"type": ["number", "null"]}
    elif ga_type == 'PERCENT':
        # TODO: Unclear whether these come back as "0.25%" or just "0.25"
        return {"type": ["number", "null"]}
    elif ga_type == 'TIME':
        return {"type": ["string", "null"]}
    elif ga_type == 'INTEGER' or field_id in integer_field_overrides::
        return {"type": ["integer", "null"]}
    elif ga_type == 'FLOAT' or field_id in float_field_overrides:
        return {"type": ["number", "null"]}
    elif ga_type == 'STRING':
        # TODO: What about date-time fields?
        return {"type": ["string", "null"]}
    else:
        raise Exception("Unknown Google Analytics type: {}".format(ga_type))

# TODO: Trim the `ga:` here?
# TODO: Do we need to generate the `XX` fields schemas here somehow? e.g., 'ga:productCategoryLevel5' vs. 'ga:productCategoryLevelXX'
# - The numeric versions are in `ga_cubes.json`
revised_field_schemas = {**{f["id"]: type_to_schema(f["dataType"]) for f in standard_fields},
                         **{f["id"]: type_to_schema(f["type"]) for f in custom_fields}}

# Expand Out standard XX fields into their numeric counterparts
# This will give us all of the fields that exist, including the actual names of standard `XX` fields
field_exclusions = discover.generate_exclusions_lookup()

# Translate the known standard `XX` fields to their numeric counterparts
# NOTE: This should probably happen before generating schemas, that way we get one for each.
xx_fields = [f for f in standard_fields if 'XX' in f['id']]
xx_field_regexes = {f['id'].replace('XX', r'\d\d?'): f for f in xx_fields}
numeric_xx_fields = [{**field_info, **{"id": numeric_field_id}}
                     for regex, field_info in xx_field_regexes.items()
                     for numeric_field_id in field_exclusions.keys() if re.match(regex, numeric_field_id)]

# TODO: Some did not get captured by this search, why?
# - Mainly entries with `XX` in the middle of the id
# ipdb> pp [f['id'] for f in standard_fields if 'XX' in f['id']]
['ga:goalXXStarts',
 'ga:goalXXCompletions',
 'ga:goalXXValue',
 'ga:goalXXConversionRate',
 'ga:goalXXAbandons',
 'ga:goalXXAbandonRate',
 'ga:searchGoalXXConversionRate',
 'ga:contentGroupUniqueViewsXX', # Matched
 'ga:dimensionXX', # Custom, ignore
 'ga:customVarNameXX', # Custom, ignore
 'ga:metricXX', # Custom, ignore
 'ga:customVarValueXX', # Custom, ignore
 'ga:landingContentGroupXX', # Matched
 'ga:previousContentGroupXX', # Matched
 'ga:contentGroupXX', # Matched
 'ga:productCategoryLevelXX' # Matched
]

# These are the ones that matched (e.g., have constant numeric definitions)
# ipdb> pp {field_info["id"] for regex, field_info in xx_field_regexes.items() for f in field_exclusions.keys() if re.match(regex, f)}
{'ga:contentGroupUniqueViewsXX',
 'ga:contentGroupXX',
 'ga:landingContentGroupXX',
 'ga:previousContentGroupXX',
 'ga:productCategoryLevelXX'}

# Looks like the only other standard fields we don't have are goals.
# Goals could be discovered through the data mangement API, if we have access
# - https://developers.google.com/analytics/devguides/config/mgmt/v3/data-management
# - https://developers.google.com/analytics/devguides/config/mgmt/v3/mgmtReference/management/goals/list

# This could be called during discovery to generate goal-related schemas for each one that's defined
# NOTE: Goals are not necessarily defined in 1, 2, 3, 4... order. We defined one at 17 for fun.
def get_goals_for_profile(access_token, account_id, web_property_id, profile_id):
    """
    Gets all goal IDs for profile to generate goal-related metric schemas.
    """
    profiles_url = 'https://www.googleapis.com/analytics/v3/management/accounts/{accountId}/webproperties/{webPropertyId}/profiles/{profileId}/goals'
    goals_response = requests.get(profiles_url.format(accountId=account_id,
                                                         webPropertyId=web_property_id,
                                                         profileId=profile_id),
                                              headers={'Authorization' : 'Bearer ' + access_token},
                                              params={"quotaUser": quota_user})
    return [g["id"] for g in goals_response.json()['items']]

# Generate Metadata - Use the "standard_fields" and "custom_fields" to do this


# Rough draft of catalog code

