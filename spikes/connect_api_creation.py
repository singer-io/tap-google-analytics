# Creating a Connection Through the API

###########################################################################################
# Goals:                                                                                  #
# Understand how the report definitions should look                                       #
# Make requests to Stitch to create a google analytics connection                         #
#                                                                                         #
# Expected Result:                                                                        #
# End to end flow of requests that will create a connection, in "Fully Configured" status #
###########################################################################################

# Stitch Connect Docs: https://www.stitchdata.com/docs/stitch-connect/api
import json
import os
import requests

from pprint import pprint
import random

headers = {"Content-Type": "application/json",
           "Authorization": "Bearer {}".format(os.getenv("CONNECT_API_TOKEN"))}
stitch_api_base_url = os.getenv("CONNECT_API_BASE_URL")

# 0. Describe source-types, and get tap-google-analytics source-type to see what comes back

source_types = requests.get(stitch_api_base_url + "/v4/source-types/", headers=headers)
ga_source_type = requests.get(stitch_api_base_url + "/v4/source-types/platform.google-analytics", headers=headers)


# 1. Create Connection

# 1a. Go through auth flow, somehow
# These values must be set up through a separate OAuth and profile (aka, "view" in google terms) selection flow.
refresh_token = os.getenv("GA_SPIKE_REFRESH_TOKEN")
client_id = os.getenv("GA_SPIKE_CLIENT_ID")
client_secret = os.getenv("GA_SPIKE_CLIENT_SECRET")
view_id = os.getenv("GA_SPIKE_VIEW_ID")

# 1b. Create a connection

# 1c. Get report card at "field_selection" step

properties = {"start_date": "2018-01-01T00:00:00Z",
              #"frequency_in_minutes": "60",
}
body = {"display_name": "google-analytics"+str(random.randint(0,100)), "type": "platform.google-analytics", "properties": properties}
source_object = requests.post(stitch_api_base_url + "/v4/sources", headers=headers, json=body)

# Source should be at Step 2 "OAuth" at this point

source_id = str(source_object.json()["id"])
oauth_properties = {"client_id": client_id, "client_secret": client_secret, "refresh_token": refresh_token, "view_id": view_id}
put_body = {"properties": oauth_properties}
oauth_step_source_object = requests.put(stitch_api_base_url+"/v4/sources/"+source_id, headers=headers, json=put_body)

# Source should be at step 3 "Discover Schema", awaiting a "check" job to finish successfully.
# - Poll `/v4/sources/{source_id}/last-connection-check` until that's the case and the step has advanced.

# Source should end up in step 4 "Field Selection"

# 2. Specify reports

streams_response = requests.get(stitch_api_base_url + "/v4/sources/{}/streams".format(source_id), headers=headers)
stream_id = streams_response.json()[0]['stream_id']
tap_stream_id = streams_response.json()[0]['tap_stream_id']
schemas_response = requests.get(stitch_api_base_url + "/v4/sources/{}/streams/{}".format(source_id, stream_id), headers=headers)

# 2a. Send metadata that defines a few reports to the API
# Use the PUT route for the `source_id` above to send over metadata for the `tap-google-analytics.reports` key, root-level
# - Refer to the stream by its tap_stream_id
# - Mark "selected: true" to pass the report card step


mdata = [
    {
        "breadcrumb": [],
        "metadata": {
            "selected": True,
            "tap-google-analytics.reports": [
                # This metadata value describes each report definition.
                # The tap will take these and emit data for each "stream"
                # defined by the (canonicalized?) name of the report.
                {
                    "name": "My Session Stream",
                    "metrics": [
                        "ga:users",
                        "ga:newUsers",
                        "ga:sessions",
                        "ga:sessionsPerUser",
                        "ga:avgSessionDuration",
                        "ga:pageviews",
                        "ga:pageviewsPerSession",
                        "ga:avgTimeOnPage",
                        "ga:bounceRate",
                        "ga:exitRate" 
                    ],
                    "dimensions": [
                        "ga:date"
                    ]
                },
                {
                    "name": "my_other_report",
                    "metrics": ["ga:users", "ga:newUsers", "ga:sessionsPerUser"],
                    "dimensions": ["ga:userType"]
                }]
        }
    }
]

put_metadata_response = requests.put(stitch_api_base_url + "/v4/sources/{}/streams/metadata".format(source_id), headers=headers, json={"streams":[{"tap-stream-id":tap_stream_id, "metadata": mdata}]})

# 2b. Get report card marked "fully_configured"
fully_configured_step_response = requests.get(stitch_api_base_url+"/v4/sources/{}".format(source_id), headers=headers)

assert fully_configured_step_response.json()['report_card']['current_step_type'] == 'fully_configured'

# PROS:
# - Low-volume of data being returned by the API (just one set of available metrics and dimensions, under the `report` stream)
# - All report configuration in a single PUT request for the report stream
# - Actually dynamic
# CONS:
# - Breaks our idea of `streams` having ALL of the streams that translate into data from the tap, by instead specifying a sort of "virtual stream" under the `tap-google-analytics.reports` key
# - Changing reports after the fact could cause conflicts in destination (unavoidable with either method)
# - Unknown style? Is this a pattern that makes sense to our users?

# Potential PRO:
# - TODO: How does this work for multiple profiles? multiple "root" streams for each profile? Including that profile's custom metrics/dimensions?
# - I think this will be easier to deal with this way

# Potential CON:
# - How to document this for folks? Connect API section in regular docs?
# - How to enforce field exclusions this way? The onus is on the user to abide by the `fieldExclusions` metadata

# ---------------------------------

# Hybrid Method
# This method would specify stream names in the config, and discover the total set of available metrics and dimensions for a generic report, per name from config
# Field selection then proceeds as normal, by marking "selected: true" metadata on each field to be included in the reoprt
# 1b. Create a connection

# 1c. Get report card at "field_selection" step

properties = {"start_date": "2018-01-01T00:00:00Z",
              "report_names": json.dumps(["My Session Stream", "my_other_report"])} # NOTE: Posting raw JSON arrays, not in string form fails
body = {"display_name": "google-analytics-method2-"+str(random.randint(0,100)), "type": "platform.google-analytics", "properties": properties}
source_object = requests.post(stitch_api_base_url + "/v4/sources", headers=headers, json=body)

# Source should be at Step 2 "OAuth" at this point

source_id = str(source_object.json()["id"])
oauth_properties = {"client_id": client_id, "client_secret": client_secret, "refresh_token": refresh_token, "view_id": view_id}
put_body = {"properties": oauth_properties}
oauth_step_source_object = requests.put(stitch_api_base_url+"/v4/sources/"+source_id, headers=headers, json=put_body)

# Source should be at step 3 "Discover Schema", awaiting a "check" job to finish successfully.
# - Poll `/v4/sources/{source_id}/last-connection-check` until that's the case and the step has advanced.

# Source should end up in step 4 "Field Selection"

# 2. Specify reports

# In this pattern, we specified `report_names` on connection creation, and
# each of these should have a schema in the catalog of ALL possible
# metrics/dimensions from the tap's discovery code
# - The idea is that field selection will involve adding `"selected": true` for each of these reports, to build up the report parameters

streams_response = requests.get(stitch_api_base_url + "/v4/sources/{}/streams".format(source_id), headers=headers)
stream_ids = [s["stream_id"] for s in streams_response.json()]
tap_stream_ids = [s["tap_stream_id"] for s in streams_response.json()]
schemas_response = [requests.get(stitch_api_base_url + "/v4/sources/{}/streams/{}".format(source_id, stream_id), headers=headers) for stream_id in stream_ids]

# 2a. Send metadata that defines a few reports to the API
# Use the PUT route for the `source_id` above to send over metadata for the `tap-google-analytics.reports` key, root-level
# - Refer to the stream by its tap_stream_id
# - Mark "selected: true" to pass the report card step


                # {
                #     "name": "My Session Stream",
                #     "metrics": [
                #         "ga:users",
                #         "ga:newUsers",
                #         "ga:sessions",
                #         "ga:sessionsPerUser",
                #         "ga:avgSessionDuration",
                #         "ga:pageviews",
                #         "ga:pageviewsPerSession",
                #         "ga:avgTimeOnPage",
                #         "ga:bounceRate",
                #         "ga:exitRate" 
                #     ],
                #     "dimensions": [
                #         "ga:date"
                #     ]
                # },
                # {
                #     "name": "my_other_report",
                #     "metrics": ["ga:users", "ga:newUsers", "ga:sessionsPerUser"],
                #     "dimensions": ["ga:userType"]
                # }]
streams = []
for tap_stream_id in tap_stream_ids:
    if tap_stream_id == "my_other_report":
        streams.append({
            "tap-stream-id": tap_stream_id,
            "metadata": [
                {
                    "breadcrumb": [],
                    "metadata": {
                        "selected": True
                    }
                },
                *[{"breadcrumb": ["properties", name], "metadata": {"selected": True}} for name in ["ga:users", "ga:newUsers", "ga:sessionsPerUser", "ga:userType"]]
            ]
        })
    else:
        streams.append({
            "tap-stream-id": tap_stream_id,
            "metadata": [
                {
                    "breadcrumb": [],
                    "metadata": {
                        "selected": True
                    }
                },
                *[{"breadcrumb": ["properties", name], "metadata": {"selected": True}} for name in ["ga:users",
                                                                                                    "ga:newUsers",
                                                                                                    "ga:sessions",
                                                                                                    "ga:sessionsPerUser",
                                                                                                    "ga:avgSessionDuration",
                                                                                                    "ga:pageviews",
                                                                                                    "ga:pageviewsPerSession",
                                                                                                    "ga:avgTimeOnPage",
                                                                                                    "ga:bounceRate",
                                                                                                    "ga:exitRate",
                                                                                                    "ga:date"]]
            ]
        })


# CON: Not clear what is a metric and what is a dimension from the code to
# mark them as "selected"
# - Maybe moot, since in a real situation, the user would have to look at
# the metadata produced by the tap marking them as metrics and dimensions

put_metadata_response = requests.put(stitch_api_base_url + "/v4/sources/{}/streams/metadata".format(source_id), headers=headers, json={"streams":streams})

# 2b. Get report card marked "fully_configured"
fully_configured_step_response = requests.get(stitch_api_base_url+"/v4/sources/{}".format(source_id), headers=headers)

assert fully_configured_step_response.json()['report_card']['current_step_type'] == 'fully_configured'



# PROS and CONS
# {'metadata': [],
#   'non-discoverable-metadata-keys': ['selected',
#                                      'replication-method',
#                                      'tap-mongodb.projection',
#                                      'replication-key',
#                                      'view-key-properties',
#                                      'tap-google-analytics.reports'],
#   'schema': '{"type":"object","properties":{"ga:date":{"type":["string","null"]},"ga:sessions":{"type":["string","null"]},"ga:userType":{"type":["string","null"]},"ga:bounceRate":{"type":["string","null"]},"ga:pageviewsPerSession":{"type":["string","null"]},"ga:pageviews"
# :{"type":["string","null"]},"ga:sessionsPerUser":{"type":["string","null"]},"ga:newUsers":{"type":["string","null"]},"ga:avgTimeOnPage":{"type":["string","null"]},"ga:users":{"type":["string","null"]},"ga:avgSessionDuration":{"type":["string","null"]},"ga:exitRate":{"type
# ":["string","null"]}}}'}
# - The 'metadata' and 'schema' objects in real life will be MUCH larger than this, and duplicated per stream specified in the config.

# PROS
# - This allows a better separation in the event that multiple profile IDs are specified
# - This fits into our "a stream in the catalog is a stream in the destination" pattern
#    - If that is important to users of the API, then that is necessary
# CONS
# - How does this handle dimensions and metrics in a multi-profile scenario?
#    - Discover the superset of all and leave it up to the user? Superset and associate them with profiles through discovered metadata?
#    - It's less clear, and could get ugly in this method
# - Data volume through the API. This will be HUGE if a user specifies a lot of reports/profiles (on the order of hundreds)
# - (soft) May be less clear which are dimensions and metrics without looking them up in the metadata
# - Even in the simple case, there are a LOT more lines of code to write metadata for each field, per report
# - Users are doing something more like option 1 right now, this is MUCH different

import ipdb; ipdb.set_trace()
1+1
