import re
from functools import reduce
import singer
from singer import metadata, Schema, CatalogEntry, Catalog

from tap_google_analytics.reports import PREMADE_REPORTS

LOGGER = singer.get_logger()

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
                         'ga:longitude',
                         'ga:avgScreenviewDuration',
                         'ga:avgSearchDuration',
                         'ga:avgSessionDuration',
                         'ga:avgTimeOnPage',
                         'ga:cohortSessionDurationPerUser',
                         'ga:cohortSessionDurationPerUserWithLifetimeCriteria',
                         'ga:searchDuration',
                         'ga:sessionDuration',
                         'ga:timeOnPage',
                         'ga:timeOnScreen'}

# pylint: disable=too-many-return-statements
def type_to_schema(ga_type, field_id):
    if field_id in datetime_field_overrides:
        return {"type": ["string", "null"], "format": "date-time"}
    elif ga_type == 'CURRENCY':
        # https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters#ecomm
        return {"type": ["number", "null"]}
    elif ga_type == 'PERCENT':
        return {"type": ["number", "null"]}
    elif ga_type == 'TIME':
        return {"type": ["string", "null"]}
    elif ga_type == 'INTEGER' or field_id in integer_field_overrides:
        return {"type": ["integer", "null"]}
    elif ga_type == 'FLOAT' or field_id in float_field_overrides:
        return {"type": ["number", "null"]}
    elif ga_type == 'STRING':
        return {"type": ["string", "null"]}
    else:
        raise Exception("Unknown Google Analytics type: {}".format(ga_type))

def is_static_XX_field(field_id, cubes_lookup):
    """
    GA has fields that are documented using a placeholder of `XX`, where
    the `XX` is replaced with a number in practice.

    Some of these are standard fields with constant numeric
    representations. These must be handled differently from other field,
    so this function will detect this case using the information we have
    gleaned in our cubes_lookup values.

    If the cubes_lookup map does NOT have the `XX` version in it, this
    function assumes that it has only the numeric versions.
    """
    return ('XX' in field_id
            and field_id not in cubes_lookup
            and field_id not in ["ga:metricXX", "ga:dimensionXX"])

def is_dynamic_XX_field(field_id, cubes_lookup):
    """
    GA has fields that are documented using a placeholder of `XX`, where
    the `XX` is replaced with a number in practice.

    Some of these are standard fields that are generated based on other
    artifacts defined for the profile (e.g., goals). These must be handled
    differently from other fields as well, since the IDs must be
    discovered through their own means.

    If the cubes_lookup map DOES have the `XX` version in it, this
    function assumes that the field is dynamically discovered.
    """
    return ('XX' in field_id
            and field_id in cubes_lookup
            and field_id not in ["ga:metricXX", "ga:dimensionXX"])

def handle_static_XX_field(field, cubes_lookup):
    """
    Uses a regex of the `XX` field's ID to discover which numeric versions
    of a given `XX` field name we have cubes for.

    Generates a schema entry and metadata for each.
    Returns:
    - Sub Schemas  {"<numeric_field_id>": {...field schema}, ...}
    - Sub Metadata {"numeric_field_id>": {...cubes metadata value}, ...}
    """
    regex_matcher = field['id'].replace("XX", r'\d\d?')
    matching_cubes = {field_id: cubes_lookup[field_id]
                      for field_id in cubes_lookup.keys()
                      if re.match(regex_matcher, field_id)}

    sub_schemas = {field_id: type_to_schema(field["dataType"], field["id"])
                   for field_id in matching_cubes.keys()}
    sub_metadata = matching_cubes

    return sub_schemas, sub_metadata


goal_related_field_ids = ['ga:goalXXStarts',
                          'ga:goalXXCompletions',
                          'ga:goalXXValue',
                          'ga:goalXXConversionRate',
                          'ga:goalXXAbandons',
                          'ga:goalXXAbandonRate',
                          'ga:searchGoalXXConversionRate']

def get_dynamic_field_names(client, field, profile_id):
    """
    For known field types, retrieve their numeric forms through the client
    on a case-by-case basis (e.g., goals)
    """
    if field['id'] in goal_related_field_ids:
        return [field['id'].replace('XX', str(i)) for i in client.get_goals_for_profile(profile_id)]
    else:
        # Skip unknown, or already handled, dynamic fields
        return []

def handle_dynamic_XX_field(client, field, cubes_lookup, profile_id):
    """
    Discovers dynamic names of a given XX field using `client` with
    `get_dynamic_field_names` and matches them with the cubes known
    for the `XX` version of the name.

    Generates a schema entry and metadata for each.
    Returns:
    - Sub Schemas  {"<numeric_field_id>": {...field schema}, ...}
    - Sub Metadata {"numeric_field_id>": {...cubes metadata value}, ...}
    """
    dynamic_field_names = get_dynamic_field_names(client, field, profile_id)

    sub_schemas = {d: type_to_schema(field["dataType"],field["id"])
                   for d in dynamic_field_names}

    sub_metadata = {r: cubes_lookup[field['id']]
                    for r in dynamic_field_names}
    return sub_schemas, sub_metadata

def write_metadata(mdata, field, cubes):
    """ Translate a field_info object and its cubes into its metadata, and write it. """
    mdata = metadata.write(mdata, ("properties", field["id"]), "inclusion", "available")
    mdata = metadata.write(mdata, ("properties", field["id"]), "tap_google_analytics.cubes", list(cubes))
    mdata = metadata.write(mdata, ("properties", field["id"]), "behavior", field["type"])
    mdata = metadata.write(mdata, ("properties", field["id"]), "tap_google_analytics.group", field["group"])

    return mdata

def generate_base_schema():
    return {"type": "object", "properties": {"_sdc_record_hash": {"type": "string"},
                                             "start_date": {"type": "string",
                                                            "format": "date-time"},
                                             "end_date": {"type": "string",
                                                          "format": "date-time"},
                                             "account_id": {"type": "string"},
                                             "web_property_id": {"type": "string"},
                                             "profile_id": {"type": "string"}}}

def generate_base_metadata(all_cubes, schema):
    mdata = metadata.get_standard_metadata(schema=schema, key_properties=["_sdc_record_hash"])
    mdata = metadata.to_map(mdata)
    mdata = metadata.write(mdata, (), "tap_google_analytics.all_cubes", list(all_cubes))
    mdata = reduce(lambda mdata, field_name: metadata.write(mdata, ("properties", field_name), "inclusion", "automatic"),
                   ["_sdc_record_hash", "start_date", "end_date", "account_id", "web_property_id", "profile_id"],
                   mdata)
    mdata = reduce(lambda mdata, field_name: metadata.write(mdata, ("properties", field_name), "tap_google_analytics.group", "Report Fields"),
                   ["_sdc_record_hash", "start_date", "end_date", "account_id", "web_property_id", "profile_id"],
                   mdata)
    return mdata

def generate_catalog_entry(client, standard_fields, custom_fields, all_cubes, cubes_lookup, profile_id):
    schema = generate_base_schema()
    mdata = generate_base_metadata(all_cubes, schema)

    for standard_field in standard_fields:
        if (standard_field['status'] == 'DEPRECATED'
                or standard_field['id'] in ["ga:metricXX", "ga:dimensionXX"]):
            continue
        if is_static_XX_field(standard_field["id"], cubes_lookup):
            sub_schemas, sub_mdata = handle_static_XX_field(standard_field, cubes_lookup)
            schema["properties"].update(sub_schemas)
            for calculated_id, cubes in sub_mdata.items():
                specific_field = {**standard_field, **{"id": calculated_id}}
                mdata = write_metadata(mdata, specific_field, cubes)
        elif is_dynamic_XX_field(standard_field["id"], cubes_lookup):
            sub_schemas, sub_mdata = handle_dynamic_XX_field(client, standard_field, cubes_lookup, profile_id)
            schema["properties"].update(sub_schemas)
            for calculated_id, cubes in sub_mdata.items():
                specific_field = {**standard_field, **{"id": calculated_id}}
                mdata = write_metadata(mdata, specific_field, cubes)
        else:
            schema["properties"][standard_field["id"]] = type_to_schema(standard_field["dataType"],
                                                                        standard_field["id"])
            mdata = write_metadata(mdata, standard_field, cubes_lookup[standard_field["id"]])

    for custom_field in custom_fields:
        if custom_field["kind"] == 'analytics#customDimension':
            cubes_lookup_name = 'ga:dimensionXX'
        elif custom_field["kind"] == 'analytics#customMetric':
            cubes_lookup_name = 'ga:metricXX'
        else:
            raise Exception('Unknown custom field "kind": {}'.format(custom_field["kind"]))

        cubes = cubes_lookup[cubes_lookup_name]

        mdata = write_metadata(mdata, custom_field, cubes)
        schema["properties"][custom_field["id"]] = type_to_schema(custom_field["dataType"],
                                                                  custom_field["id"])

    return schema, mdata

def generate_premade_catalog_entry(standard_fields, all_cubes, cubes_lookup):
    schema = generate_base_schema()
    mdata = generate_base_metadata(all_cubes, schema)

    for standard_field in standard_fields:
        # No dynamic fields in standard reports
        if (standard_field['status'] == 'DEPRECATED'
                or standard_field['id'] in ["ga:metricXX", "ga:dimensionXX"]):
            continue

        schema["properties"][standard_field["id"]] = type_to_schema(standard_field["dataType"],
                                                                    standard_field["id"])
        mdata = write_metadata(mdata, standard_field, cubes_lookup[standard_field["id"]])
    return schema, mdata

def generate_cubes_lookup(raw_cubes):
    """
    Generates a map of {field_id: cubes_list} for use in generating
    tap metadata for the catalog.
    """
    cubes_lookup = {}
    for raw_cube, fields in raw_cubes.items():
        for field in fields:
            if field not in cubes_lookup:
                cubes_lookup[field] = set()
            cubes_lookup[field].add(raw_cube)
    return cubes_lookup

def parse_cube_definitions(client):
    """
    Requests cube definitions from Google Metrics and Dimensions
    Explorer, and parses it into a structure for metadata usage.

    Returns:
       all_cubes -> names of all cubes that exist
       cubes_lookup -> mapping of field name to compatible cubes
    """
    raw_cubes = client.get_raw_cubes()
    all_cubes = set(raw_cubes.keys())
    cubes_lookup = generate_cubes_lookup(raw_cubes)
    return all_cubes, cubes_lookup

def get_custom_metrics(client, profile_id):
    custom_metrics = client.get_custom_metrics_for_profile(profile_id)
    metrics_fields = {"id", "name", "kind", "active", "min_value", "max_value"}
    account_id = client.profile_lookup[profile_id]["account_id"]
    web_property_id = client.profile_lookup[profile_id]["web_property_id"]
    profiles = client.get_profiles_for_property(account_id, web_property_id)
    return  [{"account_id": account_id,
              "web_property_id": web_property_id,
              "profiles": profiles,
              "type": "METRIC",
              "dataType": item["type"],
              "group": "Custom Variables or Columns",
              **{k:v for k,v in item.items() if k in metrics_fields}}
             for item in custom_metrics['items']]

def get_custom_dimensions(client, profile_id):
    custom_dimensions = client.get_custom_dimensions_for_profile(profile_id)
    dimensions_fields = {"id", "name", "kind", "active"}
    account_id = client.profile_lookup[profile_id]["account_id"]
    web_property_id = client.profile_lookup[profile_id]["web_property_id"]
    profiles = client.get_profiles_for_property(account_id, web_property_id)
    return [{"dataType": "STRING",
             "account_id": account_id,
             "web_property_id": web_property_id,
             "profiles": profiles,
             "type": "DIMENSION",
             "group": "Custom Variables or Columns",
             **{k:v for k,v in item.items() if k in dimensions_fields}}
            for item in custom_dimensions['items']]

def get_custom_fields(client, profile_id):
    custom_metrics_and_dimensions = []
    custom_metrics_and_dimensions.extend(get_custom_dimensions(client, profile_id))
    custom_metrics_and_dimensions.extend(get_custom_metrics(client, profile_id))
    return custom_metrics_and_dimensions


def transform_field(field):
    interesting_attributes = {k: v for k, v in field["attributes"].items()
                              if k in {"dataType", "group", "status", "type"}}
    return {"id": field["id"], "name": field["attributes"]["uiName"], **interesting_attributes}

def get_standard_fields(client):
    metadata_response = client.get_field_metadata()
    # NB: These fields' specific names aren't discoverable, we think
    #     "customVar*" is deprecated and "calcMetric" is beta.
    unsupported_fields = {"ga:customVarValueXX", "ga:customVarNameXX", "ga:calcMetric_<NAME>"}
    return [transform_field(f) for f in metadata_response["items"] if f["id"] not in unsupported_fields]

def generate_catalog(client, report_config, standard_fields, custom_fields, all_cubes, cubes_lookup, profile_id):
    """
    Generate a catalog entry for each report specified in `report_config`
    """
    catalog_entries = []
    for report in PREMADE_REPORTS:
        metrics_dimensions = set(report['metrics'] + report['dimensions'])
        selected_by_default = {*report['metrics'][:10], # Use first 10 metrics in definition
                               *report.get('default_dimensions', [])}
        premade_fields = [field for field in standard_fields if field['id'] in metrics_dimensions]
        schema, mdata = generate_premade_catalog_entry(premade_fields,
                                                       all_cubes,
                                                       cubes_lookup)

        mdata = reduce(lambda mdata, field_name: metadata.write(mdata,
                                                                ("properties", field_name),
                                                                "selected-by-default", True),
                       selected_by_default,
                       mdata)

        catalog_entries.append(CatalogEntry(schema=Schema.from_dict(schema),
                                            key_properties=['_sdc_record_hash'],
                                            stream=report['name'],
                                            tap_stream_id=report['name'],
                                            metadata=metadata.to_list(mdata)))


    for report in report_config:
        schema, mdata = generate_catalog_entry(client,
                                               standard_fields,
                                               custom_fields,
                                               all_cubes,
                                               cubes_lookup,
                                               profile_id)

        catalog_entries.append(CatalogEntry(schema=Schema.from_dict(schema),
                                            key_properties=['_sdc_record_hash'],
                                            stream=report['name'],
                                            tap_stream_id=report['id'],
                                            metadata=metadata.to_list(mdata)))
    return Catalog(catalog_entries)

def discover(client, config, profile_id):
    # Draw from spike to discover all the things
    # Get field_infos (standard and custom)
    report_config = config.get("report_definitions") or []
    LOGGER.info("Discovering standard fields...")
    standard_fields = get_standard_fields(client)
    LOGGER.info("Discovering custom fields...")
    custom_fields = get_custom_fields(client, profile_id)
    LOGGER.info("Parsing cube definitions...")
    all_cubes, cubes_lookup = parse_cube_definitions(client)
    LOGGER.info("Generating catalog...")
    return generate_catalog(client, report_config, standard_fields, custom_fields, all_cubes, cubes_lookup, profile_id)
