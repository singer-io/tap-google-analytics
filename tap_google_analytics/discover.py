import singer
from singer import metadata, Schema, CatalogEntry, Catalog
import json
import re


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

def type_to_schema(ga_type, field_id):
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
    elif ga_type == 'INTEGER' or field_id in integer_field_overrides:
        return {"type": ["integer", "null"]}
    elif ga_type == 'FLOAT' or field_id in float_field_overrides:
        return {"type": ["number", "null"]}
    elif ga_type == 'STRING':
        return {"type": ["string", "null"]}
    else:
        raise Exception("Unknown Google Analytics type: {}".format(ga_type))

def is_static_XX_field(field_id, field_exclusions):
    """
    GA has fields that are documented using a placeholder of `XX`, where
    the `XX` is replaced with a number in practice.

    Some of these are standard fields with constant numeric
    representations. These must be handled differently from other field,
    so this function will detect this case using the information we have
    gleaned in our field_exclusions values.

    If the field_exclusions map does NOT have the `XX` version in it, this
    function assumes that it has only the numeric versions.
    """
    return 'XX' in field_id and field_id not in field_exclusions

def is_dynamic_XX_field(field_id, field_exclusions):
    """
    GA has fields that are documented using a placeholder of `XX`, where
    the `XX` is replaced with a number in practice.

    Some of these are standard fields that are generated based on other
    artifacts defined for the profile (e.g., goals). These must be handled
    differently from other fields as well, since the IDs must be
    discovered through their own means.

    If the field_exclusions map DOES have the `XX` version in it, this
    function assumes that the field is dynamically discovered.
    """
    return 'XX' in field_id and field_id in field_exclusions

def handle_static_XX_field(field, field_exclusions):
    """
    Uses a regex of the `XX` field's ID to discover which numeric versions
    of a given `XX` field name we have exclusions for.

    Generates a schema entry and metadata for each.
    Returns:
    - Sub Schemas  {"<numeric_field_id>": {...field schema}, ...}
    - Sub Metadata {"numeric_field_id>": {...exclusions metadata value}, ...}
    """
    regex_matcher = field['id'].replace("XX", r'\d\d?')
    matching_exclusions = {field_id: field_exclusions[field_id]
                           for field_id in field_exclusions.keys()
                           if re.match(regex_matcher, field_id)}

    sub_schemas = {field_id: type_to_schema(field["dataType"], field["id"])
                     for field_id in matching_exclusions.keys()}
    sub_metadata = matching_exclusions

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

def handle_dynamic_XX_field(client, field, field_exclusions, profile_id):
    """
    Discovers dynamic names of a given XX field using `client` with
    `get_dynamic_field_names` and matches them with the exclusions known
    for the `XX` version of the name.

    Generates a schema entry and metadata for each.
    Returns:
    - Sub Schemas  {"<numeric_field_id>": {...field schema}, ...}
    - Sub Metadata {"numeric_field_id>": {...exclusions metadata value}, ...}
    """
    dynamic_field_names = get_dynamic_field_names(client, field, profile_id)

    sub_schemas = {d: type_to_schema(field["dataType"],field["id"])
                   for d in dynamic_field_names}

    sub_metadata = {r: field_exclusions[field['id']]
                    for r in dynamic_field_names}
    return sub_schemas, sub_metadata

def write_metadata(mdata, field, exclusions):
    """ Translate a field_info object and its exclusions into its metadata, and write it. """
    mdata = metadata.write(mdata, ("properties", field["id"]), "inclusion", "available")
    mdata = metadata.write(mdata, ("properties", field["id"]), "fieldExclusions", list(exclusions))
    mdata = metadata.write(mdata, ("properties", field["id"]), "behavior", field["type"])

    # TODO: What other pieces of metadata do we need? probably tap_google_analytics.ga_name, tap_google_analytics.profile_id, etc?
    # - Also, metric/dimension needs to be in metadata for the UI (refer to adwords for key) 'behavior'

    return mdata

def generate_catalog_entry(client, standard_fields, custom_fields, field_exclusions, profile_id):
    schema = {"type": "object", "properties": {"_sdc_record_hash": {"type": "string"}}}
    mdata = metadata.get_standard_metadata(schema=schema, key_properties=["_sdc_record_hash"])
    mdata = metadata.to_map(mdata)

    for standard_field in standard_fields:
        if standard_field['status'] == 'DEPRECATED':
            continue
        matching_fields = []
        if is_static_XX_field(standard_field["id"], field_exclusions):
            sub_schemas, sub_mdata = handle_static_XX_field(standard_field, field_exclusions)
            schema["properties"].update(sub_schemas)
            for calculated_id, exclusions in sub_mdata.items():
                specific_field = {**standard_field, **{"id": calculated_id}}
                mdata = write_metadata(mdata, specific_field, exclusions)
        elif is_dynamic_XX_field(standard_field["id"], field_exclusions):
            sub_schemas, sub_mdata = handle_dynamic_XX_field(client, standard_field, field_exclusions, profile_id)
            schema["properties"].update(sub_schemas)
            for calculated_id, exclusions in sub_mdata.items():
                specific_field = {**standard_field, **{"id": calculated_id}}
                mdata = write_metadata(mdata, specific_field, exclusions)
        else:
            schema["properties"][standard_field["id"]] = type_to_schema(standard_field["dataType"],
                                                                                 standard_field["id"])
            mdata = write_metadata(mdata, standard_field, field_exclusions[standard_field["id"]])

    for custom_field in custom_fields:
        if custom_field["kind"] == 'analytics#customDimension':
            exclusion_lookup_name = 'ga:dimensionXX'
        elif custom_field["kind"] == 'analytics#customMetric':
            exclusion_lookup_name = 'ga:metricXX'
        else:
            raise Exception('Unknown custom field "kind": {}'.format(custom_field["kind"]))

        exclusions = field_exclusions[exclusion_lookup_name]
        mdata = write_metadata(mdata, custom_field, exclusions)
        schema["properties"][custom_field["id"]] = type_to_schema(custom_field["dataType"],
                                                                          custom_field["id"])
    return schema, mdata

def get_all_exclusion_fields_available(raw_field_exclusions):
    """
    Converts the ga_cubes.json response into a set of all available fields.
    """
    return {e for value in raw_field_exclusions.values() for e in value.keys()}

def get_field_exclusions_for(field, raw_field_exclusions, all_fields):
    """
    Returns the set of all fields that never occur with the specified
    `field` in the "ga_cubes" dataset.
    """
    fields_available_with_field = {e for values in raw_field_exclusions.values() for e in values.keys() if field in values.keys()}
    return all_fields - fields_available_with_field

def generate_exclusions_lookup(client):
    """
    Generates a map of {field_id: exclusions_list} for use in generating
    tap metadata for the catalog.
    """
    raw_field_exclusions = client.get_raw_field_exclusions()
    all_fields = get_all_exclusion_fields_available(raw_field_exclusions)
    return {f: get_field_exclusions_for(f, raw_field_exclusions, all_fields) for f in all_fields}

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

def generate_catalog(client, standard_fields, custom_fields, exclusions, profile_id):
    schema, mdata = generate_catalog_entry(client, standard_fields, custom_fields, exclusions, profile_id)
    # Do the thing to generate the thing
    catalog_entry = CatalogEntry(schema=Schema.from_dict(schema),
                                 key_properties=['_sdc_record_hash'],
                                 stream='report',
                                 tap_stream_id='report',
                                 metadata=metadata.to_list(mdata))
    return Catalog([catalog_entry])

def discover(client, profile_id):
    # Draw from spike to discover all the things
    # Get field_infos (standard and custom)
    LOGGER.info("Discovering standard fields...")
    standard_fields = get_standard_fields(client)
    LOGGER.info("Discovering custom fields...")
    custom_fields = get_custom_fields(client, profile_id)
    LOGGER.info("Generating field exclusions...")
    exclusions = generate_exclusions_lookup(client)
    return generate_catalog(client, standard_fields, custom_fields, exclusions, profile_id)
