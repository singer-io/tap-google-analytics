import functools
import itertools
from datetime import timedelta

import singer
from singer import utils, get_bookmark, metadata
from singer.catalog import write_catalog, Catalog
from .client import Client
from .discover import discover
from .sync import sync_report

LOGGER = singer.get_logger()

DEFAULT_PAGE_SIZE = 1000

# TODO: Add an integration test with multiple profiles that asserts state
def clean_state_for_report(config, state, tap_stream_id):
    top_level_bookmark = get_bookmark(state,
                                      tap_stream_id,
                                      'last_report_date')
    if top_level_bookmark:
        top_level_bookmark = utils.strptime_to_utc(top_level_bookmark)
        LOGGER.info("%s - Converting state to multi-profile format.", tap_stream_id)
        view_ids = get_view_ids(config)
        for view_id in view_ids:
            state = singer.write_bookmark(state,
                                          tap_stream_id,
                                          view_id,
                                          {'last_report_date': top_level_bookmark.strftime("%Y-%m-%d")})
        state = singer.clear_bookmark(state, tap_stream_id, 'last_report_date')
        singer.write_state(state)
    return state

def get_start_date(config, view_id, state, tap_stream_id):
    """
    Returns a date bookmark in state for the given stream, or the
    `start_date` from config, if no bookmark exists.
    """
    start = get_bookmark(state,
                         tap_stream_id,
                         view_id,
                         default={}).get('last_report_date',
                                         config['start_date'])
    is_historical_sync = start == config['start_date']
    return is_historical_sync, utils.strptime_to_utc(start)

def get_end_date(config):
    """
    Returns the end_date for the reporting sync. Under normal operation,
    this is defined as that date portion of UTC now.

    This can be overridden by the `end_date` config.json value.
    """
    if 'end_date' in config:
        return utils.strptime_to_utc(config['end_date'])
    return utils.now().replace(hour=0, minute=0, second=0, microsecond=0)

def get_view_ids(config):
    return config.get('view_ids') or [config.get('view_id')]

def get_page_size(config):
    """
    This function will get page size from config,
    and will return the default value if an invalid page size is given.
    """
    page_size = config.get('page_size', DEFAULT_PAGE_SIZE)
    try:
        if int(float(page_size)) > 0:
            return int(float(page_size))
        else:
            LOGGER.warning(f"The entered page size is invalid; it will be set to the default page size of {DEFAULT_PAGE_SIZE}")
            return DEFAULT_PAGE_SIZE
    except Exception:
        LOGGER.warning(f"The entered page size is invalid; it will be set to the default page size of {DEFAULT_PAGE_SIZE}")
        return DEFAULT_PAGE_SIZE

def do_sync(client, config, catalog, state):
    """
    Translate metadata into a set of metrics and dimensions and call out
    to sync to generate the required reports.
    """
    selected_streams = catalog.get_selected_streams(state)
    # Get page size
    page_size = get_page_size(config)

    for stream in selected_streams:
        # Transform state for this report to new format before proceeding
        state = clean_state_for_report(config, state, stream.tap_stream_id)

        state = singer.set_currently_syncing(state, stream.tap_stream_id)
        singer.write_state(state)

        metrics = []
        dimensions = []
        mdata = metadata.to_map(stream.metadata)
        for field_path, field_mdata in mdata.items():
            if field_path == tuple():
                continue
            if field_mdata.get('inclusion') == 'unsupported':
                continue
            _, field_name = field_path
            if field_mdata.get('inclusion') == 'automatic' or \
               field_mdata.get('selected') or \
               (field_mdata.get('selected-by-default') and field_mdata.get('selected') is None):
                if field_mdata.get('behavior') == 'METRIC':
                    metrics.append(field_name)
                elif field_mdata.get('behavior') == 'DIMENSION':
                    dimensions.append(field_name)

        view_ids = get_view_ids(config)

        # NB: Resume from previous view for this report, dropping all
        # views before it to keep streams moving forward
        current_view = state.get('currently_syncing_view')
        if current_view:
            if current_view in view_ids:
                view_not_current = functools.partial(lambda cv, v: v != cv, current_view)
                view_ids = list(itertools.dropwhile(view_not_current, view_ids))
            else:
                state.pop('currently_syncing_view', None)

        reports_per_view = [{"profile_id": view_id,
                             "name": stream.stream,
                             "id": stream.tap_stream_id,
                             "metrics": metrics,
                             "dimensions": dimensions}
                            for view_id in view_ids]

        end_date = get_end_date(config)

        schema = stream.schema.to_dict()

        singer.write_schema(
            stream.stream,
            schema,
            stream.key_properties
            )

        for report in reports_per_view:
            state['currently_syncing_view'] = report['profile_id']
            singer.write_state(state)

            is_historical_sync, start_date = get_start_date(config, report['profile_id'], state, report['id'])

            sync_report(client, schema, report, start_date, end_date, state, page_size, is_historical_sync)
        state.pop('currently_syncing_view', None)
        singer.write_state(state)
    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)

def do_discover(client, config):
    """
    Make request to discover.py and write result to stdout.
    """
    catalog = discover(client, config, get_view_ids(config))
    write_catalog(catalog)

def validate_config_view_ids(config):
    if 'view_id' not in config and 'view_ids' not in config:
        raise Exception("Config Validation Error: config.json MUST contain one of: view_ids, view_id.")
    if 'view_id' in config and 'view_ids' in config:
        raise Exception("Config Validation Error: config.json must ONLY contain view_id or view_ids, but not both.")

@utils.handle_top_exception(LOGGER)
def main():
    required_config_keys = ['start_date']
    args = singer.parse_args(required_config_keys)
    validate_config_view_ids(args.config)
    if "refresh_token" in args.config:  # if refresh_token in config assume OAuth2 credentials
        args.config['auth_method'] = "oauth2"
        additional_config_keys = ['client_id', 'client_secret', 'refresh_token']
    else:  # otherwise, assume Service Account details should be present
        args.config['auth_method'] = "service_account"
        additional_config_keys = ['client_email', 'private_key']

    singer.utils.check_config(args.config, additional_config_keys)

    config = args.config
    client = Client(config, args.config_path)
    catalog = args.catalog or Catalog([])
    state = args.state

    if args.properties and not args.catalog:
        raise Exception("DEPRECATED: Use of the 'properties' parameter is not supported. Please use --catalog instead")

    if args.discover:
        do_discover(client, config)
    else:
        do_sync(client, config, catalog, state)

if __name__ == "__main__":
    main()
