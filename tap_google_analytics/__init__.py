from datetime import timedelta

import singer
from singer import utils, get_bookmark, metadata
from singer.catalog import write_catalog, Catalog
from .client import Client
from .discover import discover
from .sync import sync_report

def get_start_date(config, state, tap_stream_id):
    """
    Returns a date bookmark in state for the given stream, or the
    `start_date` from config, if no bookmark exists.
    """
    return utils.strptime_to_utc(get_bookmark(state, tap_stream_id, 'last_report_date', default=config['start_date']))

def get_end_date(config):
    """
    Returns the end_date for the reporting sync. Under normal operation,
    this is defined as the last full day to occur before UTC now.

    This can be overridden by the `end_date` config.json value.
    """
    if 'end_date' in config:
        return utils.strptime_to_utc(config['end_date'])
    return (utils.now() - timedelta(1)).replace(hour=0, minute=0, second=0, microsecond=0)

def do_sync(client, config, catalog, state):
    """
    Translate metadata into a set of metrics and dimensions and call out
    to sync to generate the required reports.
    """
    selected_streams = catalog.get_selected_streams(state)
    for stream in selected_streams:
        metrics = []
        dimensions = []
        mdata = metadata.to_map(stream.metadata)
        for field_path, field_mdata in mdata.items():
            if field_path == tuple():
                continue
            _, field_name = field_path
            if field_mdata.get('inclusion') == 'automatic' or field_mdata.get('selected'):
                if field_mdata.get('behavior') == 'METRIC':
                    metrics.append(field_name)
                elif field_mdata.get('behavior') == 'DIMENSION':
                    dimensions.append(field_name)

        report = {"profile_id": config['view_id'],
                  "name": stream.stream,
                  "id": stream.tap_stream_id,
                  "metrics": metrics,
                  "dimensions": dimensions}

        start_date = get_start_date(config, state, report['id'])
        end_date = get_end_date(config)

        schema = stream.schema.to_dict()

        singer.write_schema(
            report['name'],
            schema,
            stream.key_properties
            )

        sync_report(client, schema, report, start_date, end_date, state)

def do_discover(client, config):
    """
    Make request to discover.py and write result to stdout.
    """
    catalog = discover(client, config, config['view_id'])
    write_catalog(catalog)

def main():
    required_config_keys = ['start_date', 'view_id']
    args = singer.parse_args(required_config_keys)
    if "refresh_token" in args.config:  # if refresh_token in config assume OAuth2 credentials
        args.config['auth_method'] = "oauth2"
        additional_config_keys = ['client_id', 'client_secret', 'refresh_token']
    else:  # otherwise, assume Service Account details should be present
        args.config['auth_method'] = "service_account"
        additional_config_keys = ['client_email', 'private_key']

    singer.utils.check_config(args.config, additional_config_keys)

    config = args.config
    client = Client(config)
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
