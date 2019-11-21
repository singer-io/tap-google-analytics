import singer
from singer.catalog import write_catalog
from .client import Client
from .discover import discover

def do_discover(client, profile_id):
    """
    Make request to discover.py and write result to stdout.
    """
    catalog = discover(client, profile_id)
    write_catalog(catalog)

def main():
    required_config_keys = ['start_date', 'client_id', 'client_secret', 'refresh_token', 'view_id']
    args = singer.parse_args(required_config_keys)

    config = args.config
    client = Client(config)

    if args.properties:
        raise Exception("DEPRECATED: Use of the 'properties' parameter is not supported. Please use --catalog instead")

    if args.discover:
        do_discover(client, config["view_id"])
    else:
        pass
        #raise NotImplementedError("Sync mode is not currently implemented.")

if __name__ == "__main__":
    main()
