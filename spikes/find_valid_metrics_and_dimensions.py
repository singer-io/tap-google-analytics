import itertools
import json
import datetime

from singer import get_logger

from tap_google_analytics.client import Client

LOGGER = get_logger()

# 1. Get config from file and create client
with open('/tmp/tap_config.json', 'r') as f:
    config = json.load(f)

if "refresh_token" in config:  # if refresh_token in config assume OAuth2 credentials
    config['auth_method'] = "oauth2"
else:  # otherwise, assume Service Account details should be present
    config['auth_method'] = "service_account"

client = Client(config)

# 2. Configure these to match what you want to test
metrics = ['ga:users',
           'ga:bounceRate',
           'ga:pageviewsPerSession',
           'ga:avgSessionDuration',
           'ga:sessions',
           'ga:newUsers']
dimensions = ['ga:acquisitionSource',
              'ga:acquisitionSourceMedium',
              'ga:acquisitionMedium',
              'ga:acquisitionTrafficChannel',
              'ga:date']
profile_id = '123456789'
report_date = datetime.datetime(2019,2,26)

# 3. Attempt all combinations (order-indepedent) of metrics and dimensions, saving successes in file_name
file_name = "successful_mets_dims.jsonl"
def test_report_combinations(client, metrics, dimensions, profile_id, report_date, file_name):
    def get_subsets(l):
        sublists = [[]]
        for i in range(1, len(l) + 1):
            for sub in {tuple(sorted(a)) for a in itertools.combinations(l, i)}:
                sublists.append(list(sub))
        return sublists

    all_metric_combos = get_subsets(metrics)
    all_dimension_combos = get_subsets(dimensions)
    for mets, dims in sorted(itertools.product(all_metric_combos, all_dimension_combos), reverse=True):
        try:
            LOGGER.info("Trying %s, %s", str(mets), str(dims))
            thing = next(client.get_report('Singer Test Report', profile_id,
                                           report_date, mets, dims))
            LOGGER.info("SUCCESS!")
            with open(file_name, "a+") as f:
                json.dump({"metrics": mets, "dimensions": dims}, f)
                f.write("\n")
        except Exception as ex:
            LOGGER.info("FAILED!")
            pass

test_report_combinations(client, metrics, dimensions, profile_id, report_date, file_name)
