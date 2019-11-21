from datetime import timedelta
import hashlib
import json
import singer

def generate_sdc_record_hash(report, row, report_date):
    """
    Generates a SHA 256 hash to be used as the primary key for records
    associated with a report. This consists of a list containing:
    - Pairs of ("ga:dimension_name", "dimension_value")
    - Report start_date value in YYYY-mm-dd format
    - Report end_date value in YYYY-mm-dd format

    Start and end date are included to maintain flexibility in the event the
    tap is extended to support wider date ranges.

    WARNING: Any change in the hashing mechanism, data, or sorting will
    REQUIRE a major version bump! As it will invalidate all previous
    primary keys and cause new data to be appended.
    """
    dimensions_headers = report["columnHeader"]["dimensions"]

    dimensions_pairs = sorted(zip(dimensions_headers, row["dimensions"]), key=lambda x: x[0])
    start_date, end_date = report_date, report_date
    
    hash_source = json.dumps([dimensions_pairs, start_date, end_date]).encode('utf-8')
    return hashlib.sha256(hash_source).hexdigest()
    
    
def generate_report_dates(start_date, end_date):
    total_days = (end_date - start_date).days
    # NB: Add a day to be inclusive of both start and end
    for day_offset in range(total_days + 1):
        yield start_date + timedelta(days=day_offset)

def report_to_records(raw_report):
    """
    Parse a single report object into Singer records, with added runtime info and PK.

    NOTE: This function assumes one report being run for one date range
    per request. For optimizations, the structure of the response will
    change, and this will need to be refactored.
    """
    report = raw_report["reports"][0]
    column_headers = report["columnHeader"]
    metrics_headers = [mh["name"] for mh in column_headers["metricHeader"]["metricHeaderEntries"]]
    dimensions_headers = column_headers["dimensions"]
    
    for row in report.get("data", {}).get("rows", []):
        record = {}
        record.update(zip(dimensions_headers, row["dimensions"]))
        record.update(zip(metrics_headers, row["metrics"][0]["values"]))

        report_date = raw_report["reportDate"].strftime("%Y-%m-%d")
        _sdc_record_hash = generate_sdc_record_hash(report, row, report_date)
        record["_sdc_record_hash"] = _sdc_record_hash

        report_date_string = report_date
        record["start_date"] = report_date_string
        record["end_date"] = report_date_string

        yield record

def sync_report(client, report, start_date, end_date, state):
    """
    Run a sync, beginning from either the start_date or bookmarked date,
    requesting a report per day, until the last full day of data. (e.g.,
    "Yesterday")
    
    report = {"name": stream.tap_stream_id, "metrics": metrics, "dimensions": dimensions}
    """
    all_data_golden = True
    for report_date in generate_report_dates(start_date, end_date):
        for raw_report_response in client.get_report(report['profile_id'], report_date, report['metrics'], report['dimensions']):

            for rec in report_to_records(raw_report_response):
                singer.write_record(report["name"], rec)

            # NB: Bookmark all days with "golden" data until you find the first non-golden day
            # - "golden" refers to data that will not change in future
            #   requests, so we can use it as a bookmark
            is_data_golden = raw_report_response["reports"][0]["data"]["isDataGolden"]

            if not is_data_golden:
                print("FOUND NON GOLDEN DATA")
            
            if all_data_golden:
                singer.write_bookmark(state,
                                      report["name"],
                                      "last_report_date",
                                      report_date.strftime("%Y-%m-%d"))
                singer.write_state(state)
                if not is_data_golden:
                    # Stop bookmarking on first "isDataGolden": False
                    all_data_golden = False


