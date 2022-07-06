from datetime import timedelta, datetime
import hashlib
import json
import singer
from singer import Transformer

LOGGER = singer.get_logger()

def generate_sdc_record_hash(raw_report, row, start_date, end_date):
    """
    Generates a SHA 256 hash to be used as the primary key for records
    associated with a report. This consists of a UTF-8 encoded JSON list
    containing:
    - The account_id, web_property_id, profile_id of the associated report
    - Pairs of ("ga:dimension_name", "dimension_value")
    - Report start_date value in YYYY-mm-dd format
    - Report end_date value in YYYY-mm-dd format

    Start and end date are included to maintain flexibility in the event the
    tap is extended to support wider date ranges.

    WARNING: Any change in the hashing mechanism, data, or sorting will
    REQUIRE a major version bump! As it will invalidate all previous
    primary keys and cause new data to be appended.
    """
    dimensions_headers = raw_report["reports"][0]["columnHeader"].get("dimensions", [])
    profile_id = raw_report["profileId"]
    web_property_id = raw_report["webPropertyId"]
    account_id = raw_report["accountId"]

    dimensions_pairs = sorted(zip(dimensions_headers, row.get("dimensions", [])), key=lambda x: x[0])

    # NB: Do not change the ordering of this list, it is the source of the PK hash
    hash_source_data = [account_id,
                        web_property_id,
                        profile_id,
                        dimensions_pairs,
                        start_date.strftime("%Y-%m-%d"),
                        end_date.strftime("%Y-%m-%d")]

    hash_source_bytes = json.dumps(hash_source_data).encode('utf-8')
    return hashlib.sha256(hash_source_bytes).hexdigest()


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
    # TODO: Handle data sampling keys and values, either in the records or as a separate stream? They look like arrays.
    # - https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#ReportData
    report = raw_report["reports"][0]
    column_headers = report["columnHeader"]
    metrics_headers = [mh["name"] for mh in column_headers["metricHeader"]["metricHeaderEntries"]]
    dimensions_headers = column_headers.get("dimensions", [])

    for row in report.get("data", {}).get("rows", []):
        record = {}
        record.update(zip(dimensions_headers, row.get("dimensions", [])))
        record.update(zip(metrics_headers, row["metrics"][0]["values"]))

        report_date = raw_report["reportDate"]
        _sdc_record_hash = generate_sdc_record_hash(raw_report, row, report_date, report_date)
        record["_sdc_record_hash"] = _sdc_record_hash

        report_date_string = report_date.strftime("%Y-%m-%d")
        record["start_date"] = report_date_string
        record["end_date"] = report_date_string

        record["account_id"] = raw_report["accountId"]
        record["web_property_id"] = raw_report["webPropertyId"]
        record["profile_id"] = raw_report["profileId"]

        yield record

DATETIME_FORMATS = {
    "ga:dateHour": '%Y%m%d%H',
    "ga:dateHourMinute": '%Y%m%d%H%M',
    "ga:date": '%Y%m%d',
}

def parse_datetime(field_name, value):
    """
    Handle the case where the datetime value is not a valid datetime format.

    Google will return `(other)` as the value when the underlying database table
    from which the report is built reaches its row limit.

    See https://support.google.com/analytics/answer/9309767
    """
    try:
        return datetime.strptime(value, DATETIME_FORMATS[field_name]).strftime(singer.utils.DATETIME_FMT)
    except ValueError:
        LOGGER.warning("Datetime value is not in expected format. It will not be transformed.")
        return value

def transform_datetimes(rec):
    """ Datetimes have a compressed format, so this ensures they parse correctly. """
    for field_name, value in rec.items():
        if value and field_name in DATETIME_FORMATS:
            rec[field_name] = parse_datetime(field_name, value)
    return rec

def sync_report(client, schema, report, start_date, end_date, state, historically_syncing=False):
    """
    Run a sync, beginning from either the start_date or bookmarked date,
    requesting a report per day, until the last full day of data. (e.g.,
    "Yesterday")

    report = {"name": stream.tap_stream_id,
              "profile_id": view_id,
              "metrics": metrics,
              "dimensions": dimensions}
    """
    LOGGER.info("Syncing %s for view_id %s", report['name'], report['profile_id'])

    all_data_golden = True
    # TODO: Is it better to query by multiple days if `ga:date` is present?
    # - If so, we can optimize the calls here to generate date ranges and reduce request volume
    for report_date in generate_report_dates(start_date, end_date):
        for raw_report_response in client.get_report(report['name'], report['profile_id'],
                                                     report_date, report['metrics'],
                                                     report['dimensions']):

            with singer.metrics.record_counter(report['name']) as counter:
                time_extracted = singer.utils.now()
                with Transformer() as transformer:
                    for rec in report_to_records(raw_report_response):
                        singer.write_record(report["name"],
                                            transformer.transform(
                                                transform_datetimes(rec),
                                                schema),
                                            time_extracted=time_extracted)
                        counter.increment()

                # NB: Bookmark all days with "golden" data until you find the first non-golden day
                # - "golden" refers to data that will not change in future
                #   requests, so we can use it as a bookmark
                is_data_golden = raw_report_response["reports"][0]["data"].get("isDataGolden")
                if historically_syncing:
                    # Switch to regular bookmarking at first golden
                    historically_syncing = not is_data_golden

                # The assumption here is that today's data cannot be golden if yesterday's is also not golden
                if all_data_golden and not historically_syncing:
                    singer.write_bookmark(state,
                                          report["id"],
                                          report['profile_id'],
                                          {'last_report_date': report_date.strftime("%Y-%m-%d")})
                    singer.write_state(state)
                    if not is_data_golden and not historically_syncing:
                        # Stop bookmarking on first "isDataGolden": False
                        all_data_golden = False
                else:
                    LOGGER.info("Did not detect that data was golden. Skipping writing bookmark.")
    LOGGER.info("Done syncing %s for view_id %s", report['name'], report['profile_id'])
