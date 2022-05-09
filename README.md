# tap-google-analytics

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

How to use it:
- `tap-google-analytics` works together with any other [Singer Target](https://singer.io) to move data from Google Analytics API to any target destination.
- Extracts the following pre-made and any self-made reports from [Google Analytics Reporting API](https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet):
  - Audience Overview
  - Audience Geo Location
  - Audience Technology
  - Acquisition Overview
  - Behavior Overview
  - Ecommerce Overview
## Configuration

This tap requires a `config.json` which specifies details regarding [OAuth 2.0](https://developers.google.com/analytics/devguides/reporting/core/v4/authorization#OAuth2Authorizing) authentication, a cutoff date for syncing historical data, and an optional flag which controls the method of authentication. See [config.sample.json](config.sample.json) for an example. You may specify an API key instead of OAuth parameters for development purposes, as detailed below.

To run the discover mode of `tap-google-analytics` with the configuration file, use this command:

```bash
$ tap-google-analytics -c my-config.json -d
```

To run the sync mode of `tap-google-analytics` with the catalog file, use the command:

```bash
$ tap-google-analytics -c my-config.json --catalog catalog.json
```

## Service Account Authentication

Service accounts are useful for automated, offline, or scheduled access to Google Analytics data for your own account. For example, to build a live dashboard of your own Google Analytics data and share it with other users.
See the [Google Analytics Service Accounts](https://developers.google.com/analytics/devguides/reporting/core/v4/authorization#service_accounts) section for more information on how to set up the service account.

To use an API key, include a `private_key` and `client_email` configuration variables in your `config.json` and set it to the value of your credentials.

---

Copyright &copy; 2019 Stitch
