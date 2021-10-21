# Changelog

## 1.1.1
  * Add Request Timeout Option

## 1.1.0
  * Add a string fallback schema to integer override fields [#61](https://github.com/singer-io/tap-google-analytics/pull/61)

## 1.0.5
  * When loading a file via `pkgutil` it must be decoded from bytes [#57](https://github.com/singer-io/tap-google-analytics/pull/56)

## 1.0.4
  * Fix inclusion of `ga_cubes.json` and remove dynamic pulling of the file for reliability [#56](https://github.com/singer-io/tap-google-analytics/pull/56)

## [v1.0.2](https://github.com/singer-io/tap-google-analytics/tree/v1.0.2) (2021-01-05)

[Full Changelog](https://github.com/singer-io/tap-google-analytics/compare/v1.0.1...v1.0.2)

**Merged pull requests:**

- Properly retry on 503 and add unit tests for all status codes that should retry under all conditions [\#43](https://github.com/singer-io/tap-google-analytics/pull/43) ([asaf-erlich](https://github.com/asaf-erlich))
- move time off of utc midnight [\#41](https://github.com/singer-io/tap-google-analytics/pull/41) ([kspeer825](https://github.com/kspeer825))

## 1.0.3
  * Wraps access token request in a requests session [#45](https://github.com/singer-io/tap-google-analytics/pull/45)

## 1.0.1
  * Grabs error reason from 'reason', 'error_description', or falls back to full json response to ensure a good error message [#39](https://github.com/singer-io/tap-google-analytics/pull/39)
  * add context user [#38](https://github.com/singer-io/tap-google-analytics/pull/38)

## 1.0.0
  * Releasing to GA

## 0.5.1
  * Update bookmarking [#35](https://github.com/singer-io/tap-google-analytics/pull/35)
    * change bookmarking strategy
    * Add historical sync concept to bookmarking
    * Make new test fail, and add interrupted test
    * Fix broken test
    * Change historical syncing to not save bookmarks, but not prevent bookmarks from being saved

## 0.5.0
  * Cache Management API profile lookup [#34](https://github.com/singer-io/tap-google-analytics/pull/34)

## 0.4.7
  * Make fewer request to the Management API for discovery [#32](https://github.com/singer-io/tap-google-analytics/pull/32)

## 0.4.6
  * Make retries less aggressive to conserve quota [#31](https://github.com/singer-io/tap-google-analytics/pull/31)

## 0.4.5
  * All 4xx errors that are not retryable should show error message [#29](https://github.com/singer-io/tap-google-analytics/pull/29)

## 0.4.4
  * Fixes error structure issue with retryable 403s introduced in 0.4.3 [#26](https://github.com/singer-io/tap-google-analytics/pull/26)

## 0.4.3
  * Retry certain 403 error responses for the management and metadata APIs [#24](https://github.com/singer-io/tap-google-analytics/pull/24)

## 0.4.2
  * Correctly parsing datetime fields in the `Time` group using Google's compressed format [#22](https://github.com/singer-io/tap-google-analytics/pull/22)

## 0.4.1
  * Removed `ga:searchKeyword` from `Behavior Overview` default dimensions to closer match the Google Analytics UI report [#20](https://github.com/singer-io/tap-google-analytics/pull/20)

## 0.4.0
  * Add ability to specify multiple profile IDs (e.g., `view_ids`) in config [#18](https://github.com/singer-io/tap-google-analytics/pull/18)
    * Note: Custom metrics/dimensions and goal-related fields will be discovered as the intersection of all fields for all selected profiles. Selecting profiles across properties may result in some custom fields being marked `unsupported`
    * If custom fields between profiles have different data types, their JSON schemas will be merged into an `anyOf` schema
  * Will translate state to multiple profile format if not already formatted [#18](https://github.com/singer-io/tap-google-analytics/pull/18)
  * Add `currently_syncing` and `currently_syncing_view` for resuming if many reports and profiles are selected [#18](https://github.com/singer-io/tap-google-analytics/pull/18)
  * Increase 429 retry count to 10 [#18](https://github.com/singer-io/tap-google-analytics/pull/18)

## 0.3.0
  * Add pre-made reports that are always returned during discovery [#16](https://github.com/singer-io/tap-google-analytics/pull/16)
  * Report day-pagination now includes current day [#16](https://github.com/singer-io/tap-google-analytics/pull/16)
  * Make raw report data parsing safe for reports without dimensions [#16](https://github.com/singer-io/tap-google-analytics/pull/16)

## 0.2.1
  * Handles top-level exceptions through Singer function [#14](https://github.com/singer-io/tap-google-analytics/pull/11)

## 0.2.0
  * Adds a 'report_definitions' config parameter to add support for syncing multiple reports [#11](https://github.com/singer-io/tap-google-analytics/pull/11)
