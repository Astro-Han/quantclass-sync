# Update Catch-Up To Latest Design

## Context

Current `update` behavior resolves one `date_time` per product and runs at most one download-and-sync cycle.  
When local data is behind by multiple days, a single run may skip missing middle dates.

## Goal

Change default `update` behavior to catch up from local timestamp to API latest in one run.

## Validated Decisions

1. Trigger model:
`python3 quantclass_sync.py update` should catch up by default.
2. Catch-up strategy:
Use a hybrid strategy.
First consume date candidates from `latest`.
If candidate list is unavailable or clearly incomplete, fallback to day-by-day probe.
3. Failure policy:
Strict mode for real failures. Stop immediately and return non-zero.
4. Data-gap semantics:
"No data for that calendar day" is not treated as a hard failure.

## High-Level Architecture

Add a date-queue resolution layer before per-product execution:

1. Resolve local baseline date from `<data_root>/<product>/timestamp.txt`.
2. Query API latest payload and parse all candidate date strings.
3. Build `date_queue`:
   - primary: all valid candidate dates strictly newer than local date
   - fallback: probe each calendar date from local+1 to latest and keep downloadable dates
4. Execute existing `process_product` once per `date_queue` item in ascending order.
5. After each successful date:
   - write status DB fields (`data_time`, `data_content_time`, `last_update_time`)
   - write local `timestamp.txt`

## Scope

In scope:

- Default `update` behavior
- Internal date resolution helpers
- Report/log enrichment for catch-up progress
- Regression tests
- README behavior update

Out of scope:

- New CLI flags
- Changing merge behavior
- Parallelizing catch-up dates

## Error Handling

Hard failures (strict stop):

- network/auth/permission errors
- download link failures
- archive extraction failures
- merge/write failures

Soft skips:

- probed date returns no downloadable artifact

## Test Strategy

1. Unit tests for date parsing and date queue resolution.
2. Unit tests for strict stop behavior in multi-date run.
3. Unit tests for "no data day" skip behavior in fallback probe.
4. Regression checks for existing single-date `one_data --date-time`.

