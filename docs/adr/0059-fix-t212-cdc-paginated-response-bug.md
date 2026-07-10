# 0059 — Fix T212 CDC paginated response bug and test isolation leak

## Context

Both T212 and IBKR CDC normalized tables in the demo S3 bucket were empty despite having data in their raw tables. Investigation revealed two distinct issues:

1. **T212 transform skipped all CDC events.** The T212 API returns paginated responses with `{"items": [...], "nextPagePath": null}`. The `transform_cdc()` function checked `isinstance(events, list)` and skipped dicts entirely. Since all three CDC endpoints (orders, dividends, transactions) return paginated dicts, every event was discarded — producing zero rows.

2. **IBKR Flex query covered only one business day.** The demo Flex CDC query was configured with `period="LastBusinessDay"`, so the response contained empty `<Trades/>`, `<CashTransactions/>`, and `<Transfers/>` sections. The transform correctly parsed the XML but found nothing to map.

A secondary issue was found: `TestFetchConnectorIsolation` in `test_run_subcommands.py` called `fetch_connector()` without the `tmp_data_dir` fixture, causing `LocalBackend.ensure_parent()` to create a `data/raw/` directory in the project root.

## Decision

1. Add a `_unwrap_t212_events()` helper that extracts the `items` list from paginated T212 dict responses. If the payload is a bare list, return it as-is. If it's a dict with an `items` key, extract the list. Otherwise, return an empty list. This matches the pagination unwrapping already done in `Trading212Client._fetch_paginated()` at fetch time, but handles the case where raw payloads are stored before unwrapping.

2. Add `tmp_data_dir` fixture parameter to all three `TestFetchConnectorIsolation` test methods so they use isolated temp directories instead of the project's `data/` directory.

3. The IBKR CDC query date range is a configuration issue (not a code bug), so no code change is needed — the user needs to configure the Flex query to cover a wider date range.

## Constraints

- The T212 transform must handle both bare-list and paginated-dict payloads because `fetch_cdc()` with `capture_raw=True` stores per-request HTTP response bytes, which are the raw paginated dicts.
- Existing T212 CDC transform tests that pass bare lists must continue to work.
- The IBKR transform is correct as-is; the empty sections are a data issue, not a code bug.

## Consequences

- T212 CDC transform now correctly processes events from paginated API responses.
- The `data/raw/` directory is no longer leaked by tests.
- IBKR CDC data will remain empty until the Flex query is configured with a wider date range (e.g., 30+ days or a specific date range that captures historical events).

## Validation

- New `TestUnwrapT212Events` test class with 5 cases: bare list, paginated dict, empty items, dict without items, non-dict/non-list.
- New `test_transform_cdc_unwraps_paginated_dict` and `test_transform_cdc_paginated_dict_with_empty_items` tests in `TestCdcTransform`.
- All 46 T212 connector tests pass, including the new 7 tests.
- All 111 relevant tests pass (T212, IBKR, CDC consolidation, run subcommands).
- Pre-existing `test_secrets.py` failures are unrelated and unchanged.