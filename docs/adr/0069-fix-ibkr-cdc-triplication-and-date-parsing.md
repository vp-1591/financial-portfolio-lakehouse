# 0069 — Fix IBKR CDC Triplication and Date Parsing

## Context

Two bugs caused the Passive Income Timeline chart to render empty:

1. **IBKR CDC records were triplicated.** The IBKR Flex CDC query returns the full account history on every fetch. Each pipeline run appends a new raw payload, and `transform_cdc()` iterates over all payloads, emitting the same events multiple times. With 4 raw payloads, every event appeared 3 times in the normalized table.

2. **IBKR `event_datetime` formats weren't parsed.** IBKR Flex returns `dateTime` as `YYYYMMDD` (e.g. `20260204`) for CashTransactions and `YYYYMMDD;HHMMSS` (e.g. `20260702;022904`) for Trades. Neither format matched the three patterns in `_add_period_columns()`, so all INTEREST and TRADE rows were silently dropped, leaving the analytics tables empty.

## Decision

- **Normalize IBKR `event_datetime` in the transform layer.** A new `_normalize_ibkr_datetime()` helper converts compact formats to ISO 8601 (`20260204` → `2026-02-04T00:00:00Z`, `20260702;022904` → `2026-07-02T02:29:04Z`). Strings already in standard formats pass through unchanged. This is applied in all four `_process_ibkr_*` functions.

- **Add compact format patterns in the analytics layer.** Two new `str.strptime` patterns (`%Y%m%d` and `%Y%m%d %H%M%S`) are added to `_add_period_columns()` as a safety net for any data that bypasses the transform normalization. Semicolons are replaced with spaces before parsing since Polars doesn't support semicolons in format strings.

- **Deduplicate CDC events by `event_id` using Polars.** After `build_normalized_table()`, the result is converted to a Polars DataFrame, sorted by `fetched_at` descending, deduplicated on `event_id`, and sorted by `event_id` for deterministic row order. This removes triplicated events while keeping the latest version.

## Constraints

- Other brokers (T212, XTB) have truly incremental CDC data and must not be affected. The dedup is IBKR-specific.
- The fix must not break existing tests or the fixture data.
- The normalized table is overwritten on each run, so no migration is needed for existing Delta tables.

## Consequences

- The Passive Income Timeline chart will show interest data once the pipeline is re-run.
- Trade and fee analytics will also populate correctly.
- Event counts and amounts in analytics tables will no longer be inflated by duplication.
- The IBKR fixture now uses realistic compact datetime formats (`20260301` for CashTransactions, `20260115;103000` for Trades), making tests more representative.

## Validation

- `tests/test_ibkr_connector.py`: `TestNormalizeIbkrDatetime` (6 tests), `TestCdcTransform::test_transform_cdc_deduplicates_across_payloads`, `TestCdcTransform::test_transform_cdc_normalises_compact_datetime`
- `tests/test_cdc_analytics.py`: `TestDateParsing::test_ibkr_compact_date_format`, `test_ibkr_compact_datetime_format`, `test_ibkr_normalised_iso_parsed`
- Full test suite: 551 tests pass
- Ruff lint and format: clean