# 0057: Fix Bronze→Silver Dedup and Cash Extraction Bugs

## Context

The bronze→silver transform had three bugs:

1. **No date filtering**: Every `transform` run processed all historical raw rows (not just the latest snapshot), mixing stale data from multiple `fetched_at` timestamps into the normalized output. Raw tables accumulate via `mode="append"`, so data from different fetch dates was blended together.

2. **IBKR demo missing cash**: Demo Flex responses contain only a `BASE_SUMMARY` entry in `<CashReport>` — no per-currency entries. `parse_cash_report` filtered out `BASE_SUMMARY` via the `_IS_CURRENCY_RE` regex, producing zero cash rows for single-currency demo accounts.

3. **T212 demo missing cash**: The Trading 212 demo API returns `cash` as a nested dict `{"availableToTrade": 10500.0, ...}` rather than a scalar. `cash_value()` found `"cash"` via its key-probing loop but `as_float(dict)` returned `0.0`, so no cash row was created.

These bugs were identified in `docs/roadmaps/0003-transform-cash-and-dedup.md`.

## Decision

- **Dedup**: Add `filter_latest_snapshot(raw)` in `pipeline/connectors/transform_utils.py` that keeps only rows with the maximum `fetched_at` value. Call it at the start of each connector's `transform_snapshot` (IBKR, T212, XTB). Do NOT apply to `transform_cdc` — CDC data is chronological, not snapshot-based.

- **IBKR cash fallback**: Change `parse_cash_report` to return a `CashReportResult` dataclass with `per_currency` and `base_summary` fields instead of discarding summary rows. When `per_currency` is empty but `base_summary` is non-empty, synthesize a cash entry from `BASE_SUMMARY.endingCash` using the account's base currency. This avoids double-counting when per-currency entries exist.

- **T212 cash dict**: Replace the multi-key probing loop in `cash_value()` with a direct check on the `"cash"` key. When `summary["cash"]` is a dict, drill into `"availableToTrade"`. When it's a scalar, use it directly. Removed `"free"`, `"availableFunds"`, `"available"`, `"totalCash"` key probing since `"cash"` is the key the actual API returns.

## Constraints

- Raw table write mode stays `mode="append"` — dedup happens at read time in the transform step.
- CDC transforms are not filtered — CDC rows are chronological events, not replaceable snapshots.
- Existing live-account behavior must not regress: per-currency cash entries still work for IBKR, scalar `cash` still works for T212.
- `cash_value()` only handles the `"cash"` key — no fallback to `"free"` or other keys.

## Consequences

- **Positive**: Demo accounts for IBKR and T212 now produce cash rows. Normalized output reflects only the latest snapshot. Simpler `cash_value()` is easier to understand and maintain.
- **Negative**: If the T212 API ever changes the `cash` key name, `cash_value()` will need updating. The `CashReportResult` return type is a breaking change for any caller expecting a plain list.
- **Follow-up**: The productionization roadmap Phase 3 (data quality gates) should add schema validation to catch missing cash rows early.

## Validation

- `test_filter_latest_snapshot` — 5 tests covering empty, single, same-timestamp, and multi-timestamp tables
- `test_parse_cash_report_base_summary_only` — only BASE_SUMMARY produces no per-currency entries but captures base_summary
- `test_parse_cash_report_mixed_entries` — per-currency and BASE_SUMMARY are separated correctly
- `test_transform_produces_cash_from_base_summary_fallback` — demo IBKR XML produces a CASH row
- `test_transform_skips_base_summary_when_per_currency_exists` — no double-counting regression
- `test_transform_base_summary_with_currency_override` — CASH row uses override currency
- `test_cash_value_nested_dict_available_to_trade` — nested dict returns availableToTrade value
- `test_cash_value_nested_dict_no_available_to_trade` — nested dict without key returns 0.0
- `test_transform_produces_cash_from_nested_cash_dict` — T212 transform with nested cash dict produces CASH row
- All existing connector tests pass without modification (except T212 test fixture updated from `"free"` to `"cash"`)