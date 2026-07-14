# 0072 — DQ Table Overwrite and Empty-Table Freshness

## Context

The data quality (DQ) table used `mode="append"` to accumulate validation results across pipeline runs. All analytics tables (dividend_income, portfolio_holdings, etc.) use `mode="overwrite"`. This mismatch caused stale DQ warnings to persist in the report even after the underlying data was refreshed — the DQ chart and detail table showed historical warnings that no longer reflected the current data state.

Additionally, the freshness check treated empty tables (e.g., dividend_income in demo where no DIVIDEND events exist) as WARN with "Freshness column 'calculated_at' has all null values". An empty table cannot be stale — there is nothing to age. This produced a permanent WARN in the demo environment.

A pre-existing bug compounded the issue: `col.length == 0` compared a PyArrow `ChunkedArray.length` *method* to 0 (always False), so the empty-table early-return path was never reached. All empty tables fell through to the Polars `max()` check, which returns `None` for an empty column, producing the misleading "all null values" WARN.

## Decision

1. **Change DQ table write mode from `append` to `overwrite`.** Each validation run replaces the DQ table entirely, so the DQ chart and report always reflect the latest validation state — no stale warnings.

2. **Make empty-table freshness check return PASS.** Changed the status from WARN to PASS and the message to "Table {name} is empty; freshness not applicable". Empty data is not stale data.

3. **Fix `col.length == 0` bug** by replacing with `len(col) == 0`, which correctly checks PyArrow column length.

The row-count stability check (`_get_previous_row_count`) reads the existing DQ table *before* new results are written, so it still finds the previous run's data before overwrite. No change needed there.

## Constraints

- The DQ table must still contain all checks from the most recent validation run (schema, required_nulls, row_count_stability, freshness, reconciliation).
- Row-count stability comparison against previous runs must continue to work. Since `_get_previous_row_count` reads the DQ table before the overwrite, it always finds the prior run's results.
- The "all null values" path for non-empty tables (where the column exists but all values are null) remains WARN — this indicates a data integrity issue, not an empty-table scenario.

## Consequences

- DQ history is no longer retained across runs. This is intentional: the report should show current data quality, not historical accumulation.
- Empty tables (dividend_income, interest_income, cash_flow_summary when no relevant events exist) now show PASS for freshness instead of WARN.
- The demo DQ chart will no longer show permanent warnings for tables without data.

## Validation

- `test_empty_table_passes_freshness` in `tests/test_quality.py` verifies that an empty table returns PASS for the freshness check.
- All 26 quality tests pass, including the round-trip test that writes and reads back DQ results.
- Full test suite (569 tests) passes with no regressions.