# 0064 — CDC Analytics Tables and Unified Analytics Command

## Context

Phase 1 (data quality framework) is complete. Phase 2 of the reporting baseline roadmap requires gold-layer analytics tables derived from `cdc_events` to power the cash-flow-based charts in the report. The pipeline previously had a `pipeline allocate` command that only produced the `portfolio_allocation` table. Adding three new CDC analytics tables alongside it created a question: should they be a separate command or merged into the existing one?

Having two separate commands (`allocate` + a new CDC analytics command) would create confusion about pipeline order and add unnecessary subcommands. The `allocate` command was already the gold-layer generation step — it makes sense to expand it to produce all gold tables under a single `analytics` command.

## Decision

1. **Rename `pipeline allocate` to `pipeline analytics`**. The new command builds all gold tables: `portfolio_allocation`, `dividend_income`, `interest_income`, and `cash_flow_summary`.

2. **Rename `run-consolidate-allocate` to `run-consolidate-analytics`** to match the new command name. This is a breaking change for the Step Functions orchestrator, coordinated with Terraform config updates.

3. **Create three new CDC analytics tables** with these schemas:
   - `dividend_income`: dividends grouped by period (YYYY-MM, YYYY-QN), broker, security, and currency
   - `interest_income`: interest grouped by period, broker, and currency
   - `cash_flow_summary`: all CDC event types grouped by period, broker, event type, and currency

4. **CDC tables are optional**: if `cdc_events` doesn't exist, `pipeline analytics` logs a warning and continues. Allocation still succeeds.

5. **Period columns use string format**: `period_month` as `YYYY-MM` and `period_quarter` as `YYYY-QN`. Human-readable, sortable, directly usable as chart labels.

6. **`amount_base` fallback strategy**: where `amount_base` is null in CDC events, compute `cash_amount × fx_rate_to_base` as a fallback. Where both are null, leave `amount_base` null in the analytics table.

7. **All analytics tables use decrypted (plain float) columns**. Encryption is a normalized-layer concern; the gold layer stores aggregated values ready for consumption.

## Constraints

- Must not break existing `portfolio_allocation` production (the `allocate_percentages` function is unchanged)
- The `analytics` command must still accept `--fx-rate`, `--isin`, and `--isin-map-file` arguments (for allocation)
- Step Functions orchestrator references must be updated alongside the CLI rename
- The new tables must be registered in the data quality validation framework

## Consequences

- **Simpler pipeline**: one command (`analytics`) builds all gold tables instead of two separate commands
- **Breaking change**: `pipeline allocate` and `pipeline run-consolidate-allocate` are removed. Scripts and Step Functions must use `analytics` and `run-consolidate-analytics`
- **CDC tables depend on `cdc_events`**: if CDC events haven't been consolidated, the three new tables won't be produced (warning, not error)
- **Date format handling**: `event_datetime` parsing supports IBKR (`2026-03-01 00:00:00`), XTB (`2024-01-15`), and T212 (`2024-01-15T10:30:00Z`) formats. Unparseable rows are excluded with a warning

## Validation

- `tests/test_cdc_analytics.py`: unit tests for all three table builders, date parsing, null handling, schema correctness, and Delta table round-trip
- `tests/test_run_subcommands.py`: updated to test `cmd_analytics` and `cmd_run_consolidate_analytics`
- `tests/test_quality.py`: updated to include the three new tables in the quality validation fixture
- `pipeline validate` will check the three new analytics tables for schema, nulls, row count stability, and freshness