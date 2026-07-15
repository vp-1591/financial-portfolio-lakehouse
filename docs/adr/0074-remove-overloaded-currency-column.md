# 0074 — Remove Overloaded `currency` Column, Rename to Explicit Names

> **Superseded by [ADR 0077](./0077-currency-unification-phase2-schema-redesign.md)** — ADR 0077 redesigns the schema with clearer column names (security_ccy, target_ccy) that replace the names introduced here (value_currency, base_currency, security_currency).

## Context

The `currency` column was overloaded across the pipeline — it meant "account base currency" in IBKR snapshots, "wallet currency" in T212, "position currency" in XTB, "target currency after FX" in `consolidated_holdings`, and "native value currency" in `portfolio_holdings` and CDC tables. This ambiguity caused a visible bug where the "Allocation by Currency" chart grouped T212 positions by wallet currency (PLN) instead of instrument currency (GBX/GBP) — fixed in Phase 1 (ADR 0073).

Phase 1 switched the chart to use `security_currency` and rendered it as a donut. Phase 2 removes the overloaded `currency` column entirely from snapshot schemas, and renames it to unambiguous names in all downstream tables.

## Decision

Remove the `currency` column from all three snapshot schemas, keeping `value_currency` and `security_currency` (which were already present). Rename the overloaded `currency` column in downstream tables:

| Table | Old column | New column |
|---|---|---|
| `consolidated_holdings` | `currency` | `base_currency` |
| `portfolio_holdings` | `currency` | `value_currency` |
| `cdc_events_normalized` | `currency` | `value_currency` |
| `dividend_income` | `currency` | `value_currency` |
| `interest_income` | `currency` | `value_currency` |
| `cash_flow_summary` | `currency` | `value_currency` |

Remove the `real_currency()` dead code from `pipeline/normalized/consolidate.py` and its test.

The `Holding` dataclass field `currency` is **not renamed** — it is an in-memory value object, not a table column, and renaming it would cascade through every connector and test for no schema benefit.

The `extract_holdings()` fallback `row.get("value_currency", row.get("currency", ""))` is simplified to `row.get("value_currency", "")` since `currency` no longer exists in snapshots. For XTB (which has no `security_currency` field), the fallback becomes `row.get("security_currency", row.get("value_currency", ""))`.

A migration script (`scripts/migrate_rename_currency_columns.py`) is provided for existing Delta tables, supporting `--dry-run`. Local/demo users can also delete the `data/` directory and re-run the pipeline.

## Constraints

- Connectors must keep working (IBKR, T212, XTB fetch/transform/extract).
- No FX conversion logic changes — only column names change.
- `Holding.currency` dataclass field is unchanged.
- Existing Delta tables require migration (script provided).
- The `Allocation by Currency` donut chart (Phase 1) must still render correctly.

## Consequences

- Every currency column now has one unambiguous name: `value_currency` (the currency of a monetary amount), `security_currency` (the trading currency of an instrument), or `base_currency` (the target currency after FX conversion).
- The `check_reconciliation` quality check now compares `base_currency` (consolidated holdings) against `value_currency` (CDC events) — the two sides renamed to **different** names, reflecting their distinct semantics.
- Existing data lakes must be migrated or rebuilt.
- Downstream queries that referenced `currency` must be updated to use the new column names.

## Validation

- All 571 existing tests pass with the renamed columns.
- `pipeline validate` on demo data reports no schema mismatches.
- `pipeline report --output data/report.html` renders correctly with the "Currency Exposure" donut using `security_currency`.
- Migration script `--dry-run` mode identifies all affected tables without modifying them.