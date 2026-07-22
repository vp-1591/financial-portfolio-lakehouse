# Roadmap: Currency Column Clarity and Allocation Chart Fix

## Goal

Eliminate the overloaded `currency` column across the pipeline by renaming it to unambiguous names (`value_currency` for "the currency of the amount" and `security_currency` for "the instrument's trading currency"), and fix the Allocation by Currency chart to show currency exposure by instrument currency rather than wallet currency, using a donut chart consistent with the other allocation charts.

## Current state

The `currency` column is overloaded — it means different things at different pipeline stages:

| Stage | `currency` means | Problem |
|-------|-------------------|---------|
| Snapshot (IBKR) | Account base currency (e.g., EUR) | Not used downstream; `value_currency` has the position's native currency |
| Snapshot (T212) | Wallet/payment currency (e.g., PLN) | Same as `value_currency` — completely redundant |
| Snapshot (XTB) | Position currency (e.g., USD) | Same as `value_currency` — completely redundant |
| Consolidated holdings | Target currency after FX conversion (e.g., EUR) | Unclear name; `portfolio_holdings` already calls this `base_currency` |
| Portfolio holdings | Snapshot's `value_currency` | Ambiguous; same column name as consolidated but different meaning |
| CDC tables | Transaction currency | Clear in context, but inconsistent with other tables |

This causes a visible bug: the "Allocation by Currency" chart groups by `portfolio_holdings.currency` (which for T212 is the wallet currency PLN, not the instrument's trading currency). A UK stock on a PLN-denominated T212 account appears under "PLN" instead of "GBX" or "GBP".

Additionally, the chart uses a bar chart with EUR values labeled by currency code — confusing because a reader can't tell if "PLN: 5,000" means 5,000 PLN or 5,000 EUR in PLN-denominated assets. The other two allocation charts (by broker, by position type) are donut charts with percentages, which are clearer.

Relevant ADRs: [ADR 0046](../adr/0046-fix-consolidated-currency-column.md) (fixed `consolidated_holdings.currency` to mean target currency), [ADR 0066](../adr/0066-portfolio-holdings-gold-table-and-report-generation.md) (added `portfolio_holdings` and report).

## Success criteria

- [ ] The "Allocation by Currency" chart groups by `security_currency` and renders as a donut chart with `label+percent`
- [ ] T212 positions with GBX/GBP instruments on a PLN account appear under "GBX"/"GBP" (not "PLN") in the chart
- [ ] No `currency` column exists in any snapshot schema — only `value_currency` and `security_currency`
- [ ] No `currency` column exists in `consolidated_holdings` — replaced by `base_currency`
- [ ] No `currency` column exists in `portfolio_holdings` — replaced by `value_currency`
- [ ] No `currency` column exists in CDC analytics schemas — replaced by `value_currency`
- [ ] All existing tests pass after the rename
- [ ] `real_currency()` dead code and its orphaned test are removed
- [ ] `pipeline report --output data/report.html` on demo data produces a visible donut chart for currency allocation

## Alternatives considered

| Approach | Why rejected |
|----------|-------------|
| Keep `currency` column but add comments | Comments don't prevent misuse; the T212 bug proves the overload is actively harmful |
| Switch chart to use `value_currency` instead of `security_currency` | Would still group by wallet currency for T212, which is the original bug |
| Remove `currency` from snapshots only, keep in gold/CDC tables | Inconsistent naming across layers is the root problem; half-fixes invite future confusion |

## Phases

### Phase 1 — Fix allocation chart bug and switch to donut *[status: done]*

Fix the immediate bug where the currency allocation chart groups by wallet currency instead of instrument currency, and switch the chart type from bar to donut for consistency with the other allocation charts.

**Scope:**
- [ ] Change `allocation_by_currency()` in `pipeline/report/charts.py` to `group_by("security_currency")` instead of `group_by("currency")`
- [ ] Change the chart from `go.Bar` to `go.Pie` with `hole=0.4` and `textinfo="label+percent"`, matching `allocation_by_broker` and `allocation_by_position_type`
- [ ] Update chart title to "Currency Exposure" to clarify it shows instrument currency, not wallet currency
- [ ] Update the `yaxis_title` / axis labels accordingly
- [ ] Add/update tests for the chart change
- [ ] Run `pipeline report --output data/report.html` on demo data and verify the donut chart renders correctly

**Out of scope:**
- Renaming columns in schemas or pipeline code
- Changing any other charts
- CDC table changes

**Files:** `pipeline/report/charts.py`, `tests/test_charts.py`

**Links:** ADR 0066

---

### Phase 2 — Remove `currency` column and rename to explicit names across all layers *[status: done]*

Remove the redundant `currency` column from snapshot schemas and rename the overloaded `currency` column in consolidated holdings, portfolio holdings, and CDC tables to unambiguous names. These changes are tightly coupled — removing `currency` from snapshots requires updating `holdings.py` which also renames the column in `portfolio_holdings`.

**Scope:**
- [ ] Remove `currency` field from all three snapshot schemas in `pipeline/normalized/models.py` (`ibkr_snapshot_normalized_schema`, `trading212_snapshot_normalized_schema`, `xtb_snapshot_normalized_schema`)
- [ ] Stop populating `currency` in all three broker transforms:
  - `pipeline/connectors/ibkr/transform.py` — remove `"currency": base_currency`
  - `pipeline/connectors/trading212/transform.py` — remove `"currency": position_currency(...)`
  - `pipeline/connectors/xtb/transform.py` — remove `"currency": pos.currency`
- [ ] Rename `consolidated_holdings.currency` to `base_currency` in `pipeline/normalized/models.py` and `pipeline/normalized/consolidate.py`
- [ ] Rename `portfolio_holdings.currency` to `value_currency` in `pipeline/analytics/models.py` and `pipeline/analytics/holdings.py`
- [ ] Keep `portfolio_holdings.base_currency` — it labels what currency `value_base` is in and is used by the report summary table
- [ ] Rename `currency` to `value_currency` in `cdc_events_normalized_schema` in `pipeline/normalized/models.py` and all CDC transform code
- [ ] Rename `currency` to `value_currency` in `dividend_income_schema`, `interest_income_schema`, `cash_flow_summary_schema` in `pipeline/analytics/models.py`
- [ ] Update `pipeline/analytics/holdings.py` — remove the `value_currency if ... else "currency"` fallback at line 121-122 since `value_currency` is now the only column; rename the output column from `"currency"` to `"value_currency"`
- [ ] Update `pipeline/analytics/cdc_tables.py` — change all `currency` column references to `value_currency`
- [ ] Update `pipeline/analytics/quality.py` — change `currency` references to `value_currency` in quality checks
- [ ] Update `pipeline/connectors/xtb/connector.py` — remove fallback to `currency` column (`row.get("value_currency", row.get("currency", ""))` becomes `row.get("value_currency", "")`)
- [ ] Update `pipeline/connectors/ibkr/connector.py` — same fallback removal
- [ ] Update `pipeline/report/charts.py` — change any remaining `currency` references to `value_currency`
- [ ] Update `pipeline/report/loader.py` — update column name dicts for CDC tables
- [ ] Update `pipeline/report/renderer.py` — update any `currency` references in the summary table and passive income table
- [ ] Remove `real_currency()` dead code from `pipeline/normalized/consolidate.py` and its test from `tests/test_consolidate.py`
- [ ] Update all affected tests
- [ ] Run full test suite and verify no regressions

**Out of scope:**
- Changing the report layout or adding new charts
- Adding new currency-related features
- Changing the FX conversion logic

**Files:** `pipeline/normalized/models.py`, `pipeline/normalized/consolidate.py`, `pipeline/connectors/ibkr/transform.py`, `pipeline/connectors/trading212/transform.py`, `pipeline/connectors/xtb/transform.py`, `pipeline/analytics/models.py`, `pipeline/analytics/holdings.py`, `pipeline/analytics/cdc_tables.py`, `pipeline/analytics/quality.py`, `pipeline/connectors/xtb/connector.py`, `pipeline/connectors/ibkr/connector.py`, `pipeline/report/charts.py`, `pipeline/report/loader.py`, `pipeline/report/renderer.py`, `tests/`

**Links:** ADR 0046, ADR 0066

---

### Phase 3 — Verify report output *[status: planned]*

End-to-end verification that the report renders correctly with the renamed columns and the new donut chart.

**Scope:**
- [ ] Verify the "Currency Exposure" donut chart (from Phase 1) still works — it uses `security_currency` which is unchanged
- [ ] Verify `_summary_table()` still reads `base_currency` correctly for EUR labels
- [ ] Verify the DQ section uses `value_currency` correctly
- [ ] End-to-end test: run `pipeline report --output data/report.html` and visually confirm all sections render correctly

**Out of scope:**
- Adding new report sections
- Changing report styling

**Files:** `pipeline/report/templates/report.html`, `pipeline/report/renderer.py`, `pipeline/report/charts.py`

**Links:** ADR 0066