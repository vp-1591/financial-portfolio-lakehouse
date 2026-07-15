# Roadmap: Gold Table Cleanup, Positions Chart & README Screenshots

## Goal

Consolidate `portfolio_allocation` into `portfolio_holdings` (removing a vestigial table), replace the "Allocation by Position Type" donut with a "Positions" bar chart showing each holding ranked by weight, encrypt gold-layer value columns for consistency with raw/normalized layers, and add representative screenshots to the README so visitors can see the project works.

## Current state

- **`portfolio_allocation`** is vestigial. ADR 0066 created `portfolio_holdings` with absolute values and `position_type`; every chart and the primary report path now reads from `portfolio_holdings`. `portfolio_allocation` is only used as a degraded plain-HTML fallback when `portfolio_holdings` is empty (which should never happen in normal operation). Its pre-computed `percentage` column is never consumed by any chart.
- **No positions chart exists.** The three donut charts show aggregate allocation (by broker, position type, currency), but no chart shows individual positions ranked by portfolio weight. The "Allocation by Position Type" donut shows only EQUITY vs CASH — a single number disguised as a chart. A horizontal bar chart of all holdings (including CASH) would subsume it: the EQUITY/CASH split is visible at a glance as the CASH bar vs everything above it.
- **Gold tables store plaintext values.** ADR 0003 specifies "Analytics layer: no encryption." Raw payloads and normalized value columns are Fernet-encrypted; gold tables (`portfolio_holdings`, `dividend_income`, etc.) store decrypted `Float64` values. This creates an inconsistency: if an attacker gains S3 access without the encryption key, they can read all financial amounts directly from gold tables. Encrypting only the numeric value columns (not the whole table) preserves queryability for structure/metadata while protecting monetary amounts. With a plaintext `percentage` column, the allocation donut charts and positions chart still render without decryption — only the absolute-value charts (Passive Income, Cash Flow, Total Value card) require the key.
- **README has no visual proof.** The project description is text-only; no screenshot shows that the pipeline produces a working interactive report.

### Relevant ADRs

- [ADR 0003](../adr/0003-medallion-architecture.md) — Medallion architecture (states "Analytics layer: no encryption")
- [ADR 0066](../adr/0066-portfolio-holdings-gold-table-and-report-generation.md) — Created `portfolio_holdings`; made `portfolio_allocation` vestigial
- [ADR 0073](../adr/0073-currency-exposure-donut-chart.md) — Fixed currency exposure chart to group by `security_ccy`

## Success criteria

- [ ] `portfolio_allocation` table, builder, loader, schema, and all references are removed; `pipeline analytics` no longer produces it
- [ ] `portfolio_holdings` includes a `percentage` column (Float64, rounded to 4 decimal places) representing each position's weight in the total portfolio
- [ ] The "Allocation by Position Type" donut chart is replaced by a "Positions" horizontal bar chart showing all holdings (including CASH) ranked by percentage, with ticker labels and percentage annotations; the EQUITY/CASH split is visible as the CASH bar vs the equity bars
- [ ] An EQUITY/CASH summary card (e.g. "Equity 87.3% · Cash 12.7%") is shown in the portfolio section, preserving the single number the donut provided
- [ ] The degraded fallback path (plain HTML table from `portfolio_allocation` when holdings are empty) is removed from the renderer
- [ ] Data quality validation no longer references `portfolio_allocation`
- [ ] All existing tests pass after the merge; tests covering `portfolio_allocation` are updated or removed
- [ ] Gold-layer value columns (`security_value`, `target_value`, `cash_amount`, `gross_amount`, `fee_amount`, `tax_amount`, `price`, `quantity`, `target_fx_rate`) are Fernet-encrypted at rest, matching the normalized layer pattern; non-value columns (`percentage`, `ticker`, `broker`, `security_ccy`, etc.) remain plaintext
- [ ] Allocation donut charts (by broker, by currency) and positions chart render from the plaintext `percentage` column without needing the decryption key; absolute-value charts (Passive Income, Cash Flow, Total Value card) decrypt at render time
- [ ] An ADR documents the gold-layer encryption decision (superseding ADR 0003's "Analytics layer: no encryption")
- [ ] `pipeline/query.py` decrypts gold value columns on read so DuckDB ad-hoc queries still work
- [ ] README contains 2–4 screenshots of the demo report showing key sections
- [ ] The table lineage diagram (`docs/table-lineage.md`) is updated to remove `portfolio_allocation` and add the positions chart

## Alternatives considered

| Approach | Why rejected |
|----------|-------------|
| Keep `portfolio_allocation` alongside `portfolio_holdings` | Two tables computing the same data from the same source; `allocation` is never used by any chart and creates confusion about which is the source of truth |
| Add `percentage` to `portfolio_holdings` but keep `portfolio_allocation` as a materialized view | Redundant storage and maintenance; no consumer reads the allocation table |
| Keep "Allocation by Position Type" donut alongside new positions chart | Two-slice EQUITY/CASH donut is a single number disguised as a chart; the positions bar chart subsumes it (CASH bar vs equity bars shows the same split). Replacing frees visual space and removes redundancy. |
| Keep gold values as plaintext (ADR 0003 status quo) | An attacker with S3 access can read all financial amounts directly from gold Delta tables without needing the encryption key. This defeats the purpose of encrypting raw/normalized layers. |
| Encrypt entire gold tables (all columns) | Metadata columns (`ticker`, `broker`, `security_ccy`, `percentage`, `position_type`) are non-sensitive and needed for grouping/filtering; encrypting them adds complexity with no security benefit. |
| Screenshot the entire report as one image | Too large; key information gets lost at thumbnail size. Focused section screenshots are more scannable. |

## Phases

### Phase 1 — Fold `portfolio_allocation` into `portfolio_holdings` *[status: planned]*

Merge the vestigial `portfolio_allocation` gold table into `portfolio_holdings` by adding a `percentage` column, then remove the allocation table entirely.

**Scope:**
- [ ] Add `percentage` (Float64) column to `portfolio_holdings_schema` in `pipeline/analytics/models.py`
- [ ] Compute `percentage` in `pipeline/analytics/holdings.py::build_portfolio_holdings()` as `(target_value / total_target_value) * 100`, rounded to 4 decimal places
- [ ] Add `percentage` to `_PORTFOLIO_HOLDINGS_COLUMNS` in `pipeline/report/loader.py`
- [ ] Remove `portfolio_allocation_schema` from `pipeline/analytics/models.py`
- [ ] Delete `pipeline/analytics/allocation.py`
- [ ] Remove `portfolio_allocation_schema` from `pipeline/analytics/__init__.py` exports
- [ ] Remove `load_portfolio_allocation()` from `pipeline/report/loader.py` and the `_PORTFOLIO_ALLOCATION_COLUMNS` dict
- [ ] Remove `portfolio_allocation` from `load_all()` return dict
- [ ] Remove `allocation` from `render_report()` — eliminate the fallback path that renders a plain HTML table when holdings are empty
- [ ] Remove `portfolio_allocation` from `pipeline/analytics/quality.py` (`TABLE_SCHEMAS`, `REQUIRED_FIELDS`, `FRESHNESS_COLUMNS`, `all_tables`)
- [ ] Remove `portfolio_allocation` from `pipeline/run.py` (the `allocate_percentages` call in the analytics command)
- [ ] Update `pipeline/report/renderer.py` to remove all `allocation` references and the fallback path
- [ ] Update `docs/table-lineage.md` — remove `portfolio_allocation` node and fallback arrow
- [ ] Update or remove tests covering `portfolio_allocation`; add/update tests for `portfolio_holdings` `percentage` column
- [ ] Run `ruff check --fix . && ruff format .` then re-run tests

**Out of scope:**
- No new charts in this phase (Phase 2)
- No changes to `dividend_income`, `interest_income`, `cash_flow_summary`, or `data_quality` gold tables
- No encryption changes (Phase 3)

**Files:** `pipeline/analytics/models.py`, `pipeline/analytics/holdings.py`, `pipeline/analytics/allocation.py` (delete), `pipeline/analytics/__init__.py`, `pipeline/report/loader.py`, `pipeline/report/renderer.py`, `pipeline/report/charts.py`, `pipeline/analytics/quality.py`, `pipeline/run.py`, `docs/table-lineage.md`, tests

**Links:** [ADR 0066](../adr/0066-portfolio-holdings-gold-table-and-report-generation.md), [ADR 0073](../adr/0073-currency-exposure-donut-chart.md)

---

### Phase 2 — Replace "Allocation by Position Type" with "Positions" chart *[status: planned]*

Replace the EQUITY/CASH donut with a horizontal bar chart showing all holdings ranked by portfolio weight. The CASH bar at the bottom makes the EQUITY/CASH split visible at a glance, subsuming the old donut. Add an EQUITY/CASH summary card (e.g. "Equity 87.3% · Cash 12.7%") to preserve the single number the donut was showing.

**Scope:**
- [ ] Add `positions_chart(holdings: pl.DataFrame) -> go.Figure` to `pipeline/report/charts.py` — horizontal bar chart showing all holdings (including CASH) sorted by percentage descending, with ticker labels and percentage annotations; CASH rows visually distinguished (e.g. different color)
- [ ] Remove `allocation_by_position_type()` from `pipeline/report/charts.py`
- [ ] Add an EQUITY/CASH summary card to `render_report()` in `pipeline/report/renderer.py` — shows equity % and cash % as compact text, replacing the information the donut provided
- [ ] Remove `allocation_position_type` from `chart_names`, `figs_by_name`, and the chart rendering logic in `render_report()`
- [ ] Update `pipeline/report/templates/report.html` — replace `{{ charts.allocation_position_type }}` slot with `{{ charts.positions }}` and add the EQUITY/CASH summary card in the portfolio section
- [ ] Update `docs/table-lineage.md` — remove "Allocation by Position Type" node, add "Positions" chart node
- [ ] Add unit tests for `positions_chart` in `tests/test_charts.py`; update/remove tests for `allocation_by_position_type`
- [ ] Run `ruff check --fix . && ruff format .` then re-run tests

**Out of scope:**
- No changes to "Allocation by Broker" or "Currency Exposure" donut charts
- No changes to gold-table schemas (percentage column already added in Phase 1)
- No new data pipeline steps

**Files:** `pipeline/report/charts.py`, `pipeline/report/renderer.py`, `pipeline/report/templates/report.html`, `docs/table-lineage.md`, `tests/test_charts.py`

**Links:** [ADR 0066](../adr/0066-portfolio-holdings-gold-table-and-report-generation.md), [ADR 0073](../adr/0073-currency-exposure-donut-chart.md)

---

### Phase 3 — Encrypt gold value columns & README screenshots *[status: planned]*

Encrypt financial value columns in gold Delta tables (matching the normalized-layer pattern), update chart code to use `percentage` where possible, and add representative screenshots to the README.

**Encryption design:** Only numeric value columns are Fernet-encrypted; metadata columns (`ticker`, `broker`, `security_ccy`, `percentage`, `position_type`, `event_type`, etc.) remain plaintext. This means allocation charts and the positions chart render without the decryption key (they use `percentage`). Charts that display absolute monetary amounts (Passive Income, Cash Flow, Total Value card) decrypt at render time.

**Scope:**
- [ ] Change `portfolio_holdings_schema` value columns (`security_value`, `target_value`) from `pa.float64()` to `pa.binary()` in `pipeline/analytics/models.py`
- [ ] Change CDC gold table schemas (`cash_amount`, `target_value`, `gross_amount`, `fee_amount`, `tax_amount`, `price`, `quantity`, `target_fx_rate`) from `pa.float64()` to `pa.binary()` (nullable where already nullable) in `pipeline/analytics/models.py`
- [ ] Update `build_portfolio_holdings()` to encrypt `security_value` and `target_value` before writing
- [ ] Update `build_dividend_income()`, `build_interest_income()`, `build_cash_flow_summary()` to encrypt value columns before writing
- [ ] Update `pipeline/report/loader.py` to decrypt gold value columns when loading (or update `pipeline/query.py` to decrypt gold columns on read for DuckDB ad-hoc queries)
- [ ] Update chart code: `allocation_by_broker` and `allocation_by_currency` to use `percentage` column instead of summing `target_value`; `positions_chart` uses `percentage` natively
- [ ] Update `render_report()` summary card to decrypt `target_value` for Total Value display
- [ ] Update `_passive_income_table()` and `cash_flow_breakdown()` to decrypt value columns
- [ ] Update `pipeline/analytics/quality.py` — gold-table schema checks must match the new binary types; required-fields checks must handle binary columns
- [ ] Update `pipeline/query.py` — `decrypt_df()` should auto-detect and decrypt gold value columns alongside normalized ones
- [ ] Create an ADR superseding ADR 0003's "Analytics layer: no encryption" — gold value columns are now Fernet-encrypted, metadata columns remain plaintext
- [ ] Generate the demo report locally: `pipeline run full` then `pipeline report --output data/report.html`
- [ ] Screenshot 2–4 focused sections of the report for README:
  - One allocation donut chart (e.g. "Allocation by Broker")
  - The new positions chart
  - Data Quality badges section
  - Optionally: Passive Income Timeline or Cash Flow Breakdown
- [ ] Add screenshots to `docs/screenshots/` directory
- [ ] Update `README.md` with an image gallery section showing the screenshots with brief captions
- [ ] Update the README data-flow Mermaid diagram to remove `portfolio_allocation` → analytics arrow and add `portfolio_holdings` → report arrow
- [ ] Update `docs/table-lineage.md` diagram to reflect encryption in gold layer
- [ ] Add/update tests for encrypted gold column read/write roundtrips
- [ ] Run `ruff check --fix . && ruff format .` then re-run tests

**Out of scope:**
- No PDF or image export from the report generator
- No automated screenshot process (manual screenshots are sufficient)
- No changes to raw-layer encryption (already encrypts entire payload)
- No encryption of `data_quality` table (no financial values)

**Files:** `pipeline/analytics/models.py`, `pipeline/analytics/holdings.py`, `pipeline/analytics/cdc_tables.py`, `pipeline/analytics/quality.py`, `pipeline/report/loader.py`, `pipeline/report/renderer.py`, `pipeline/report/charts.py`, `pipeline/query.py`, `docs/adr/`, `README.md`, `docs/screenshots/` (new), `docs/table-lineage.md`, tests

**Links:** [ADR 0003](../adr/0003-medallion-architecture.md), [ADR 0066](../adr/0066-portfolio-holdings-gold-table-and-report-generation.md)