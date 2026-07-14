# 0073 — Currency Exposure Donut Chart (Phase 1)

## Context

The "Allocation by Currency" chart in the portfolio report groups `portfolio_holdings` by the `currency` column. For Trading 212 positions, `currency` is the wallet/payment currency (e.g. PLN), not the instrument's trading currency. A UK stock (GBX) held on a PLN-denominated T212 account therefore appears under "PLN" instead of "GBX" — a visible misrepresentation of currency exposure.

The chart also uses a `go.Bar` bar chart with EUR values labeled by currency code, which is confusing (the reader cannot tell whether "PLN: 5,000" means 5,000 PLN or 5,000 EUR in PLN assets) and inconsistent with the other two allocation charts (`allocation_by_broker`, `allocation_by_position_type`), which are donut charts showing `label+percent`.

The `security_currency` column already exists in `portfolio_holdings` (added by [ADR 0066](./0066-portfolio-holdings-gold-table-and-report-generation.md) and populated from `consolidated_holdings`). It carries the instrument's native trading currency — exactly what the chart should group by.

## Decision

Switch `allocation_by_currency()` in `pipeline/report/charts.py` from a bar chart grouping by `currency` to a donut chart grouping by `security_currency`:

1. **Change grouping column** from `"currency"` to `"security_currency"` — fixes the T212 wallet-currency bug.
2. **Change chart type** from `go.Bar` to `go.Pie` with `hole=0.4` and `textinfo="label+percent"` — matches the other two allocation charts and eliminates the ambiguous "EUR in PLN" reading.
3. **Change title** from "Allocation by Currency" to "Currency Exposure" — clarifies that the chart shows instrument currency exposure, not wallet currency.
4. **Remove axis titles** (`xaxis_title`, `yaxis_title`) and adjust bottom margin (`b=40` → `b=20`) — Pie charts don't use axes.

No changes to the data pipeline, schemas, or report template. The `security_currency` column is already present in the holdings DataFrame passed to the chart (confirmed in `pipeline/report/loader.py` and `pipeline/analytics/models.py`).

## Constraints

- Must not modify any schema or pipeline code — those belong to Phases 2–4 of the roadmap.
- Must not change the other two allocation charts.
- Must not change the report template or renderer.

## Consequences

- **Positive:** T212 positions now appear under their instrument currency (GBX/GBP) instead of wallet currency (PLN). All three allocation charts share a consistent donut style.
- **Positive:** Adding unit tests for `allocation_by_currency` in `tests/test_charts.py` provides regression coverage for the T212 wallet-currency bug — previously there were zero chart unit tests.
- **Neutral:** The chart no longer shows absolute EUR values per currency — it shows percentages instead. Users who relied on the bar chart's numeric labels can still hover for values.
- **Follow-up:** Phases 2–4 will rename/remove the overloaded `currency` column across all schemas, at which point the `currency` column will no longer exist in `portfolio_holdings`.

## Validation

- `tests/test_charts.py::TestAllocationByCurrency` — 5 tests covering donut shape, title, security_currency grouping, aggregation, and empty input.
- `tests/test_report.py` integration tests — all pass (chart-type agnostic, verify Plotly.newPlot count and section markers).
- `ruff check --fix . && ruff format .` — clean.
- Full test suite: 574 passed.