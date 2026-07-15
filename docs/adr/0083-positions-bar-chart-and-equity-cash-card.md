# 0083 — Replace "Allocation by Position Type" donut with "Positions" bar chart and EQUITY/CASH summary card

## Context

Phase 1 (ADR 0082) folded `portfolio_allocation` into `portfolio_holdings` by adding a `percentage` column. The "Allocation by Position Type" donut chart now shows only two slices — EQUITY and CASH — which is a single number disguised as a chart. Meanwhile, the portfolio section lacks a view of individual positions ranked by weight. A horizontal bar chart showing every holding (including CASH) ranked by percentage would subsume the donut: the EQUITY/CASH split is visible at a glance as the CASH bar vs the equity bars, while also showing the distribution across individual positions.

## Decision

Replace the "Allocation by Position Type" donut chart with a "Positions" horizontal bar chart and add a compact EQUITY/CASH summary card:

1. **Add `positions_chart()`** to `pipeline/report/charts.py` — a horizontal bar chart that shows all holdings sorted by percentage descending, with EQUITY bars in green (`#2ecc71`), CASH bars in amber (`#f39c12`), and unknown types in gray (`#95a5a6`). Each bar is annotated with its percentage value. The y-axis is reversed so the highest-weighted position appears at the top.

2. **Remove `allocation_by_position_type()`** from `pipeline/report/charts.py` — the two-slice donut is no longer needed.

3. **Add `_equity_cash_card()`** to `pipeline/report/renderer.py` — a compact HTML summary card showing "Equity XX.X% · Cash YY.Y%" using the existing `.metric-card` CSS. This preserves the single-number overview the donut provided.

4. **Update `render_report()`** — wire `positions_chart` into the chart pipeline (replacing `allocation_by_position_type`), add `equity_cash_card_html` to the template context.

5. **Update `report.html`** — replace `{{ charts.allocation_position_type }}` with `{{ charts.positions }}` and add `{{ equity_cash_card_html }}` in the portfolio section.

6. **Update `docs/table-lineage.md`** — replace the "Allocation by Position Type" chart node with "Positions".

## Constraints

- The "Allocation by Broker" and "Currency Exposure" donut charts remain unchanged.
- No changes to gold-table schemas or the data pipeline — the `percentage` column was added in Phase 1.
- The `_summary_table()` HTML table (By Broker / By Position Type) remains unchanged; it complements the summary card.

## Consequences

- **Positive:** Every position is now visible in the report, not just the aggregate EQUITY/CASH split. The summary card provides the quick percentage overview; the bar chart provides the detailed breakdown.
- **Positive:** One fewer Plotly Pie chart reduces the report's visual clutter — the EQUITY/CASH donut added no information beyond a single number.
- **Positive:** The positions chart uses the `percentage` column directly, making it compatible with Phase 3 (gold-layer encryption) where absolute values require decryption but percentages are plaintext.
- **Negative:** For portfolios with many holdings (50+), the bar chart will be tall. This is acceptable because scrolling is natural for long lists and the percentage annotations make each bar readable at any scroll position.

## Validation

- All 622 tests pass, including 13 new `TestPositionsChart` tests covering: orientation, sort order, color differentiation, equity/cash colors, percentage annotations, title, y-axis reversed, x-axis title, empty input, all-equity, all-cash, and single position.
- `grep -r "allocation_position_type\|allocation_by_position_type" pipeline/` returns zero hits — the old function is fully removed.
- `positions_chart` is imported and used in `renderer.py` and tested in `test_charts.py`.