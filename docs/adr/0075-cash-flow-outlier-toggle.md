# 0075 — Cash Flow Breakdown Outlier Toggle

## Context

The cash flow breakdown chart displays monthly cash flow grouped by event
type (DEPOSIT, INTEREST, TRADE, FEE, etc.) as a linear-scale grouped bar
chart. When one event type dominates — e.g., a $1M deposit alongside $50
interest payments — the Y-axis scales to the largest value, making smaller
flows invisible. This defeats the purpose of the chart, which is to give an
overview of all cash flow activity.

A log scale was considered but rejected because log(0) is undefined (months
with zero values disappear) and negative values (WITHDRAWAL) cannot be
displayed on a log axis.

## Decision

Add an interactive Plotly `updatemenus` toggle to the chart that appears
when outlier event types are detected. The toggle offers two views:

1. **All Events** — the default, linear-scale view showing everything.
2. **Hide Outliers** — hides event types whose peak monthly value exceeds
   10× the median of the other peaks, and lets Plotly auto-rescale the
   Y-axis so smaller flows become visible.

Outlier detection uses a **median-of-others** algorithm: each event type's
peak absolute monthly value is compared to the median of all other event
types' peaks. This prevents a single extreme value from inflating the
baseline, which would happen with a simple median of all peaks.

When no outliers exist (all peaks are within a reasonable ratio), the
toggle is not shown — the chart remains as before.

## Constraints

- Must work in static HTML reports (Plotly JS handles interactivity
  client-side).
- Must not change the chart's default appearance when no outliers exist.
- Must not break existing report generation or integration tests.
- The outlier ratio (10×) is a hardcoded constant; tuning it requires a
  code change.

## Consequences

- Small cash flows (interest, fees, trades) become visible with one click
  when outliers exist.
- The chart's default view still shows all data — no information is lost.
- The median-of-others approach correctly handles the two-value case (e.g.,
  DEPOSIT vs INTEREST), unlike a simple median.
- If all event types have similar magnitudes, no toggle appears — the
  chart is unchanged from before.

## Validation

- `tests/test_charts.py::TestCashFlowBreakdown` — unit tests for the
  chart builder covering toggle presence, visibility lists, title changes,
  column selection, and empty input.
- `tests/test_charts.py::TestClassifyOutliers` — unit tests for the
  outlier detection helper covering extreme outliers, two-value cases,
  zero baselines, empty input, and custom ratios.
- `tests/test_report.py` — existing integration tests still pass.