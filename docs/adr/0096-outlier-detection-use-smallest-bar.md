# 0096 — Outlier Detection: Use Smallest Bar Instead of Median

> **Supersedes [ADR 0075](./0075-cash-flow-outlier-toggle.md)** — median-of-others fails with 3+ mixed-magnitude groups.

## Context

The cash flow breakdown chart (ADR 0075) uses a **median-of-others** algorithm to detect outlier event types whose bars dwarf smaller categories on the linear Y-axis. With 3+ event types of widely different magnitudes — DEPOSIT=€1M, TRADE=€272K, INTEREST=€1.3K — the middle value (TRADE) inflates the median baseline to €137K, pushing the 10× threshold to €1.37M. The €1M deposit no longer qualifies as an outlier, even though it makes the €1.3K interest bars invisible on the chart.

This is fundamentally a **UI dynamic range problem**, not a statistical distribution problem. The question is not "is this value an outlier in a statistical sense?" but "does this bar dwarf the smallest active category, making it invisible on a linear scale?"

### Why not pure min-of-others

A pure `min(other_peaks)` baseline would make a single €1 noise value the baseline. With `ratio=10`, the threshold becomes €10, flagging nearly every category as an outlier. Example: `[1M, 10K, 1K, 1]` — the €1 value sets baseline=1, making 1M, 10K, and 1K all "outliers." Hiding all three leaves only the €1 bar, defeating the toggle's purpose.

## Decision

Replace median-of-others with **min-of-others, floored at `max_peak / ratio²`**:

- The baseline for each peak is `max(min(other_nonzero_peaks), floor)`, where `floor = max_peak / ratio²`.
- This compares each bar against the smallest meaningful other bar.
- The floor prevents tiny noise values from setting an absurdly low baseline.
- The floor is derived from the existing `ratio` parameter — no new magic number.

For `ratio=10`, the floor is 1% of the maximum peak. The effective threshold never drops below `max_peak / ratio` = 10% of the maximum.

### Examples

| Peaks | Floor | Baseline | Threshold | Outliers |
|---|---|---|---|---|
| 1M, 272K, 1.3K | 10K | 10K (floor) | 100K | 1M, 272K |
| 1M, 10K, 1K, 1 | 10K | 10K (floor) | 100K | 1M |
| 100, 200, 300 | 3 | 100 (min) | 1K | None |
| 1M, 900K | 10K | 900K (min) | 9M | None |

## Constraints

- Must work in static HTML reports (Plotly JS handles interactivity client-side).
- Must not change the chart's default appearance when no outliers exist.
- Must not break existing report generation or integration tests.
- The outlier ratio (10×) remains a hardcoded constant; the floor is derived from it.

## Consequences

- Outlier detection now fires correctly for the 3+ mixed-magnitude case (DEPOSIT, TRADE, INTEREST).
- Both DEPOSIT and TRADE are flagged as outliers — hiding them reveals the INTEREST pattern.
- Tiny noise values (below 1% of the max) no longer trigger false positives.
- The median-of-others algorithm is fully replaced; no fallback path remains.

## Validation

- `tests/test_charts.py::TestClassifyOutliers` — updated unit tests including two new cases:
  - `test_three_mixed_magnitudes` — the original 1M/272K/1.3K scenario
  - `test_tiny_noise_ignored_by_floor` — the 1M/10K/1K/1 noise scenario
- `tests/test_charts.py::TestCashFlowBreakdown` — existing integration tests for toggle behavior
- All existing tests continue to pass with the new algorithm