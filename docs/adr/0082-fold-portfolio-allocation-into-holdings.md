# 0082 — Fold portfolio_allocation into portfolio_holdings

## Context

ADR 0066 created `portfolio_holdings` as a richer replacement for `portfolio_allocation`. The allocation table stored only `ticker`, `broker`, `percentage`, `identifier`, `security_ccy`, and `description` — all of which are already present in `portfolio_holdings` plus much more (security_value, target_value, target_ccy, position_type). The allocation table was only used as a degraded plain-HTML fallback in the report when holdings were empty. Adding a `percentage` column to `portfolio_holdings` makes the allocation table fully redundant.

## Decision

Fold `portfolio_allocation` into `portfolio_holdings` by:

1. Adding a `percentage` Float64 column to `portfolio_holdings_schema`, computed as `(target_value / total_target_value) * 100` rounded to 4 decimal places.
2. Deleting `pipeline/analytics/allocation.py` and removing all references to `portfolio_allocation` across the codebase (loader, renderer, quality checks, run command, query helpers, storage paths).
3. Removing the allocation fallback path from `render_report()` — if holdings are empty, the portfolio section is hidden entirely.
4. Updating all tests to remove allocation-specific test code and add tests for the new `percentage` column.

## Constraints

- `portfolio_holdings` must still produce correct data without `portfolio_allocation` as a dependency.
- The report must still render correctly with just `portfolio_holdings` — no broken sections.
- All existing tests must pass.
- The `scripts/migrate_phase2_phase3_schema.py` migration script is left unchanged as historical record.

## Consequences

- **Positive:** One fewer gold table to maintain, validate, and encrypt. Report code is simpler — no fallback path. `percentage` is now available alongside full position data in a single table.
- **Positive:** Data quality checks cover one fewer table, reducing validation surface.
- **Negative:** The report will hide the portfolio section entirely when `portfolio_holdings` is empty, rather than showing a degraded allocation table. This is acceptable because empty holdings means no data to show.
- **Future:** Phase 2 of the roadmap will replace the "Allocation by Position Type" donut with a "Positions" bar chart, which will use the `percentage` column directly.

## Validation

- All 609 tests pass, including 3 new tests for the `percentage` column (present, positive values, sum to ~100).
- `grep -r "portfolio_allocation\|allocate_percentages" pipeline/` returns zero hits.
- `python -c "from pipeline.analytics import portfolio_holdings_schema"` succeeds.
- The Mermaid diagram in `docs/table-lineage.md` has no `portfolio_allocation` node.