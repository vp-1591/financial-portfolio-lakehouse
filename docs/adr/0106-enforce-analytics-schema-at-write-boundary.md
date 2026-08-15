# 0106: Enforce Analytics Schema at the Write Boundary (Polars Int32/Int64 Drift)

## Context

ADR 0045 introduced `build_normalized_table()` for the silver/normalized layer: build in Polars, then convert to PyArrow and cast to the exact target `pa.Schema`. The gold/analytics layer (CDC income tables, portfolio holdings) did not follow that convention. Its builders (`build_dividend_income`, `build_interest_income`, `build_cash_flow_summary`, `build_portfolio_holdings`) materialized every column to Python lists via `.to_list()`, rebuilt `pa.table({...}, schema=...)`, and the shared writer `_write_analytics_table` ran a defensive per-column cast loop.

This change (a YAGNI/simplification pass) made the analytics builders return `pl.DataFrame` and write it directly via `write_deltalake`, which CLAUDE.md notes accepts `pl.DataFrame` (PyArrow is reserved for schemas and S3). Removing the per-column cast loop looked like routine dead-defense removal — consistent with the pass's goal.

A round-trip guard test (write via a builder, re-open the Delta table, compare the on-disk schema to the declared schema) proved the cast was **not** entirely dead: **Polars `count()` aggregation yields `Int32`, but the declared analytics schemas require `Int64`** (e.g. `event_count`). Without schema enforcement, `write_deltalake` would silently write an `Int32` Delta column where `Int64` is required — a latent type drift invisible without the guard. The `to_list()`+`pa.table()` rebuild had masked this by forcing the declared schema on assembly.

## Decision

Adopt ADR 0045's "build in Polars, cast to the exact target schema" convention in the analytics layer, enforced at the **write boundary** in one shared helper rather than a per-column loop:

- Analytics builders return `pl.DataFrame` and route through `_finalize_analytics(agg, schema, analytics_path, *, calculated_at)` in `pipeline/analytics/cdc_tables.py`, which stamps `calculated_at`, selects columns in schema order, and writes via `write_deltalake`. `build_portfolio_holdings` (`pipeline/analytics/holdings.py`) shares the same helper.
- Schema enforcement is centralized in `_finalize_analytics` via a single `agg.to_arrow().cast(schema)` (the same cast step `build_normalized_table` performs). This cast is **load-bearing**, not dead defense: it corrects Polars `count()`'s `Int32` to the declared `Int64` and pins nullability that `to_arrow()` alone does not reproduce.
- The old per-column cast loop in `_write_analytics_table` and the `.to_list()`+`pa.table()` rebuilds are deleted; `_write_analytics_table` now accepts and returns `pl.DataFrame`.

Alternatives rejected:
- **No cast; let `write_deltalake` infer types from the `pl.DataFrame`.** Rejected: it silently writes `Int32` for `event_count` where the schema requires `Int64` — the exact drift the guard test caught.
- **Keep the per-column cast loop.** Rejected: it was dead for every column except the `Int32→Int64` correction; one centralized `cast(schema)` captures the same guarantee in a single line and routes through the shared helper.

## Constraints

- The declared analytics `pa.Schema` remains the source of truth for column names, dtypes, and order; the `pl.DataFrame` must match it after the cast.
- The on-disk Delta schema must equal the declared schema (verified by the round-trip guard test), regardless of whether `write_deltalake` receives a cast `pa.Table` or a `pl.DataFrame`.
- Existing callers and downstream readers must accept the `pl.DataFrame` return contract (tests migrated; no external consumer breaks).
- This does not change the silver/normalized layer (`build_normalized_table`, ADR 0045); analytics now mirrors its convention. Future analytics builders must route through `_finalize_analytics` rather than writing a raw `pl.DataFrame`.

## Consequences

- **Simpler**: the per-column cast loop and `.to_list()`+`pa.table()` rebuilds (roughly 50 lines across four builders plus the writer) collapse into one shared `_finalize_analytics` helper.
- **New latent-drift trap, now documented**: a future analytics builder that bypasses `_finalize_analytics` and writes a raw `pl.DataFrame` will reintroduce `Int32`/`Int64` (or other Polars-dtype) drift. The `# Decision: docs/adr/0106-...` comment at the cast site and the guard tests defend against this.
- **Schema correctness is test-enforced, not structurally enforced**: the guarantee rests on the round-trip guard test (`_assert_on_disk_schema_matches`) plus the centralized cast — not on a cast loop's structural presence. Removing the cast **or** the guard test reintroduces silent drift; the ADR exists so a future YAGNI pass does not treat the cast as dead defense.
- **Return contract changed**: builders return `pl.DataFrame`, not `pa.Table`, aligning with CLAUDE.md's "Polars for all data manipulation" rule. Test assertions migrated to Polars-side equivalents.

## Validation

- `tests/test_cdc_analytics.py` and `tests/test_portfolio_holdings.py`: `_assert_on_disk_schema_matches` writes via each builder, re-opens the Delta table, and asserts the on-disk Arrow schema `.equals` the declared `*_schema` (columns, dtypes, nullability). This guard is what caught the `Int32→Int64` drift.
- Removing the `agg.to_arrow().cast(schema)` from `_finalize_analytics` makes `_assert_on_disk_schema_matches` fail on `event_count` (`Int32` != `Int64`), confirming the cast is load-bearing.
- Full suite `pytest tests/ -q` -> 747 passed; `pyright pipeline/ tests/` -> 0 errors; `ruff check --fix .` + `ruff format .` clean.