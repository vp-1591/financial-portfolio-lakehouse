# ADR 0002: Add consolidate step and fix duplicate rows in transform

## Context

The pipeline had two bugs:

1. **Missing consolidation step**: The pipeline flow was `fetch → transform → allocate`, but `allocate` reads from `consolidated_holdings` which was never created. The `consolidate_holdings()` function existed in `pipeline/normalized/consolidate.py` but no CLI step invoked it.

2. **Duplicate rows in normalized tables**: `cmd_transform()` used `mode="append"` when writing Delta tables, causing duplicate rows on every re-run. This made the IBKR `value` column appear as a list when grouped — 8 positions became 16 rows, and any groupby on the binary `value` column collected values into a list instead of summing scalars.

## Decision

- **Add `consolidate` subcommand** to `pipeline/run.py` that reads all normalized broker snapshots, extracts `Holding` objects via `pipeline/normalized/extract.py`, applies currency conversion and ISIN overrides, and writes the `consolidated_holdings` Delta table.

- **Change Delta write mode** from `"append"` to `"overwrite"` in both `cmd_transform()` and `consolidate_holdings()`. Each pipeline step is idempotent — it reprocesses all available data from the previous layer — so overwriting is correct and prevents duplicates.

- **Pipeline flow** is now: `fetch → transform → consolidate → allocate`.

## Consequences

- The `python -m pipeline.run consolidate` command is now available with `--target-currency`, `--fx-rate`, and `--isin-map-file` options.
- `python -m pipeline.run full` runs all four steps in sequence.
- Re-running `transform` or `consolidate` no longer creates duplicate rows.
- The `pipeline/normalized/extract.py` module provides `extract_holdings()` to convert each broker's normalized snapshot into `Holding` objects.

## Validation

- All 159 existing tests pass.
- `transform` step now produces 8 IBKR rows (was 16 with duplicates).
- `consolidate` step successfully creates `consolidated_holdings` with 26 rows from all 3 brokers.
- `allocate` step successfully reads consolidated data and prints portfolio allocation.