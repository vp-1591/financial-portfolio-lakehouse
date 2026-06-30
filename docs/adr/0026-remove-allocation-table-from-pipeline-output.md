# 0026 — Remove allocation table from pipeline output

## Context

The `cmd_allocate` function in `pipeline/run.py` called `print_allocation()` after computing portfolio allocations, which printed the full gold table (ticker, percentage, broker, identifier, currency, description) to stdout. When the pipeline runs in GitHub Actions, this output appears in the workflow step logs, which are visible to anyone with read access to the repository. The repo is currently private but is planned to be made public, at which point historical pipeline runs containing personal portfolio details would be exposed.

## Decision

Remove the `print_allocation()` function and its call from `pipeline/run.py`. The allocation data is still persisted to the Delta table (the actual output), so no data is lost — only the console printing is removed.

The `print_rows()` function in `scripts/portfolio_percentages.py` (the legacy standalone script) is left untouched, since it is used for local interactive runs and is not invoked by the CI pipeline.

## Consequences

- **Positive**: Portfolio allocation details no longer appear in GitHub Actions logs, preventing information leakage when the repo becomes public.
- **Positive**: Cleaner pipeline output in CI — only status/progress messages remain.
- **Negative**: Running `python -m pipeline.run allocate` locally no longer prints the table to the terminal. Users who want to see the allocation can query the Delta table directly or use the legacy `scripts/portfolio_percentages.py` script.

## Validation

- All 258 tests pass after the change.
- Ruff linting and formatting applied with no remaining issues.