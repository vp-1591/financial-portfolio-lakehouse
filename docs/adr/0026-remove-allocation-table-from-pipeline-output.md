# 0026 — Remove allocation table and row counts from pipeline output

## Context

The `cmd_allocate` function in `pipeline/run.py` called `print_allocation()` after computing portfolio allocations, which printed the full gold table (ticker, percentage, broker, identifier, currency, description) to stdout. Additionally, all pipeline steps (`cmd_fetch`, `cmd_transform`, `cmd_consolidate`, `cmd_allocate`) used `print()` to log progress messages including row counts ("9 rows written", "17 holdings extracted", etc.). When the pipeline runs in GitHub Actions, this output appears in the workflow step logs, which are visible to anyone with read access to the repository. The repo is currently private but is planned to be made public, at which point historical pipeline runs containing personal portfolio details would be exposed.

## Decision

1. **Remove `print_allocation()`**: The function and its call are deleted from `pipeline/run.py`. The allocation data is still persisted to the Delta table, so no data is lost.

2. **Replace informational `print()` with `logging.debug()`**: All progress/status messages (row counts, connector skip messages, "not implemented" notices) are changed from `print()` to `logger.debug()`. These are invisible by default and only appear when the `pipeline` logger is set to `DEBUG` level. Error messages that go to `sys.stderr` remain as `print()` calls since they report real problems.

3. **Leave legacy script untouched**: The `print_rows()` function in `scripts/portfolio_percentages.py` is used for local interactive runs and is not invoked by the CI pipeline.

## Consequences

- **Positive**: No portfolio details or row counts appear in GitHub Actions logs by default, preventing information leakage when the repo becomes public.
- **Positive**: Cleaner pipeline output in CI — only errors are printed.
- **Positive**: Debug output is still available locally via `logging.DEBUG` if needed for troubleshooting.
- **Negative**: Running `python -m pipeline.run allocate` locally no longer prints the table. Users who want to see the allocation can query the Delta table directly or use the legacy `scripts/portfolio_percentages.py` script.

## Validation

- All 258 tests pass after the change.
- Ruff linting and formatting applied with no remaining issues.