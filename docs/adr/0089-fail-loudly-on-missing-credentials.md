# ADR 0089: Fail Loudly on Missing Broker Credentials

## Context

When all broker credentials are missing, `fetch_connector()` returns exit code 0 (success), making `cmd_fetch()` silently succeed. The pipeline then proceeds through transform and consolidate before failing with a misleading "No holdings found. Run the transform step first." error. The actual root cause — no credentials configured — is logged at DEBUG level and invisible at default log levels.

This is documented in the Phase 1 scope of roadmap 0012 (simplify-pipeline-execution).

## Decision

Introduce a `FetchResult` enum (`SUCCESS=0`, `ERROR=1`, `SKIPPED=2`) as the return type of `fetch_connector()`. When `fetch_kwargs()` returns an empty dict (no credentials) or XTB has no `--xtb-file`, the function returns `FetchResult.SKIPPED` instead of `0`.

In `cmd_fetch()`, accumulate `FetchResult` values from each connector and check:
- **All skipped**: print a clear error message to stderr ("No broker credentials found...") and return 1.
- **Any error**: print a summary of successes vs failures and return 1.
- **At least one success, no errors**: return 0 (pipeline proceeds normally).
- **All disabled** (no results collected): return 0 (no error, but nothing to do).

In `cmd_run_connector()`, when `fetch_connector()` returns `SKIPPED`, return 0 immediately without calling `transform_connector` or `run_validation`. There is no data to process, so skipping is correct. This also avoids a false-positive validation failure on missing CDC tables (ADR 0087).

## Constraints

- `FetchResult` is an `IntEnum` so that `SUCCESS == 0` and `ERROR == 1` are backward-compatible with existing int comparisons.
- `cmd_run_connector()` must return exit code 0 for skipped connectors (not 2), because the Step Functions orchestrator expects 0 or 1.
- Phase 1 does not change the `DEMO` env var pattern, add `--mode`, or modify any other subcommands.
- The error message references `--mode staging/--mode prod` even though the `--mode` flag doesn't exist yet. This is intentional — Phase 2 will add the flag, and the message prepares users for it.

## Consequences

- Users running `pipeline run fetch` or `pipeline run full` with no broker credentials will immediately see a clear, actionable error instead of a misleading message much later in the pipeline.
- `cmd_run_connector()` no longer calls `transform_connector` or `run_validation` when a connector has no credentials. This is cleaner than the previous behavior of calling transform with no raw data, which would silently skip and potentially trigger a false-positive validation failure.
- The `FetchResult.SKIPPED` path should never occur in production (ECS tasks always have credentials injected by SSM). It is primarily a safety net for local development.
- `TestFetchConnectorIsolation.test_skips_connector_when_kwargs_empty` now asserts `FetchResult.SKIPPED` instead of `0`. `TestCmdRunConnector.test_enabled_connector_calls_fetch_then_transform` now uses `FetchResult.SUCCESS` instead of `0` as the mock return value.

## Validation

- `test_all_skipped_returns_nonzero`: all connectors return `SKIPPED` -> `cmd_fetch` returns 1 with error message.
- `test_all_skipped_error_message`: stderr contains "No broker credentials found", "IBKR_FLEX_TOKEN", and "--mode staging".
- `test_one_success_overrides_all_skipped`: at least one `SUCCESS` -> `cmd_fetch` returns 0.
- `test_one_error_returns_nonzero`: any `ERROR` -> `cmd_fetch` returns 1 with summary.
- `test_all_disabled_returns_zero`: all connectors disabled -> `cmd_fetch` returns 0, `fetch_connector` not called.
- `test_fetch_result_enum_values`: `FetchResult.SUCCESS == 0`, `ERROR == 1`, `SKIPPED == 2`.
- `test_skipped_connector_returns_zero`: `cmd_run_connector` returns 0 and skips transform/validation when fetch returns `SKIPPED`.
- `test_xtb_returns_skipped_when_no_file`: XTB without `--xtb-file` returns `FetchResult.SKIPPED`.
- All existing tests in `test_run_subcommands.py` and `test_connector_protocol.py` pass.