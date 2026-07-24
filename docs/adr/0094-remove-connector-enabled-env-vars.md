# 0094: Remove Connector *_ENABLED Environment Variable Toggles

## Context

Roadmap 0012 ("Simplify Pipeline Execution Model") replaced the old `DEMO`/`STORAGE_TYPE` configuration with a `--mode docker|staging|prod` CLI flag. As part of that work, Phase 2 removed the `DEMO` and `STORAGE_TYPE` env vars, and Phase 3 removed `IBKR_ENABLED`, `T212_ENABLED`, and `XTB_ENABLED` from the ECS task environment blocks (since each ECS task runs exactly one connector via `run-connector <name>`).

However, the Python code still had `is_enabled()`, the `BrokerConnector.enabled_env_var` protocol attribute, and the `*_ENABLED` env var toggles. The `cmd_run_connector` function checked `is_enabled(connector.enabled_env_var)` before running a connector, and `_run_connectors_parallel` filtered connectors by the same check. The `.env.example` file documented these toggles, and the tests exercised them.

These toggles are now dead code. In the current architecture:
- `run-connector <name>` always runs the named connector — there is no reason to disable it.
- `full --mode docker` runs all connectors in parallel — the `*_ENABLED` flags just silently skip connectors if credentials are missing, but `fetch_connector` already returns `FetchResult.SKIPPED` when `fetch_kwargs()` returns `{}` (no credentials).
- In staging/prod, `full` triggers Step Functions, which only invokes the connectors it was told to.

## Decision

Remove the `*_ENABLED` environment variable toggles and all supporting code:

1. Remove `is_enabled()` from `pipeline/secrets.py`.
2. Remove `enabled_env_var` from the `BrokerConnector` protocol and all three connector implementations.
3. Remove the `is_enabled(connector.enabled_env_var)` check from `cmd_run_connector` in `pipeline/run.py`.
4. Remove the `is_enabled(c.enabled_env_var)` filter from `_run_connectors_parallel` — now runs all connectors unconditionally.
5. Remove `*_ENABLED` env vars from `.env.example`, `docs/configuration.md`, and broker docs.
6. Remove `TestIsEnabled` and `TestEnabledEnvVar` test classes.
7. Remove `@patch("pipeline.run.is_enabled", ...)` mocks from `TestCmdRunConnector` and `TestRunConnectorsParallel` tests.
8. Remove `IBKR_ENABLED`, `T212_ENABLED`, `XTB_ENABLED` from the env var isolation list in `tests/conftest.py`.

## Constraints

- Connectors that lack credentials are still skipped gracefully — `fetch_kwargs()` returns `{}` when secrets are missing, and `fetch_connector` returns `FetchResult.SKIPPED`. This behavior is unchanged.
- The `run-connector` subcommand always runs the named connector. Skipping was the only use of `*_ENABLED` at the individual-connector level.
- ECS task definitions were already updated in Phase 3 to remove these env vars.

## Consequences

- Simpler mental model: if you invoke a connector, it runs. If it has no credentials, it skips with a clear log message.
- Fewer env vars to document and configure.
- `.env.example` is shorter and less confusing.
- The `is_enabled()` function and `enabled_env_var` protocol attribute are gone — less code to maintain.

## Validation

- All 129 tests in `test_secrets.py`, `test_connector_protocol.py`, and `test_run_subcommands.py` pass after the removal.
- `pyright` reports 0 errors on the changed files.
- `ruff check` and `ruff format` pass.