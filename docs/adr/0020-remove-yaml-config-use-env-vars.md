# 0020: Remove YAML config, use environment variables

## Context

The pipeline used two YAML config files:
- `pipeline.defaults.yaml` — committed defaults with all connectors `enabled: false`
- `pipeline.yaml` — gitignored local overrides to enable connectors

This caused a critical problem in CI: `pipeline.yaml` is gitignored so it never
reaches GitHub Actions. In CI, only `pipeline.defaults.yaml` exists, where all
connectors are `enabled: false`. The pipeline skips every connector and produces
no data.

The YAML config also overlapped with the GitHub Actions workflow file
(`pipeline.yml`), which already defined `command` and `target-currency` as
workflow inputs — two separate places for pipeline configuration.

## Decision

1. **Delete `pipeline.defaults.yaml` and `pipeline.yaml`.** Remove the two-file
   config system entirely. No config files are committed or gitignored.

2. **Delete `pipeline/config.py`.** The `load_config()`, `_deep_merge()`, and
   `get_connector_config()` functions are no longer needed.

3. **Move all config to environment variables.** Each YAML config field becomes
   an env var with a sensible default. Connectors are **enabled by default** —
   set `IBKR_ENABLED=0`, `T212_ENABLED=0`, or `XTB_ENABLED=0` to disable.

4. **Add `get_config()` and `is_enabled()` to `pipeline/secrets.py`.** These
   replace `get_connector_config()` and the `conn_cfg.get("enabled")` pattern.
   `is_enabled()` returns `True` unless the env var is explicitly set to `0`,
   `false`, or `no`.

5. **Add connector toggle inputs to the GitHub Actions workflow.** The workflow
   now has `ibkr-enabled`, `t212-enabled`, and `xtb-enabled` boolean inputs
   (default: true) that map to `IBKR_ENABLED`, `T212_ENABLED`, and `XTB_ENABLED`
   env vars.

6. **Move `IBKR_FLEX_QUERY_ID` to GitHub Secrets.** It was previously a YAML
   config field. Now it's a secret alongside `IBKR_FLEX_TOKEN`.

7. **Remove `pyyaml` dependency.** It was only used by `config.py`.

## Consequences

- **No config files in the repository.** All configuration is through env vars
  (`.env` file locally, GitHub Secrets/workflow inputs in CI).
- **Connectors are enabled by default.** This means a fresh install will attempt
  to fetch from all brokers. Set `*_ENABLED=0` to disable specific connectors.
- **`.env` file is the single local config source.** Both secrets and connector
  settings go in `.env`.
- **Simpler CI.** The workflow file is the only source of pipeline configuration
  for CI runs — no separate YAML config file needed.
- **`test_config.py` is deleted.** The YAML merge and connector config tests
  are no longer relevant.

## Validation

- All tests pass after removing `config.py` and `test_config.py`
- `grep -r "load_config\|get_connector_config" pipeline/` returns no results
- `grep -r "import yaml" pipeline/` returns no results
- `IBKR_ENABLED=0 python -m pipeline.run fetch` skips IBKR connector
- GitHub Actions workflow dispatch with all connectors enabled runs successfully