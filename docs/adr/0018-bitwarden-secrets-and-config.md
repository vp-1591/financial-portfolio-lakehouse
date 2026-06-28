# 0018: Bitwarden Secrets and YAML Config

## Context

The pipeline had two problems:

1. **Coding agents could read prod data and secrets.** All paths and secrets were exposed via CLI flags (`--t212-api-key`, `--ibkr-flex-token`, `--env prod`) or environment variables. A coding agent running in the repo could see prod paths and accidentally use them.

2. **No centralized non-secret config.** Broker settings, currencies, and other operational params were CLI-only with no defaults file, requiring repetitive flag entry.

The user explicitly required: data paths must be secrets (stored in Bitwarden, not on disk), no config files with prod paths that agents can read, and connector settings should live in a config file rather than CLI flags.

## Decision

1. **Secrets come from Bitwarden (`bw`) or env vars only.** Created `pipeline/secrets.py` which resolves secrets at runtime:
   - Priority: already-set env vars (CI) → `bw get password` with `BW_SESSION` → missing (pipeline errors when needed)
   - Secret names match env var names exactly: `IBKR_FLEX_TOKEN`, `T212_API_KEY`, `T212_API_SECRET`, `PIPELINE_DATA_DIR`, `PORTFOLIO_ENCRYPTION_KEY`
   - No secrets in config files or CLI flags

2. **Non-secret config in `pipeline.defaults.yaml` + `pipeline.yaml`.** Created `pipeline/config.py` with a deep-merge loader:
   - `pipeline.defaults.yaml` is version-controlled (safe for agents to read — no secrets, no paths)
   - `pipeline.yaml` is gitignored local overrides (user-specific settings)
   - Precedence: CLI flags → `pipeline.yaml` overrides → `pipeline.defaults.yaml` defaults

3. **Removed `--env` flag and secret CLI args.** `pipeline/storage.py` now uses `PIPELINE_DATA_DIR` env var instead of `--env prod|dev`. The `env` field was removed from `StorageConfig` — `data_dir` is the single source of truth.

4. **Removed all broker CLI flags.** Connector enable switches and broker settings now come from YAML config. CLI flags for secrets (`--t212-api-key`, `--t212-api-secret`, `--ibkr-flex-token`) and broker toggles (`--ibkr`, `--t212-demo`) are gone. Remaining CLI flags: `--xtb-file`, `--target-currency`, `--fx-rate`, `--isin`, `--isin-map-file`.

5. **`.secrets/` stays at project root.** Secrets don't belong next to encrypted data. The `keygen` command still writes to `.secrets/encryption.key` (gitignored) as a local dev convenience, but in production `PORTFOLIO_ENCRYPTION_KEY` comes from Bitwarden.

6. **Agent blocked from calling `bw`.** Added `.claude/hooks/block-secret-access.sh` to prevent the coding agent from running Bitwarden CLI commands.

## Consequences

- **Agent can never see prod secrets or paths.** No config file on disk contains them, and the agent is blocked from calling `bw`.
- **CI uses GitHub Secrets as env vars.** Same env-var interface, no `bw` needed in CI.
- **`pipeline.yaml` is safe for agents to read.** It contains no secrets and no paths.
- **`bw` can be replaced with `bws` later** for CI/Dagster without changing pipeline code — both just set env vars.
- **`.secrets/` stays at project root** — not inside the data directory, since secrets should not be stored next to encrypted data.
- **`data-dev/` directory removed.** Data directories are now configured via `PIPELINE_DATA_DIR` env var.

## Validation

- All 228 tests pass
- `test_secrets.py` covers env-var priority, Bitwarden lookup, failure handling, and injection
- `test_config.py` covers YAML loading, deep merge, and connector config lookup
- `test_storage_config.py` covers `PIPELINE_DATA_DIR` env var resolution and project-root secrets dir
- Claude Code hook blocks `bw` commands