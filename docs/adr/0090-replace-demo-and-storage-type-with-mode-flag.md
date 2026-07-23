# 0090 — Replace DEMO and STORAGE_TYPE with --mode flag

## Context

The pipeline had three overlapping configuration axes: `DEMO` (boolean), `STORAGE_TYPE` (`cloud|minio|local`), and `*_DEMO`-suffixed secret env vars. Running the pipeline locally against AWS required setting ~15 environment variables correctly. Running `DEMO=true` with missing `_DEMO` secrets silently skipped all brokers and printed a misleading error. There were three separate entry points (`pipeline.run full`, `scripts/run_prod_pipeline.py`, and GitHub Actions workflow dispatch) with no single command for all contexts. Local `full` didn't mirror the Step Functions workflow — it chained fetch→transform→consolidate→analytics sequentially with no validation between steps.

Phase 1 (ADR 0089) made `fetch` fail loudly when all broker credentials were missing. Phase 2 replaces `DEMO` and `STORAGE_TYPE` with a single `--mode docker|staging|prod` CLI flag and removes the `fetch`, `transform`, `consolidate`, and `analytics` subcommands.

## Decision

1. **`--mode` flag replaces `DEMO` and `STORAGE_TYPE`.** A `mode_parser` parent argument parser with `--mode docker|staging|prod` (required) is attached to all subcommands that touch storage/credentials (`full`, `query`, `report`, `validate`, `run-connector`, `run-consolidate-analytics`, `upload-xtb`, `run-migration`). `keygen` is mode-independent. The flag appears after the subcommand (`pipeline run full --mode docker`) matching the ECS command-array form.

2. **Mode propagation via module global.** `pipeline/secrets.py` gains `set_mode()`, `get_mode()`, and `reset_mode()`. `main()` calls `set_mode(resolve_mode(args))` after argparse, before `resolve_storage()`. `is_demo()` now returns `get_mode() == "staging"` instead of reading the `DEMO` env var. All callers of `is_demo()` (connectors, crypto, query, storage) transparently pick up the new behavior.

3. **Storage resolution from mode.** `resolve_storage()` dispatches on `get_mode()`: docker→S3Backend with MinIO endpoint, staging→S3Backend with demo bucket (`S3_BUCKET_DEMO`), prod→S3Backend with production bucket (`S3_BUCKET`). `LocalBackend` is no longer produced by `resolve_storage()` — docker mode always uses MinIO. `LocalBackend` remains for test injection via `use_storage()`.

4. **`DEMO` and `STORAGE_TYPE` env vars removed.** `get_storage_type()`, `STORAGE_TYPE_CLOUD/MINIO/LOCAL`, and `VALID_STORAGE_TYPES` are deleted. The `DEMO` env var is no longer read. `_DEMO`-suffixed env var names remain (Phase 4 removes them).

5. **`cmd_full` docker-only orchestrator.** `cmd_full` in docker mode mirrors the SFN workflow: runs each enabled connector via `cmd_run_connector` (fetch+transform+validate) in parallel using `ThreadPoolExecutor` with fail-fast, then calls `cmd_run_consolidate_analytics`. `cmd_full --mode staging` and `--mode prod` error with a Phase 3 message — local-against-S3 writes are rejected per the roadmap alternatives table.

6. **Subcommands removed.** `fetch`, `transform`, `consolidate`, and `analytics` subcommands and their handler functions (`cmd_fetch`, `cmd_transform`) are removed. `cmd_consolidate` and `cmd_analytics` remain as internal helpers called by `cmd_run_consolidate_analytics`.

7. **`.github/workflows/pipeline.yml` deleted.** It ran the pipeline locally in CI using `DEMO`/`STORAGE_TYPE`/`*_DEMO` and offered deleted subcommands. Staging deploys already trigger SFN via `deploy-staging.yml`; this file was Phase 4 scope pulled forward.

## Constraints

- `_DEMO`-suffixed env var names (`IBKR_FLEX_TOKEN_DEMO`, etc.) are NOT removed — Phase 4 removes them and renames SSM parameters.
- `scripts/run_prod_pipeline.py` is NOT deleted — Phase 3 absorbs it into `cmd_full`.
- `cmd_full --mode staging/prod` triggering SFN is Phase 3.
- `LocalBackend` class stays for test injection; `resolve_storage()` no longer produces it.
- ECS task definitions must pass `--mode` as a command argument (e.g., `["run-connector", "ibkr", "--mode", "staging"]`).

## Consequences

- **Simpler configuration.** One flag (`--mode`) replaces two env vars (`DEMO`, `STORAGE_TYPE`). No more `DEMO=true` + `STORAGE_TYPE=minio` + `_DEMO` secrets combination to get right.
- **Single entry point.** `pipeline run full --mode docker` is the only way to run the full pipeline locally. The SFN building blocks (`run-connector`, `run-consolidate-analytics`) remain for per-step debugging.
- **No silent skip.** Running `pipeline run full` without `--mode` prints argparse usage with the three choices. Missing credentials fail loudly (Phase 1).
- **Parallel connectors in docker mode.** The ThreadPoolExecutor in `cmd_full` mirrors the SFN Map state. Connectors run concurrently with fail-fast on first error.
- **Breaking change for ECS tasks.** Task definitions must add `--mode staging`/`--mode prod` to the command and remove `DEMO=true` from the environment. This is a Terraform migration.
- **Breaking change for local users.** `DEMO` and `STORAGE_TYPE` env vars no longer work. Users must pass `--mode docker` (or `staging`/`prod`). The `.env.example` documents the new setup.
- **`fetch`/`transform`/`consolidate`/`analytics` subcommands are gone.** Users who relied on them must use `run-connector <name>` or `run-consolidate-analytics` instead.
- **`LocalBackend` moved to `tests/local_backend.py`.** Because `resolve_storage()` only ever returns `S3Backend` now (there is no `--mode local`), the local-filesystem backend is test-only. The `StorageConfig.backend` field is now required (the `__post_init__` default that silently substituted `LocalBackend` is removed), so every `StorageConfig` must be constructed with an explicit backend. `keygen` no longer resolves storage — it prints `ENCRYPTION_KEY` instructions, which is the only supported path now that all modes are S3-backed.

## Validation

- All 635 existing + new tests pass after the refactor.
- `pipeline run full` without `--mode` → argparse error listing choices.
- `pipeline run full --mode staging` → exit 1 with Phase 3 message.
- `pipeline run full --mode docker` → runs connectors in parallel via ThreadPoolExecutor then consolidate-analytics.
- `pipeline run run-connector ibkr --mode docker` and `pipeline run query "SELECT 1" --mode docker` still work.
- `is_demo()` returns `True` only when `--mode staging`.
- `resolve_storage()` produces `S3Backend` with MinIO endpoint for docker, demo bucket for staging, prod bucket for prod. Missing `S3_BUCKET` raises `ValueError`.
- `DEMO` and `STORAGE_TYPE` env vars are not read anywhere in the codebase.