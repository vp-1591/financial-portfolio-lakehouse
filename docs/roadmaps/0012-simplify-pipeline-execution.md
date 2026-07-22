# Roadmap: Simplify Pipeline Execution Model

## Goal

Replace the current tangle of `DEMO`, `STORAGE_TYPE`, and `_DEMO`-suffixed env vars with a single `--mode docker|staging|prod` flag that determines storage backend, credential resolution, and execution model. In staging and prod modes, `full` triggers a Step Functions execution instead of running locally. The pipeline CLI becomes the single entry point for all three execution contexts, and `scripts/run_prod_pipeline.py` is absorbed into it.

## Current state

The pipeline has **three overlapping configuration axes** that must all be set correctly for a given execution context:

1. **`DEMO=true|false`** — switches broker API URLs (T212 demo endpoint), injects `$1M` synthetic deposit for IBKR, and routes secrets to `_DEMO`-suffixed env vars. Documented in ADR 0037.

2. **`STORAGE_TYPE=cloud|minio|local`** — determines where Delta tables live. `cloud` requires `S3_BUCKET`, `minio` requires `S3_ENDPOINT_URL`, `local` uses the filesystem. Documented in ADR 0039.

3. **`*_DEMO` env vars** — every secret has a `_DEMO` variant (`IBKR_FLEX_TOKEN_DEMO`, `T212_API_KEY_DEMO`, `AWS_ACCESS_KEY_ID_DEMO`, etc.). These are required in demo mode and must not be mixed with production secrets. Documented in ADRs 0037–0044.

The result: running the pipeline locally against AWS requires setting ~15 environment variables correctly. Running `DEMO=true` with missing `_DEMO` secrets silently skips all brokers and prints a misleading "run transform first" error. Running `scripts/run_prod_pipeline.py` is a separate, undocumented workflow.

Key pain points:

- **Silent failure**: `fetch_connector()` returns exit code `0` when secrets are missing, so `cmd_full` proceeds through transform and consolidate before failing with "No holdings found. Run the transform step first." (run.py:148, run.py:345-347).
- **Credential explosion**: Each secret has a `_DEMO` variant. Users must configure both sets.
- **`STORAGE_TYPE` confusion**: `STORAGE_TYPE=local` with `S3_BUCKET` set previously defaulted to cloud. `STORAGE_TYPE=cloud` without `S3_BUCKET` is an error. `STORAGE_TYPE` is an implementation detail that users shouldn't need to think about.
- **Three separate entry points**: `pipeline.run full` (local), `scripts/run_prod_pipeline.py` (prod SFN), and GitHub Actions workflow dispatch (demo/prod CI). No single command for all contexts.

## Success criteria

- [ ] `pipeline run full --mode staging` triggers the demo Step Functions execution and returns the ARN — no local broker or AWS data credentials needed on the caller's machine
- [ ] `pipeline run full --mode prod` triggers the prod Step Functions execution — same UX as staging
- [ ] `pipeline run full --mode docker` runs the full pipeline locally against MinIO — equivalent to the current `cmd_full` behavior
- [ ] Running `pipeline run full` without `--mode` or `PIPELINE_MODE` prints a clear error listing the three modes
- [ ] When all broker credentials are missing, `fetch` exits with a non-zero code and a clear message (e.g., "No broker credentials found. In Docker mode, set IBKR_FLEX_TOKEN or T212_API_KEY in .env. In staging/prod mode, use --mode staging or --mode prod.")
- [ ] `scripts/run_prod_pipeline.py` is deleted — its functionality is absorbed into `cmd_full`
- [ ] `DEMO` and `STORAGE_TYPE` env vars are removed from user-facing docs and `.env.example`; `PIPELINE_MODE` replaces them
- [ ] `query` and `report` commands work in all three modes (read-only S3 access for staging/prod, MinIO for docker)
- [ ] Individual step commands (`fetch`, `transform`, `consolidate`, `analytics`) require `--mode docker` and error in staging/prod modes
- [ ] All existing tests pass after the refactor
- [ ] ECS task definitions still work (they set `PIPELINE_MODE` env var instead of `DEMO`)

## Alternatives considered

| Approach | Why rejected |
|----------|-------------|
| Keep `DEMO` env var alongside `--mode` flag | Two ways to set the same thing creates ambiguity about which takes precedence. One mechanism is simpler. |
| Auto-detect mode from `S3_BUCKET` presence | Implicit behavior is surprising. `S3_BUCKET` being set doesn't mean you want to trigger SFN. Explicit is better. |
| Separate `cloud` subcommand instead of `--mode` | Adds a subcommand that duplicates `full`. A flag on `full` is cleaner because the subcommand structure stays flat. |
| Keep `STORAGE_TYPE` as an escape hatch | If mode always determines storage, `STORAGE_TYPE` is redundant. Escape hatches accumulate into the complexity we're removing. |
| Keep local-against-S3 path (`STORAGE_TYPE=cloud` without SFN) | Risk of accidentally writing to prod data from a local machine. Forces users to manage S3 creds locally. SFN is the correct execution model for AWS writes. |

## Phases

### Phase 1 — Fix silent skip and error early *[status: planned]*

Make `fetch` fail loudly when all broker credentials are missing, instead of silently returning success.

**Scope:**
- [ ] Change `fetch_connector()` (run.py:148) to track which connectors were skipped vs ran
- [ ] When all connectors are skipped (no credentials for any broker), print a clear error message and return exit code 1
- [ ] The error message should guide the user: "No broker credentials found. In Docker mode, set IBKR_FLEX_TOKEN or T212_API_KEY. In staging/prod mode, use --mode staging or --mode prod."
- [ ] Add tests for the new error path

**Out of scope:**
- `--mode` flag (Phase 2)
- Changing the `DEMO` env var pattern
- SFN trigger behavior

**Files:** `pipeline/run.py`, `tests/test_run.py`

**Links:** Issue: `fetch_connector` returns 0 when secrets are missing (run.py:148)

---

### Phase 2 — Add `--mode` flag and `PIPELINE_MODE` env var *[status: planned]*

Add a `--mode docker|staging|prod` CLI flag and a `PIPELINE_MODE` env var that together replace `DEMO` and `STORAGE_TYPE`. Mode determines storage backend, credential resolution strategy, and whether `full` runs locally or triggers SFN.

**Scope:**
- [ ] Add `--mode` flag to `pipeline run` top-level parser (applies to all subcommands)
- [ ] Add `PIPELINE_MODE` env var as fallback (for ECS task definitions)
- [ ] Add `resolve_mode()` function that reads `--mode` flag → `PIPELINE_MODE` env var → error if unset
- [ ] Derive storage config from mode: docker → MinIO, staging → demo S3, prod → prod S3
- [ ] Derive `is_demo()` from mode: staging → True, docker/prod → False
- [ ] Keep `DEMO` env var as deprecated alias for `PIPELINE_MODE=staging` (with deprecation warning)
- [ ] Remove `STORAGE_TYPE` env var — mode determines storage
- [ ] Add `--mode` to `query` and `report` commands (determines which S3 bucket to read)
- [ ] Individual step commands (`fetch`, `transform`, `consolidate`, `analytics`) require `--mode docker` and error in staging/prod modes
- [ ] Update `.env.example` to show `PIPELINE_MODE=docker` instead of `STORAGE_TYPE` and `DEMO`
- [ ] Update README and configuration docs

**Out of scope:**
- Making `full` trigger SFN (Phase 3)
- Removing `_DEMO` env var pattern from ECS tasks (Phase 4)
- Removing `scripts/run_prod_pipeline.py` (Phase 3)

**Files:** `pipeline/run.py`, `pipeline/secrets.py`, `pipeline/storage.py`, `pipeline/query.py`, `.env.example`, `docs/configuration.md`

**Links:** ADRs 0037–0044 (demo mode, storage type, credential isolation)

---

### Phase 3 — Make `full` trigger Step Functions in staging/prod modes *[status: planned]*

When `--mode staging` or `--mode prod`, `cmd_full` starts a Step Functions execution instead of running the pipeline locally. Absorb `scripts/run_prod_pipeline.py` into `cmd_full`.

**Scope:**
- [ ] Add `boto3` dependency for Step Functions API (or reuse from `scripts/run_prod_pipeline.py`)
- [ ] `cmd_full` in staging mode: call `sfn.start_execution()` with the demo state machine ARN and execution input
- [ ] `cmd_full` in prod mode: call `sfn.start_execution()` with the prod state machine ARN
- [ ] Print execution ARN and AWS console URL (same UX as `run_prod_pipeline.py`)
- [ ] Add `--wait` flag to `full` that polls the execution until completion (useful for CI)
- [ ] Support `--with-xtb` and `--xtb-file` flags for SFN input (absorbed from `run_prod_pipeline.py`)
- [ ] Add state machine ARNs to configuration (env vars or hardcoded from Terraform outputs)
- [ ] Delete `scripts/run_prod_pipeline.py`
- [ ] Update Terraform ECS task definitions to use `PIPELINE_MODE` env var instead of `DEMO`

**Out of scope:**
- Changing the SFN state machine definition itself
- Adding new connectors
- `query` and `report` in staging/prod mode (they already read from S3, no SFN needed)

**Files:** `pipeline/run.py`, `pipeline/sfn.py` (new), `scripts/run_prod_pipeline.py` (delete), `terraform/`

**Links:** `scripts/run_prod_pipeline.py`, ADR 0038 (demo Terraform infrastructure)

---

### Phase 4 — Remove `_DEMO` env var pattern from user-facing config *[status: planned]*

With mode determining credential resolution, users no longer need `_DEMO`-suffixed env vars. The `_DEMO` pattern becomes an internal implementation detail for ECS task definitions (SSM injects env vars with `_DEMO` suffix for backward compat).

**Scope:**
- [ ] In Docker mode, `resolve_secret()` reads base secret names only (no `_DEMO` suffix lookup)
- [ ] In staging mode (local CLI, e.g. `query --mode staging`), `resolve_secret()` reads `_DEMO` variants from env or uses S3 read-only creds
- [ ] In staging mode (ECS task), SSM injects secrets as env vars; mode flag determines which SSM path to use
- [ ] Remove `_DEMO` env vars from `.env.example` (users don't set them; SSM handles them in AWS)
- [ ] Simplify `secrets.py`: remove `DEMO_SECRET_MAP` pattern in favor of mode-based resolution
- [ ] Keep `_DEMO` env var support as an internal fallback for ECS tasks during migration
- [ ] Update ADRs 0037–0044 or create a new ADR documenting the `PIPELINE_MODE` approach

**Out of scope:**
- Changing SSM parameter paths (they're fine as-is with `/pipeline/demo/` and `/pipeline/prod/` prefixes)
- Changing Terraform SSM parameter injection (ECS tasks still need env vars)
- Changing the T212 demo API URL behavior (staging mode still uses demo endpoint)

**Files:** `pipeline/secrets.py`, `pipeline/storage.py`, `.env.example`, `docs/configuration.md`, `docs/adr/`

**Links:** ADRs 0037–0044, `terraform/demo/main.tf`, `terraform/prod/main.tf`