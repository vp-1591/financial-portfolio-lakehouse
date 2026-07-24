# Roadmap: Simplify Pipeline Execution Model

## Goal

Replace the current tangle of `DEMO`, `STORAGE_TYPE`, and `_DEMO`-suffixed env vars with a single `--mode docker|staging|prod` flag that determines storage backend, credential resolution, and execution model. In staging and prod modes, `full` triggers a Step Functions execution instead of running locally. In docker mode, `full` mirrors the SFN workflow locally (parallel connectors with validation between steps) instead of the current naive sequential chain. The pipeline CLI becomes the single entry point for all three execution contexts, and `scripts/run_prod_pipeline.py` is absorbed into it.

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
- **Local `full` doesn't mirror SFN workflow**: The SFN runs connectors in parallel, validates after each connector, retries on failure, and only then runs consolidate+analytics. Local `full` just chains fetch → transform → consolidate → analytics sequentially with no validation between steps and no retry logic. The SFN's two internal commands (`run-connector` and `run-consolidate-analytics`) already encode the correct workflow — local `full` should use the same building blocks.
- **Dead command in CI**: `.github/workflows/pipeline.yml` still offers `allocate` as a workflow dispatch option, but `allocate` was removed per ADR 0026. It also offers `fetch`, `transform`, `consolidate` — all being removed.

## Success criteria

- [ ] `pipeline run full --mode staging` triggers the demo Step Functions execution and returns the ARN — only AWS credentials (for `states:StartExecution`) are needed locally; broker secrets (IBKR_FLEX_TOKEN, T212_API_KEY) are NOT required because they are injected by SSM into ECS containers at runtime
- [ ] `pipeline run full --mode prod` triggers the prod Step Functions execution — same credential model: AWS creds locally, broker creds handled by infrastructure
- [ ] `pipeline run full --mode docker` runs the full pipeline locally against MinIO, mirroring the SFN workflow: connectors run via `run-connector` (each: fetch → transform → validate), then `run-consolidate-analytics` (consolidate + CDC + analytics + validate)
- [ ] In docker mode, `full` runs connectors in parallel (threading) and fails fast if any connector fails — same semantics as the SFN Map state
- [ ] Running `pipeline run full` without `--mode` prints a clear error listing the three modes
- [ ] When all broker credentials are missing, `fetch` exits with a non-zero code and a clear message (e.g., "No broker credentials found. In Docker mode, set IBKR_FLEX_TOKEN or T212_API_KEY in .env. In staging/prod mode, use --mode staging or --mode prod.")
- [ ] `scripts/run_prod_pipeline.py` is deleted — its functionality is absorbed into `cmd_full`
- [ ] `DEMO`, `STORAGE_TYPE`, and all `_DEMO`-suffixed env vars are removed — `--mode` flag replaces them; GitHub Secrets use `_STAGING` suffix where environment scoping is needed (e.g., `AWS_ACCESS_KEY_ID_STAGING`); ECS tasks pass `--mode` in the command and inject secrets under base names (no suffix) since each environment is an isolated container
- [ ] `query` and `report` commands work in all three modes (read-only S3 access for staging/prod, MinIO for docker)
- [ ] Remove `fetch`, `transform`, `consolidate`, `analytics` subcommands entirely — `run-connector <name>` and `run-consolidate-analytics` are the only building blocks needed (used by both SFN and local `full`)
- [ ] Remove `cmd_fetch`, `cmd_transform`, `cmd_consolidate`, `cmd_analytics` functions and their CLI parser registrations
- [ ] All existing tests pass after the refactor
- [ ] ECS task definitions still work (they pass `--mode staging` or `--mode prod` in the command instead of setting `DEMO` env var)

## Alternatives considered

| Approach | Why rejected |
|----------|-------------|
| ~~Keep `DEMO` env var as deprecated alias~~ | Rejected: two ways to set the same thing creates ambiguity. Clean cut is simpler than a deprecation period — the only consumers are `.env` files and ECS task definitions, both easily updated. |
| `PIPELINE_MODE` env var alongside `--mode` flag | Two sources creates "which wins?" ambiguity (env var says staging, flag says prod). Single source of truth (`--mode` flag only) is simpler — ECS tasks pass `--mode` in their command, which is already the pattern for `--target-currency`. |
| Auto-detect mode from `S3_BUCKET` presence | Implicit behavior is surprising. `S3_BUCKET` being set doesn't mean you want to trigger SFN. Explicit is better. |
| Separate `cloud` subcommand instead of `--mode` | Adds a subcommand that duplicates `full`. A flag on `full` is cleaner because the subcommand structure stays flat. |
| Keep `STORAGE_TYPE` as an escape hatch | If mode always determines storage, `STORAGE_TYPE` is redundant. Escape hatches accumulate into the complexity we're removing. |
| Keep local-against-S3 path (`STORAGE_TYPE=cloud` without SFN) | Risk of accidentally writing to prod data from a local machine. Forces users to manage S3 creds locally. SFN is the correct execution model for AWS writes. |
| Keep `fetch`/`transform`/`consolidate`/`analytics` as subcommands | These are raw building blocks that leak orchestration to the user and skip validation between steps. `run-connector <name>` and `run-consolidate-analytics` already provide per-step debugging with validation. The individual commands run all connectors instead of one, skip validation, and produce incomplete state — they're worse for debugging than the orchestrator commands. |

## Phases

### Phase 1 — Fix silent skip and error early *[status: done]*

Make `fetch` fail loudly when all broker credentials are missing, instead of silently returning success.

**Scope:**
- [ ] Change `fetch_connector()` (run.py:148) to track which connectors were skipped vs ran
- [ ] When all connectors are skipped (no credentials for any broker), print a clear error message and return exit code 1
- [ ] The error message should guide the user: "No broker credentials found. In Docker mode, set IBKR_FLEX_TOKEN or T212_API_KEY in .env. In staging/prod mode, use --mode staging or --mode prod to trigger the pipeline in AWS (no local broker credentials needed)."
- [ ] Add tests for the new error path

**Out of scope:**
- `--mode` flag (Phase 2)
- Changing the `DEMO` env var pattern
- SFN trigger behavior

**Files:** `pipeline/run.py`, `tests/test_run.py`

**Links:** Issue: `fetch_connector` returns 0 when secrets are missing (run.py:148)

---

### Phase 2 — Add `--mode` flag *[status: done]*

Add a `--mode docker|staging|prod` CLI flag that replaces `DEMO` and `STORAGE_TYPE`. Mode determines storage backend, credential resolution strategy, and whether `full` runs locally or triggers SFN. There is no env var — `--mode` is the single source of truth. ECS task definitions pass `--mode` as a command argument (e.g., `["run-connector", "ibkr", "--mode", "staging"]`).

**Scope:**
- [ ] Add `--mode` flag via a **parent parser** shared by all subparsers, so it appears *after* the subcommand name — e.g. `pipeline run full --mode docker`, `pipeline run run-connector ibkr --mode staging`, `pipeline run query "..." --mode docker`. This matches the ECS command form `["run-connector", "ibkr", "--mode", "staging"]`. Do NOT put `--mode` on the top-level parser (before the subcommand) — under argparse that would require `--mode` to precede the subcommand, conflicting with the ECS command form and the success-criteria examples
- [ ] Add `resolve_mode()` function that reads `--mode` flag from parsed args — error if unset
- [ ] Derive storage config from mode: docker → MinIO, staging → demo S3, prod → prod S3
- [ ] Derive `is_demo()` from mode: staging → True, docker/prod → False
- [ ] Remove `DEMO` and `STORAGE_TYPE` env vars entirely
- [ ] Add `--mode` to `query` and `report` commands (determines which S3 bucket to read)
- [ ] Rewrite `cmd_full` in docker mode to mirror the SFN workflow: run each connector via `cmd_run_connector` (fetch + transform + validate), then `cmd_run_consolidate_analytics` (consolidate + CDC + analytics + validate) — same building blocks the SFN uses
- [ ] In docker mode, run connectors in parallel using `concurrent.futures.ThreadPoolExecutor` with fail-fast on any connector error
- [ ] `cmd_full --mode staging` and `--mode prod` print a clear "not yet implemented" error and exit 1 — the Step Functions trigger lands in Phase 3. They must NOT fall back to running the orchestrator locally against S3 (that path is rejected in the alternatives table — risk of writing to prod data from a local machine). Message: `full --mode staging is not yet implemented (Step Functions trigger lands in Phase 3). Use --mode docker, or run run-connector / run-consolidate-analytics directly.`
- [ ] Remove `fetch`, `transform`, `consolidate`, `analytics` subcommands, their handler functions (`cmd_fetch`, `cmd_transform`), and CLI parser registrations — `cmd_consolidate` and `cmd_analytics` stay as internal helpers called by `cmd_run_consolidate_analytics`
- [x] Delete `.github/workflows/pipeline.yml` entirely (pulled forward from Phase 4). It runs the pipeline locally in CI using `DEMO`/`STORAGE_TYPE`/`*_DEMO` env vars and offers the deleted `fetch`/`transform`/`consolidate`/`allocate` commands — exactly the pattern Phase 2 removes, so a "minimal `--mode` update" would actually be a full rewrite of a file that is doomed in Phase 4. Staging deploys already trigger SFN via `deploy-staging.yml`; prod via `deploy-prod.yml`. No remaining purpose.
- [ ] Update comment in `pipeline/connectors/xtb/connector.py:32` that references `cmd_fetch`
- [ ] Update `README.md`, `docs/deployment/local.md`, `docs/brokers/xtb.md` to remove references to deleted commands
- [ ] Delete `TestCmdFetchRegression` and `TestCmdTransformRegression` from `tests/test_run_subcommands.py` — they test deleted commands
- [ ] Delete `TestCmdFullRegression` from `tests/test_run_subcommands.py` — it tests the old sequential chain; new tests for the orchestrator-based `cmd_full` will be written alongside the implementation
- [ ] `run-connector` and `run-consolidate-analytics` are the canonical building blocks — used by both SFN and local `full`
- [ ] Update `.env.example` to remove `STORAGE_TYPE` and `DEMO`; document `--mode docker` as the local dev default
- [ ] Update README and configuration docs

**Out of scope:**
- Making `full` trigger SFN (Phase 3)
- Removing `_DEMO` env var pattern from ECS tasks (Phase 4)
- Removing `scripts/run_prod_pipeline.py` (Phase 3)

**Files:** `pipeline/run.py`, `pipeline/secrets.py`, `pipeline/storage.py`, `pipeline/query.py`, `.env.example`, `docs/configuration.md`

**Links:** ADRs 0037–0044 (demo mode, storage type, credential isolation)

---

### Phase 3 — Make `full` trigger Step Functions in staging/prod modes *[status: done]*

When `--mode staging` or `--mode prod`, `cmd_full` starts a Step Functions execution instead of running the pipeline locally. Absorb `scripts/run_prod_pipeline.py` (a manual trigger script, not used by any CI/CD workflow) into `cmd_full`. This replaces the Phase 2 "not yet implemented" stub for staging/prod `full` with the real SFN trigger.

**Credential model:** In staging/prod modes, the caller's machine needs only AWS credentials with `states:StartExecution` permission — no broker API keys, no S3 data-plane credentials, no `_DEMO` env vars. Broker secrets are injected into ECS containers by SSM at task launch time. The Step Functions orchestrator handles all connector execution in AWS, so the local CLI only needs permission to trigger the run, not to access broker data.

```
Local machine                     AWS
──────────────                    ──────────────
AWS creds only
(IBKR_FLEX_TOKEN NOT needed)
       │
       ▼
boto3 → start_execution() ───────► Step Functions
                                     │
                                     ▼
                                  ECS Fargate tasks
                                     │
                                     ▼
                                  SSM injects broker secrets
                                  as env vars into containers
                                     │
                                     ▼
                                  Connectors run with
                                  IBKR_FLEX_TOKEN, T212_API_KEY
                                  (from /pipeline/demo/ or /pipeline/prod/)
```

**SFN execution input schema:** `cmd_full` builds an execution input matching the state machine's expected schema. Each environment has its own state machine (`portfolio-pipeline-orchestrator-demo` and `portfolio-pipeline-orchestrator`), but they share the same ASL definition. The input tells the state machine which ECS task definitions to run and what commands to pass:

```json
{
  "connectors": [
    {"name": "ibkr", "task_def_arn": "arn:aws:ecs:...", "command": ["run-connector", "ibkr", "--mode", "staging", "--target-currency", "EUR"]},
    {"name": "trading212", "task_def_arn": "arn:aws:ecs:...", "command": ["run-connector", "trading212", "--mode", "staging", "--target-currency", "EUR"]}
  ],
  "consolidate_allocate_task_def_arn": "arn:aws:ecs:...",
  "consolidate_command": ["run-consolidate-analytics", "--mode", "staging", "--target-currency", "EUR"]
}
```

The `demo` field is dropped from the input — it was vestigial (the ASL never references `$.demo`). The `consolidate_command` field is new (see ASL change below). Task definition ARNs are resolved at runtime via `boto3 ecs.describe_task_definition` using family names (`portfolio-pipeline-{env}-{connector}`), not hardcoded.

**Scope:**
- [x] Use `boto3` for Step Functions and ECS APIs. *(Correction: the original note that boto3 was "already a dependency — `pipeline/s3.py` imports it" was wrong; `pipeline/s3.py` uses `pyarrow.fs`. boto3 was added to `[project.optional-dependencies] pipeline` as `boto3==1.37.0`. See ADR 0091.)*
- [x] `cmd_full` in staging mode: call `sfn.start_execution()` with the staging state machine ARN and execution input
- [x] `cmd_full` in prod mode: call `sfn.start_execution()` with the prod state machine ARN and execution input
- [x] Default connector list for SFN input: `ibkr` and `trading212` (XTB is not supported in staging/prod `full` — see below)
- [x] `--with-xtb` and `--xtb-file` flags in staging/prod mode raise an error with guidance to use `upload-xtb` + EventBridge trigger instead
- [x] Resolve ECS task definition ARNs at runtime via `boto3 ecs.describe_task_definition` (family names follow `portfolio-pipeline-{env}-{connector}` for connectors and `portfolio-pipeline-{env}-consolidate-allocate` for the consolidate step)
- [x] Print the execution ARN and a clickable Step Functions console URL after starting the execution
- [x] Validate that AWS credentials are configured before calling SFN; print actionable error if missing (e.g., "Run `aws configure` or set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY")
- [x] Add `--wait` flag to `full` that polls the SFN execution until completion (default timeout: 900 seconds / 15 minutes, polling interval: 30 seconds). On `SUCCEEDED`, exit 0. On `FAILED`, `TIMED_OUT`, or `ABORTED`, exit 1 and print failure details: (1) fetch and parse SFN execution history events (TaskFailed, TaskTimedOut, ExecutionFailed) and (2) fetch and print CloudWatch container logs for each connector task — absorbing the functionality of `.github/scripts/parse_stepfunctions_event.py` and `.github/scripts/format_log_events.py`
- [x] Add `STAGING_STATE_MACHINE_ARN` and `PROD_STATE_MACHINE_ARN` environment variables for state machine ARN configuration (set by Terraform outputs in `.env` or CI secrets, not hardcoded)
- [x] Update the SFN state machine ASL definition to read the consolidate command from execution input (`"Command.$": "$.consolidate_command"`) instead of hardcoding `["run-consolidate-analytics", "--target-currency", "EUR"]`. This is a one-line ASL change that allows `--mode` to be passed through to the consolidate step. The Map→ConsolidateAllocate flow stays the same
- [x] Delete `scripts/run_prod_pipeline.py` (manual prod trigger script, now absorbed into `cmd_full`)
- [x] Simplify `.github/workflows/deploy-staging.yml`: replace the "Trigger demo pipeline", "Wait for demo pipeline", and "Print container logs on failure" steps with `python -m pipeline.run full --mode staging --wait`; keep Docker build/push steps
- [x] Delete `.github/scripts/parse_stepfunctions_event.py` and `.github/scripts/format_log_events.py` — failure detail printing moves into `--wait`
- [x] Update Terraform ECS task definitions: add `--mode staging` or `--mode prod` to connector commands and the consolidate-allocate command (e.g., `["run-connector", "ibkr", "--mode", "staging", "--target-currency", "EUR"]`). Remove `DEMO`, `STORAGE_TYPE`, `IBKR_ENABLED`, `T212_ENABLED`, and `XTB_ENABLED` from the `common_environment` blocks — each ECS task runs exactly one connector via `run-connector <name>`, so the enabled flags are redundant (they default to enabled when unset), and `DEMO`/`STORAGE_TYPE` are dead env vars since Phase 2 removed the Python code that reads them. `S3_BUCKET`, `S3_BUCKET_DEMO`, `S3_PREFIX_DEMO`, and `AWS_REGION` stay (still read by `resolve_storage()` and `resolve_aws_credentials()`). `_DEMO`-suffixed secret env var names remain until Phase 4
- [x] Update EventBridge input templates in the orchestrator module: add `--mode staging` or `--mode prod` to connector commands, add `consolidate_command` with `--mode`, and drop the vestigial `demo` field from the input

**Out of scope:**
- Changing the SFN state machine structure (Map → ConsolidateAllocate flow stays the same — only the consolidate command is parameterized)
- Adding new connectors
- XTB connector in staging/prod `full` command (use `upload-xtb` + EventBridge trigger instead)
- Adding `--wait` or SFN trigger to `deploy-prod.yml` (it only builds/pushes Docker; prod execution is triggered via the daily EventBridge schedule or manual `full --mode prod`)
- `query` and `report` in staging/prod mode (they already read from S3, no SFN needed)

**Files:** `pipeline/run.py`, `pipeline/sfn.py` (new), `scripts/run_prod_pipeline.py` (delete), `.github/workflows/deploy-staging.yml`, `.github/scripts/parse_stepfunctions_event.py` (delete), `.github/scripts/format_log_events.py` (delete), `terraform/modules/orchestrator/main.tf`, `terraform/demo/main.tf`, `terraform/prod/main.tf`

**Links:** `scripts/run_prod_pipeline.py`, ADR 0051 (Step Functions orchestration), ADR 0038 (demo Terraform infrastructure)

---

### Phase 4 — Remove `_DEMO` env var pattern *[status: done]*

The `_DEMO` suffix is a double indirection: SSM parameter names have `_DEMO`, env var names have `_DEMO`, and Python has `DEMO_SECRET_MAP` to swap them. But demo and prod ECS tasks are **completely separate containers** — they never share an environment. There is no reason for the env var names to differ between them. The SSM path prefix (`/pipeline/demo/` vs `/pipeline/prod/`) already provides isolation; the env var suffix is redundant.

**Before (current):**
```
Demo ECS task:  SSM /pipeline/demo/IBKR_FLEX_TOKEN_DEMO → env var IBKR_FLEX_TOKEN_DEMO
                Python: resolve_secret("IBKR_FLEX_TOKEN") → reads IBKR_FLEX_TOKEN_DEMO

Prod ECS task:  SSM /pipeline/prod/IBKR_FLEX_TOKEN      → env var IBKR_FLEX_TOKEN
                Python: resolve_secret("IBKR_FLEX_TOKEN") → reads IBKR_FLEX_TOKEN
```

**After:**
```
Demo ECS task:  SSM /pipeline/demo/IBKR_FLEX_TOKEN → env var IBKR_FLEX_TOKEN
                Python: resolve_secret("IBKR_FLEX_TOKEN") → reads IBKR_FLEX_TOKEN

Prod ECS task:  SSM /pipeline/prod/IBKR_FLEX_TOKEN → env var IBKR_FLEX_TOKEN
                Python: resolve_secret("IBKR_FLEX_TOKEN") → reads IBKR_FLEX_TOKEN
```

**Scope:**
- [ ] Rename SSM parameters from `/pipeline/demo/<NAME>_DEMO` to `/pipeline/demo/<NAME>` (Terraform migration: create new params, update task definitions, delete old params)
- [ ] Update demo ECS task definitions: inject secrets as base env var names (`IBKR_FLEX_TOKEN` instead of `IBKR_FLEX_TOKEN_DEMO`)
- [ ] Remove `S3_BUCKET_DEMO` and `S3_PREFIX_DEMO` env vars from demo task definitions — `S3_BUCKET` and `S3_PREFIX` already have the correct values in each environment
- [ ] ~~Remove `DEMO=true/false`, `STORAGE_TYPE`, `IBKR_ENABLED`, `T212_ENABLED`, `XTB_ENABLED` from ECS task `environment` blocks~~ — already done in Phase 3
- [ ] Delete `DEMO_SECRET_MAP`, `REQUIRED_SECRETS_DEMO`, `REQUIRED_SECRETS_S3_DEMO`, `REQUIRED_SECRETS_DEMO_NON_AWS` from `secrets.py`
- [ ] Simplify `resolve_secret()` to always read the base env var name — no suffix swapping
- [ ] Simplify `is_demo()` to check `--mode staging` instead of `DEMO=true`
- [ ] Remove `_DEMO` env vars from `.env.example`
- [ ] ~~Delete `.github/workflows/pipeline.yml`~~ — moved to Phase 2 (the file was already broken by Phase 2's command/env-var deletions, so deletion was pulled forward)
- [ ] Update `.github/workflows/deploy-staging.yml`: rename GitHub Secrets references from `_DEMO` suffix to `_STAGING` (e.g., `secrets.AWS_ACCESS_KEY_ID_DEMO` → `secrets.AWS_ACCESS_KEY_ID_STAGING`, `secrets.DEMO_STATE_MACHINE_ARN` → `secrets.STAGING_STATE_MACHINE_ARN`). The `demo: true` field was already dropped from the SFN input in Phase 3
- [ ] Update `.github/workflows/deploy-prod.yml`: no env var changes needed (mode is a CLI flag, not an env var)
- [ ] Update ADRs 0037–0044 or create a new ADR documenting the `--mode` approach

**Out of scope:**
- Changing the T212 demo API URL behavior (staging mode still uses demo endpoint)
- Changing the SFN state machine definition
- Adding new connectors

**Migration notes:** The SSM parameter rename requires a Terraform migration. Create new parameters under `/pipeline/demo/<NAME>` (without `_DEMO`), update the demo ECS task definitions to reference the new parameter names, then remove the old `/pipeline/demo/<NAME>_DEMO` parameters. The old parameter values should be copied to the new ones before deletion. This must be done in a single Terraform apply to avoid downtime.

**Files:** `pipeline/secrets.py`, `pipeline/storage.py`, `.env.example`, `docs/configuration.md`, `docs/adr/`, `terraform/demo/main.tf`, `terraform/prod/main.tf`

**Links:** ADRs 0037–0044, `terraform/demo/main.tf`, `terraform/prod/main.tf`