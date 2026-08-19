# Rename Plan — CDC→events and demo→staging

Ordered execution plan for one PR implementing CAP-1…CAP-4. Downstream planning/implementation skills execute this in order; migrations run manually against staging before the deploy that ships the renamed code.

## Track A — CDC → events (issue #131)

Rename inventory (verified 2026-08-19):

| Kind | Old | New |
|---|---|---|
| Delta table (raw) | `raw/{broker}_cdc` | `raw/{broker}_events` |
| Delta table (normalized) | `normalized/{broker}_cdc` | `normalized/{broker}_events` |
| Delta table (consolidated) | `normalized/cdc_events` | `normalized/events` |
| Module | `pipeline/normalized/consolidate_cdc.py` | `consolidate_events.py` |
| Module | `pipeline/analytics/cdc_tables.py` | `events_tables.py` (open Q3) |
| Method | `fetch_cdc`, `fetch_cdc_kwargs` (base + 3 connectors) | `fetch_events`, `fetch_events_kwargs` |
| Function | `dedup_cdc_events` (transform_utils) | `dedup_events` |
| Function | `consolidate_cdc_events` | `consolidate_events` |
| Schema | `cdc_events_normalized_schema` | `events_normalized_schema` |
| Constant | `_REQUIRED_CDC_BROKERS` | `_REQUIRED_EVENTS_BROKERS` |
| Paths env names | `RAW_*_CDC`, `NORMALIZED_*_CDC` (paths.py) | `*_EVENTS` |
| CLI | `consolidate-cdc`, `normalize-cdc` subcommands | `consolidate-events`, `normalize-events` |
| Misc | comments, docstrings, logger labels, report sections, DQ config keys | `events` equivalents |

Scope size: ~27 pipeline sources, ~20 test files, 37 ADRs (historical — untouched), ~11 docs. The `event_*` columns do **not** change.

**Migration A** — new script `pipeline/migrations/migrate_cdc_to_events.py`:
1. For each `{broker}_cdc` raw and normalized Delta table present in the environment bucket: read location, rename to `{broker}_events` (Delta `ALTER TABLE RENAME` or S3 copy preserving the `_delta_log`), skip absent tables.
2. Rename `cdc_events` → `events`.
3. Idempotent: exit 0 when all target names already exist / sources absent; raise on auth/region/permission errors or unexpected schema (mirror `migrate_snapshot_schema_unify.py` conventions).
4. Run manually pre-deploy, per env: `.venv/Scripts/python -m pipeline.migrations.migrate_cdc_to_events --mode staging [--dry-run]`.
5. Verify: `pipeline.run query "SELECT count(*) FROM events" --decrypt --mode staging` equals pre-migration `cdc_events` count.

## Track B — demo → staging (staging AWS env)

Rename inventory (verified 2026-08-19):

| Kind | Old | New |
|---|---|---|
| S3 bucket | `investment-portfolio-pipeline-demo` | `investment-portfolio-pipeline-staging` |
| Data prefix | `pipeline_demo` | **removed (empty)** — buckets already isolate envs (ADR 0038/0039); prod `pipeline` prefix deferred |
| IAM user / access key | `pipeline-demo` / `aws_iam_user.pipeline_demo` | `pipeline-staging` |
| IAM policies | `pipeline-demo-s3-access`, `pipeline-demo-cicd` | `pipeline-staging-s3-access`, `pipeline-staging-cicd` |
| IAM role patterns (shared) | `pipeline-task-exec-demo-*`, `pipeline-task-demo-*` | `-staging-*` |
| VPC / IGW / SG / subnets | `pipeline_demo` | `pipeline_staging` |
| SSM params | `/portfolio/demo/*` | `/portfolio/staging/*` |
| State machine | `portfolio-pipeline-orchestrator-demo` | `portfolio-pipeline-orchestrator-staging` |
| ECS task defs / log groups | `pipeline-task-*-demo-*` | `-staging-*` |
| Module var / env label | `var.demo` (`demo = true/false`), `env_label = "demo"` | `var.staging` |
| Code mapping | `MODE_TO_ENV_LABEL = {"staging": "demo", "prod": "prod"}` (sfn.py) | identity `{"staging": "staging", "prod": "prod"}` |
| Terraform tags / comments | `Project = "…-demo"` etc. | `-staging` |
| Backend state key | `financial-portfolio-lakehouse-demo/terraform.tfstate` (sample) | `-staging` |

**Migration B1 — S3 data copy + prefix removal:** bucket rename is a global rename → create `investment-portfolio-pipeline-staging`, copy all objects (`aws s3 sync` preserving encryption) from `pipeline_demo/*` to the **bucket root** (no prefix). Re-point storage config / `S3_BUCKET`. Prefix-removal couplings updated in the same PR:
- `pipeline/storage.py` staging default prefix `"pipeline_demo"` → `""` (S3Backend + `query._discover_tables_s3` already support empty prefix).
- `terraform/staging/main.tf`: `s3_prefix` var default → `""`, `xtb_staging_prefix` → `"xtb_uploads/"` (EventBridge/orchestrator filter follows via `terraform/modules/orchestrator/main.tf:179` `key = [{ prefix = var.xtb_staging_prefix }]`).
- `tests/test_storage_config.py` empty-`S3_PREFIX` fallback assertions flip to empty-prefix-stays-empty.

**Migration B2 — SSM:** create `/portfolio/staging/{IBKR_FLEX_TOKEN, IBKR_FLEX_QUERY_ID, T212_API_KEY, T212_API_SECRET, ENCRYPTION_KEY}` with the same values; update `deploy-staging.yml` + terraform `ssm` references; retire `/portfolio/demo/*` after confirm (open Q2).

**Migration B3 — terraform state:** `moved` blocks in staging/shared configs (or `terraform state mv`) for bucket, IAM, VPC, state machine, task defs; orchestrator module `demo` var → `staging` with `prod/main.tf` flipping `demo = false` → `staging = false`. Apply staging only; never prod.

## Shared ordering

1. Track A code rename (symbols, files, config, CLI) + test updates — tests green locally.
2. Track B terraform + sfn.py/run.py renames (no apply yet).
3. Write migration scripts A1 + B2; dry-run A1 against staging.
4. Apply terraform to staging (moved blocks); run B2 SSM swap; run B1 data copy; run A1 table rename; verify counts.
5. Deploy staging; full check suite (`ruff`, `pyright`, `pytest`) green.
6. Open one PR (both tracks), then record the ADR via `manage-adr` — new ADR for the events rename (old ADRs stay untouched); add ADR for demo→staging if the change merits one.
