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
| Module | `pipeline/analytics/cdc_tables.py` | `events_tables.py` |
| Method | `fetch_cdc`, `fetch_cdc_kwargs` (base + 3 connectors) | `fetch_events`, `fetch_events_kwargs` |
| Function | `dedup_cdc_events` (transform_utils) | `dedup_events` |
| Function | `consolidate_cdc_events` | `consolidate_events` |
| Schema | `cdc_events_normalized_schema` | `events_normalized_schema` |
| Constant | `_REQUIRED_CDC_BROKERS` | `_REQUIRED_EVENTS_BROKERS` |
| Paths env names | `RAW_*_CDC`, `NORMALIZED_*_CDC` (paths.py) | `*_EVENTS` |
| CLI | step helpers `_consolidate_cdc()`/`_normalize_cdc(args)` (run.py — no argparse subcommands) + user-facing error strings "Run the consolidate-cdc step first" / "run normalize-cdc before analytics" | `_consolidate_events()`/`_normalize_events()`, "Run the consolidate-events step first" / "run normalize-events before analytics" |
| DQ config | DQ/quality-check config keys referencing `cdc`/`cdc_events` | `events` equivalents |
| Misc | comments, docstrings, logger labels, report sections | `events` equivalents |
| Data value | raw `source` value `flex_cdc` (IBKR payload; AD-2(d)) | `flex_events` + A1 in-place rewrite of historical values |
| Docs | `docs/ibkr/flex-query-required-fields-cdc.md` + other non-ADR `docs/` "cdc" hits | renamed to `events` per CAP-1 bar (`docs/adr/` exempt) |

Scope size: ~27 pipeline sources, ~20 test files, 37 ADRs (historical — untouched), ~11 docs. The `event_*` columns do **not** change. User-confirmed scope: the rename extends to `pipeline/analytics/cdc_tables.py` **and** all DQ/quality-check config keys; the success bar is `grep -rni "cdc" pipeline/ tests/ docs/` (excluding `docs/adr/`) returning zero matches.

**Migration A** — new script `pipeline/migrations/migrate_cdc_to_events.py`:
1. For each `{broker}_cdc` raw and normalized Delta table present in the environment bucket: read location, rename to `{broker}_events` (Delta `ALTER TABLE RENAME` or S3 copy preserving the `_delta_log`), skip absent tables.
2. Rename `cdc_events` → `events`.
3. Rewrite historical raw `source` values `flex_cdc` → `flex_events` in place (AD-2(d)) — a code-only rename would silently skip every historical IBKR row.
4. Idempotent: exit 0 when all target names already exist / sources absent; raise on auth/region/permission errors or unexpected schema (mirror `migrate_cdc_events_drop_gross_amount.py` conventions — the current live migration; `migrate_xtb_purge_legacy_raw.py` and `migrate_snapshot_schema_unify.py` were removed).
5. Run manually pre-deploy, per env: `.venv/Scripts/python -m pipeline.migrations.migrate_cdc_to_events --mode staging [--dry-run]`.
6. Verify: `pipeline.run query "SELECT count(*) FROM events" --decrypt --mode staging` equals pre-migration `cdc_events` count; for the data rewrite, count `source`-gated `events` rows.

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
| Mode predicate | `is_demo()` (pipeline/secrets.py staging-mode gate; used by query.py, crypto.py, connectors) + `TestIsDemo` | `is_staging()` |
| IBKR account tier | `is_demo` (transform param), `_inject_demo_deposit`, `_DEMO_INITIAL_DEPOSIT_AMOUNT`, "demo account" text | `is_paper`, `_inject_paper_deposit`, `_PAPER_INITIAL_DEPOSIT_AMOUNT`, "paper account" |
| Terraform tags / comments | `Project = "…-demo"` etc. | `-staging` |
| Backend state key | `financial-portfolio-lakehouse-demo/terraform.tfstate` (sample) | `-staging` |

Success bar: `grep -rni "demo"` over `pipeline/ tests/ terraform/ .github/ docs/ README.md` returns zero matches outside the carve-outs (`docs/adr/`, `docs/_vendor/`, `docs/roadmaps/`, `_bmad-output/`) and the Trading 212 allow-list (`demo.trading212.com` URL + `DEMO_BASE_URL`/`_DEMO_BASE_URL` — "demo" is T212's own paper-trading tier name). **IBKR "demo" is project-stale terminology, not broker usage** (verified): the account class here is IBKR's *paper trading account* (DU prefix, $1M virtual money, persistent Flex history); IBKR's own "demo" (edemo/fdemo) is a transient daily-reset account. So IBKR renames to "paper" — `is_demo` → `is_paper`, `_inject_demo_deposit` → `_inject_paper_deposit`, `_DEMO_INITIAL_DEPOSIT_AMOUNT` → `_PAPER_INITIAL_DEPOSIT_AMOUNT`, "demo account" → "paper account"; the ibkr connector passes the renamed `is_staging()` into `is_paper`. Stale planning doc `docs/xtb/xtb_overhaul_plan.md` is **removed** (its 22 binding decisions D1–D22 are recorded in ADR 0108, partially superseded by ADR 0110), so `docs/xtb/` needs no carve-out. Arbitrary test fixture strings (`bucket-demo`, `staging_demo`, `demo-bucket`) rename for the bar.

**Migration B1 — S3 data copy + prefix removal:** bucket rename is a global rename → create `investment-portfolio-pipeline-staging`, copy all objects (`aws s3 sync` preserving encryption) from `pipeline_demo/*` to the **bucket root** (no prefix). Re-point storage config / `S3_BUCKET`. Prefix-removal couplings updated in the same PR:
- `pipeline/storage.py` staging default prefix `"pipeline_demo"` → `""` (S3Backend + `query._discover_tables_s3` already support empty prefix).
- `terraform/staging/main.tf`: `s3_prefix` var default → `""`, `xtb_staging_prefix` → `"xtb_uploads/"` (EventBridge/orchestrator filter follows via `terraform/modules/orchestrator/main.tf:179` `key = [{ prefix = var.xtb_staging_prefix }]`).
- `tests/test_storage_config.py` empty-`S3_PREFIX` fallback assertions flip to empty-prefix-stays-empty.

**Migration B2 — SSM:** user sets `/portfolio/staging/{IBKR_FLEX_TOKEN, IBKR_FLEX_QUERY_ID, T212_API_KEY, T212_API_SECRET, ENCRYPTION_KEY}` with the same secret values **before** terraform apply (the apply references the new paths); same PR updates `deploy-staging.yml` + terraform `ssm` references; retire `/portfolio/demo/*` **immediately** after the swap is live (Q2 resolved: immediate).

**Migration B3 — terraform state:** `moved` blocks in staging/shared configs (or `terraform state mv`) for bucket, IAM, VPC, state machine, task defs; orchestrator module `demo` var → `staging` with `prod/main.tf` flipping `demo = false` → `staging = false`. Apply staging only; never prod.

## Shared ordering

1. Track A code rename (symbols, files, config, CLI) + test updates — tests green locally.
2. Track B terraform + sfn.py/run.py renames (no apply yet).
3. Write migration scripts A1 + B2; dry-run A1 against staging.
4. User sets `/portfolio/staging/*` SSM values; apply terraform to staging (moved blocks); retire `/portfolio/demo/*` immediately; run B1 data copy; run A1 table rename; verify counts.
5. Deploy staging; full check suite (`ruff`, `pyright`, `pytest`) green.
6. Open one PR (both tracks), then record the ADR via `manage-adr` — new ADR for the events rename (old ADRs stay untouched); add ADR for demo→staging if the change merits one.
