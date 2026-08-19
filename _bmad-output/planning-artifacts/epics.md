---
stepsCompleted:
  - step-01-validate-prerequisites.md
  - step-02-design-epics.md
  - step-03-create-stories.md
inputDocuments:
  - _bmad-output/specs/spec-rename-cdc-events-demo-staging/SPEC.md
  - _bmad-output/specs/spec-rename-cdc-events-demo-staging/rename-plan.md
  - _bmad-output/specs/spec-rename-cdc-events-demo-staging/ARCHITECTURE-SPINE.md
---

# financial-portfolio-lakehouse - Epic Breakdown

## Overview

This document provides the complete epic and story breakdown for financial-portfolio-lakehouse, decomposing the requirements from the PRD, UX Design if it exists, and Architecture requirements into implementable stories.

## Requirements Inventory

### Functional Requirements

FR1: Rename the broker activity log layer from "CDC" to "events" in every pipeline source, test file, config path, CLI entry, report section, quality check, and Delta table name, exactly per the rename map (CAP-1).
FR2: Ensure `grep -rni "cdc" pipeline/ tests/ docs/` returns zero matches outside the carve-outs (`docs/adr/` records, migration scripts `pipeline/migrations/*` and their tests, terraform `moved`/`state mv` blocks) and the full test suite passes with the renamed symbols (CAP-1).
FR3: Rename the staging AWS environment from "demo" to "staging" in every terraform identifier, live AWS resource name, SSM parameter path, data prefix, and env label, per the rename map (CAP-2).
FR4: Ensure `grep -rni "demo"` over `pipeline/ tests/ terraform/ .github/ docs/ README.md` returns zero matches outside the carve-outs (`docs/adr/`, `docs/_vendor/`, `docs/roadmaps/`, `_bmad-output/`) and the Trading 212 tier allow-list (`demo.trading212.com`, `DEMO_BASE_URL`/`_DEMO_BASE_URL`) (CAP-2).
FR5: Rename IBKR "demo" terminology to IBKR's own "paper trading" vocabulary (`is_demo` → `is_paper`, `_inject_demo_deposit` → `_inject_paper_deposit`, `_DEMO_INITIAL_DEPOSIT_AMOUNT` → `_PAPER_INITIAL_DEPOSIT_AMOUNT`), while the ibkr connector passes the renamed `is_staging()` into `is_paper` (CAP-2).
FR6: Create migration A1 (`pipeline/migrations/migrate_cdc_to_events.py`) that renames Delta tables `{broker}_cdc` → `{broker}_events` (raw + normalized) and `cdc_events` → `events`, and rewrites historical raw `source = "flex_cdc"` values to `flex_events` in place (CAP-3).
FR7: Create migration B1 that creates the new staging bucket and copies all encrypted objects from `pipeline_demo/*` to the bucket root (empty prefix), preserving Fernet encryption, leaving the old bucket untouched (CAP-3).
FR8: Create migration B2 that sets SSM `/portfolio/staging/*` with the same secret values (before terraform apply), repoints deploy workflow references, and retires `/portfolio/demo/*` immediately after the swap is live (CAP-3).
FR9: Create migration B3 that applies planned terraform state moves (`moved` blocks or `state mv`) for bucket, IAM, VPC, state machine, and task definitions, and flips `prod/main.tf` `demo = false` → `staging = false` (CAP-3).
FR10: Ship both rename tracks (Track A + Track B) plus migrations, updated tests, and a new ADR as a single reviewed PR (CAP-4).
FR11: Keep `pipeline/migrations/migrate_cdc_events_drop_gross_amount.py` (PR #143) never renamed; apply only the lockstep compatibility edit: its `from pipeline.normalized.models import cdc_events_normalized_schema` import must track the schema constant rename to `events_normalized_schema` (same one-line fix in `tests/test_migrate_cdc_events_drop_gross_amount.py`) (CAP-1, AD-3).

### NonFunctional Requirements

NFR1: Old ADRs are permanent records — never rewritten to "events"; the rename supersedes the naming, not the decisions.
NFR2: `event_*` column names are already correct and must not change.
NFR3: Migrations follow the existing `pipeline/migrations/` pattern: idempotent (exit 0 on absent or already-migrated, raise on genuine failures), run manually **before** deploying code referencing the new names, via the `pipeline.run` CLI — never manual `DeltaTable()` construction.
NFR4: S3 bucket name is globally unique, so the demo→staging bucket change is a **new bucket + full encrypted-data copy**, never an in-place rename.
NFR5: The staging data prefix is **removed entirely** (empty prefix), not renamed to `pipeline_staging`; prod's `pipeline` prefix stays.
NFR6: `/portfolio/demo/*` → `/portfolio/staging/*`: user sets the new SSM values **before** terraform apply; demo values retired **immediately** after the swap is live.
NFR7: Terraform renames use planned state migration (`moved` blocks) or deliberate destroy/recreate — never apply prod terraform.
NFR8: All financial data remains Fernet-encrypted at rest throughout the renames and copies.
NFR9: Migration A1 runs only after PR #143's `migrate_cdc_events_drop_gross_amount.py` has been applied per env (its `_CDC_TABLES` are the pre-rename names; after A1 they no longer exist).
NFR10: All three checks pass before the PR opens: `ruff`, `pyright`, `pytest`; tests run after linting to catch auto-fix regressions.
NFR11: The whole chunk of work is decomposed into as many independent parallel pieces as it naturally splits into — e.g. 10 independent units get 10 parallel agents, not just two tracks. Track A (CDC→events) and Track B (demo→staging) are the coarse grain; within each, every file-group/function-group/migration that is independent is its own parallel workstream. All pieces converge only in the single PR; nothing may block on another piece's intermediate state except genuine ordering constraints (migrations before deploy, shared-symbol conflicts).

### Additional Requirements

- ADD-AD1: The rename map (rename-plan.md old→new tables) is the single source of truth for every new name; any "Old" token found outside `docs/adr/` that is not in the map is a gap — raise it, never improvise.
- ADD-AD2: Rename by semantic role, never by token string — (a) pipeline environment mode → `staging`, (b) broker product tier → the broker's own vocabulary (Trading 212 keeps `demo`; IBKR → `paper`), (c) stale local jargon → the map's target, (d) data-embedded sentinels (`source` value `flex_cdc`) are data, not names — renamed in code **and** rewritten in place by A1.
- ADD-AD3: The grep-zero sweeps are the closed enforcement gate; the carve-out list is closed — no additions without a spec decision; a unit is done only when its sweep returns zero.
- ADD-AD4: Migration-first, deploy-last: strict sequence per environment — (1) user sets `/portfolio/staging/*`, (2) B1 object copy to new bucket root, (3) terraform apply to staging, (4) retire `/portfolio/demo/*`, (5) A1 table renames + sentinel rewrite, (6) verify identical row counts via `pipeline.run query --decrypt --mode staging`, (7) deploy the renamed code last.
- ADD-AD5: The immutables: `docs/adr/` never rewritten; `event_*` columns unchanged; prod environment and its `pipeline` prefix untouched (except the `prod/main.tf` `staging = false` flip, which is a code change, not an apply); Trading 212's tier tokens are the sole surviving "demo".
- ADD-AD6: Both tracks ship as one PR; merge only after all checks green and migrations applied to staging with counts verified; a new ADR recording the events rename is created via `manage-adr` after merge (plus a demo→staging ADR if the change merits one).
- ADD-ORD: Shared ordering: Track A code rename + test updates first (tests green locally), then Track B terraform + sfn.py/run.py renames (no apply), then write migration scripts A1 + B2 and dry-run A1 against staging, then user sets SSM values → apply terraform → retire demo SSM → run B1 copy → run A1 rename → verify counts, then deploy staging and run the full check suite, then open one PR and record the ADR.
- ADD-PROD: **Flag prod to the user on completion** — after the rename PR merges and deploys to staging, explicitly surface to the user that prod still carries the old naming (prod resources untouched, prod `pipeline` prefix deferred) so staging does not silently drift out of sync with prod for too long; prod-side renames and the prod `pipeline` prefix remain an open follow-up owned by the user.

### UX Design Requirements

No UX design contract applies — this is a data-pipeline/infrastructure rename with no user-facing UI. No UX-DRs extracted.

### FR Coverage Map

FR1: Epic 1 - Event layer renamed CDC→events across all pipeline sources, tests, config, CLI, DQ, docs
FR2: Epic 1 - grep-zero "cdc" bar over pipeline/ tests/ docs/ (carve-outs only) + full suite green
FR3: Epic 2 - Staging AWS environment renamed demo→staging (terraform, live resources, SSM, prefix, env label)
FR4: Epic 2 - grep-zero "demo" bar over pipeline/ tests/ terraform/ .github/ docs/ README.md (carve-outs + T212 allow-list only)
FR5: Epic 2 - IBKR demo→paper vocabulary; ibkr connector passes is_staging() into is_paper
FR6: Epic 3 - Migration A1: Delta renames {broker}_cdc→{broker}_events, cdc_events→events + flex_cdc data rewrite
FR7: Epic 3 - Migration B1: new staging bucket, encrypted copy pipeline_demo/* → bucket root (empty prefix)
FR8: Epic 3 - Migration B2: SSM /portfolio/staging/* before apply; /portfolio/demo/* retired immediately
FR9: Epic 3 - Migration B3: terraform state moves (moved/state mv); prod/main.tf staging=false flip
FR10: Epic 4 - One PR (Track A + Track B + migrations + tests + new ADR); checks green; migrations applied; ADR recorded
FR11: Epic 4 - migrate_cdc_events_drop_gross_amount.py never renamed; lockstep cdc_events_normalized_schema→events_normalized_schema compat edit

## Epic List

### Epic 1: The event layer reads as "events," not "CDC"
The broker activity log is called `events` everywhere in code, tests, config, CLI, quality checks, and Delta table names, per the rename map; the CAP-1 grep-zero bar holds.
**FRs covered:** FR1, FR2

### Epic 2: The staging environment reads as "staging," not "demo"
Every staging AWS resource and function carries "staging" names (terraform, S3 bucket, IAM, VPC, state machine, SSM, env label); IBKR uses "paper"; Trading 212's demo tier survives allow-listed.
**FRs covered:** FR3, FR4, FR5

### Epic 3: Existing encrypted data survives every rename
Data-preserving migrations A1 (Delta renames + flex_cdc rewrite), B1 (bucket copy + prefix removal), B2 (SSM swap), B3 (terraform state moves) run idempotently pre-deploy; every encrypted row is verifiably intact.
**FRs covered:** FR6, FR7, FR8, FR9

### Epic 4: Both renames ship as one reviewed PR, decision recorded
Track A + Track B + migrations + updated tests land in a single reviewed PR; ruff/pyright/pytest green; migrations applied to staging with counts verified; a new ADR records the events rename; prod drift is flagged to the user.
**FRs covered:** FR10, FR11

<!-- Epic 1 (approved 2026-08-19) -->

## Epic 1: The event layer reads as "events," not "CDC"

The broker activity log is called `events` everywhere in code, tests, config, CLI, quality checks, and Delta table names, per the rename map; the CAP-1 grep-zero bar holds.
**FRs covered:** FR1, FR2

### Story 1.1: Connector fetch layer reads "events"

As a pipeline operator,
I want the connector fetch layer to name the broker activity log "events",
So that the codebase reads consistently with the rename map and the CAP-1 bar holds.

**Acceptance Criteria:**

**Given** the rename map's `fetch_cdc` family (`fetch_cdc`, `fetch_cdc_kwargs`, `fetch_cdc_via_flex`) and the connector modules `pipeline/connectors/{base.py, ibkr/connector.py, ibkr/fetch.py, trading212/connector.py, trading212/fetch.py, trading212/client.py, xtb/connector.py, xtb/fetch.py, xtb/parser.py}`,
**When** the symbols are renamed to their map targets (`fetch_events`, `fetch_events_kwargs`, `fetch_events_via_flex`) and every residual `*cdc*` token in those files is renamed by pattern to `*_events*`,
**Then** no "cdc" token remains in the 9 connector files
**And** the connector test files (`tests/test_ibkr_connector.py`, `tests/test_trading212_connector.py`, `tests/test_xtb_connector.py`, `tests/test_connector_protocol.py`, `tests/test_connector_registry.py`, `tests/fixtures/ibkr.py`, `tests/fixtures/xtb.py`) pass with the renamed symbols
**And** every new name comes verbatim from the map's New column (AD-1).

### Story 1.2: Broker transforms, dedup, and raw schemas read "events"

As a data engineer,
I want the dedup helper and raw schema constants to read "events",
So that normalization consumes events-named symbols consistently.

**Acceptance Criteria:**

**Given** `dedup_cdc_events` (`pipeline/connectors/transform_utils.py:390`) and `ibkr_cdc_raw_schema` (`pipeline/raw/models.py`) are renamed to `dedup_events` and `ibkr_events_raw_schema`,
**When** all import and call sites in the broker transform modules (`pipeline/connectors/ibkr/transform.py`, `pipeline/connectors/trading212/transform.py`, `pipeline/connectors/xtb/transform.py`) and the raw layer (`pipeline/raw/models.py`, `pipeline/raw/__init__.py`) are updated to the map targets,
**Then** no "cdc" token remains in those 6 files
**And** `tests/test_transform_pipeline.py` and `tests/test_transform_utils.py` pass
**And** the schema-constant import sites in the transforms reference `events_normalized_schema` (the definition is renamed in Story 1.3; the map is the lock, so both stories converge on the same name at merge).

### Story 1.3: Normalized consolidation produces "events" tables

As a data engineer,
I want the consolidation layer to produce "events" tables via `consolidate_events`,
So that normalized output and the consolidated events table read events.

**Acceptance Criteria:**

**Given** `pipeline/normalized/consolidate_cdc.py` is renamed to `consolidate_events.py` with `consolidate_cdc_events` → `consolidate_events`, `_REQUIRED_CDC_BROKERS` → `_REQUIRED_EVENTS_BROKERS`, and `cdc_events_normalized_schema` → `events_normalized_schema` (definition in `pipeline/normalized/models.py`),
**When** the module, its exports, the table-path strings (`{broker}_cdc`, `cdc_events`), and the error string "Run the consolidate-cdc step first" in `pipeline/normalized/normalize.py` are renamed to the map targets,
**Then** `tests/test_consolidate_cdc.py` passes with the renamed symbols
**And** `pipeline.normalized.consolidate_events` is importable and exports `consolidate_events`
**And** no "cdc" token remains in `pipeline/normalized/{consolidate_events, models, normalize, __init__}.py`
**And** this story does not touch `pipeline/migrations/*` — the exempt migration script's lockstep import (FR11) is handled in Epic 4.

### Story 1.4: Analytics tables and quality checks read "events"

As an analyst,
I want quality checks and analytics tables keyed "events",
So that reports and DQ checks read events and the CAP-1 bar holds over analytics.

**Acceptance Criteria:**

**Given** `pipeline/analytics/cdc_tables.py` is renamed to `events_tables.py`,
**When** the DQ config keys (`"cdc_events"`, `"ibkr_cdc"`, `"trading212_cdc"`, `"xtb_cdc"`) in `pipeline/analytics/quality.py`, the `pipeline/analytics/holdings.py` import, `pipeline/analytics/models.py`, and the error string "run normalize-cdc before analytics" are renamed to their events equivalents,
**Then** `tests/test_cdc_analytics.py`, `tests/test_quality.py`, `tests/test_portfolio_holdings.py`, and `tests/test_report.py` pass
**And** no "cdc" token remains in the 4 analytics files.

### Story 1.5: Path env vars and run.py CLI read "events"

As a CLI user,
I want run.py steps and path env vars in events terms,
So that consolidate-events/normalize-events error strings and `*_EVENTS` env names match the map.

**Acceptance Criteria:**

**Given** the path env names `RAW_*_CDC`/`NORMALIZED_*_CDC` in `pipeline/paths.py` and the run.py step helpers `_consolidate_cdc()`/`_normalize_cdc(args)` with their call sites,
**When** they are renamed to `*_EVENTS`, `_consolidate_events()`, `_normalize_events()`, and the `pipeline/sfn.py` comment `xtb_cdc` → `xtb_events`,
**Then** `tests/test_run_subcommands.py`, `tests/test_query_cli.py`, `tests/test_pipeline_integration.py`, `tests/test_consolidate_pipeline.py`, `tests/test_normalize_currency.py`, and `tests/conftest.py` pass
**And** no "cdc" token remains in `pipeline/paths.py`, `pipeline/run.py`, or `pipeline/sfn.py`
**And** the user-facing error strings read "Run the consolidate-events step first" / "run normalize-events before analytics" (map CLI row).

### Story 1.6: Docs renamed and the repo-wide sweep proves the CAP-1 bar

As a maintainer,
I want the repo-wide sweep to prove the rename,
So that the CAP-1 grep-zero bar holds and the epic is done.

**Acceptance Criteria:**

**Given** the 10 non-ADR docs files carrying "cdc" (`docs/architecture.md`, `docs/ruff-ble001-technical-debt.md`, `docs/ruff-per-file-ignores-technical-debt.md`, `docs/table-lineage.md`, `docs/ibkr/flex-query-required-fields-cdc.md`, `docs/configuration.md`, `docs/brokers/ibkr.md`, `docs/brokers/xtb.md`, `docs/roadmaps/0010-currency-unification.md`, `docs/roadmaps/0007-market-data-reporting.md`) and any residual example/`.env` hits,
**When** all are renamed to events (including the file name `docs/ibkr/flex-query-required-fields-cdc.md` → `flex-query-required-fields-events.md`) and this story runs after Stories 1.1–1.5 complete,
**Then** `grep -rni "cdc" pipeline/ tests/ docs/` returns zero matches outside the carve-outs (`docs/adr/`, `pipeline/migrations/*` scripts and their tests, terraform `moved`/`state mv` blocks)
**And** `ruff`, `pyright`, and `pytest` all pass (NFR10)
**And** `docs/adr/` content is untouched (NFR1) and `event_*` column names are unchanged (NFR2).

<!-- End Epic 1 -->

<!-- Epic 2 (approved 2026-08-19) -->

## Epic 2: The staging environment reads as "staging," not "demo"

Every staging AWS resource and function carries "staging" names (terraform, S3 bucket, IAM, VPC, state machine, SSM, env label); IBKR uses "paper"; Trading 212's demo tier survives allow-listed.
**FRs covered:** FR3, FR4, FR5

### Story 2.1: The staging-mode predicate reads "staging"

As a pipeline operator,
I want the staging-mode predicate named `is_staging()`,
So that mode gating reads the environment, not a broker tier.

**Acceptance Criteria:**

**Given** `secrets.is_demo()` and its consumers (`pipeline/crypto.py`, `pipeline/query.py`, `pipeline/run.py`, `pipeline/connectors/base.py` comment, `pipeline/connectors/trading212/connector.py` import/call),
**When** the predicate is renamed to `is_staging()` with `/portfolio/staging/*` code refs, `TestIsDemo` → `TestIsStaging`, and the "Demo-mode resolution" comment → "Staging-mode resolution",
**Then** `tests/test_mode.py`, `tests/test_secrets.py`, `tests/test_crypto.py`, `tests/test_run_subcommands.py`, and `tests/conftest.py` pass
**And** no "demo" token remains in the 6 code files except the Trading 212 tier tokens (`_DEMO_BASE_URL`, `demo.trading212.com`), which stay (AD-2)
**And** the Trading 212 connector's `is_demo()` call becomes `is_staging()` while its tier tokens are untouched.

### Story 2.2: Env label, state machine, and storage prefix read "staging"

As a deployer,
I want the env label and state machine to read "staging" and the staging data prefix removed,
So that staging resources are named staging and the empty prefix isolates environments (NFR5).

**Acceptance Criteria:**

**Given** `pipeline/sfn.py` (`MODE_TO_ENV_LABEL`, `STATE_MACHINE_NAMES`, `_env_label`) and `pipeline/storage.py` (staging prefix `"pipeline_demo"`, `S3_BUCKET` default `investment-portfolio-pipeline-demo`),
**When** `MODE_TO_ENV_LABEL` is **removed** and `_env_label(mode)` returns `mode` directly (keeping its unsupported-mode `ValueError` guard — decision 2026-08-19), `STATE_MACHINE_NAMES` staging entry becomes `portfolio-pipeline-orchestrator-staging`, the staging prefix becomes `""` (empty, not `pipeline_staging`), and the bucket default becomes `investment-portfolio-pipeline-staging`,
**Then** `tests/test_sfn.py`, `tests/test_storage_config.py`, `tests/test_query_s3.py`, and `tests/test_s3_helpers.py` pass
**And** no "demo" token remains in `pipeline/sfn.py` or `pipeline/storage.py`
**And** the env label for task families and log groups is derived as the mode itself, never hardcoded (AD-2(a)).

### Story 2.3: IBKR reads "paper," not "demo"

As an IBKR user,
I want the paper-trading vocabulary,
So that the account class matches IBKR's own terminology (DU prefix, persistent Flex history).

**Acceptance Criteria:**

**Given** `is_demo` (transform param), `_inject_demo_deposit`, `_DEMO_INITIAL_DEPOSIT_AMOUNT`, and "demo account" text in `pipeline/connectors/ibkr/transform.py` and `pipeline/connectors/ibkr/connector.py`,
**When** they are renamed to `is_paper`, `_inject_paper_deposit`, `_PAPER_INITIAL_DEPOSIT_AMOUNT`, and "paper account", and the ibkr connector passes the renamed `is_staging()` into `is_paper` (FR5),
**Then** `tests/test_ibkr_connector.py`, `tests/test_transform_pipeline.py`, and `tests/test_connector_protocol.py` pass
**And** no "demo" token remains in the 2 IBKR files
**And** the rename follows the broker's own vocabulary, not a uniform token sweep (AD-2(b)).

### Story 2.4: Staging terraform reads "staging"

As an infrastructure engineer,
I want the staging terraform config to carry "staging" names,
So that applied AWS resources are named staging and the CAP-2 bar holds over `terraform/staging/`.

**Acceptance Criteria:**

**Given** `terraform/staging/main.tf`, `terraform/staging/outputs.tf`, and `terraform/staging/backend.tf.sample`,
**When** the bucket (`investment-portfolio-pipeline-demo` → `-staging`), IAM (`pipeline-demo` → `pipeline-staging`), VPC (`pipeline_demo` → `pipeline_staging`), state machine (`-demo` → `-staging`), SSM paths (`/portfolio/demo/*` → `/portfolio/staging/*`), `env_label = "demo"` → `"staging"`, `s3_prefix` → `""`, `xtb_staging_prefix` → `"xtb_uploads/"`, and the backend state key (`-demo` → `-staging`) are renamed per the map,
**Then** no "demo" token remains in the 3 files
**And** `s3_prefix` defaults to `""` and `xtb_staging_prefix` to `"xtb_uploads/"` (NFR5)
**And** no `force_destroy` is set on the staging bucket (AD-4).

### Story 2.5: Terraform modules, shared config, and the prod flip read "staging"

As an infrastructure engineer,
I want the modules, shared config, and the one in-scope prod edit consistent,
So that shared naming and the prod flip match the map without touching prod infrastructure.

**Acceptance Criteria:**

**Given** `terraform/modules/orchestrator/{main,variables}.tf`, `terraform/modules/ecs-task/{main,variables}.tf`, `terraform/shared/main.tf`, and `terraform/prod/main.tf`,
**When** `var.demo` → `var.staging`, task-definition and IAM role patterns (`pipeline-task-*-demo-*` / `pipeline-task-exec-demo-*` → `-staging-*`), and `prod/main.tf`'s `demo = false` → `staging = false` are renamed per the map,
**Then** no "demo" token remains in the 6 files
**And** the prod edit is a code change only — prod terraform is never applied (NFR7, AD-5).

### Story 2.6: Trading 212's demo tier is verified as the sole surviving "demo"

As a maintainer,
I want the Trading 212 tier verified as the only surviving "demo",
So that the CAP-2 allow-list is exactly the tier tokens and nothing else.

**Acceptance Criteria:**

**Given** `pipeline/connectors/trading212/connector.py`, `pipeline/connectors/trading212/client.py`, `tests/test_trading212_connector.py`, and `tests/fixtures/trading212.py`,
**When** the files are verified after Story 2.1 has renamed the `is_demo()` predicate call,
**Then** every remaining "demo" token is one of the allow-listed tier tokens (`demo.trading212.com`, `DEMO_BASE_URL`/`_DEMO_BASE_URL`) or tier-vocabulary comment text
**And** `tests/test_trading212_connector.py` passes
**And** no rename is applied to the tier tokens (AD-2(b), AD-5).

### Story 2.7: Docs renamed and the repo-wide sweep proves the CAP-2 bar

As a maintainer,
I want the repo-wide demo sweep to prove the rename,
So that the CAP-2 grep-zero bar holds and the epic is done.

**Acceptance Criteria:**

**Given** the 5 non-carve-out docs files carrying "demo" (`docs/configuration.md`, `docs/brokers/ibkr.md`, `docs/brokers/xtb.md`, `docs/brokers/trading212.md`, `docs/deployment/aws.md`) and `README.md`,
**When** all are renamed to staging (Trading 212 tier mentions excepted) and this story runs after Stories 2.1–2.6 complete,
**Then** `grep -rni "demo"` over `pipeline/ tests/ terraform/ .github/ docs/ README.md` returns zero matches outside the carve-outs (`docs/adr/`, `docs/_vendor/`, `docs/roadmaps/`, `_bmad-output/`) and the Trading 212 allow-list (`demo.trading212.com`, `DEMO_BASE_URL`/`_DEMO_BASE_URL`)
**And** `ruff`, `pyright`, and `pytest` all pass (NFR10)
**And** `docs/adr/` content is untouched (NFR1) and prod resources are untouched (AD-5).

<!-- End Epic 2 -->

<!-- Epic 3 (approved 2026-08-19) -->

## Epic 3: Existing encrypted data survives every rename

Data-preserving migrations A1 (Delta renames + flex_cdc rewrite), B1 (bucket copy + prefix removal), B2 (SSM swap), B3 (terraform state moves) run idempotently pre-deploy; every encrypted row is verifiably intact.
**FRs covered:** FR6, FR7, FR8, FR9

### Story 3.1: Migration A1 renames Delta tables and rewrites the flex_cdc sentinel

As a data engineer,
I want existing Delta tables renamed to events with historical data intact,
So that no financial row is lost or silently skipped by the rename.

**Acceptance Criteria:**

**Given** the live migration pattern (`pipeline/migrations/migrate_cdc_events_drop_gross_amount.py` + `pipeline/migrations/_storage_options.py`) and the pre-rename table names (`{broker}_cdc` raw + normalized, `cdc_events`),
**When** `pipeline/migrations/migrate_cdc_to_events.py` renames `{broker}_cdc` → `{broker}_events` (raw + normalized) and `cdc_events` → `events`, and rewrites historical raw `source = "flex_cdc"` → `"flex_events"` in place (AD-2(d)),
**Then** the migration is idempotent — exit 0 on absent or already-migrated tables, raise on auth/region/permission errors or unexpected schema (NFR3)
**And** it runs via `python -m pipeline.migrations.migrate_cdc_to_events --mode staging [--dry-run]`, never hand-constructing `DeltaTable()` (NFR3)
**And** post-migration `pipeline.run query --decrypt --mode staging` counts match pre-migration — table counts and, for the sentinel, `source`-gated `events` rows (CAP-3)
**And** it runs only after PR #143's `migrate_cdc_events_drop_gross_amount.py` has been applied per env (NFR9 — its `_CDC_TABLES` are the pre-rename names).

### Story 3.2: Migration B1 copies the encrypted bucket to the staging bucket

As a data engineer,
I want the encrypted bucket contents copied to the staging bucket,
So that the global bucket rename (NFR4) loses no data.

**Acceptance Criteria:**

**Given** the new `investment-portfolio-pipeline-staging` bucket (globally unique — a new bucket + full copy, never an in-place rename, NFR4),
**When** all objects are copied from `pipeline_demo/*` to the bucket root (empty prefix — NFR5) preserving Fernet encryption (NFR8),
**Then** the old `investment-portfolio-pipeline-demo` bucket is left untouched
**And** object count and encryption are verified post-copy
**And** no `force_destroy` is ever set on the staging bucket (AD-4)
**And** prod's `pipeline` prefix is untouched (AD-5).

### Story 3.3: Migration B2 moves SSM secrets to staging paths

As an operator,
I want SSM secrets moved to staging paths,
So that the apply references the new paths with identical values.

**Acceptance Criteria:**

**Given** the live secrets at AWS SSM Parameter Store paths `/portfolio/demo/{IBKR_FLEX_TOKEN, IBKR_FLEX_QUERY_ID, T212_API_KEY, T212_API_SECRET, ENCRYPTION_KEY}` (AWS-side parameter names, not local paths),
**When** the B2 script/runbook is written — reads each `/portfolio/demo/<SECRET>` value and writes it to `/portfolio/staging/<SECRET>` (SecureString, same KMS key) with values never printed, plus `--dry-run` and a retire step (`delete-parameter` on `/portfolio/demo/*`),
**Then** the script is idempotent and dry-run verified
**And** the value copy and retire are **user-executed manual steps** — the implementation agent has no secret access; the runbook documents the exact commands and the AD-4 sequencing (copy **before** the terraform apply that repoints ECS to the new paths; retire **immediately after** the swap is live, no grace period — Q2)
**And** values are copied, never regenerated or printed (NFR8)
**And** deploy-workflow references are repointed where they reference SSM paths (grep shows `.github/` has no demo tokens today — verification-only unless a path is built dynamically).

### Story 3.4: Migration B3 moves terraform state to staging names

As an infrastructure engineer,
I want terraform state moved to staging names,
So that apply renames resources without destroying data.

**Acceptance Criteria:**

**Given** the planned state migration for bucket, IAM, VPC, state machine, and task definitions,
**When** `moved` blocks (or `terraform state mv`) are added in `terraform/staging/main.tf` and `terraform/shared/main.tf`,
**Then** `terraform plan` shows no destroy of data-bearing resources
**And** prod terraform is never applied (NFR7)
**And** the `prod/main.tf` `demo = false` → `staging = false` flip is Story 2.5's code change, not this story's apply
**And** the `moved`/`state mv` blocks reference pre-rename names as their inputs — exempt from the grep bars (AD-3 historical-reference exemption).

<!-- End Epic 3 -->

<!-- Epic 4 (approved 2026-08-19) -->

## Epic 4: Both renames ship as one reviewed PR, decision recorded

Track A + Track B + migrations + updated tests land in a single reviewed PR; ruff/pyright/pytest green; migrations applied to staging with counts verified; a new ADR records the events rename; prod drift is flagged to the user.
**FRs covered:** FR10, FR11

### Story 4.1: The historical drop-gross-amount migration stays runnable

As a maintainer,
I want the historical migration to stay runnable,
So that PR #143's script keeps working on envs where the drop has not yet applied.

**Acceptance Criteria:**

**Given** `pipeline/migrations/migrate_cdc_events_drop_gross_amount.py` is never renamed — filename, docstring, `_CDC_TABLES`, and module path stay as-is (exempt historical artifact, AD-3),
**When** its `from pipeline.normalized.models import cdc_events_normalized_schema` import tracks the schema constant rename to `events_normalized_schema` (same one-line fix in `tests/test_migrate_cdc_events_drop_gross_amount.py`),
**Then** the script stays runnable on any env where the drop has not yet applied
**And** `tests/test_migrate_cdc_events_drop_gross_amount.py` passes
**And** the script references no other renamed symbol (verified against the map).

### Story 4.2: The full check suite is green before the PR opens

As a maintainer,
I want the whole PR green,
So that the rename ships without regressions.

**Acceptance Criteria:**

**Given** all renames, migrations, and updated tests are complete,
**When** `ruff check --fix .` + `ruff format .`, then `pyright pipeline/ tests/`, then `pytest tests/ -q -rf` run,
**Then** all three checks pass (NFR10)
**And** tests are re-run after linting to catch auto-fix regressions
**And** both grep bars (CAP-1 over `pipeline/ tests/ docs/`, CAP-2 over `pipeline/ tests/ terraform/ .github/ docs/ README.md`) return zero over live names outside their carve-outs.

### Story 4.3: Both renames ship as one reviewed PR, the ADR is recorded, and prod drift is flagged

As a maintainer,
I want both renames to ship as one reviewed PR with the decision recorded,
So that the rename lands atomically, is documented, and staging does not silently drift from prod.

**Acceptance Criteria:**

**Given** Track A + Track B + migrations + updated tests are complete,
**When** the single PR opens and merges — only after all checks green and migrations applied to staging with counts verified (ADD-AD6),
**Then** the events-rename ADR is recorded via `manage-adr` after merge (old ADRs untouched, NFR1)
**And** a demo→staging ADR is added if the change merits one
**And** issue #131 is closed after merge — the rename it requested is shipped
**And** prod drift is explicitly flagged to the user: prod still carries the old naming and the prod `pipeline` prefix is deferred, an open user-owned follow-up (ADD-PROD).

<!-- End Epic 4 -->
