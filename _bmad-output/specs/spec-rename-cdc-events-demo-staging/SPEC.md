---
id: SPEC-rename-cdc-events-demo-staging
companions:
  - rename-plan.md
  - ARCHITECTURE-SPINE.md
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability only — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Rename CDC→events and demo→staging in one PR

## Why

Two naming debts in one codebase, both resolved in a single PR because each requires a migration and they touch overlapping plumbing (Delta tables on S3, config paths, deploy workflow, SSM).

1. **Pain (issue #131):** The pipeline calls its broker activity log "CDC" (Change Data Capture) — `{broker}_cdc`, `cdc_events`, `consolidate_cdc.py`, `fetch_cdc`, `dedup_cdc_events`, `cdc_events_normalized_schema`, `_REQUIRED_CDC_BROKERS`. What these tables hold is a chronological log of broker account events — trades, dividends, deposits, withdrawals, fees, taxes, interest, transfers, adjustments. CDC in data engineering means row-level database replication; no broker uses the term (Trading 212: orders/dividends/transactions; IBKR Flex: "activity"; XTB: report sheets). The name misleads any data-engineering reader, and the schema already speaks `event_*` vocabulary (`event_type`, `event_id`, `event_datetime`), so the honest rename is cheap.

2. **Opportunity:** The staging environment is internally named "demo" — S3 bucket `investment-portfolio-pipeline-demo`, IAM user `pipeline-demo`, VPC/IGW/SG `pipeline_demo`, SSM params under `/portfolio/demo/`, state machine `portfolio-pipeline-orchestrator-demo`, env label `demo`, IAM role patterns `pipeline-task-*-demo-*`, data prefix `pipeline_demo`. "demo" is the wrong name for a persistent, data-bearing staging environment; operators and docs say "staging" while the infra says "demo".

Both renames change names that live in Delta table paths / AWS resource names / SSM parameter names, so they need migration scripts and state-safe terraform changes — which is why they ship together.

## Capabilities

- **CAP-1 — codebase calls the event layer "events", not "CDC"**
  - **intent:** Reader and operator can refer to the consolidated broker activity log as `events` across pipeline sources, test files, config paths, CLI subcommands, report sections, quality checks, and Delta table names — no "CDC" remaining.
  - **success:** `grep -rni "cdc" pipeline/ tests/ docs/` returns zero matches outside the historical carve-outs — `docs/adr/` records and migration artifacts (`pipeline/migrations/*` scripts and their tests, plus terraform `moved`/`state mv` blocks, which reference pre-rename names as their inputs) — and the full test suite passes with the renamed symbols.
- **CAP-2 — staging AWS environment is named "staging", not "demo"**
  - **intent:** Operator can see every staging-environment AWS resource and function named "staging" — terraform identifiers, live resource names (S3 bucket, IAM user/policies/roles, VPC, SG, state machine), SSM parameter paths, data prefix, and env label.
  - **success:** `grep -rni "demo"` over `pipeline/ tests/ terraform/ .github/ docs/ README.md` returns zero matches outside the carve-outs (`docs/adr/`, `docs/_vendor/`, `docs/roadmaps/`, `_bmad-output/`) and the Trading 212 paper-trading-tier allow-list (`demo.trading212.com` URL + `DEMO_BASE_URL`/`_DEMO_BASE_URL`); IBKR "demo" references are renamed to IBKR's own "paper trading" terminology (`is_demo` → `is_paper`, `_inject_demo_deposit` → `_inject_paper_deposit`, `_DEMO_INITIAL_DEPOSIT_AMOUNT` → `_PAPER_INITIAL_DEPOSIT_AMOUNT`); applied staging AWS resources carry `-staging`/`pipeline_staging` names.
- **CAP-3 — renames preserve existing encrypted Delta data**
  - **intent:** After the migration(s) run pre-deploy, all historical broker data is queryable under the new table/path names with identical rows and intact Fernet encryption.
  - **success:** `pipeline.run query "SELECT count(*) FROM events" --decrypt --mode staging` equals the pre-migration `cdc_events` count; migration scripts are idempotent (exit 0 on absent or already-migrated, raise on genuine failures); staging data sits at the bucket root with no `pipeline_demo` prefix; `/portfolio/staging/*` SSM params hold the live secret values and `/portfolio/demo/*` is retired.
- **CAP-4 — both renames ship as one reviewed PR**
  - **intent:** Reviewer can review a single PR containing both rename tracks plus migrations, updated tests, and a new ADR recording the event-layer rename.
  - **success:** PR opens with both tracks; `ruff`, `pyright`, `pytest` all pass; migrations applied to staging; a new ADR records the rename.

## Constraints

- Old ADRs are permanent records — never rewritten to "events"; the rename supersedes the naming, not the decisions.
- `event_*` column names are already correct and must not change.
- Migrations follow the existing `pipeline/migrations/` pattern: idempotent, raise on genuine failures, run manually **before** deploying code referencing new names, via `pipeline.run` CLI (never manual `DeltaTable()` construction).
- S3 bucket name is globally unique, so `investment-portfolio-pipeline-demo` → `investment-portfolio-pipeline-staging` means a **new bucket + full encrypted-data copy**, not an in-place rename.
- The staging data prefix is **removed entirely** (empty prefix), not renamed to `pipeline_staging` — buckets already isolate environments (ADR 0038/0039), so `pipeline`/`pipeline_demo` inside the bucket is redundant; prod's `pipeline` prefix stays (prod apply is out of scope).
- SSM `/portfolio/demo/*` → `/portfolio/staging/*`: the user sets the `/portfolio/staging/*` secret values **before** terraform apply (the apply references the new paths); `/portfolio/demo/*` is retired **immediately** after the swap is live.
- Terraform renames use planned state migration (`moved` blocks) or deliberate destroy/recreate — never apply prod terraform.
- Both tracks land in one PR (explicit user direction).

## Non-goals

- Rewriting old ADRs to use "events" — they stay as historical decision records.
- Changing `event_*` column names.
- Renaming prod-environment resources (prod keeps its names; only the staging/demo side and shared patterns that name staging resources change).
- Renaming docker/MinIO local-mode resource names, except where a shared naming code path (e.g. `MODE_TO_ENV_LABEL`) forces consistency.
- Renaming Trading 212's paper-trading API tier (`demo.trading212.com`, `DEMO_BASE_URL`/`_DEMO_BASE_URL`) — "demo" is T212's own product name for its practice API, not the staging env; it is the sole allow-listed survivor of CAP-2's grep bar. Vendored third-party and historical content (`docs/_vendor/`, `docs/roadmaps/`, `docs/adr/`) is likewise not renamed (carve-outs).

## Success signal

The demo→staging and CDC→events renames are merged as one PR and applied to staging: `grep` sweeps come back clean — "cdc" (over `pipeline/ tests/ docs/`) with zero matches outside the historical carve-outs (`docs/adr/` records, `pipeline/migrations/*` scripts and their tests, terraform `moved`/`state mv` blocks), "demo" (over `pipeline/ tests/ terraform/ .github/ docs/ README.md`) zero matches outside the CAP-2 carve-outs (`docs/adr/`, `docs/_vendor/`, `docs/roadmaps/`, `_bmad-output/`) and the Trading 212 tier allow-list — the migration scripts ran idempotently against staging and preserved every encrypted row (counts verified via `pipeline.query --decrypt`), `/portfolio/staging/*` is live and `/portfolio/demo/*` retired, and the full test suite is green. The word "CDC" survives only in historical records — ADRs and migration artifacts (`pipeline/migrations/*` scripts and their tests); "demo" survives only as Trading 212's paper-trading tier name and in historical/vendored records.

## Assumptions

- "Function names" in the user's phrasing means Step Function (state machine) names plus the Python `fetch_cdc` / `dedup_cdc_events` functions — all covered by the two rename tracks.
- The residual "demo" naming in staging infra — terraform module var `demo`, `env_label = "demo"`, `MODE_TO_ENV_LABEL = {"staging": "demo"}` — is in scope; `prod/main.tf`'s `demo = false` flips to `staging = false`.

## Open Questions

None — all three originally raised (physical bucket rename, SSM retirement timing, CDC scope) are resolved in the memlog.
