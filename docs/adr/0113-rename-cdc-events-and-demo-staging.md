# 0113: Rename CDC Events Layer to events and Demo Staging Environment to staging

> **Naming, not decisions** — this ADR renames the identifiers of the broker activity event layer ("CDC" → "events", issue #131) and the staging environment ("demo" → "staging"). Every prior decision those identifiers refer to remains in force; only the names change. This ADR supersedes no prior ADR. Where a prior ADR names a renamed identifier (e.g. ADR 0058/0077's `cdc_events_normalized_schema`, ADR 0090's `--mode` labels, ADR 0065's `cdc_events` tables), read the new name as substituted.

## Context

Two naming problems accumulated in this pipeline:

**The "CDC" layer is not change-data-capture.** The raw/normalized broker activity tables (`cdc_events`, `{broker}_cdc`, `ibkr_cdc`, `trading212_cdc`, `xtb_cdc`) are built from broker Flex report exports and REST API pulls — the full broker activity event stream, not CDC deltas of another table. The name misleads new readers into expecting database change feeds. The codebase named everything after it: `consolidate_cdc.py`, `fetch_cdc`/`transform_cdc`/`_build_cdc_record`, `dedup_cdc_events`, `decrypt_cdc_payloads`, `cdc_raw_layer`, `cdc_events_normalized_schema`, `pipeline/analytics/cdc_tables.py`, DQ config keys `"cdc_events"`/`"{broker}_cdc"`, `RAW_*_CDC` path constants, and the `_consolidate_cdc`/`_normalize_cdc` run entry points. Issue #131 tracks the rename to "events".

**The staging environment is mislabeled "demo".** The environment used for pre-prod verification holds the full staging data and runs real connector code against it, yet is named "demo" everywhere: the `is_demo` staging-mode predicate, `MODE_TO_ENV_LABEL`, the demo S3 prefix, the `investment-portfolio-pipeline-demo` bucket, `/portfolio/demo/*` SSM parameter paths, and the `terraform/demo`-era module structure (later `terraform/staging`). "Demo" understates the environment's role and collides with the genuinely different IBKR "paper trading" tier, which the same `is_demo` flag was overloaded to select.

## Decision

Rename both layers per the rename map in `_bmad-output/specs/spec-rename-cdc-events-demo-staging/rename-plan.md` (the single source of truth for new names), shipping both tracks as one reviewed PR with data-preserving migrations.

**Track A — CDC → events (issue #131):**

- Delta tables: `raw/{broker}_cdc` → `raw/{broker}_events`, `normalized/{broker}_cdc` → `normalized/{broker}_events`, `normalized/cdc_events` → `normalized/events`.
- Code: `consolidate_cdc.py` → `consolidate_events.py`; `fetch_cdc` → `fetch_events`, `transform_cdc` → `transform_events`, `cdc_raw_layer` → `events_raw_layer`, `_build_cdc_record` → `_build_event_record`; `dedup_cdc_events` → `dedup_events`, `decrypt_cdc_payloads` → `decrypt_events_payloads`; broker schemas `*_cdc_raw_schema` → `*_events_raw_schema`, `cdc_events_normalized_schema` → `events_normalized_schema`; `pipeline/analytics/cdc_tables.py` → `events_tables.py`; DQ config keys `"cdc_events"`/`"ibkr_cdc"`/`"trading212_cdc"`/`"xtb_cdc"` → `"events"`/`"ibkr_events"`/`"trading212_events"`/`"xtb_events"`; `RAW_*_CDC` → `RAW_*_EVENTS`; `_consolidate_cdc`/`_normalize_cdc` → `_consolidate_events`/`_normalize_events`.
- Migration A1 (`pipeline/migrations/migrate_cdc_to_events.py`) renames the Delta table directories server-side (S3 copy + verify + delete, idempotent, conflict-safe) and rewrites historical raw `source` values `flex_cdc` → `flex_events` in place (AD-2(d)); the rename is a code-only change that would otherwise silently skip every historical IBKR Flex row.
- Column names are untouched (AD-5 immutables): the `event_*` columns keep their names.

**Track B — demo → staging environment:**

- `is_demo` → `is_staging` for the staging-mode predicate (which environment is active), while the IBKR tier flag becomes `is_paper` (a real broker tier, not a demo) with `_inject_paper_deposit`/`_PAPER_INITIAL_DEPOSIT_AMOUNT`. Trading 212 keeps "demo" vocabulary where it names the real broker domain `demo.trading212.com` and its `DEMO_BASE_URL`/`_DEMO_BASE_URL` constants (tier allow-list in the success bars).
- `MODE_TO_ENV_LABEL` removed; the staging env label derives from `--mode` directly (keeps the ValueError guard).
- Storage: staging prefix becomes empty (bucket root), `S3_BUCKET` → `investment-portfolio-pipeline-staging`; SSM paths `/portfolio/demo/*` → `/portfolio/staging/*`; state machine → `portfolio-pipeline-orchestrator-staging`.
- Terraform: `terraform/staging` modules and shared config read staging; `moved` blocks in `terraform/staging/main.tf` carry existing demo-named resources to staging names; prod flips `staging = false` (the only prod change).
- Migrations B1 (`migrate_demo_bucket_to_staging.py`) copies the encrypted bucket contents, B2 (`migrate_demo_ssm_to_staging.py`) moves SSM secrets to staging paths, B3 moves terraform state to staging names.

**Sequencing (AD-4, migration-first deploy-last):** A1 runs only after `migrate_cdc_events_drop_gross_amount.py` has been applied per environment (that script's `_CDC_TABLES` are the pre-rename names and would no longer exist after A1). B1/B2/B3 run before the renamed code deploys so the renamed names exist when the first renamed run looks for them.

**Alternative considered and rejected:** keep the names and document the misnomer. Rejected because the names are load-bearing everywhere (paths, env vars, terraform state, SSM), so drift between docs and reality would persist; the rename is mechanical and verified by grep-zero bars.

## Constraints

- **Success bars (AD-3):** `grep -rni "cdc" pipeline/ tests/ docs/` returns zero outside `docs/adr/` and migration artifacts (historical-reference exemption: migration scripts, their tests, and terraform `moved` blocks intentionally retain old names). `grep -rni "demo" pipeline/ tests/ terraform/ .github/ docs/ README.md` returns zero outside `docs/adr/`, `docs/_vendor/`, `docs/roadmaps/`, `_bmad-output/`, and the Trading 212 tier allow-list (`demo.trading212.com`, `DEMO_BASE_URL`, `_DEMO_BASE_URL`).
- **Immutables (AD-5):** `docs/adr/` files are never rewritten (old ADRs keep their "cdc"/"demo" references; this ADR is the bridge); `event_*` column names unchanged; prod is untouched except the `staging = false` flip.
- Migration A1 must never overwrite a destination object whose size differs from the source (conflict raises), and must delete source objects only after post-copy verification.
- Environments that ran pre-rename code must run A1 before the renamed deploy, or they silently start with empty tables and lose history.
- Prod data originally lived under the `pipeline` storage prefix; the `S3_PREFIX` concept was removed entirely as a follow-up applied 2026-08-19 (see Consequences), so prod now stores at the bucket root like staging and docker.

## Consequences

- **Positive:** "events" accurately describes the broker activity event layer; "staging" accurately describes the pre-prod environment and frees "demo" for the Trading 212 tier where it names a real broker domain. The IBKR paper-tier flag is no longer conflated with the staging-mode flag.
- **Positive:** the grep-zero bars give a closed, mechanically verifiable enforcement gate for the rename.
- **Negative:** historical names persist in exempt artifacts — migration scripts, their tests, terraform `moved` blocks, and all prior ADRs — and are the intended inputs to the one-time migrations. Anyone reading old ADRs must apply the substitution "old name → new name."
- **Negative:** environments must run the migrations in the documented order (drop-gross-amount → A1 → B1/B2/B3) with a deploy window; a skipped migration silently loses history (A1) or breaks secret resolution (B2).
- **Negative (resolved):** prod originally still carried the old `pipeline` prefix naming. Follow-up applied 2026-08-19: the `S3_PREFIX` concept was deleted entirely (no `S3_DEFAULT_PREFIX`, no `S3Backend.prefix`, no `S3_PREFIX` env var, no `s3_prefix` terraform variable) and prod's `pipeline/*` data was moved to the bucket root by the one-time migration `migrate_prod_pipeline_prefix_to_root.py` before the prefix-removed code deployed. All environments (prod, staging, docker/MinIO) now read/write at the bucket root; ADR 0108's `pipeline/xtb_uploads/` references are superseded for prod by `xtb_uploads/`.

## Validation

- Both grep bars return zero outside the documented carve-outs (verified against the committed tree).
- Full test suite passes (834 tests), `ruff check`/`ruff format` clean, `pyright` clean.
- `tests/test_migrate_cdc_to_events.py` exercises A1: rename (copy+delete), idempotent no-op, absent-table skip, dry-run, destination-size-conflict `RuntimeError`, and `flex_cdc` → `flex_events` rewrite with schema guard; B1/B2 have dedicated migration tests.
- Migration tests exempt from the "cdc"/"demo" bars (historical-reference exemption).
- Manual (staging): run drop-gross-amount, then A1 with `--dry-run` then for real; `pipeline validate` passes; `pipeline.run query "SELECT count(*) FROM events" --decrypt --mode staging` matches the pre-migration `cdc_events` count; run B1, then B2; `terraform apply` in staging succeeds with the `moved` blocks; verify SSM staging paths resolve.
