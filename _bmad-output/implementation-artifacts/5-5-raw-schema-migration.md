# Story 5.5: Raw-schema migration — backfill XTB `account_id`, drop `source_file` (AD-7)

Status: ready-for-dev

## Story

As a deployer,
I want a migration that rewrites each `raw/{broker}` to the new `RAW_SCHEMA`, backfilling XTB `account_id` from `source_file` before dropping it,
so that the new schema deploys against pre-existing tables without NULL-keyed re-insertion or lost account identity (CAP-1, deploy gate).

**This is the deploy gate of the epic.** Implement per AD-7 of the spine. It
prevents two deploy-time failures: deploying code that merges on `account_id`
against tables that predate it (every legacy XTB row becomes NULL-keyed →
re-inserted every run instead of replaced), and dropping `source_file` before
the backfill has read it (the account identity is gone forever).

## Acceptance Criteria

### AC-1 — The migration rewrites each `raw/{broker}` to the new `RAW_SCHEMA` (AD-7)

**Given** the live per-broker raw tables carrying the old `RAW_SCHEMA` (with `source_file`, no `account_id`),
**When** `pipeline/migrations/migrate_raw_account_id.py` runs,
**Then** each `raw/{broker}` is rewritten to the new `RAW_SCHEMA` (`fetched_at, broker, source, payload, payload_hash, account_id`)
**And** XTB `account_id` is backfilled by parsing `source_file` (the retained filename → account id)
**And** `source_file` is then dropped.

### AC-2 — The backfill parses the filename only (AD-7, adversarial F4 pin)

**Given** a legacy XTB row,
**When** the backfill runs,
**Then** the backfill parses the filename only — an unparseable filename yields `NULL`, matching AD-1's append-for-null-key rule
**And** no payload parsing happens at migration time, so legacy rows get exactly one deterministic value
**And** the transform's payload-parse recovery remains the sole recovery path.

### AC-3 — Idempotent, `--dry-run`, destination conflict raises (ADR 0112 A1 convention)

**Given** the migration convention in `pipeline/migrations/migrate_single_bronze.py` + `_storage_options.py`,
**When** the migration runs,
**Then** it is idempotent — exit 0 on absent or already-migrated tables, raise on auth/region/permission errors or unexpected schema
**And** it supports `--dry-run` (prints the plan, writes nothing)
**And** it runs via `python -m pipeline.migrations.migrate_raw_account_id --mode <docker|staging|prod> [--dry-run]` — never hand-constructed `DeltaTable()`
**And** a destination conflict raises rather than clobbers.

### AC-4 — Events tables need no change (AD-7)

**Given** the CAP-2 change swaps the events write mechanism, not the schema,
**When** the migration runs,
**Then** the `{broker}_events` normalized tables are untouched.

### AC-5 — Deploy sequencing documented (AD-7, ADR 0110)

**Given** the migration is the deploy gate,
**When** the migration is documented,
**Then** the runbook states: per environment, pause scheduled Step Functions executions (connectors idle = executions stopped, ADR 0110's file-arrival task included), run `--dry-run`, verify, run for real, confirm `raw/{broker}` counts, then resume executions and deploy the new code.

## Tasks / Subtasks

- [ ] T1: `pipeline/migrations/migrate_raw_account_id.py` (NEW) (AC-1, AC-2, AC-3)
  - [ ] T1.1 Copy the convention from `migrate_single_bronze.py` + `_storage_options.py`: argparse `--mode docker|staging|prod` + `--dry-run`; `pipeline.secrets.load_env()` + `set_mode`; `get_storage_options_with_credentials()`; exit 0 on absent/already-migrated; `RuntimeError`/`ClientError` → `SystemExit(1)`
  - [ ] T1.2 Per broker: read `raw/{broker}` (old schema), backfill XTB `account_id` by parsing `source_file` (filename only — unparseable → `NULL`), drop `source_file`, write the new `RAW_SCHEMA` — `pl.DataFrame` accepted directly, do NOT convert to `pa.Table` for writes
  - [ ] T1.3 Idempotent recovery: re-running against an already-migrated table exits 0; a destination schema that does not equal the new `RAW_SCHEMA` raises rather than clobbers (ADR 0113 A1 conflict convention)
  - [ ] T1.4 `--dry-run` prints the plan and writes nothing
- [ ] T2: Tests — `tests/test_migrate_raw_account_id.py` (AC-1, AC-2, AC-3)
  - [ ] T2.1 Model on `tests/test_migrate_single_bronze.py` (FakeS3 / FakeBackend / `_fake_config` pattern — do NOT mock deltalake/polars; write real local Delta tables in `tmp_path`)
  - [ ] T2.2 Backfill: XTB rows get `account_id` from `source_file`; unparseable filename → `NULL`; IBKR/T212 rows get `NULL`
  - [ ] T2.3 `source_file` dropped; new schema matches `RAW_SCHEMA`
  - [ ] T2.4 Idempotent no-op on already-migrated; absent source exits 0; dry-run does not write; conflict raises
- [ ] T3: Deploy sequencing runbook in the migration docstring (AC-5)
- [ ] T4: Full checks: `ruff check --fix . && ruff format .`; `pyright pipeline/ tests/`; `pytest tests/ -q -rf`; tests re-run after lint

## Dev Notes

### Current state (verified 2026-08-22)

- **Migration pattern to copy** — `pipeline/migrations/migrate_single_bronze.py` + `_storage_options.py`: argparse `--mode docker|staging|prod` + `--dry-run`; `pipeline.secrets.load_env()` then `set_mode`; S3 client via `_build_s3_client()`; idempotent absent/already-migrated → exit 0; `get_storage_options_with_credentials()` injects real AWS creds into deltalake storage options.
- **Migration test pattern** — `tests/test_migrate_single_bronze.py`: `FakeS3` client double (in-memory objects) + `_FakeBackend`/`_fake_config` with `use_storage(...)`, `storage_options` returns fake creds so the boto3 chain is skipped. Reuse this.
- **The migration reads the OLD schema** (with `source_file`) — its fixtures use the old schema, NOT the new one. This is the one place `source_file` legitimately survives (grep carve-out).
- **XTB filename pattern** — `{CCY}_{account_id}_{from}_{to}.xlsx` (see `_account_id_from_filename`, `xtb/transform.py:80-95`). The migration reuses this parse.

### What the developer MUST NOT change (preserve exactly)

- **`migrate_single_bronze.py` and `migrate_cdc_to_events.py`** — historical artifacts, never renamed or rewritten.
- **The new `RAW_SCHEMA`** — defined by 5-1; the migration writes exactly that schema.
- **Events tables** — untouched (AC-4).
- **No payload parsing at migration time** (AC-2, adversarial F4 pin) — the backfill is filename-only.

### Deploy sequencing (AD-7, migration-first)

1. Per environment: (a) pause scheduled Step Functions executions (connectors idle — ADR 0110's file-arrival task included), (b) run `migrate_raw_account_id --mode <env> --dry-run`, (c) verify, then run for real, (d) confirm `raw/{broker}` counts and `account_id` backfill via `pipeline.run query --decrypt --mode staging`, (e) resume executions, (f) deploy the new code (5-1's schema + 5-2's merge) last.
2. If the migration runs AFTER the new code, the merge-on-`account_id` code reads tables that predate the column → every legacy XTB row becomes NULL-keyed → re-inserted every run. The migration must precede the code deploy per environment.

### Review pins carried into this story

- **Adversarial review F4 (MEDIUM-HIGH)** — migration backfill scope: filename-only parse; unparseable legacy rows become `NULL` (feeding AD-1's append-for-null-key rule). AC-2.

### Check & workflow commands (run from the repo root)

```bash
# project venv (never system Python); worktree has no .venv — use main repo's
.venv/Scripts/python -m ruff check --fix . && .venv/Scripts/python -m ruff format .
.venv/Scripts/python -m pyright pipeline/ tests/
.venv/Scripts/python -m pytest tests/ -q -rf
```

### Testing standards

- Migration tests use the FakeS3/FakeBackend pattern (`tests/test_migrate_single_bronze.py`) — do NOT mock deltalake/polars; write real local Delta tables in `tmp_path`.
- Run all three checks before finishing; re-run tests after linting.

## Dependencies

- **Blocked by:** 5-1 (the new `RAW_SCHEMA` contract).
- **Parallel with:** 5-2 (different files).
- **Deploy gate for:** 5-1's schema + 5-2's merge code (must run per environment before they deploy).

## Project Structure Notes

- Target structural seed (from the spine):

```text
pipeline/
  migrations/            # migrate_raw_account_id.py: backfill XTB account_id, drop source_file (AD-7)
```

- The migration script + its test are the **only** legitimate `source_file` references in live code (grep carve-out alongside `docs/adr/`).
- No new dependencies; reuse the pinned stack.

## References

- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/ARCHITECTURE-SPINE.md` — AD-7 (raw-schema migration is the deploy gate), Inherited Invariants (ADR 0112 A1 convention)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/SPEC.md` — Constraints, Success signal
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/reviews/review-adversarial.md` — F4 (migration backfill scope)
- Code: `pipeline/migrations/migrate_single_bronze.py`, `pipeline/migrations/_storage_options.py`, `tests/test_migrate_single_bronze.py`, `pipeline/connectors/xtb/transform.py` (`_account_id_from_filename`)
- ADRs: `docs/adr/0112` (A1 migration convention), `0113` (A1 conflict convention), `0110` (file-arrival task paused across the migration window)

## Dev Agent Record

### Agent Model Used

(To be filled by the implementing subagent.)

### Debug Log References

### Completion Notes List

### File List
