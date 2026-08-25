# Story 5.1: RAW_SCHEMA evolution — nullable `account_id`, `source_file` dropped (AD-2)

Status: ready-for-dev

## Story

As a data engineer,
I want `RAW_SCHEMA` to carry a nullable `account_id` and drop `source_file`, with XTB populating `account_id` from the report filename at fetch time,
so that the raw layer has a stable per-account retention identity for XTB and the schema matches the bounded-bronze contract (CAP-1).

**This is the schema foundation of the bounded-bronze epic.** Every other story
reads `RAW_SCHEMA`; 5-2's XTB merge key and 5-5's migration backfill both
depend on this story's schema contract. Implement per AD-2 of the spine.

## Acceptance Criteria

### AC-1 — `RAW_SCHEMA` carries nullable `account_id`, no `source_file` (AD-2)

**Given** the current `RAW_SCHEMA` (`pipeline/raw/models.py:14-23`:
`fetched_at, broker, source, payload, payload_hash, source_file`),
**When** the schema is changed to `{fetched_at, broker, source, payload, payload_hash, account_id}`,
**Then** `account_id` is a nullable `pa.string()` field and `source_file` is removed
**And** every raw writer (`xtb/fetch.py`, `trading212/fetch.py`, `ibkr/fetch.py`) produces this schema
**And** `pipeline/raw/__init__.py` re-exports the updated `RAW_SCHEMA` unchanged in name.

### AC-2 — XTB fetch populates `account_id` from the report filename (AD-2)

**Given** an XTB report file whose filename follows the new-format pattern `{CCY}_{account_id}_{from}_{to}.xlsx`,
**When** `XtbConnector.fetch_snapshot` builds the raw row,
**Then** `account_id` is populated from the filename at fetch time (filename-first — the transform no longer derives it from `source_file`)
**And** an unparseable filename yields `NULL` `account_id` (the contract's named risk; the transform's payload-parse recovery is the fallback)
**And** IBKR and Trading 212 store `NULL` `account_id` and never merge on it.

### AC-3 — XTB transform groups on the raw `account_id`, recovers by payload parse when null (AD-2)

**Given** a merged `raw/xtb` whose rows carry the raw `account_id` column,
**When** `transform_snapshot` / `transform_events` run,
**Then** the per-account latest logic (`_latest_per_account`, `xtb/transform.py:97`) groups on the raw `account_id` column instead of `_account_id_from_filename(row.source_file)`
**And** when the raw `account_id` is `NULL`, the transform recovers it by parsing the raw payload (R1 `Account number`), matching the spec's null-recovery fallback
**And** `fetched_at` plus `payload_hash` provide any required deterministic tie-break
**And** `(source, account_id)` with null `account_id` is **never** treated as one shared cross-broker key.

### AC-4 — `source_file` removed from the shared transform helpers (AD-2)

**Given** `DecodedRow` (`transform_utils.py:26-34`) and `iter_raw_payloads` (`transform_utils.py:128-193`) carry `source_file`,
**When** the column is dropped from `RAW_SCHEMA`,
**Then** `DecodedRow.source_file` and the `iter_raw_payloads` `source_file` read are removed
**And** every consumer (Trading 212 transform via `iter_raw_payloads`; IBKR reads raw columns directly) is updated and passes.

### AC-5 — No `source_file` references remain in live code; full suite green

**Given** all code, fixture, and test changes are complete,
**When** `grep -rni "source_file" pipeline/ tests/` runs and the full check suite runs,
**Then** the grep returns zero outside the carve-outs (the 5-5 migration script + its test, `docs/adr/`)
**And** `ruff check --fix . && ruff format .`, then `pyright pipeline/ tests/`, then `pytest tests/ -q -rf` all pass (tests re-run after lint).

## Tasks / Subtasks

- [ ] T1: `pipeline/raw/models.py` — `RAW_SCHEMA` gains nullable `account_id`, drops `source_file` (AC-1)
- [ ] T2: `pipeline/connectors/xtb/fetch.py` — populate `account_id` from the report filename at fetch time; move the filename→account-id parse here (AC-2)
  - [ ] T2.1 Move `_account_id_from_filename` (`xtb/transform.py:80-95`) into `fetch.py`; `fetch_snapshot` sets `account_id` from it (unparseable → `NULL`)
  - [ ] T2.2 IBKR/T212 fetches add `account_id` as `NULL` (AC-2)
- [ ] T3: `pipeline/connectors/xtb/transform.py` — group on the raw `account_id`; payload-parse recovery when null (AC-3)
  - [ ] T3.1 `_latest_per_account` reads `row.account_id` (raw column) instead of `_account_id_from_filename(row.source_file)`
  - [ ] T3.2 Keep the payload-parse recovery (R1 `Account number`) as the null fallback; keep the `fetched_at`+`payload_hash` tie-break
- [ ] T4: `pipeline/connectors/transform_utils.py` — remove `source_file` from `DecodedRow` and `iter_raw_payloads` (AC-4)
- [ ] T5: Fixtures + tests (AC-5)
  - [ ] T5.1 `tests/fixtures/{xtb,ibkr,trading212}.py` — add `account_id` (XTB: real value; IBKR/T212: `NULL`), drop `source_file`
  - [ ] T5.2 Update `tests/test_xtb_connector.py`, `tests/test_transform_utils.py`, `tests/test_ibkr_connector.py`, `tests/test_trading212_connector.py`, `tests/test_single_bronze_routing.py` for the new schema
  - [ ] T5.3 Regression: an XTB row with `NULL` raw `account_id` still produces silver rows via payload-parse recovery
  - [ ] T5.4 Regression: two XTB accounts in one `raw/xtb` table group separately (per-account latest preserved)
- [ ] T6: Full checks (AC-5): `ruff check --fix . && ruff format .`; `pyright pipeline/ tests/`; `pytest tests/ -q -rf`; tests re-run after lint

## Dev Notes

### Current state (verified 2026-08-22)

- **`RAW_SCHEMA`** (`pipeline/raw/models.py:14-23`): `fetched_at, broker, source, payload, payload_hash, source_file`. Re-exported by `pipeline/raw/__init__.py`.
- **XTB fetch** (`pipeline/connectors/xtb/fetch.py:67-93`): `fetch_snapshot` builds one raw row per file with `source == "XTB_REPORT"` and `source_file = filename`. No `account_id` today.
- **XTB transform** (`pipeline/connectors/xtb/transform.py`): `_account_id_from_filename` (line 80) parses `{CCY}_{account_id}_{from}_{to}.xlsx`; `_latest_per_account` (line 97) groups by that filename-derived id, falling back to payload parse (R1 `Account number`) when the filename yields nothing. `transform_snapshot`/`transform_events` keep exact `source == "XTB_REPORT"` gates.
- **Shared helpers** (`pipeline/connectors/transform_utils.py`): `DecodedRow` (lines 26-34) carries `source_file`; `iter_raw_payloads` (lines 128-193) reads `raw.column("source_file")` (line 157) and yields it. Consumers: Trading 212 transform (`trading212/transform.py:26,44`) uses `iter_raw_payloads` but does **not** read `row.source_file`; IBKR transform reads raw columns directly (`ibkr/transform.py:79-81, 262-264`) and does not use `source_file`.
- **`filter_latest_snapshot`** (`transform_utils.py:55-97`) uses only `fetched_at` + `source` — no `source_file`. **`dedup_events`** (line 390) uses only the event identity subset — no `source_file`. Neither needs a change.

### What the developer MUST NOT change (preserve exactly)

- **The source vocabulary** (AD-2): `flex`/`flex_events`/the five T212 paths/`XTB_REPORT` stay exactly; fetch sets `source`, transform never rewrites it.
- **Silver (normalized) schemas and contents** — `events_normalized_schema`, `snapshot_normalized_schema` untouched. `account_id` in silver is unchanged; this story only adds the raw-layer column.
- **`filter_latest_snapshot` per-source keying** (ADR 0100) and XTB's per-`account_id` latest (ADR 0108 D18) — add regression guards, never convert to a global max.
- **`dedup_events`** (ADR 0105 latest-wins) — untouched (5-3's concern).
- **`pipeline/migrations/*`** — do NOT touch; the 5-5 migration reads the OLD schema (with `source_file`) and is the deploy gate.

### Deploy sequencing (AD-7 — owned by 5-5, but this story must not break it)

The 5-5 migration backfills `account_id` from `source_file` **before** `source_file` is dropped, and runs per environment **before** this story's code deploys there. This story's code change and the migration are separate PRs-in-one; the orchestrator sequences them. Do not add a payload-parse backfill to the migration — the migration parses the filename only (adversarial review F4 pin).

### Review pins carried into this story

- **Adversarial review F4** — migration backfill scope: filename-only parse; unparseable legacy rows become `NULL` (feeding AD-1's append-for-null-key rule). The transform's payload-parse recovery is the sole recovery path.
- **Reconcile review F8** — XTB event identity is `(event_type, event_id, account_id)` (5-3's concern, but the raw `account_id` column this story adds is what makes it possible).

### Check & workflow commands (run from the repo root)

```bash
# project venv (never system Python); worktree has no .venv — use main repo's
.venv/Scripts/python -m ruff check --fix . && .venv/Scripts/python -m ruff format .
.venv/Scripts/python -m pyright pipeline/ tests/
.venv/Scripts/python -m pytest tests/ -q -rf
grep -rni "source_file" pipeline/ tests/
```

### Testing standards

- pytest + fixtures in `tests/fixtures/{ibkr,trading212,xtb}.py` (all `pa.Table` shaped to `RAW_SCHEMA`).
- New behavior (filename-first `account_id`, null-recovery, per-account grouping) gets focused tests; existing tests updated only where they name `source_file` or the schema.
- Run all three checks before finishing; re-run tests after linting.

## Dependencies

- **Blocked by:** none — this is the foundation.
- **Blocks:** 5-2 (XTB merge key = `account_id`), 5-5 (migration backfill).
- **Parallel with:** 5-3, 5-4 (mutually independent).

## Project Structure Notes

- Target structural seed (from the spine):

```text
pipeline/
  raw/
    models.py            # RAW_SCHEMA: fetched_at, broker, source, payload, payload_hash, account_id (nullable)
  connectors/
    xtb/fetch.py         # account_id from report filename at fetch time (AD-2); parse recovery in transform
    transform_utils.py   # DecodedRow/iter_raw_payloads without source_file
```

- Naming: the raw `account_id` column is plaintext (never encrypted — merge keys stay plaintext, spine Consistency Conventions). `source_file` disappears from live code; the only surviving references are the 5-5 migration script + its test and `docs/adr/`.
- No new dependencies; reuse the pinned stack (deltalake 1.6.0, polars 1.42.0, pyarrow 24.0.0 — pyarrow for schemas + S3 fs only).

## References

- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/ARCHITECTURE-SPINE.md` — AD-2 (RAW_SCHEMA + XTB identity), Conflicts surfaced (supersedes ADR 0047's `account_id` exclusion), Consistency Conventions (merge keys plaintext)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/SPEC.md` — Constraints (nullable `account_id`, `source_file` removed, XTB filename-first, null-recovery)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/retention-and-events-contract.md` — Broker policy (XTB nullable raw `account_id`)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/reviews/review-adversarial.md` — F4 (migration backfill scope)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/reviews/review-reconcile.md` — F8 (XTB event identity)
- Code: `pipeline/raw/models.py`, `pipeline/connectors/xtb/{fetch,transform}.py`, `pipeline/connectors/transform_utils.py`, `tests/fixtures/{xtb,ibkr,trading212}.py`
- ADRs: `docs/adr/0047` (raw stores bytes; `account_id` exclusion superseded here), `0108` (D18 per-account latest), `0100` (per-source dedup)

## Dev Agent Record

### Agent Model Used

(To be filled by the implementing subagent.)

### Debug Log References

### Completion Notes List

### File List
