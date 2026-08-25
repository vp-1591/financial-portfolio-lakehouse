# Story 5.2: Write-time merge-on-key retention + per-run VACUUM (AD-1, AD-3)

Status: ready-for-dev

## Story

As a data engineer,
I want each broker's raw write to be a Delta MERGE on the broker retention key with a per-run VACUUM,
so that raw storage stays bounded and the read/dedup work per fetch stops growing (CAP-1, CAP-4, issue #154).

**This is the core mechanism of the bounded-bronze epic.** Implement per AD-1
and AD-3 of the spine. The cross-run `dedup_raw` accumulation scan is deleted;
the merge *is* the bounded scan this epic exists to ship.

## Acceptance Criteria

### AC-1 — The raw write is a `DeltaTable.merge` on the broker retention key (AD-1)

**Given** a current fetch batch for broker `B` and the existing `raw/{B}` table,
**When** the batch is written,
**Then** the write is `DeltaTable.merge(source, predicate)` keyed on B's retention key — XTB `account_id`, Trading 212 and IBKR `source` (endpoint / Flex query)
**And** `when_matched_update(updates=…)` replaces the matched row with the current fetch row (latest `fetched_at` wins by construction — every write is a current fetch)
**And** `when_not_matched_insert_all()` inserts new keys
**And** rows whose key is absent from the current batch are **untouched** — a fetch never deletes a key it did not see.

### AC-2 — In-batch dedup stays; the cross-run `dedup_raw` scan is deleted (AD-1)

**Given** the current `dedup_raw` cross-run scan (`raw/ingest.py:27-70`) that reads accumulated `(source, payload_hash)` back,
**When** the merge write lands,
**Then** the batch is deduped in-batch on `(source, payload_hash)` **before** the merge so one batch cannot insert two identical rows
**And** the cross-run accumulated-table scan is deleted (the read/dedup work the feature exists to kill).

### AC-3 — NULL retention keys are appended, never merged (AD-1, adversarial F1.1 pin)

**Given** an XTB row whose `account_id` is `NULL` (unparseable filename — the contract's named risk),
**When** the merge runs,
**Then** the NULL-keyed row is **appended, never merged** (Delta MERGE predicates never match `NULL`, so a NULL-keyed row would insert on every run; the append + in-batch `(source, payload_hash)` dedup keeps it present and bounded instead)
**And** this admitted growth is the accepted trade-off for unparseable filenames (memlog decision 2026-08-22).

### AC-4 — Trading 212's merge key is the declared endpoint base (AD-1, reconcile H1 pin)

**Given** Trading 212 paginated responses whose raw `source` values carry per-run cursor suffixes (`nextPagePath`),
**When** the retention key is computed,
**Then** the pagination suffix is stripped from `source` before keying, so cursor pages cannot fragment the key
**And** the endpoint's final page (the complete response per ADR 0116) is what "latest complete response per endpoint" retains
**And** `SELECT DISTINCT source` per broker is unchanged (Consistency Conventions regression guard).

### AC-5 — VACUUM per run, Delta 7-day default, `dry_run=False` (AD-3, adversarial F3 pin)

**Given** the merge creates tombstones during the write,
**When** each broker run finishes,
**Then** the run invokes `DeltaTable.vacuum(dry_run=False)` with the default retention (`retention_hours` omitted, `enforce_retention_duration` stays `True`)
**And** `dry_run=False` is mandatory — deltalake 1.6.0 defaults `vacuum()` to a no-op dry run that only lists files
**And** each connector task vacuums its own `raw/{broker}` only — this policy never vacuums the silver event tables
**And** the XTB EventBridge file-arrival task (ADR 0110) also vacuums `raw/xtb`.

### AC-6 — Per-endpoint isolation and the T212 fail-loud contract survive (AD-1)

**Given** the current per-endpoint `try/except` write-what-succeeded isolation and Trading 212's all-events-endpoints-empty `RuntimeError` (`trading212/fetch.py:127-136`),
**When** the merge write lands,
**Then** both survive unchanged (carried forward from the superseded parent AD-5).

### AC-7 — Regression guards (Consistency Conventions)

**Given** the merge write and VACUUM are complete,
**When** the regression tests run,
**Then** a re-fetch with a byte-identical payload does not add a row; a key absent from the current response stays in raw; merging the same batch twice is a no-op; `SELECT DISTINCT source` per broker is unchanged; paginated T212 pages merge onto their endpoint, never onto the cursor `source`; a `dry_run=False` VACUUM physically removes tombstoned files past the 7-day threshold (CAP-4's "tested or verified for every environment").

## Tasks / Subtasks

- [ ] T1: `pipeline/raw/retention.py` (NEW) — per-broker retention key + vacuum invocation, thin single source of truth (AC-1, AC-4, AC-5)
  - [ ] T1.1 Retention keys: XTB `account_id`, Trading 212/IBKR `source`; T212 strips the pagination suffix before keying (AC-4)
  - [ ] T1.2 `vacuum_raw(path)` helper: `DeltaTable.vacuum(dry_run=False)`, default retention, per-broker only (AC-5)
- [ ] T2: `pipeline/raw/ingest.py` — merge write + in-batch dedup, cross-run scan deleted (AC-1, AC-2, AC-3)
  - [ ] T2.1 `ingest_raw` becomes: encrypt → in-batch dedup on `(source, payload_hash)` → `DeltaTable.merge` on the retention key; NULL-keyed rows appended (AC-3)
  - [ ] T2.2 Delete the cross-run `dedup_raw` accumulated-table scan (AC-2)
  - [ ] T2.3 **Keep the pre-dedup encrypted-table return** — the handoff contract (5-6 removes it; do not break `tests/test_transform_connector_handoff.py`)
- [ ] T3: `pipeline/run.py` `fetch_connector` — call the merge write; invoke VACUUM at the end of each broker run (AC-1, AC-5)
  - [ ] T3.1 Replace the `ingest_raw` append call sites with the merge write (AC-1)
  - [ ] T3.2 After the run's writes, call `retention.vacuum_raw(raw_path)` (AC-5)
  - [ ] T3.3 Keep the handoff dict building intact (5-6 removes it)
- [ ] T4: Tests (AC-7)
  - [ ] T4.1 Merge semantics: replace-on-key, insert-new, untouched-absent, same-batch-twice no-op
  - [ ] T4.2 NULL-key append + in-batch dedup bound (AC-3)
  - [ ] T4.3 T212 paginated pages merge onto the endpoint base (AC-4)
  - [ ] T4.4 VACUUM `dry_run=False` physically removes tombstoned files past the threshold (AC-5, AC-7)
  - [ ] T4.5 `SELECT DISTINCT source` per broker unchanged (AC-7)
- [ ] T5: Full checks: `ruff check --fix . && ruff format .`; `pyright pipeline/ tests/`; `pytest tests/ -q -rf`; tests re-run after lint

## Dev Notes

### Current state (verified 2026-08-22)

- **`ingest_raw`** (`pipeline/raw/ingest.py:73-126`): encrypt → `dedup_raw` (cross-run scan, lines 27-70) → `write_deltalake(mode="append")`. Returns the Fernet-encrypted **pre-dedup** table for the handoff (lines 80-88, 114, 126).
- **`fetch_connector`** (`pipeline/run.py:123-236`): iterates `fetch_kwargs` batches, calls `ingest_raw` per batch (lines 174, 215), builds the handoff dict for `handoff_supported` connectors (lines 157-159, 175-182, 216-221). The events fetch appends to the same `raw/{name}` (lines 201-234).
- **Trading 212 pagination** (`pipeline/connectors/trading212/client.py:133-162`): `_fetch_paginated` follows `nextPagePath` links; each page's `path` is captured in `captured_responses` → the raw `source` values include cursor suffixes. **This is the reconcile review's H1 gap** — the merge-on-`source` must strip the suffix before keying (AC-4).
- **T212 fail-loud** (`pipeline/connectors/trading212/fetch.py:127-136`): `RuntimeError` when any/all events endpoints fail — survives unchanged (AC-6).
- **VACUUM** — deltalake 1.6.0 `DeltaTable.vacuum()` defaults to `dry_run=True` (a no-op that only lists files). `dry_run=False` is mandatory (AC-5, adversarial F3 pin).

### What the developer MUST NOT change (preserve exactly)

- **The handoff return contract** of `ingest_raw` and the handoff threading in `fetch_connector` — 5-6 removes them; this story must keep them working (T2.3, T3.3).
- **The source vocabulary** (AD-2): fetch sets `source`, transform never rewrites it; `flex`/`flex_events`/the five T212 paths/`XTB_REPORT` stay exactly. AC-4's suffix stripping happens **before keying**, not by rewriting the stored `source` (verify the mechanism: normalize at fetch vs. strip in the key computation — the spine pins the *key*, not the storage column).
- **Per-endpoint fetch isolation** (AD-5 parent): IBKR's two Flex queries and Trading 212's five endpoints stay separate fetches; per-endpoint `try/except` write-what-succeeded stays; T212's fail-loud `RuntimeError` survives.
- **Silver tables** — never vacuumed (AC-5); schemas/contents untouched.
- **`filter_latest_snapshot` per-source keying** and XTB's per-`account_id` latest — unchanged (5-1's concern, but do not regress).
- **`pipeline/migrations/*`** — do NOT touch.

### Review pins carried into this story

- **Adversarial F1.1 (HIGH)** — NULL retention keys: append + in-batch dedup, never a null-safe merge predicate (which would collapse distinct unparseable accounts onto one row). AC-3.
- **Adversarial F1.2 (MEDIUM)** — batch row order: a run that carries two rows with the same retention key must not let loop order decide the winner. Pin: dedup in-batch on the retention key too, or sort the combined batch by `fetched_at` ascending before one merge. Choose one and test it.
- **Adversarial F3 (HIGH)** — `vacuum()` is a no-op under `dry_run=True`; `dry_run=False` is mandatory. AC-5.
- **Reconcile H1 (HIGH)** — paginated T212 `source` values fragment the merge-on-`source` key; strip the pagination suffix before keying. AC-4.

### Check & workflow commands (run from the repo root)

```bash
# project venv (never system Python); worktree has no .venv — use main repo's
.venv/Scripts/python -m ruff check --fix . && .venv/Scripts/python -m ruff format .
.venv/Scripts/python -m pyright pipeline/ tests/
.venv/Scripts/python -m pytest tests/ -q -rf
```

### Testing standards

- Write real local Delta tables in `tmp_path` (do NOT mock deltalake/polars) — the merge and VACUUM must be exercised against real Delta behavior.
- New behavior (merge semantics, NULL-key append, endpoint-base keying, VACUUM) gets focused tests; existing tests updated only where they assert append-mode writes or the cross-run dedup scan.
- Run all three checks before finishing; re-run tests after linting.

## Dependencies

- **Blocked by:** 5-1 (XTB merge key = `account_id` needs the raw `account_id` column).
- **Blocks:** 5-6 (the bounded table is what makes the handoff removable).
- **Parallel with:** 5-5 (migration — different files).
- **Shared contract:** `run.py` `fetch_connector` return signature (5-4 no longer adds fetch times — its run-aware plumbing was dropped as redundant; 5-6 removes the handoff — keep the current shape intact here).

## Project Structure Notes

- Target structural seed (from the spine):

```text
pipeline/
  raw/
    ingest.py            # write_raw via DeltaTable.merge on the retention key; batch dedup (source, payload_hash);
                         #   cross-run dedup scan removed (AD-1); returns nothing / no in-memory handoff (5-6)
    retention.py         # per-broker retention key + vacuum invocation (AD-1, AD-3) — thin, one source of truth
```

- Naming: raw tables/paths/aliases unchanged (parent AD-1); the retention key is `account_id` for XTB, `source` for Trading 212/IBKR — never a `kind`/`layer` column (Consistency Conventions).
- Merge keys are never encrypted fields (`account_id`, `source`, `fetched_at` stay plaintext columns); the `payload` column stays Fernet-encrypted bytes (ADR 0047).
- No new dependencies; reuse the pinned stack (deltalake 1.6.0 — `DeltaTable.merge`/`vacuum` available).

## References

- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/ARCHITECTURE-SPINE.md` — AD-1 (write-time merge-on-key), AD-3 (VACUUM per run), Consistency Conventions (tests & regression guards), Deferred (physical merge granularity)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/SPEC.md` — CAP-1, CAP-4, Constraints (Delta defaults are not a retention policy)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/retention-and-events-contract.md` — Delta retention facts, Broker policy
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/reviews/review-adversarial.md` — F1.1 (NULL keys), F1.2 (batch order), F3 (VACUUM dry_run)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/reviews/review-reconcile.md` — H1 (paginated T212 sources)
- Code: `pipeline/raw/ingest.py`, `pipeline/run.py` (`fetch_connector`), `pipeline/connectors/trading212/{client,fetch}.py`
- ADRs: `docs/adr/0116` (handoff baseline; no-merge clause superseded by this epic), `0110` (XTB file-arrival task vacuums `raw/xtb`), `0047` (raw stores bytes)

## Dev Agent Record

### Agent Model Used

(To be filled by the implementing subagent.)

### Debug Log References

### Completion Notes List

### File List
