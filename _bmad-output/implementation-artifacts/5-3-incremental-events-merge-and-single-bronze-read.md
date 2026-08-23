# Story 5.3: Incremental events — append-preserving MERGE + single bronze read (AD-4, AD-6)

Status: ready-for-dev

## Story

As a data engineer,
I want the events write to be an append-preserving MERGE on the per-broker event identity, fed by one shared bronze read per broker run,
so that events absent from the current broker response survive and no downstream table re-reads the same bronze payload (CAP-2, CAP-5, issue #155).

**This is the durable-history half of the epic.** Implement per AD-4 and AD-6
of the spine. The current `mode="overwrite"` events write (`run.py:301-306`)
discards every event absent from the current broker response — the Flex
window-loss failure CAP-2 exists to kill.

## Acceptance Criteria

### AC-1 — The events write is a `DeltaTable.merge` on the full per-broker event identity (AD-4)

**Given** a run's normalized event rows (pre-deduped in-batch by `dedup_events`) and the existing `normalized/{broker}_events` table,
**When** the events write runs,
**Then** it is a `DeltaTable.merge(source, predicate)` on the **full per-broker event identity, which equals each broker's in-batch dedup subset**:
  - IBKR: `event_id`
  - Trading 212: `(event_type, event_id)` (ADR 0105 — `event_type` scopes separate ID spaces)
  - XTB: `(event_type, event_id, account_id)`
**And** the predicate uses the broker's full subset — **never `event_id` alone** — so the merge is idempotent and cannot collapse same-ID events across XTB accounts or across T212 order/dividend/transaction ID spaces.

### AC-2 — Update-only-if-newer, insert otherwise, never delete (AD-4)

**Given** an incoming event row whose identity already exists in the table,
**When** the merge runs,
**Then** `when_matched_update(updates=…)` fires **only when the incoming row's `fetched_at` is newer**
**And** `when_not_matched_insert_all()` inserts otherwise
**And** nothing is ever deleted by a merge — an event absent from the current response stays
**And** re-running a fetch converges (idempotent).

### AC-3 — In-batch pre-dedup by `dedup_events` stays (AD-4)

**Given** the current `dedup_events` (`transform_utils.py:390-435`, ADR 0105 latest-wins),
**When** the events write runs,
**Then** the normalized rows are pre-deduped in-batch by `dedup_events` on the broker's identity subset before the merge
**And** `dedup_events` itself is unchanged.

### AC-4 — One bronze read per broker run feeds both normalized outputs (AD-6)

**Given** `transform_connector` (`run.py:239-318`) currently reads `raw/{broker}` once **per layer** inside the loop,
**When** the transform runs,
**Then** `transform_connector` reads `raw/{broker}` **once** after the run's merge write, and the same read/decoded result is routed to both the snapshot and the events transforms
**And** no downstream table independently re-reads or re-parses the bronze payload.

### AC-5 — Regression guards (Consistency Conventions)

**Given** the events merge and single read are complete,
**When** the regression tests run,
**Then** an IBKR event missing from a later Flex response remains in normalized storage; repeated `event_id` values resolve to the latest version; moving the Flex query window does not remove existing events; merging the same batch twice is a no-op; XTB events with the same `event_id` across two accounts stay distinct.

## Tasks / Subtasks

- [ ] T1: `pipeline/run.py` `transform_connector` — single bronze read + events merge write (AC-1, AC-2, AC-4)
  - [ ] T1.1 Hoist the `DeltaTable(raw_path)` read out of the layer loop — read `raw/{broker}` once, route the same table to both transforms (AC-4)
  - [ ] T1.2 Replace the events `write_deltalake(mode="overwrite")` (`run.py:301-306`) with `DeltaTable.merge` on the broker's full event identity (AC-1, AC-2)
  - [ ] T1.3 Keep the snapshot write as-is (snapshot stays a full overwrite — only events become incremental)
  - [ ] T1.4 Keep the empty-raw WARN + skip (ADR 0087 #5) and the `NotImplementedError` handling
- [ ] T2: `pipeline/connectors/transform_utils.py` — verify `dedup_events` stays (no edit expected) (AC-3)
- [ ] T3: Tests (AC-5)
  - [ ] T3.1 IBKR event absent from a later Flex response stays (CAP-2 success)
  - [ ] T3.2 Repeated `event_id` resolves to the latest `fetched_at` version
  - [ ] T3.3 Moving the Flex query window does not remove existing events
  - [ ] T3.4 Same batch merged twice is a no-op
  - [ ] T3.5 XTB same-`event_id` across two accounts stays distinct (full identity subset)
  - [ ] T3.6 Single-read guard: the bronze table is read once per broker run (assert the read count)
- [ ] T4: Full checks: `ruff check --fix . && ruff format .`; `pyright pipeline/ tests/`; `pytest tests/ -q -rf`; tests re-run after lint

## Dev Notes

### Current state (verified 2026-08-22)

- **`transform_connector`** (`pipeline/run.py:239-318`): loops `TRANSFORM_LAYERS = ("snapshot", "events")`; reads `raw_path = get_raw_path(connector.name)` **inside the loop** (line 265) and `dt.to_pyarrow_table()` per layer (line 281); normalized write is `write_deltalake(norm_path, normalized, mode="overwrite")` (lines 301-306). The handoff path (`raw_tables` param, lines 242, 266-281) is used for `handoff_supported` connectors — 5-6 removes it; this story must keep it working.
- **`dedup_events`** (`transform_utils.py:390-435`): sorts `fetched_at` descending, `unique(subset, keep="first")` — ADR 0105 latest-wins. Already correct; do not touch.
- **Event identity today** — IBKR `transform_events` produces `event_id`; Trading 212 produces `(event_type, event_id)`; XTB produces `(event_type, event_id, account_id)` (see `xtb/transform.py:362-369` — D9 dedup subset). The merge predicate must use these exact subsets (AC-1).
- **The events write lives in `transform_connector`** (run.py), NOT `normalize.py` — the spine's Capability map row "CAP-2 lives in `normalize.py` events write" is a typo; AD-4 and the structural seed pin `run.py`'s `transform_connector` per-broker path. `normalize.py` is the consolidated-currency read-modify-write and is unchanged.

### What the developer MUST NOT change (preserve exactly)

- **The snapshot write** — stays a full overwrite (`mode="overwrite"`); only the events write becomes a merge.
- **`dedup_events`** (ADR 0105) — unchanged.
- **Silver schemas** — `events_normalized_schema` untouched; the merge changes the write mechanism, not the schema (AD-7: "Events tables need no change").
- **The handoff path** (`raw_tables` param) — 5-6 removes it; keep it working here.
- **Empty-raw WARN + skip** (ADR 0087 #5) and `NotImplementedError` handling.
- **`filter_latest_snapshot` per-source keying** and XTB's per-`account_id` latest — unchanged.
- **`pipeline/migrations/*`** — do NOT touch.

### Review pins carried into this story

- **Adversarial review F8 (HIGH)** — AD-4's event identity subset must be enumerated per broker and include XTB's `(event_type, event_id, account_id)`. AC-1.
- **Adversarial review (lower-severity)** — `>` vs `>=` on the events `fetched_at` comparison: pin one (strictly-newer update) and test it. AC-2.
- **Reconcile review F8** — XTB event identity under-specification resolved by AC-1's full subset.

### Check & workflow commands (run from the repo root)

```bash
# project venv (never system Python); worktree has no .venv — use main repo's
.venv/Scripts/python -m ruff check --fix . && .venv/Scripts/python -m ruff format .
.venv/Scripts/python -m pyright pipeline/ tests/
.venv/Scripts/python -m pytest tests/ -q -rf
```

### Testing standards

- Write real local Delta tables in `tmp_path` (do NOT mock deltalake/polars) — the merge must be exercised against real Delta behavior.
- New behavior (append-preserving merge, single read) gets focused tests; existing tests updated only where they assert the overwrite-mode events write.
- Run all three checks before finishing; re-run tests after linting.

## Dependencies

- **Blocked by:** none (independent of the raw-schema change — the events write operates on normalized rows).
- **Blocks:** 5-6 (the single read is the handoff's replacement).
- **Parallel with:** 5-1, 5-4.
- **Shared contract:** `run.py` `transform_connector` — 5-6 removes the `raw_tables` handoff param; keep it working here.

## Project Structure Notes

- Target structural seed (from the spine):

```text
pipeline/
  run.py                 # transform_connector: events write becomes DeltaTable.merge on the broker event identity (AD-4);
                         #   single bronze read per broker run (AD-6); per-account staleness lives in quality.py (5-4)
  connectors/
    transform_utils.py   # dedup_events stays (in-batch pre-dedup for AD-4)
```

- Naming: event tables `{broker}_events` unchanged (ADR 0113); the merge key is the broker's event identity subset — never `event_id` alone.
- Merge keys are never encrypted fields (`event_id`, `event_type`, `account_id`, `fetched_at` stay plaintext columns); `cash_amount`/`target_value` stay Fernet-encrypted (ADR 0047).
- No new dependencies; reuse the pinned stack (deltalake 1.6.0 — `DeltaTable.merge` available).

## References

- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/ARCHITECTURE-SPINE.md` — AD-4 (append-preserving MERGEs on event identity), AD-6 (one bronze read per broker run), Consistency Conventions (tests & regression guards), Deferred (physical merge granularity)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/SPEC.md` — CAP-2, CAP-5, Constraints (append-preserved by `event_id`; shared result of one bronze read)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/retention-and-events-contract.md` — Event preservation
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/reviews/review-adversarial.md` — F8 (XTB event identity), lower-severity `>` vs `>=`
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/reviews/review-reconcile.md` — F8 (XTB event identity)
- Code: `pipeline/run.py` (`transform_connector`), `pipeline/connectors/transform_utils.py` (`dedup_events`), `pipeline/connectors/{ibkr,trading212,xtb}/transform.py`
- ADRs: `docs/adr/0105` (events dedup latest-wins), `0113` (events naming final), `0087` (#5 empty-raw WARN + skip)

## Dev Agent Record

### Agent Model Used

(To be filled by the implementing subagent.)

### Debug Log References

### Completion Notes List

### File List
