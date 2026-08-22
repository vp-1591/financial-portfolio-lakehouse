# Story 5.6: Trading 212 handoff removed as a measured experiment (AD-8)

Status: ready-for-dev

## Story

As a data engineer,
I want the Trading 212 in-memory encrypted-fetch handoff removed and its removal measured against the ADR 0116 baseline,
so that the single bronze read (AD-6) is the only path and any material regression is caught and rolled back (CAP-5).

**This is the optimization half of the epic.** Implement per AD-8 of the spine.
The handoff was introduced to avoid Trading 212 re-reading accumulated raw
history; bounded retention (5-2) and the single shared bronze read (5-3) remove
the original unbounded-table-read pressure, so the handoff's complexity is no
longer justified — unless removal measurably regresses.

## Acceptance Criteria

### AC-1 — The handoff capability and threading are removed (AD-8)

**Given** `handoff_supported` (`connectors/base.py:26`), the Trading 212 declaration (`connectors/trading212/connector.py:31`), the handoff dict building in `fetch_connector` (`run.py:157-159, 175-182, 216-221`), the `raw_tables` param in `transform_connector` (`run.py:242, 266-281`), the threading in `cmd_run_connector` (`run.py:736-752`), and the pre-dedup return of `ingest_raw` (`raw/ingest.py:73-126`),
**When** the handoff is removed,
**Then** `handoff_supported` is gone from the protocol and the connector
**And** `fetch_connector` no longer builds a handoff dict
**And** `transform_connector` no longer accepts `raw_tables` — the single bronze read (AD-6) is the only path
**And** `ingest_raw` no longer returns the pre-dedup encrypted table
**And** `cmd_run_connector` no longer threads the handoff.

### AC-2 — The single bronze read guarantee stays intact (AD-8, handoff-decision-matrix)

**Given** the handoff removal,
**When** the transform runs,
**Then** the one-shared-bronze-read guarantee (AD-6) is preserved — removing the handoff must not reintroduce independent reads for the snapshot and event outputs.

### AC-3 — The removal is measured against the ADR 0116 baseline (AD-8)

**Given** the existing handoff memory and runtime measurements (ADR 0116 — the 1039 MB transform-peak analysis),
**When** a representative Trading 212 run is measured without the handoff,
**Then** the memory peak and runtime are compared against the baseline
**And** if removal causes a **material** regression, the handoff is restored
**And** either outcome keeps the one-shared-bronze-read guarantee (AD-6) intact.

### AC-4 — Handoff tests removed/rewritten; protocol/registry tests updated

**Given** `tests/test_transform_connector_handoff.py` (the handoff contract tests) and the protocol/registry assertions,
**When** the handoff is removed,
**Then** the handoff test file is removed or rewritten to assert the post-removal contract
**And** `tests/test_connector_protocol.py` and `tests/test_connector_registry.py` no longer reference `handoff_supported`
**And** the full suite passes.

## Tasks / Subtasks

- [ ] T1: `pipeline/connectors/base.py` — remove `handoff_supported` (AC-1)
- [ ] T2: `pipeline/connectors/trading212/connector.py` — remove `handoff_supported = True` and the ADR 0116 comment (AC-1)
- [ ] T3: `pipeline/run.py` — remove the handoff threading (AC-1)
  - [ ] T3.1 `fetch_connector`: drop the handoff dict (lines 157-159, 175-182, 216-221); return `(FetchResult, fetch_times)` per the 5-4 contract
  - [ ] T3.2 `transform_connector`: drop the `raw_tables` param and the handoff branch (lines 242, 266-281); the single bronze read is the only path (AC-2)
  - [ ] T3.3 `cmd_run_connector`: drop the handoff threading (lines 736-752)
- [ ] T4: `pipeline/raw/ingest.py` — `ingest_raw` no longer returns the pre-dedup encrypted table (AC-1)
- [ ] T5: Tests (AC-4)
  - [ ] T5.1 Remove or rewrite `tests/test_transform_connector_handoff.py` for the post-removal contract
  - [ ] T5.2 Update `tests/test_connector_protocol.py`, `tests/test_connector_registry.py` — no `handoff_supported` references
  - [ ] T5.3 Regression: T212 transform output is identical with the table-read path (golden vs the pre-removal handoff output)
- [ ] T6: Measurement (AC-3)
  - [ ] T6.1 Run a representative Trading 212 run without the handoff; record memory peak + runtime
  - [ ] T6.2 Compare against the ADR 0116 baseline; document the result and the material-regression decision
- [ ] T7: Full checks: `ruff check --fix . && ruff format .`; `pyright pipeline/ tests/`; `pytest tests/ -q -rf`; tests re-run after lint

## Dev Notes

### Current state (verified 2026-08-22)

- **`handoff_supported`** (`pipeline/connectors/base.py:26`): protocol attribute, default `False`; Trading 212 declares `True` (`connectors/trading212/connector.py:31`) with the ADR 0116 comment.
- **`fetch_connector`** (`pipeline/run.py:123-236`): builds `handoff: dict[str, pa.Table] | None = {} if getattr(connector, "handoff_supported", False) else None` (lines 157-159); concatenates per-batch encrypted tables into `handoff[SNAPSHOT_LAYER]` (lines 175-182) and `handoff[EVENTS_LAYER]` (lines 216-221). Returns `(FetchResult, handoff)`.
- **`transform_connector`** (`pipeline/run.py:239-318`): `raw_tables` param (line 242); a layer present in the handoff is used directly, otherwise falls back to the Delta read (lines 266-281).
- **`cmd_run_connector`** (`pipeline/run.py:718-768`): `rc, handoff = fetch_connector(...)` (line 736); `transform_connector(connector, fernet_key, raw_tables=handoff)` (line 747); `del handoff` after transform (line 752).
- **`ingest_raw`** (`pipeline/raw/ingest.py:73-126`): returns the Fernet-encrypted **pre-dedup** table (lines 80-88, 114, 126) so the handoff reaches the transform even when an unchanged endpoint deduped out of the write.
- **ADR 0116 baseline** — the handoff's memory/runtime measurements (1039 MB transform-peak analysis) are the baseline for AD-8's measurement. The handoff capability design and the encrypted-pre-dedup contract remain the baseline; only the append-only/no-merge clause is superseded by this epic.

### What the developer MUST NOT change (preserve exactly)

- **The single bronze read (AD-6)** — removing the handoff must not reintroduce independent reads for snapshot and events (AC-2, handoff-decision-matrix decision rule).
- **The `fetch_connector` fetch-times return** (5-4's contract) — this story defines the final return shape: `(FetchResult, fetch_times)`.
- **Trading 212's fail-loud all-events-endpoints-empty `RuntimeError`** (`trading212/fetch.py:127-136`) — survives unchanged.
- **The source vocabulary, silver schemas, `dedup_events`, `filter_latest_snapshot`** — unchanged.
- **`pipeline/migrations/*`** — do NOT touch.

### Review pins carried into this story

- **Reconcile review** — the handoff removal is a measured experiment; the decision rule (from `handoff-decision-matrix.md`): keep the removal if memory peak and runtime remain within the agreed budget; restore the handoff if either regresses materially. Either choice must preserve the one-shared-bronze-read capability.

### Check & workflow commands (run from the repo root)

```bash
# project venv (never system Python); worktree has no .venv — use main repo's
.venv/Scripts/python -m ruff check --fix . && .venv/Scripts/python -m ruff format .
.venv/Scripts/python -m pyright pipeline/ tests/
.venv/Scripts/python -m pytest tests/ -q -rf
```

### Testing standards

- The golden test (T5.3) proves the table-read path produces output identical to the pre-removal handoff path.
- Run all three checks before finishing; re-run tests after linting.

## Dependencies

- **Blocked by:** 5-2 (the bounded table is what makes the handoff removable) + 5-3 (the single read is the handoff's replacement).
- **Shared contract:** `run.py` `fetch_connector` return signature — this story defines the final shape `(FetchResult, fetch_times)` after 5-4 added the fetch times.

## Project Structure Notes

- Target structural seed (from the spine):

```text
pipeline/
  connectors/
    trading212/         # handoff_supported removed, memory baseline measured (AD-8)
    base.py             # BrokerConnector: no raw-layer override, no per-name branches (parent AD-6)
  raw/
    ingest.py           # returns nothing / no in-memory handoff (AD-1, AD-8)
```

- No new dependencies; reuse the pinned stack.

## References

- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/ARCHITECTURE-SPINE.md` — AD-8 (T212 handoff removed as a measured experiment), AD-6 (single bronze read), Conflicts surfaced (ADR 0116's handoff capability design remains the baseline)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/SPEC.md` — Constraints (handoff removed for a measurement experiment), Success signal
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/handoff-decision-matrix.md` — Decision rule (measure, restore on material regression)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/retention-and-events-contract.md` — Handoff assessment
- Code: `pipeline/connectors/base.py`, `pipeline/connectors/trading212/connector.py`, `pipeline/run.py`, `pipeline/raw/ingest.py`, `tests/test_transform_connector_handoff.py`
- ADRs: `docs/adr/0116` (in-memory encrypted-fetch handoff — baseline; no-merge clause superseded by this epic)

## Dev Agent Record

### Agent Model Used

(To be filled by the implementing subagent.)

### Debug Log References

### Completion Notes List

### File List
