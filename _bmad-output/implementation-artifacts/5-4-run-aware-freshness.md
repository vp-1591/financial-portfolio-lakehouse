# Story 5.4: Run-aware freshness — the run stores "it's fresh" (AD-5)

Status: ready-for-dev

## Story

As an analyst,
I want freshness to reflect a successful current fetch even when the payload is byte-identical,
so that a repeated identical fetch does not warn stale solely because its payload hash was previously stored (CAP-3, issue #157).

**This is the freshness half of the epic.** Implement per AD-5 of the spine.
Today freshness = `max(fetched_at)` of stored rows, which for a byte-identical
re-fetch stays at the *first* stored payload's timestamp and warns stale
although the run succeeded.

## Acceptance Criteria

### AC-1 — The DQ freshness pass accepts per-broker last-successful-fetch timestamps (AD-5)

**Given** `run_validation` (`analytics/quality.py:469-682`) and `check_freshness` (`analytics/quality.py:310-362`),
**When** the pipeline runs the DQ pass,
**Then** the pipeline passes per-broker last-successful-fetch timestamps into the freshness pass
**And** a table whose broker was fetched successfully within the freshness window **passes regardless of whether any payload changed** (CAP-3).

### AC-2 — Table→broker mapping (AD-5)

**Given** the freshness override timestamps,
**When** a table's freshness is checked,
**Then** each `{broker}_snapshot` / `{broker}_events` table maps to its single broker
**And** the multi-broker consolidated tables (`events`, `consolidated_holdings`) map to the **max** last-successful-fetch over the brokers in that run.

### AC-3 — Fallback to table `max(fetched_at)` when the run context is absent (AD-5, ADR 0072)

**Given** DQ invoked standalone (no fetch times in memory — the "no new metadata table" rule means nothing persists),
**When** a table's freshness is checked,
**Then** the existing table `max(fetched_at)` behavior is used unchanged (ADR 0072).

### AC-4 — Regression: a byte-identical re-fetch does not warn stale (CAP-3, issue #157)

**Given** a broker fetched successfully at `T` with a payload byte-identical to a previous fetch,
**When** the DQ pass runs with the run's fetch times,
**Then** the broker's tables pass freshness (no stale warning) even though the stored `max(fetched_at)` predates the window.

## Tasks / Subtasks

- [ ] T1: `pipeline/analytics/quality.py` — run-aware freshness (AC-1, AC-2, AC-3)
  - [ ] T1.1 `run_validation` gains a per-broker fetch-times parameter (e.g. `fetch_times: dict[str, datetime] | None = None`)
  - [ ] T1.2 `check_freshness` (or the runner) consults the override: a table whose broker was fetched within the window passes; multi-broker tables use the max over the run's brokers (AC-2)
  - [ ] T1.3 When the run context is absent, fall back to the existing table `max(fetched_at)` behavior unchanged (AC-3)
- [ ] T2: `pipeline/run.py` — pass the run's fetch times to the DQ pass (AC-1)
  - [ ] T2.1 `fetch_connector` records per-broker last-successful-fetch timestamps (a successful fetch at `T` — the run's fetch result drives the freshness pass)
  - [ ] T2.2 `cmd_run_connector` passes the connector's fetch time to its `run_validation` call (line 762)
  - [ ] T2.3 `cmd_run_consolidate_analytics` passes the run's broker fetch times to its `run_validation` calls (lines 793, 809)
  - [ ] T2.4 **Keep the handoff return contract intact** — 5-6 removes it; the fetch times are an addition, not a replacement
- [ ] T3: Tests (AC-4)
  - [ ] T3.1 Byte-identical re-fetch within the window passes freshness (issue #157 regression)
  - [ ] T3.2 Multi-broker `events`/`consolidated_holdings` use the max fetch time over the run's brokers
  - [ ] T3.3 Standalone DQ (no run context) falls back to table `max(fetched_at)` (ADR 0072)
  - [ ] T3.4 A broker NOT fetched in the run still warns stale when its stored data is old
- [ ] T4: Full checks: `ruff check --fix . && ruff format .`; `pyright pipeline/ tests/`; `pytest tests/ -q -rf`; tests re-run after lint

## Dev Notes

### Current state (verified 2026-08-22)

- **`check_freshness`** (`pipeline/analytics/quality.py:310-362`): computes `max(fetched_at)` from the table, compares against `now - freshness_days`. Empty table → PASS; all-null column → WARN.
- **`run_validation`** (`analytics/quality.py:469-682`): signature `(fernet_key, freshness_days, fail_on_warn, tables, connectors)`. Calls `check_freshness` per table (lines 620-627). `FRESHNESS_COLUMNS` (lines 83-98) maps each table to its freshness column.
- **Call sites** — `cmd_run_connector` (`run.py:762`): `run_validation(fernet_key, tables=[f"{name}_snapshot", f"{name}_events"], connectors=[name])`. `cmd_run_consolidate_analytics` (`run.py:793, 809`): silver tables then gold tables.
- **`fetch_connector`** (`run.py:123-236`): returns `(FetchResult, handoff)`. The fetch times are a new addition to this return (or a separate mechanism) — coordinate with 5-2 (merge write) and 5-6 (handoff removal) via the shared contract.

### What the developer MUST NOT change (preserve exactly)

- **The "no new metadata table" rule** — run-aware freshness deliberately avoids adding an observability/metadata table (spine Deferred). The fetch times live in the run's memory only.
- **`FRESHNESS_COLUMNS`** and the existing freshness semantics for standalone DQ (ADR 0072).
- **The handoff return contract** of `fetch_connector` — 5-6 removes it; keep it working here.
- **`data_quality` output table** — schema and append/overwrite behavior unchanged.
- **`pipeline/migrations/*`** — do NOT touch.

### Review pins carried into this story

- **Adversarial review F5 (MEDIUM)** — the table→broker mapping and what "within the window" means for multi-broker tables is unpinned. AC-2 pins it: single-broker tables map to their broker; `events`/`consolidated_holdings` map to the max over the run's brokers.
- **Reconcile review** — AD-5's fallback to ADR 0072 when DQ runs standalone (the consolidated validation runs in its own Fargate task with no fetch times in memory).

### Check & workflow commands (run from the repo root)

```bash
# project venv (never system Python); worktree has no .venv — use main repo's
.venv/Scripts/python -m ruff check --fix . && .venv/Scripts/python -m ruff format .
.venv/Scripts/python -m pyright pipeline/ tests/
.venv/Scripts/python -m pytest tests/ -q -rf
```

### Testing standards

- Focused tests on `check_freshness`/`run_validation` with and without the run context; a regression test for issue #157 (byte-identical re-fetch does not warn stale).
- Run all three checks before finishing; re-run tests after linting.

## Dependencies

- **Blocked by:** none.
- **Parallel with:** 5-1, 5-3.
- **Shared contract:** `run.py` `fetch_connector` return signature — this story adds fetch times; 5-6 removes the handoff; the orchestrator resolves the final shape.

## Project Structure Notes

- Target structural seed (from the spine):

```text
pipeline/
  analytics/quality.py   # run-aware freshness: per-broker last-successful-fetch override + table fallback (AD-5)
  run.py                 # passes run fetch times to freshness (AD-5)
```

- No new metadata table; no new dependencies; reuse the pinned stack.

## References

- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/ARCHITECTURE-SPINE.md` — AD-5 (run-aware freshness), Deferred (no second observability/metadata table)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/SPEC.md` — CAP-3, Constraints
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/retention-and-events-contract.md` — Issue mapping (#157)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/reviews/review-adversarial.md` — F5 (freshness table→broker mapping)
- Code: `pipeline/analytics/quality.py`, `pipeline/run.py` (`fetch_connector`, `cmd_run_connector`, `cmd_run_consolidate_analytics`)
- ADRs: `docs/adr/0072` (empty-table freshness behavior)

## Dev Agent Record

### Agent Model Used

(To be filled by the implementing subagent.)

### Debug Log References

### Completion Notes List

### File List
