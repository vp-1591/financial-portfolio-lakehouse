# Epic 5 Orchestration Plan — Bounded Bronze Retention and Incremental Events

Status: ready-for-dev

## Purpose

Split `_bmad-output/specs/spec-bronze-retention-and-incremental-events/ARCHITECTURE-SPINE.md`
(AD-1..AD-8) into six stories that can be delegated to **parallel/sequential
orchestrated subagents**, preventing context bloat of a single dev agent. All
six converge in **ONE PR** (spec Success signal: "One implementation PR
demonstrates…").

The spine is the authoritative build contract; each story cites the ADs it
implements and the review pins it must honor. A subagent implements its story
without reading the whole spine — the story file carries the binding rules.

## Story → AD map

| Story | ADs | Primary files | Phase |
| --- | --- | --- | --- |
| 5-1 raw-schema-account-id-evolution | AD-2 | `raw/models.py`, `connectors/xtb/{fetch,transform}.py`, `connectors/transform_utils.py` | 1 (parallel) |
| 5-2 write-time-merge-retention-and-vacuum | AD-1, AD-3 | `raw/ingest.py`, `raw/retention.py` (new), `run.py` fetch_connector | 2 (parallel) |
| 5-3 incremental-events-merge-and-single-bronze-read | AD-4, AD-6 | `run.py` transform_connector | 1 (parallel) |
| 5-4 run-aware-freshness | AD-5 | `analytics/quality.py`, `run.py` validation call sites | 1 (parallel) |
| 5-5 raw-schema-migration | AD-7 | `migrations/migrate_raw_account_id.py` (new) | 2 (parallel) |
| 5-6 t212-handoff-removal-measurement | AD-8 | `connectors/base.py`, `connectors/trading212/connector.py`, `run.py` handoff threading, `raw/ingest.py` return | 3 (sequential) |

## Dependency graph

```text
5-1 (schema) ──► 5-2 (XTB merge key = account_id)
5-1 (schema) ──► 5-5 (migration backfills account_id from source_file)
5-2 (bounded table) ──► 5-6 (handoff removable only once the table is bounded)
5-3 (single read) ──► 5-6 (single read is the handoff's replacement)
5-1, 5-3, 5-4 are mutually independent
```

## Phases

- **Phase 1 — parallel (3 agents):** 5-1, 5-3, 5-4. File-disjoint except
  `run.py` (5-3 edits `transform_connector`, 5-4 edits the validation call
  sites — different functions; separate worktrees merge cleanly).
- **Phase 2 — parallel (2 agents):** 5-2, 5-5. Both blocked by 5-1; mutually
  independent (ingest/retention/run vs migrations).
- **Phase 3 — sequential (1 agent):** 5-6. Blocked by 5-2 + 5-3.
- **Phase 4 — orchestrator:** full check suite, docs sweep, ADR via
  `manage-adr`, one PR.

## Shared-file contract points

These are the seams where two stories touch the same symbol. Each story keeps
the contract it does not own intact; the orchestrator resolves the final shape
at merge.

- **`run.py` `fetch_connector` return signature** — 5-2 (merge write inside),
  5-4 (add per-broker fetch times), 5-6 (remove the handoff dict). 5-4 and 5-6
  both change the return; 5-6 lands last and defines the final signature.
- **`raw/ingest.py`** — 5-2 rewrites the write to a MERGE but **keeps the
  pre-dedup encrypted-table return** (the handoff contract); 5-6 removes the
  return. 5-2 must not break `tests/test_transform_connector_handoff.py`; 5-6
  rewrites that test.
- **`connectors/transform_utils.py`** — 5-1 removes `source_file` from
  `DecodedRow`/`iter_raw_payloads`; 5-3 only verifies `dedup_events` stays
  (no edit). No conflict.

## Worktree guidance

Each story runs in its own worktree under `.claude/worktrees/`
(`EnterWorktree`). Worktrees have **no `.venv`** — use the main repo's
`.venv\Scripts\python.exe` (with `--pythonpath` for pyright). All branches
converge into `feat/bronze-retention-incremental-events`, one PR.

## Final orchestrator checklist

1. Full check suite: `ruff check --fix . && ruff format .`, then
   `pyright pipeline/ tests/`, then `pytest tests/ -q -rf` (tests re-run after
   lint).
2. Grep bar: no `source_file` references in `pipeline/` outside carve-outs
   (the migration script + its test, `docs/adr/`).
3. Docs: `docs/table-lineage.md`, `docs/architecture.md`, broker docs updated
   for the new `RAW_SCHEMA` and the retention/events behavior.
4. ADR via `manage-adr` (do not hand-write) recording the bounded-bronze +
   incremental-events decision, superseding ADR 0116's no-merge clause and
   ADR 0047's `account_id` exclusion.
5. One PR; merge only after all checks green and the migration (5-5) applied
   per environment with counts verified.
