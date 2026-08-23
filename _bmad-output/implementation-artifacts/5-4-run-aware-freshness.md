# Story 5.4: Stale-account freshness flag + account purge escape hatch

Status: ready-for-dev

## Story

As an analyst,
I want freshness to flag the data that is genuinely stale — an XTB account whose last report is no longer refreshed — while keeping that account's snapshot rows intact,
so that stale data is visible in DQ without breaking events-to-snapshot consistency, and I can deliberately remove an account when it must legitimately disappear (CAP-3, user decision 2026-08-23).

**This story REPLACES the spine's AD-5 "run-aware freshness" mechanism.** Two user edge cases are binding:

1. **Stale report → flag in freshness, never delete.** A closed XTB account's snapshot data must stay: deleting it breaks events-to-snapshot consistency (deposit 10k, trade 5k → net should be 5k + equity; filtering the snapshot shows net 0 while the account is actually net 5k + equity).
2. **All records of an account → explicit purge escape hatch.** A legitimately removed account (e.g., a third-party account that asked to be deleted) needs a manual, deliberate path that removes its data everywhere.

### Why the AD-5 run-aware plumbing is dropped (deviation from the spine)

The 5-2 merge write (AD-1) already fixes issue #157: `when_matched_update` replaces the matched row with the current fetch row, bumping `fetched_at` to the fetch time **even for a byte-identical payload**, and the transforms propagate raw's `fetched_at` to silver (`transform_utils.py iter_raw_payloads` line 154; `xtb/transform.py:113-174` keeps the latest per account). So stored `max(fetched_at)` = last successful fetch with no run-context plumbing. The run-aware `fetch_times` threading AD-5 proposed is redundant.

What AD-5 missed — and the user surfaced — is the **per-account** stale signal: a closed XTB account's row is never rewritten (AD-1 "untouched" rule), so its silver snapshot rows carry an old `fetched_at` forever while the global `max(fetched_at)` is masked by the fresh account. That signal is what this story adds — statically, with no run context, directly implementing the user's "read the oldest data" instinct against the in-memory silver table (no raw read needed).

## Acceptance Criteria

### AC-1 — Stale accounts are flagged per-account; fresh accounts are not (edge case 1)

**Given** the DQ pass runs on `xtb_snapshot` (any snapshot table registered with an account key column),
**When** the table holds rows from a stale account (latest `fetched_at` older than `freshness_days`) alongside rows from a fresh account,
**Then** the DQ pass emits a WARN that names the stale account_id(s) and their age
**And** the existing table-level freshness check (global `max(fetched_at)`) still PASSes because the fresh account's rows are within the window
**And** no data is deleted or rewritten — flagging is read-only.

### AC-2 — Static registry; no run context; standalone DQ unchanged (ADR 0072)

**Given** the per-account staleness check,
**When** the DQ pass runs,
**Then** the check keys on a static registry (`ACCOUNT_STALENESS_KEYS: {"xtb_snapshot": "account_id"}`) — no per-run fetch timestamps, no `fetch_times` parameter, no new metadata table
**And** a snapshot table with no registered key column is not affected (not applicable)
**And** an empty table or a missing key column is PASS — freshness is not applicable (ADR 0072 empty-table behavior preserved)
**And** standalone DQ invocation (`cmd_validate`, `cmd_run_connector` line 762, `cmd_run_consolidate_analytics` lines 793/809) behaves identically — nothing is in-memory from the run.

### AC-3 — Byte-identical re-fetch regression (issue #157) is guaranteed by 5-2

**Given** a broker fetched successfully at time `T` with a payload byte-identical to a previous fetch,
**When** the DQ pass runs after 5-2,
**Then** the broker's snapshot rows carry `fetched_at = T` (5-2's `when_matched_update` replaced the row), so both the table-level max and the per-account staleness check PASS with no stale warning — this story adds no additional mechanism for issue #157.

### AC-4 — Purge escape hatch removes all records of one account (edge case 2)

**Given** a broker account that must be legitimately removed (e.g., a third-party account deletion request),
**When** the operator runs the purge command,
**Then** the command removes the account's records from all broker-scoped tables: `raw/{broker}` rows on the broker retention key, `{broker}_snapshot` rows where `account_id` matches, and `{broker}_events` rows where `account_id` matches
**And** gold tables are NOT directly deleted — `consolidated_holdings`/`events` rows are broker+ticker aggregates with no per-account key (`REQUIRED_FIELDS` has no `account_id` for gold), so the account's contribution disappears on the next consolidate run
**And** the command requires explicit confirmation (`--yes` flag; without it, it prints the affected tables and predicates and exits without deleting)
**And** records of other accounts and other brokers are untouched.

### AC-5 — Purge scope constraints

**Given** the purge command,
**When** it targets an account,
**Then** raw rows with a NULL retention key (unparseable XTB filename, AD-1 append path) are NOT matched by the `account_id` predicate - but the XTB transform's fallback **parses** those rows to recover the account (`xtb/transform.py:125-141`), so a NULL-keyed row whose payload parses to the purged account would re-materialize it on the next XTB transform; the purge therefore WARNs (with a count) when NULL-keyed raw rows remain for the target broker instead of claiming complete removal (fully removing those residuals requires decrypt+parse of raw payloads - out of v1 scope) — documented: such rows are absent from silver anyway (skipped with a warning in `xtb/transform.py:127-141`)
**And** `purge-account` for a broker whose snapshot/raw key is not a per-account identity (Trading 212 snapshot `account_id` is `""`, retention key is `source`) raises a `RuntimeError` explaining the unsupported combination - XTB is the v1 scope; this branch is covered by a test (T3.4)
**And** the event history of OTHER accounts is untouched (purge is not a stale-cleanup tool; staleness is handled by AC-1).

### AC-6 — Regression guards (Consistency Conventions)

**Given** the per-account staleness check and the purge command,
**When** the regression tests run,
**Then** a stale account is flagged while a fresh account in the same table is not; a byte-identical re-fetch does not add a row or warn stale; purging one account leaves other accounts and other brokers untouched; purged account disappears from gold after the next consolidate; the existing `data_quality` output table schema/behavior is unchanged (the new check appends rows like any other check).

## Tasks / Subtasks

- [ ] T1: `pipeline/analytics/quality.py` — per-account staleness check (AC-1, AC-2)
  - [ ] T1.1 Add registry `ACCOUNT_STALENESS_KEYS: dict[str, str] = {"xtb_snapshot": "account_id"}` near `FRESHNESS_COLUMNS`
  - [ ] T1.2 New `check_account_freshness(table_name, arrow_table, key_column, freshness_days) -> CheckResult`: group by the key column, compute `max(fetched_at)` per key, WARN listing keys older than the window with their age; empty table / missing key column → PASS
  - [ ] T1.3 Call it in `run_validation` right after the table-level `check_freshness` for registered tables (reuse the already-loaded `arrow_table` — no extra read); metadata `(table_name, "account_freshness")`
- [ ] T2: `pipeline/run.py` — purge escape hatch (AC-4, AC-5)
  - [ ] T2.1 New `cmd_purge_account(args)` — broker + account_id + `--yes`; resolves `raw/{broker}` (reuse `get_raw_path`, line 434) and normalized snapshot/events paths; `DeltaTable.delete(predicate)` per table; logs each predicate and affected count (verify what the pinned deltalake `delete()` returns at implementation); after deleting, WARN with a count when NULL-keyed raw rows remain for the target broker (AC-5 residue)
  - [ ] T2.2 Wire `purge-account` subparser in `main()` (`subparsers.add_parser("purge-account", ...)`)
  - [ ] T2.3 Without `--yes`: print what would be deleted, exit 0. With `--yes`: delete raw rows on the retention key, snapshot rows on `account_id`, events rows on `account_id`; RuntimeError for unsupported broker (v1: XTB only)
- [ ] T3: Tests (AC-6) — real local Delta tables in `tmp_path`, no mocking of deltalake/polars
  - [ ] T3.1 Stale + fresh account in one snapshot → per-account WARN lists the stale account; table-level freshness PASSes
  - [ ] T3.2 All accounts fresh → PASS; empty table → PASS; T212/IBKR snapshot (unregistered) → not checked
  - [ ] T3.3 Byte-identical re-fetch regression: after a merge write updates `fetched_at`, no stale warning (issue #157)
  - [ ] T3.4 Purge removes raw + snapshot + events rows for the target account only; other accounts/brokers untouched; NULL-keyed raw row untouched (residual WARN per AC-5); `purge-account trading212 <id>` raises `RuntimeError`
  - [ ] T3.5 Purge without `--yes` deletes nothing
- [ ] T4: Full checks: `ruff check --fix . && ruff format .`; `pyright pipeline/ tests/`; `pytest tests/ -q -rf`; tests re-run after lint
- [ ] T5: Record ADR via the manage-adr skill (freshness = per-account staleness flag + purge escape hatch; supersedes ADR 0116 freshness framing if cited) — do not hand-write the ADR during planning

## Dev Notes

### Current state (verified 2026-08-23)

- **`check_freshness`** (`pipeline/analytics/quality.py:310-362`): global `max(fetched_at)` vs `now - freshness_days`; empty table → PASS; all-null → WARN. **This is the stale-hiding path:** a closed account's old rows are masked by the fresh account's max.
- **Shared age helper (implementation note):** `check_freshness` normalizes timezone and computes age at `quality.py:339-358`; `check_account_freshness` needs the same block - extract a small `_age_days(max_ts)` helper used by both so the ISO/UTC handling can't drift.
- **`run_validation`** (`quality.py:469-682`): loads each table once as `arrow_table` (line 582) and runs schema → required_nulls → row_count_stability → freshness → non_empty → reconciliation. The per-account check slots in next to freshness on the already-loaded table.
- **Snapshot schema** (`pipeline/normalized/models.py:33-45`): `account_id` (string) is a real column in ALL broker snapshot tables. XTB populates it from the report (`xtb/transform.py:188-218`); **Trading 212 writes `""`** (`trading212/transform.py:98,114`) — so per-account staleness is meaningful only where the account key is real (XTB).
- **XTB raw retention key** = `account_id` (AD-2/5-1); snapshot keeps the latest report per account (`xtb/transform.py:113-174`), so a closed account's silver rows keep the old `fetched_at`.
- **CLI** (`run.py:853-1047`): argparse subparsers; `cmd_run_connector` (718-768) calls `run_validation(tables=[f"{name}_snapshot", f"{name}_events"], connectors=[name])` at line 762 **without `fail_on_warn`** — a stale-account WARN does not fail the run by default (consistent with existing freshness WARN behavior). `get_raw_path` at line 434.
- **Gold tables** have no per-account key (`consolidated_holdings` `REQUIRED_FIELDS`: `fetched_at, broker, ticker, target_ccy, target_value`) — gold self-heals on the next consolidate after a purge; do NOT delete gold rows by predicate.

### What the developer MUST NOT change (preserve exactly)

- **The merge/events writes** (5-2 AC-1/AC-2, 5-3 AC-1/AC-2) and the 5-3 single bronze read (AD-6) — untouched by this story.
- **`FRESHNESS_COLUMNS`** and the existing table-level freshness semantics (ADR 0072 standalone behavior).
- **`data_quality` output table** — schema, append/overwrite behavior, print summary unchanged (new checks append rows like any other check).
- **The handoff return contract** of `fetch_connector` — this story does NOT touch the return signature (5-6 removes it).
- **The source vocabulary** and `SELECT DISTINCT source` — unchanged.
- **`pipeline/migrations/*`** — do NOT touch.
- **Snapshot/events schemas** — no new columns; the staleness flag lives in DQ results, not in silver.

### Review pins carried into this story

- **User edge case 1 (binding)**: stale report → flag in freshness, never delete the snapshot — events-to-snapshot consistency (deposit 10k, trade 5k → net 5k + equity; filtering the snapshot shows net 0). AC-1.
- **User edge case 2 (binding)**: all account records for a broker need a legitimate deletion escape hatch. AC-4.
- **AD-5 dropped**: run-aware freshness plumbing is redundant (5-2 merge already fixes #157). Deviation from the spine recorded in the memlog; the spine itself is owned by bmad-architecture and NOT edited here.

### Check & workflow commands (run from the repo root)

```bash
# project venv (never system Python); worktree has no .venv — use main repo's
.venv/Scripts/python -m ruff check --fix . && .venv/Scripts/python -m ruff format .
.venv/Scripts/python -m pyright pipeline/ tests/
.venv/Scripts/python -m pytest tests/ -q -rf
```

### Testing standards

- Write real local Delta tables in `tmp_path` (do NOT mock deltalake/polars) — staleness grouping and purge predicates must run against real Delta behavior.
- New behavior (per-account staleness, purge) gets focused tests; existing tests updated only where they assert something this story changes.
- Run all three checks before finishing; re-run tests after linting.

## Dependencies

- **Blocked by:** 5-2 (the merge write is what makes `fetched_at` reflect the current fetch — the stale-account signal is only meaningful once fresh keys get rewritten).
- **Parallel with:** 5-3 (different functions in `run.py` — `transform_connector` vs `main()`/new command; minor overlap risk, no file conflicts).
- **Shared contract:** `run.py` `fetch_connector` return signature — this story does NOT change it (5-4 previously added fetch times; that change is dropped; 5-6 removes the handoff).

## Project Structure Notes

- Target structural seed:

```text
pipeline/
  analytics/quality.py   # ACCOUNT_STALENESS_KEYS registry + check_account_freshness (per-account staleness, ADR-0072 preserved)
  run.py                 # cmd_purge_account + purge-account subcommand (AC-4)
```

- Naming: the account key column in silver is `account_id` — never invent a `kind`/`layer` column (Consistency Conventions). The registry holds table→key only for tables with a REAL per-account key (XTB).
- The staleness signal is **not** a raw read — the snapshot table is already in memory in `run_validation`. No new dependencies; reuse the pinned stack (deltalake `DeltaTable.delete`).

## References

- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/ARCHITECTURE-SPINE.md` — AD-1 (merge write; the actual fix for `#157`), AD-3 (VACUUM), AD-4 (events), AD-5 (this story deviates: run-aware plumbing dropped), AD-6 (single read), Deferred (no second observability table)
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/SPEC.md` — CAP-3 (current-fetch freshness), Constraints ("Delta's defaults are not an application retention policy"), Success signal
- `_bmad-output/specs/spec-bronze-retention-and-incremental-events/.memlog.md` — decision entry for this deviation (added 2026-08-23)
- Code: `pipeline/analytics/quality.py` (310-362, 469-682), `pipeline/run.py` (434, 718-768, 853-1047), `pipeline/normalized/models.py` (33-53), `pipeline/connectors/xtb/transform.py` (113-174), `pipeline/connectors/trading212/transform.py` (67-114)
- ADRs: `docs/adr/0072` (standalone freshness), `docs/adr/0105` (T212 dedup), `docs/adr/0108` (XTB overhaul), `docs/adr/0116` (handoff baseline; its freshness assumptions are superseded per this story + 5-2)

## Dev Agent Record

### Agent Model Used

(To be filled by the implementing subagent.)

### Debug Log References

### Completion Notes List

### File List
