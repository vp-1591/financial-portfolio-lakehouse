# Web-Reality Review — ARCHITECTURE-SPINE.md (bounded-bronze-incremental-events)

Reviewer scope: every committed decision naming a technology or platform fact, verified against
the web (delta-rs / Delta Lake docs) and the local repo. Venv: `.venv/Scripts/python.exe`
(deltalake 1.6.0). Date: 2026-08-22.

## Verdict

**PASS with three low-severity corrections.** All committed platform and repo claims verified
accurate. No high- or medium-severity contradictions found. Three items an implementer will trip
on: the vacuum dry-run default, snake_case merge-clause names, and two imprecise source
references.

## Verification results by claim

### 1. deltalake 1.6.0 exposes DeltaTable.vacuum / merge / delete — VERIFIED

Confirmed from the venv: `deltalake.__version__ == 1.6.0`; all three methods exist with docstrings
(`DeltaTable.vacuum.__doc__`, `DeltaTable.merge.__doc__`, `DeltaTable.delete.__doc__` all present).

### 2. Vacuum default retention semantics, no override needed — VERIFIED

- venv docstring: `retention_hours` — "if none then the value from `delta.deletedFileRetentionDuration`
  is used or default of 1 week otherwise"; `enforce_retention_duration` — "when disabled, accepts
  retention hours smaller than the value from `delta.deletedFileRetentionDuration`".
- Delta docs (`https://docs.delta.io/table-properties/`): `delta.deletedFileRetentionDuration`
  default = `interval 1 week`. So with `retention_hours` omitted and `enforce_retention_duration=True`
  (defaults), the 7-day tombstone threshold holds and no low-retention override is needed. AD-3's
  claim is accurate.

**Caveat (finding 1):** the same docstrings and delta-rs docs state `vacuum(dry_run=True)` is the
default — a literal `DeltaTable.vacuum()` invocation lists files and deletes nothing. AD-3's rule
text names only `retention_hours`/`enforce_retention_duration` as the parameters, so an implementer
following it literally ships a no-op physical-retention mechanism. `retention.py` must call
`vacuum(dry_run=False)`.

### 3. Delta log retention default 30 days — VERIFIED

`delta.logRetentionDuration` default = `interval 30 days`
(`https://docs.delta.io/table-properties/`). Spine AD-3 and the Deferred section's "30-day default,
not tuned here" are accurate.

### 4. merge(source, predicate) → TableMerger with whenMatchedUpdate / whenNotMatchedInsert — VERIFIED (naming nit)

- `DeltaTable.merge` returns `TableMerger` (docstring: "Returns: TableMerger Object").
- `TableMerger` exposes `when_matched_update`, `when_not_matched_insert`, plus
  `when_matched_delete`, `when_matched_update_all`, `when_not_matched_insert_all`, and `execute`.
- Empirical: merged a Polars `pl.DataFrame` source into a Delta table in 1.6.0 — metrics
  `num_target_rows_updated: 1`, `num_target_rows_inserted: 1`. Polars source works (repo is
  Polars-first — compatible).

**Naming nit (finding):** the spine's `whenMatchedUpdate` / `whenNotMatchedInsert` are the SQL/Scala
MERGE-clause names. The delta-rs Python API is **snake_case** (`when_matched_update`,
`when_not_matched_insert`) and both clauses require an `updates` mapping plus an optional
`predicate`. An implementer grepping for the spine's names will find nothing.

### 5. Repo facts claimed as change rationale — VERIFIED (two line references imprecise)

- `pipeline/raw/ingest.py` dedups on `(source, payload_hash)` cross-run — CONFIRMED.
  `dedup_raw` opens the existing Delta table at `existing_path`, projects
  `columns=["source", "payload_hash"]`, and filters the incoming batch. AD-1's "cross-run
  `dedup_raw` scan is deleted" accurately describes today's code. (Note current write is
  `mode="append"`, `ingest.py:119`.)
- Events written `mode="overwrite"` — CONFIRMED at two sites: `normalize.py:218` (exact line the
  spine cites) and `pipeline/run.py:301-306` (per-broker `{broker}_{layer}`, which is the actual
  write path for `{broker}_events`). Both overwrite. The AD-4 rationale holds; see finding on the
  line reference.
- `analytics/quality.py` freshness = max of freshness column — CONFIRMED
  (`check_freshness`: `df.select(pl.col(freshness_column).max())`, line 332). AD-5's issue #157
  framing matches the code.
- Parent spine `single-bronze-per-broker` pins the same stack — CONFIRMED (its Stack table lists
  deltalake 1.6.0, duckdb 1.5.4, polars 1.42.0, pyarrow 24.0.0; it does not pin ruff/pyright — the
  child spine adds those, which match pyproject).

**Imprecision (finding):** `normalize.py:218` is the currency-normalization overwrite of the
consolidated `events` table, not the per-broker `{broker}_events` write. The per-broker events
overwrite that CAP-2's Flex-window-loss failure is about lives at `run.py:301-306`
(`transform_connector`). Both are `mode="overwrite"`, so the claim is true — but the line citation
points at the less on-point site.

### 6. Stack pins — VERIFIED

`pyproject.toml` (repo root): `requires-python = ">=3.11"`, `deltalake==1.6.0`,
`duckdb==1.5.4`, `pyarrow==24.0.0`, `polars==1.42.0`, `ruff==0.16.0`, `pyright==1.1.411`.
Venv interpreter: Python 3.11.15. All spine pins match.

### 7. XTB account_id from report filename — PLAUSIBLE, with a precedence nuance

- Filename→account mapping exists TODAY in `pipeline/connectors/xtb/transform.py`
  (`_account_id_from_filename`, lines 80-93): parses `{CCY}_{account_id}_{from}_{to}.xlsx`.
  `fetch.py:90` stores `source_file = <basename>` in the raw row, so the mapping input is present.
- BUT today the mapping runs in the **transform**, and the report payload (R1 "Account number",
  `parser.py` `_account_id_from_rows`) is **authoritative**; the filename parse is a
  fallback/grouping guard, and a filename-vs-R1 mismatch logs a warning
  (`transform.py:157-162`). So "account_id comes from source_file in silver" is a simplification:
  silver account_id today derives from the payload first, filename second.
- Moving extraction to fetch time (AD-2) is plausible — the parser logic already exists — but it
  inverts today's precedence (payload-R1-authoritative → filename-authoritative). AD-2's "recovers
  it by parsing the raw payload when null" keeps a payload path, and the memlog already names this
  as the "acknowledged filename risk" (NULL-key fallback to append + in-batch dedup). Also note the
  transform currently uses `(fetched_at, source_file)` as its deterministic tie-break
  (`transform.py:108-119`); AD-2 replaces the tie-break with `fetched_at` + `payload_hash`, so the
  migration must be careful that two XTB rows with equal `fetched_at` from different files stay
  deterministically ordered after `source_file` is dropped.

## Findings

| # | Severity | Finding |
|---|----------|---------|
| F1 | LOW | AD-3's "invokes `DeltaTable.vacuum()` with the default" is a **dry-run no-op** — delta-rs default `dry_run=True`. `retention.py` must pass `dry_run=False` or AD-3 reclaims no physical bytes. The spine names only `retention_hours`/`enforce_retention_duration`, so this is a literal-reading trap. |
| F2 | LOW | Merge clauses are `when_matched_update` / `when_not_matched_insert` (snake_case) in the delta-rs Python API, each requiring an `updates` mapping. The spine's camelCase `whenMatchedUpdate` / `whenNotMatchedInsert` (SQL terminology) will not match a grep. |
| F3 | LOW | `normalize.py:218` citation is the consolidated-`events` currency-normalization overwrite; the per-broker `{broker}_events` overwrite (the CAP-2 Flex-window target) is `run.py:301-306`. Claim true at both sites; cite the latter for precision. |
| F4 | LOW | "account_id comes from source_file in silver" is a simplification: today the transform uses the payload R1 account number as authoritative and the filename parse as fallback/grouping. Moving to fetch-time filename-first inverts that precedence; the tie-break also changes from `(fetched_at, source_file)` to `fetched_at` + `payload_hash`. |

No UNVERIFIED items remain — every claim was checked against the venv, pyproject.toml,
`docs.delta.io/table-properties/`, the delta-rs docstrings, and repo source.
