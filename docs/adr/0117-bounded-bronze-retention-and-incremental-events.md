# 0117: Bounded Bronze Retention and Incremental Events

## Context

Raw (bronze) tables were append-only. Trading 212's events endpoints return the
complete order/dividend/transaction history every run, so the accumulated
`raw/trading212` table grew by a full copy of that history each run — the
memory driver behind the staging OOMs that ADR 0116 worked around. The
transform re-read the whole accumulated table every run and decrypted rows only
to discard them. XTB's multi-account flow needed per-account retention, but
ADR 0047 had removed `account_id` from `RAW_SCHEMA`, so the raw layer had no
per-account key to retain on.

## Decision

The goal is a bounded bronze layer whose size reflects the broker's current
state, not the number of runs, and incremental events that never duplicate
history. The mechanisms chosen:

1. **Merge-on-key retention (AD-1).** Raw writes are a `DeltaTable.merge` on
   the broker retention key: XTB `account_id`, Trading 212/IBKR
   pagination-stripped `source` (Trading 212's key is the endpoint base without
   query params, so a re-fetch of the same page set matches). Matched keys are
   replaced by the current fetch row, new keys inserted, absent keys untouched.
   Rows whose retention key is NULL (an unparseable XTB filename) are appended,
   never merged — a MERGE predicate never matches NULL.

2. **Nullable `account_id` returns to `RAW_SCHEMA` (AD-2); `source_file` is
   dropped.** `account_id` is XTB's retention key, so it must live in the raw
   layer. This reverses ADR 0047's removal of the column; the raw layer still
   stores original unmodified broker payloads (0047's medallion principle
   carries forward unchanged — parsing stays in the transform).

3. **Per-run VACUUM (AD-3).** Each fetch run VACUUMs the raw table with
   `dry_run=False` — mandatory, because deltalake 1.6.0 defaults to a no-op
   dry run. This reclaims the storage of merge-replaced files.

4. **Events writes are append-preserving MERGEs (AD-4).** `transform_events`
   merges on the full per-broker event identity, so a re-fetched event updates
   in place and history is never duplicated.

5. **Single bronze read per broker run (AD-6).** The transform reads the one
   `raw/{broker}` table once per run; the in-memory handoff of ADR 0116 is
   removed (see ADR 0119).

6. **Raw-schema migration is a deploy gate (AD-7).** `migrate_raw_account_id.py`
   backfills XTB's `account_id` from the retained legacy `source_file` filename
   (unparseable → NULL), then drops `source_file`. It must run before the
   5-1/5-2 code is deployed in each environment, or the new schema's quality
   checks flag the mismatch.

## Constraints

- Merge keys are plaintext; the `payload` column stays Fernet-encrypted.
- In-batch dedup runs on `(source, payload_hash)` first, then on the retention
  key (newest `fetched_at` wins, tie → last in batch order) so merge/loop order
  never decides the winner.
- NULL-keyed rows are appended, never merged; distinct `(source, payload_hash)`
  null-keyed rows get only the in-batch dedup.
- The events MERGE must preserve append history — it is not a delete-and-reload.
- ADR 0110's EventBridge task needs no change: it runs `run-connector xtb`,
  which VACUUMs via `fetch_connector`.

## Consequences

- Bronze is bounded: a re-fetched endpoint updates its row in place instead of
  accumulating a copy per run, and VACUUM reclaims replaced files.
- The transform's input no longer grows with run count, removing the memory
  driver ADR 0116 addressed.
- Deploying requires running the raw-schema migration first (AD-7) — a new
  deploy step, enforced by the schema gate.
- XTB multi-account retention now keys on the raw `account_id` column; the
  transform's per-account latest-row selection uses `(fetched_at,
  payload_hash)` as its sort key.

## Validation

- `tests/test_ingest_raw.py` (or the story-5-2 merge tests): merge-on-key
  replaces matched keys, inserts new keys, appends NULL-keyed rows; in-batch
  retention-key dedup keeps newest `fetched_at` (tie → last in batch).
- `tests/test_events_merge.py`: events MERGE updates in place without
  duplicating history.
- `tests/test_migrate_raw_account_id.py`: migration backfills XTB `account_id`
  from `source_file`, drops the column, is idempotent, and handles already-
  migrated/drifted tables.
- Full suite: `pytest tests/ -q -rf` → 922 passed; `ruff` clean; `pyright`
  0 errors.

Supersedes: ADR 0047 (account_id returns to RAW_SCHEMA). Carried forward
unchanged: raw layer stores original unmodified broker payloads (ADR 0047
§Decision), single bronze raw table per broker (ADR 0114), phase-level RSS
observability (ADR 0115).
