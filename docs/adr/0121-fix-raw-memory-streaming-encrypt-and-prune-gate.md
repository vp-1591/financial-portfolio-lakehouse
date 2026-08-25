# 0121 - Fix Raw-Table Memory in Code: Streaming Encrypt Plus a Pre-Deploy Prune Gate

## Context

Since epic 5 (ADR 0119) removed the T212 in-memory fetch handoff, every
staging deploy has failed: the trading212 connector task (256 CPU / 512 MB)
is OOM-killed three times per run (exit 137,
`OutOfMemoryError: container killed due to memory usage`), so the Step
Functions state machine never reaches consolidate-allocate. ADR 0115 had
already measured the same ceiling (~1039 MB peak locally vs the 512 MB limit)
and recorded a follow-up to raise task memory; it was never applied.

The memory anatomy, reproduced in a 512 MB-constrained local container against
MinIO with a staging-shaped table (121 rows x ~0.93 MB Fernet tokens):

- `encrypt_raw_payloads` materialized several full-size copies at once
  (`to_pylist()` list + per-row `Fernet(key)` construction + output token
  list ~1.33x).
- delta-rs' MERGE reads the whole merge target into Rust memory. The raw
  table accumulated pre-retention rows (trading212: 121 rows, ~113 MB of
  encrypted payloads), and retention pruning only happens inside a
  *successful* fetch -- a catch-22: the first post-deploy fetch must merge
  the bloated table to prune it, and merging it is exactly what OOMs.

Raising the task's memory in terraform was ruled out by the product owner;
re-introducing the handoff was already rejected by ADR 0119.

## Decision

Fix the memory ceiling in code, in two layers:

1. **Streaming encrypt** (always on): `encrypt_raw_payloads` reuses a single
   `Fernet` instance and iterates the payload column directly instead of
   building an intermediate Python list of decrypted values. This removes one
   redundant full-size copy from every ingest.
2. **Pre-deploy prune gate**: a one-off migration script
   (`pipeline/migrations/prune_raw_retention.py`) applies the exact end-state
   a first successful fetch would produce -- newest row per retention key
   (AD-1), ties resolved to the last row in batch order (AC-4), reusing
   `_dedup_by_retention_key` byte-for-byte -- to each accumulated
   `raw/{broker}` table via the staged boto3 rewrite proven by
   `migrate_raw_account_id` (local Delta write, transfer-manager upload, commit
   rebuilt with `remove` actions uploaded last). Run it against each
   environment BEFORE deploying code that changes raw-table accumulation.

Alternatives rejected:

- **Raise Fargate memory to 1024-2048 MB (ADR 0115 follow-up)**: rejected by
  the product owner ("this problem must be fixed elsewhere"); also masks
  rather than removes the copies.
- **Re-add the in-memory handoff**: rejected in ADR 0119 (complexity,
  duplicate state, masked the underlying growth instead of bounding it).
- **Streaming chunked encrypt-to-parquet writes**: unnecessary once measured
  -- after pruning, the dominant term is input table + output tokens, which
  fit.

Measured outcome (same constrained container): an 80-row fetch batch was
killed at >= 414 MB before the fix, still exit-137 after the streaming-encrypt
fix alone (the bloated-target merge dominates); after pruning 121 -> 12 rows
the same batch peaks at **491.3 MB and exits 0**.

Goal: deploys must succeed through consolidate-allocate within the existing
512 MB task envelope, without infrastructure changes and without reintroducing
the handoff.

## Constraints

- No terraform changes; the fix lives entirely in pipeline code plus manual
  script runs.
- Retention semantics stay identical to the fetch batch (AD-1 keys, AC-4
  tie-break) -- the prune may not choose different survivors than a
  successful fetch would have.
- The rewrite path must be crash-safe: a mid-upload failure leaves the old
  table untouched and re-runs resume (migrate_raw_account_id behavior).
- The prune refuses tables not readable as `RAW_SCHEMA` (conflict guard, per
  ADR 0112 A1 / ADR 0113 A1 convention).
- Out of scope: raising task memory, changing the retention policy itself, or
  automating the gate into CI/CD (it is a documented manual step).

## Consequences

- Positive: deploys fit the existing 512 MB envelope; the raw table stays at
  one row per key from the first post-gate fetch onward; every future ingest
  saves one full-size payload copy.
- Accepted downside: headroom after pruning is thin (491.3 MB peak vs the
  512 MB limit at the measured load) -- a materially larger single-run fetch
  could still OOM; the honest fix for sustained growth would be payload-level
  streaming or a smaller per-page footprint, not more RAM.
- Accepted downside: each environment needs a manual
  `python -m pipeline.migrations.prune_raw_retention --mode <env>` run before
  deploying; forgetting it reproduces the exit-137 failures until it runs.
- Superseded files remain as orphaned S3 objects until VACUUM (per-broker
  VACUUM continues to clean them per AD-3).
- Follow-up candidates: fold the prune check into a deploy-time quality probe;
  revisit ADR 0115's memory follow-up as officially declined rather than open.

## Validation

- `tests/test_pipeline_integration.py::TestEncryptRawPayloadsSetColumn::
  test_encrypt_payloads_decrypt_to_originals` -- round-trip decrypt equality
  of the rewritten encrypt path.
- `tests/test_prune_raw_retention.py` (12 tests) -- per-broker key semantics
  (T212 pagination-stripped base, XTB account_id, IBKR raw source),
  fetched_at tie keeps last-in-order, idempotent no-op when already pruned,
  absent-table skip, dry-run writes nothing, schema-conflict raises without
  clobbering, `run_prune` wiring over real local Delta tables, S3 client
  required for s3:// paths.
- Manual reproduction (docker bench): seed MinIO with a staging-shaped table,
  run the container with `--memory=512m --memory-swap=512m`, drive
  `ingest_raw` over an 80-row batch -> `BENCH-DONE`, peak 491.3 MB, exit 0.
