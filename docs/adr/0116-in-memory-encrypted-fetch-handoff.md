# 0116: In-Memory Encrypted Fetch Handoff for Trading212

## Context

After the single-bronze consolidation (ADR 0114), the trading212 connector on the staging Fargate task (256 CPU / 512 MB) was OOM-killed three times. Phase-level RSS observability (ADR 0115) measured a 1039 MB transform peak. Root cause: `transform_connector` re-reads the whole accumulated `raw/trading212` Delta table every run (which grows every run — the T212 events endpoints return the complete history each run, so the accumulated table is N copies of that history) and decrypts rows only to discard them.

The raw table read is unnecessary for t212. Its events endpoints (`_fetch_paginated`) return the complete order/dividend/transaction history every run; `dedup_events` keeps the latest `fetched_at` per `(event_type, event_id)`, which is always the current run's copy. Snapshot endpoints return the current account state; `filter_latest_snapshot` keeps the latest per source — the current fetch's rows. Because the transform now consumes only the current fetch, the events fetch fails loud: ANY endpoint failure raises a `RuntimeError` (partial data must never reach the transform — with the handoff, a silently skipped endpoint's events would be dropped instead of backfilled from the accumulated table). The raise surfaces as `FetchResult.ERROR` → exit 1 before transform.

ibkr and xtb are different: the ibkr flex-query Period is a portal-side setting the repo cannot enforce (no date/period parameter is ever sent), IBKR caps it at 365 days, and ADR 0059 records a real `LastBusinessDay` incident where the events sections came back empty — so events older than the query window survive only via the accumulated `raw/ibkr` table. xtb's `transform_events` applies `_latest_per_account` over the whole raw table to retain last-known reports for accounts not re-uploaded this run. A current-fetch-only handoff would silently drop out-of-window events for both.

## Decision

Goal: eliminate the transform's re-read of the accumulated raw table for connectors whose current fetch already contains everything the transform needs, so the transform peak no longer grows with the accumulated history.

`fetch_connector` builds an in-memory **handoff** — the Fernet-encrypted PRE-DEDUP current fetch (`{"snapshot": ..., "events": ...}`, what `ingest_raw` encrypts before its dedup write) — and returns it alongside the `FetchResult`. `transform_connector` accepts `raw_tables: dict[str, pa.Table] | None` and uses a layer from the handoff when present, falling back to the Delta table read otherwise. `cmd_run_connector` threads the handoff from fetch to transform.

The handoff is a declared per-connector capability: `handoff_supported: bool = False` on the `BrokerConnector` Protocol. trading212 declares it `True`; ibkr and xtb declare `False` and keep the table read. There is no per-broker branch in `run.py` — `getattr(connector, "handoff_supported", False)` gates building the handoff, and the transform's `raw_tables` parameter gates using it.

The handoff carries the **encrypted pre-dedup** current fetch, not the deduped write and not plaintext. The pre-dedup guarantee matches today's table-read behavior: an unchanged endpoint (deduped out of the write) still reaches the transform. The handoff's rows carry the CURRENT `fetched_at`, where the accumulated path would have read the older deduped row — normalized content is identical, the timestamp is fresher (intended). Encrypted data preserves the transform contract — transforms still Fernet-decrypt, so passing plaintext would make every decrypt fail and silently drop all rows. `dedup_raw` now projects only its three dedup-key columns (`broker`, `source`, `payload_hash`) for its existing-key scan, keeping dedup semantics byte-identical.

The raw table is still written with `mode="append"` (history/audit) and dedup still runs; only the transform's read is removed.

## Constraints

- The handoff is only built for connectors that declare `handoff_supported` — no per-broker branch in `run.py`.
- The handoff passes encrypted pre-dedup data. Never pass plaintext fetch data to the transform.
- Transform functions are unchanged — they still decrypt; the handoff data is encrypted.
- The raw table append write and dedup behavior are unchanged; `dedup_raw` keeps byte-identical semantics (projected key scan only, no `DeltaTable.merge`/bounded scans).
- ibkr and xtb keep the accumulated table read. Their transforms are designed around the accumulation; a handoff would drop out-of-window or not-re-uploaded data.
- No schema migration and no change to `pipeline/migrations/migrate_single_bronze.py`.
- Temporarily raising the Fargate task memory is out of scope for this change.
- The transform keeps the existing 0-row skip (a layer with 0 rows skips with the existing warning; normalized layer not rewritten).

## Consequences

- **Positive:** trading212's transform input drops from the accumulated table (grows every run) to the current fetch (~73 MB raw per issue #154) — the memory driver is removed. The remaining transform peak is the events concat + dedup on the current history, the same data the transform always processed.
- **Positive:** dedup now reads only three key columns instead of full encrypted payloads, reducing raw-table scan memory for all connectors.
- **Positive:** the handoff is opt-in per connector via a declared capability, so ibkr/xtb behavior is untouched.
- **Negative (behavior change, ask-first):** trading212 normalized events now reflect the CURRENT history — an event deleted from T212's API (e.g. a cancelled order) disappears from the normalized events table instead of surviving in the accumulated history. This is arguably more correct but must be confirmed by the user before merge.
- **Fallback risk:** if `transform_connector` is ever called without the handoff (no standalone transform subcommand exists today; `cmd_run_connector`/`cmd_full` always fetch first), it falls back to the table read — identical output, just slower.
- **Follow-up:** if re-measurement still exceeds the task limit, the next lever is per-endpoint writes (issue item 10), deferred.
- **Follow-up (protocol generalization, issue #155):** the binary `handoff_supported` flag cannot express the middle grounds ibkr/xtb need — per-layer handoff, per-layer filtered reads, and a bounded dedup key scan. That work is tracked in issue #155 and extends this ADR's capability design: it will amend/extend this ADR, not create a second one — this ADR remains the decision record for the handoff problem.

## Validation

- `tests/test_transform_connector_handoff.py`:
  - `TestHandoffCapability` — trading212 declares `handoff_supported`; ibkr/xtb keep the default `False`.
  - `TestTransformConnectorHandoff::test_handoff_output_identical_to_table_read` — handoff output matches the table-read output and the round-trip fixture (decrypted contents; Fernet ciphertext is randomized per encrypt).
  - `test_empty_handoff_skips_without_rewriting` — 0-row layer skips with the existing warning; existing normalized table untouched.
  - `test_missing_handoff_layer_falls_back_to_table_read` — a layer absent from the handoff is read from the table.
  - `TestCmdRunConnectorThreadsHandoff` — `cmd_run_connector` passes the fetch handoff into `transform_connector(raw_tables=...)`.
  - `TestIngestRawReturnsPreDedupHandoff` — `ingest_raw` returns the pre-dedup encrypted fetch (an unchanged endpoint deduped out of the write still reaches the transform); the Delta table still holds only the first run's rows.
  - `TestDedupRawProjected` — projected key read dedups identically.
- `pytest tests/ -q -rf` passes; `ruff check --fix .` and `ruff format .` clean; `pyright pipeline/ tests/` no errors.
- Staging: `run-connector trading212 --mode staging` completes without OOM and the `[mem]` `post-transform` peak is below the task limit.
- Manual: confirm ibkr/xtb output unchanged (they keep the table read; existing tests cover it).

Supersedes: none. Carried forward unchanged: single-bronze raw table per broker (ADR 0114), phase-level RSS observability (ADR 0115), ibkr flex-query period behavior (ADR 0059).
