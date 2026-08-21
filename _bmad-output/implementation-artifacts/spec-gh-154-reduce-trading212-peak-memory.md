---
title: 'Reduce trading212 connector peak memory'
type: 'feature'
created: '2026-08-21'
status: 'done'
baseline_commit: '143191bd59b36fa83e3e1cb9f648757ab1debd3d'
review_loop_iteration: 1
context: []
---

## Intent

**Problem:** After the single-bronze consolidation (ADR 0114), staging OOM-killed the trading212 Fargate task 3x (256 CPU / 512 MB). Verified root cause: the transform re-reads the whole accumulated `raw/trading212` table (which grows every run) and decrypts rows only to discard them. Phase RSS: 1039 MB transform peak.

**Approach:** Stop reading the raw table in the transform entirely. The current fetch already contains everything the transform needs — T212's events endpoints return the complete order/dividend/transaction history every run, and the snapshot endpoints return the current account state; any fetch error fails the run before transform (`FetchResult.ERROR → exit 1`). To keep that invariant true for the events path, `fetch_events` raises when ANY endpoint fails (review finding: it previously logged-and-continued, returning a partial current fetch as SUCCESS — a partial fetch would overwrite accumulated normalized history). Hand the encrypted current-fetch tables from `fetch_connector` straight to `transform_connector` in memory (the transform still decrypts — the handoff data is encrypted). The raw table is still written (append) for history/audit; only the transform's read is eliminated. Also project only the dedup-key columns in `dedup_raw` (its existing-key scan stays). **The handoff is a declared per-connector capability** — `handoff_supported: bool` on the `BrokerConnector` Protocol (default False). t212 declares it; ibkr/xtb keep the table read (Design Notes). No per-broker branch in `run.py`. The PR also strengthens the ibkr flex-query period requirement in the setup docs.

## Boundaries & Constraints

**Always:**
- The handoff passes the ENCRYPTED pre-dedup current fetch (what `ingest_raw` encrypts at line 79), NOT the deduped write — an unchanged endpoint (e.g. account summary) is deduped out of the write but must still reach the transform, exactly as it does today via the accumulated table.
- The handoff is a declared per-connector capability: `handoff_supported: bool = False` on the `BrokerConnector` Protocol. t212's connector declares it; ibkr/xtb leave the default. `fetch_connector` builds the handoff when declared, `transform_connector` uses it when present — no per-broker branch in `run.py`.
- Transform functions are unchanged: they still Fernet-decrypt, so the handoff data must be encrypted.
- The raw table is still written with `mode="append"` (history/audit) and dedup still runs; only the transform's table read is removed.
- Fallback: `transform_connector` reads the table when no handoff data is provided (defensive; no standalone transform subcommand exists today — `cmd_run_connector` and `cmd_full` always fetch first).
- Skip semantics: a layer with 0 rows in the handoff skips with the existing warning (same as today's 0-row check).

**Ask First:**
- Behavior change: normalized events now reflect T212's CURRENT history (events deleted from T212's API, e.g. cancelled orders, disappear) instead of the accumulated history. This is arguably more correct; confirm before merging.
- IBKR caps the flex-query Period at 365 days — a single fetch can never return full account history. Events older than the query window survive only via the accumulated raw table, so the handoff stays off for ibkr.
- Temporarily raising the Fargate task memory (512 → 1024/2048 MB) is NOT part of this change.

**Never:**
- Do NOT switch `dedup_raw` to `DeltaTable.merge`/bounded scans (issue item 4) — dedup semantics stay byte-identical.
- Do NOT convert the events transform to lazy/streaming (item 6), chunked encryption (item 7), row-group write options (item 8), compaction/vacuum (item 9), or per-endpoint writes (item 10) — deferred.
- Do NOT touch `pipeline/migrations/migrate_single_bronze.py`.
- Do NOT pass plaintext fetch data to the transform (decrypt would fail and silently drop rows).
- Do NOT enable the handoff for ibkr/xtb in this PR — their transforms keep reading the accumulated raw table.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| HAPPY_PATH | fetch succeeds (snapshot + events) | transform uses in-memory encrypted current fetch; normalized output identical to today | n/a |
| FETCH_ERROR | any fetch batch raises | FetchResult.ERROR → exit 1, transform never runs (same as today) | existing error path |
| PARTIAL_ENDPOINT | one events endpoint fails (e.g. orders) transiently | fetch_events raises → FetchResult.ERROR → exit 1; transform never runs; yesterday's normalized events preserved (was: warning + continue → partial fetch overwrote normalized history) | fail-loud raise |
| UNCHANGED_ENDPOINT | account summary deduped out of the write | handoff is pre-dedup → summary still reaches the transform → output complete (same as today) | n/a |
| EMPTY_FETCH | current fetch has 0 rows (pathological) | layer skips with the existing warning; normalized layer not rewritten (today: stale output from prior data) | n/a |
| EVENTS_DELETED | T212 no longer returns a cancelled order | normalized events drop it (today: kept from accumulated history) — documented behavior change | n/a |
| PAGINATED_EVENTS | full history across pages | `_fetch_paginated` follows all pages → complete current fetch | n/a |
| DEDUP | existing raw rows | dedup set from projected columns; dedup result identical | n/a |
| NO_HANDOFF | transform called without handoff (defensive) | falls back to the Delta read (today's behavior) | n/a |
| IBKR_WINDOW | flex period narrower than account history (portal-side) | ibkr keeps the table read → out-of-window events survive from accumulated raw (unchanged) | n/a |
| XTB_PARTIAL_UPLOAD | run uploads only some accounts | xtb keeps the table read → last-known report per account retained (unchanged) | n/a |

## Code Map

- `pipeline/raw/ingest.py` — `ingest_raw` (67-104) returns the encrypted pre-dedup table (it already computes `encrypted` at 79); `dedup_raw` (23-64) projects `["broker","source","payload_hash"]` at line 42.
- `pipeline/run.py` — `fetch_connector` (115-193) collects `{"snapshot": enc, "events": enc}` and returns `(FetchResult, dict)`; `transform_connector` (196-259) accepts `raw_tables: dict[str, pa.Table] | None`, uses `raw_tables[layer]` when present else reads the table; `cmd_run_connector` (659-702) threads the handoff (677 → 685).
- `pipeline/connectors/base.py` — add `handoff_supported: bool = False` to the `BrokerConnector` Protocol (15-66); `pipeline/connectors/trading212/connector.py` sets it `True`; ibkr/xtb keep the default.
- `pipeline/connectors/trading212/fetch.py` — `fetch_snapshot` (25-72) + `fetch_events` (75+) produce the complete current fetch; `client.py` `_fetch_paginated` (133-162) follows all pages.
- `pipeline/connectors/transform_utils.py` — `filter_latest_snapshot` (55-97), `iter_raw_payloads` (128+), `decrypt_events_payloads` (260-314) — unchanged; they operate on the handoff frame.
- `docs/ibkr/flex-query-required-fields-events.md` — flex query Period requirement (Last365Days / full history); ADR 0059 documents the `LastBusinessDay` incident (empty events sections).
- `pipeline/observability.py` — `log_memory` (39-51), `MemorySampler` (54-129); `cmd_run_connector` emits `connector:trading212:post-transform` at `run.py:686`.
- Tests — `tests/test_single_bronze_routing.py` (golden: T212 silver identical from merged raw, ~261), `tests/test_transform_pipeline.py` (`test_t212_transform_snapshot_golden`, 337), `tests/test_trading212_connector.py`, `tests/test_run_subcommands.py`, `tests/test_pipeline_integration.py`.

## Tasks & Acceptance

**Execution:**
- [x] `pipeline/raw/ingest.py` — `ingest_raw` returns the encrypted pre-dedup table; `dedup_raw` projects the 3 dedup-key columns.
- [x] `pipeline/connectors/base.py` + `pipeline/run.py` — add `handoff_supported: bool = False` to the Protocol; `fetch_connector` returns `(FetchResult, handoff_dict | None)` built when `connector.handoff_supported`; `transform_connector` takes `raw_tables` and uses them when present; `cmd_run_connector` threads the handoff. No per-broker branch.
- [x] `docs/ibkr/flex-query-required-fields-events.md` — state the 365-day Period cap and that events older than the query window survive only via the accumulated raw table (cite ADR 0059).
- [x] `tests/` — handoff-vs-table-read output-identical test (golden), unchanged-endpoint (pre-dedup) test, empty-fetch skip test, dedup-projection equality test, fallback test.
- [x] Run the three checks (ruff/pyright/pytest — 878 passed); confirm ibkr/xtb output unchanged (table read); record ADR (via manage-adr skill → ADR 0116). Staging `[mem]` re-measure: **pending** — no T212 credentials in this env; must be confirmed on a real staging deploy.

**Acceptance Criteria:**
- Given a successful fetch from a `handoff_supported` connector, when `transform_connector` runs with the handoff, then the normalized snapshot/events tables are identical to the table-read output (golden tests pass).
- Given an unchanged endpoint (deduped out of the write), when the transform runs, then its data still reaches the transform (pre-dedup handoff).
- Given a fetch error, when `cmd_run_connector` runs, then it exits 1 before transform (unchanged).
- Given `dedup_raw` runs, then dedup behavior is unchanged with the projected read.
- Given a staging `run-connector trading212`, when the run completes, then `[mem]` `post-transform` peak is below the task limit and the run does not OOM.
- Given ibkr/xtb runs, when no handoff is provided, then `transform_connector` falls back to the table read and their output is unchanged (existing tests pass).

## Spec Change Log

**v1 (review loop 1):**
- Triggering finding: two review layers independently flagged that `fetch_events` tolerates per-endpoint failures (`fetch.py:103-110`, warning + continue), so a partial current fetch could reach the transform and `mode="overwrite"` accumulated normalized events — the Intent's premise "partial data never reaches the transform" was false for the events path.
- Amended: Intent + I/O matrix now require fail-loud — `fetch_events` raises when ANY endpoint fails; the two pinned fetch tests are updated accordingly.
- Known-bad state avoided: `normalized/trading212_events` overwritten with partial order/dividend/transaction history on a transient endpoint failure.
- KEEP: pre-dedup encrypted handoff; table-read fallback; `handoff_supported` capability flag; dedup 3-column projection; golden handoff-vs-table-read output-identity test; fail-loud events endpoints.

## Design Notes

**Why the t212 transform doesn't need the raw table (verified):**
- T212 events endpoints return the COMPLETE order/dividend/transaction history every run (`_fetch_paginated` follows every `nextPagePath`); the accumulated table is N copies of that history with different hashes. `dedup_events` keeps the latest `fetched_at` per `(event_type, event_id)` — which is always the current run's copy. So the current fetch alone yields the identical normalized events output.
- Snapshot endpoints return the current account state; `filter_latest_snapshot` keeps the latest per source — the current fetch's rows are the latest. An unchanged endpoint (deduped out of the write) is still present in the pre-dedup handoff, matching today's table-read behavior.
- Any fetch error → `FetchResult.ERROR` → `cmd_run_connector` returns 1 before transform (run.py:683-684). Partial data never reaches the transform.

**Why ibkr/xtb keep the table read (verified):**
- ibkr: the flex query Period is a portal-side setting the repo cannot enforce (no date/period param is ever sent — `client.py:118,162`), and IBKR caps it at 365 days — one fetch can never return the full account history. ADR 0059 records a real `LastBusinessDay` incident where the events sections came back empty. The normalized events table is overwritten each run, so events older than the query window survive ONLY via the accumulated raw table — a handoff would silently drop them.
- xtb: the fetch ingests the passed file(s) (`--xtb-file` / EventBridge); `transform_events` applies `_latest_per_account` over the WHOLE raw table (`transform.py:94-174`) to retain the last-known report for accounts not re-uploaded this run. A handoff containing only this run's files would drop those accounts' events.
- **Partitioning (by `source` or `fetched_at`) does not fix this** — the accumulated history is still read and re-decrypted; t212's handoff already removes its read in-memory, and ibkr/xtb cannot prune their reads because their transforms are designed around the accumulation.

**Encryption contract:** the transform decrypts (`decode_payload`), so the handoff must carry the ENCRYPTED current fetch — `ingest_raw` already computes it (`encrypt_raw_payloads`, line 79). Passing plaintext would make every decrypt fail and silently drop all rows.

**Memory:** for t212, the transform's input drops from the accumulated table (grows every run) to the current fetch (~73 MB raw per issue #154). The remaining transform peak is the events `pl.concat` + dedup on the current history — the same data the transform always processed, now without the table read. If re-measurement still exceeds the limit, the next lever is per-endpoint writes (issue item 10), deferred.

## Verification

**Commands:**
- `.venv/Scripts/python -m ruff check --fix . && .venv/Scripts/python -m ruff format .` — expected: clean
- `.venv/Scripts/python -m pyright pipeline/ tests/` — expected: no new errors
- `.venv/Scripts/python -m pytest tests/ -q -rf` — expected: all pass
- `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m pipeline.run run-connector trading212 --mode staging` — expected: completes; `[mem]` `post-transform` peak below task limit

**Manual checks (if no CLI):**
- Confirm ibkr/xtb output is unchanged (they keep the table read; existing tests cover it).
- Review the `transform_connector` diff: handoff used when present, table-read fallback otherwise.

## Suggested Review Order

**Handoff plumbing (entry point)**

- fetch→transform handoff is threaded here, then released before validation so it can't inflate the post-transform peak
  [`run.py:736`](../../pipeline/run.py#L736)

- capability-gated handoff build; per-batch concat keeps multi-batch `fetch_kwargs` contracts safe
  [`run.py:123`](../../pipeline/run.py#L123)

- handoff layer used when present, accumulated-table read as fallback (ibkr/xtb always)
  [`run.py:239`](../../pipeline/run.py#L239)

- single source of truth for the layer keys the whole handoff is keyed on
  [`run.py:55`](../../pipeline/run.py#L55)

**Connector capability**

- `handoff_supported` declared on the Protocol (default False); no per-broker branch in the runner
  [`base.py:26`](../../pipeline/connectors/base.py#L26)

- t212 declares the capability; ibkr/xtb explicitly keep the default (pyright requires the class attr)
  [`connector.py:31`](../../pipeline/connectors/trading212/connector.py#L31)
  [`connector.py:28`](../../pipeline/connectors/ibkr/connector.py#L28)
  [`connector.py:26`](../../pipeline/connectors/xtb/connector.py#L26)

**Raw ingest / dedup contract**

- returns the encrypted PRE-DEDUP current fetch (unchanged endpoints must still reach the transform) and logs rows written
  [`ingest.py:77`](../../pipeline/raw/ingest.py#L77)

- existing-key scan projected to the 3 dedup columns — payload bytes never enter memory
  [`ingest.py:27`](../../pipeline/raw/ingest.py#L27)

**Fail-loud events fetch (review fix)**

- any endpoint failure raises — a partial current fetch can never overwrite accumulated normalized history
  [`fetch.py:127`](../../pipeline/connectors/trading212/fetch.py#L127)

**Decision record + ibkr docs**

- ADR 0116: in-memory encrypted handoff, capability flag, fail-loud rationale, fetched_at nuance
  [`0116-in-memory-encrypted-fetch-handoff.md:1`](../../docs/adr/0116-in-memory-encrypted-fetch-handoff.md#L1)

- ibkr 365-day Period cap justifies keeping the accumulated-table read (ADR 0114/0059, not a t212 decision)
  [`flex-query-required-fields-events.md`](../../docs/ibkr/flex-query-required-fields-events.md)

**Tests**

- handoff capability, golden output-identity vs table read, pre-dedup, empty-fetch skip, fallback, DeltaTable-never-opened guard, real ingest_raw threading
  [`test_transform_connector_handoff.py`](../../tests/test_transform_connector_handoff.py)

- fail-loud single-endpoint + partial-data tests (renamed from the old tolerant test)
  [`test_trading212_connector.py`](../../tests/test_trading212_connector.py)

- mock/tuple updates for the new `(FetchResult, handoff)` return shape
  [`test_run_subcommands.py`](../../tests/test_run_subcommands.py)
  [`test_pipeline_integration.py`](../../tests/test_pipeline_integration.py)
  [`test_connector_registry.py`](../../tests/test_connector_registry.py)
