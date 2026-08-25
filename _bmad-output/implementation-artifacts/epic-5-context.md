# Epic 5 Context: Bounded Bronze Retention and Incremental Events

<!-- Compiled from planning artifacts. Edit freely. Regenerate with compile-epic-context if planning docs change. -->

## Goal

Bound the raw (bronze) layer into a write-time cache — exactly one row per broker retention key (XTB account, Trading 212 endpoint, IBKR source) — enforced by a Delta MERGE during fetch, plus per-run VACUUM, so repeated fetches no longer grow raw storage or the read/deduplication work (issue #154 memory driver). Normalized (silver) events become incremental: append-preserving merges on per-broker event identity keep events that are absent from the latest broker response, and a byte-identical re-fetch cannot read as stale because freshness is a property of the run, not of the stored bytes. Each broker run reads its bronze blob exactly once and routes the decoded result to both the snapshot and the events transforms. The epic also evolves `RAW_SCHEMA` (nullable `account_id`, drops `source_file`), ships the raw-schema migration as the deploy gate, and removes the Trading 212 in-memory fetch handoff as a measured experiment. Paradigm: bounded bronze, durable silver.

## Stories

- Story 5.1: Raw schema account-id evolution — nullable `account_id` joins `RAW_SCHEMA`, `source_file` leaves; XTB identity moves to fetch time
- Story 5.2: Write-time merge retention and vacuum — raw write becomes a Delta MERGE on the retention key; per-run VACUUM
- Story 5.3: Incremental events merge and single bronze read — events writes become append-preserving MERGEs; one read feeds both outputs
- Story 5.4: Run-aware freshness — DQ freshness reflects the run's last-successful-fetch, with table fallback
- Story 5.5: Raw-schema migration — backfills XTB `account_id` from filenames, drops `source_file`
- Story 5.6: T212 handoff removal measurement — removes the in-memory handoff, measured against the existing baseline

## Requirements & Constraints

- **Bounded bronze retention (CAP-1):** repeated fetches must not grow raw storage or raw read/deduplication work; the retained set is fixed at write time, never recomputed by a read-back scan. XTB keeps the latest report per account, Trading 212 the latest response per endpoint, IBKR only the latest report per source.
- **Incremental normalized events (CAP-2):** events absent from the current broker response remain in normalized storage; repeated `event_id`s resolve to the latest version; moving the Flex query window must never remove existing events.
- **Current-fetch freshness (CAP-3):** a repeated identical fetch must not trigger a stale warning solely because its payload hash was previously stored.
- **Explicit retention policy (CAP-4):** every broker run performs retention maintenance and runs VACUUM; behavior is documented and tested or verified for every environment. Delta's defaults (7-day tombstone, 30-day log history, no auto-VACUUM) are not a retention policy.
- **Single bronze read (CAP-5):** each broker run reads `raw/{broker}` once and routes the shared result to both snapshot and event normalization; no downstream table re-reads or re-parses the same bronze payload.
- One raw Delta table per broker; no second metadata/observability table by default. `source` stays a routing value; the retention key is never a `kind`/`layer` column.
- `RAW_SCHEMA` gains nullable `account_id` and drops `source_file`; a null `account_id` is never treated as one shared cross-broker key (unrelated non-XTB rows must not collapse onto it).
- Trading 212 responses are treated as complete per the connector contract; XTB account is the retention boundary; no partial-response/correction handling without evidence.
- Merge keys and `fetched_at` stay plaintext columns; the `payload` column stays Fernet-encrypted. No new dependencies; no semantic canonicalization of payloads before hashing.
- Full check suite green (ruff, pyright, pytest — tests re-run after lint); grep bar: no `source_file` in `pipeline/` outside carve-outs (migration script + its test, `docs/adr/`); docs (`docs/table-lineage.md`, `docs/architecture.md`, broker docs) updated; one implementation PR with focused tests.

## Technical Decisions

- **Retention is write-time merge-on-key (AD-1).** The raw write for broker B is `DeltaTable.merge(source, predicate)` of the current fetch batch against `raw/{B}`, keyed on B's retention key — XTB `account_id`; Trading 212 and IBKR `source` (the T212 pagination suffix is stripped from `source` before keying, so cursor pages cannot fragment the endpoint). `when_matched_update` replaces the matched row (latest `fetched_at` wins by construction); `when_not_matched_insert_all` inserts new keys; keys absent from the current batch are untouched — a fetch never deletes keys it did not see. The batch is pre-deduped on `(source, payload_hash)`; a NULL retention key (XTB filename yielding no account id) is appended, never merged (merges never match NULL). Per-endpoint write-what-succeeded isolation and the T212 fail-loud all-endpoints-empty `RuntimeError` survive. The cross-run `dedup_raw` accumulation scan is deleted.
- **`RAW_SCHEMA` evolution (AD-2).** `RAW_SCHEMA` = `{fetched_at, broker, source, payload, payload_hash, account_id}` (nullable `account_id`, no `source_file`). XTB's fetch populates `account_id` from the report filename at fetch time (filename-first); the transform parses the payload only when the raw value is null; IBKR and Trading 212 store NULL and never merge on it.
- **VACUUM per run, no override (AD-3).** Each broker run ends with `DeltaTable.vacuum(dry_run=False)` and the Delta 7-day default tombstone retention (no `retention_hours`, `enforce_retention_duration` stays True). Each connector task vacuums only its own `raw/{broker}` — including XTB's file-arrival task — never the silver event tables. Logical removal (merge replace), physical removal (VACUUM), and the 30-day transaction log are distinct mechanisms.
- **Events are append-preserving MERGEs (AD-4).** The events write becomes a `DeltaTable.merge` of the run's normalized rows (pre-deduped in-batch by `dedup_events`) on the full broker event identity: IBKR `event_id`; Trading 212 `(event_type, event_id)`; XTB `(event_type, event_id, account_id)`. `when_matched_update` fires only when the incoming `fetched_at` is newer; `when_not_matched_insert_all` otherwise; nothing is deleted. Re-running a fetch converges. This replaces today's `mode="overwrite"` events write.
- **Freshness is run-aware (AD-5).** The pipeline passes per-broker last-successful-fetch timestamps into the DQ pass. Single-broker tables map to their broker; the consolidated tables (`events`, `consolidated_holdings`) map to the max last-successful-fetch over the run's brokers. When the run context is absent (standalone DQ), fall back to stored `max(fetched_at)` unchanged (ADR 0072).
- **One bronze read per broker run (AD-6).** `transform_connector` reads `raw/{broker}` once after the merge write and routes the same result to both snapshot and events transforms.
- **Raw-schema migration as the deploy gate (AD-7).** A migration rewrites each `raw/{broker}` to the new schema, backfilling XTB `account_id` by parsing `source_file`'s filename only (unparseable → NULL, matching the append-for-null rule — no payload parsing at migration time), then drops `source_file`. Idempotent with `--dry-run`; runs manually per environment with scheduled executions paused across the window (ADR 0112 A1 convention). Events tables need no schema change — only the write mechanism changes.
- **T212 handoff removed as a measured experiment (AD-8).** The in-memory encrypted-fetch handoff is removed so the single bronze read is the only path; a representative Trading 212 run is measured (memory peak and runtime) against the ADR 0116 baseline; the handoff is restored only if removal causes a material regression, and the single-shared-read guarantee holds either way.
- **Supersedes earlier decisions:** ADR 0116's no-`DeltaTable.merge` raw-write clause (the merge is the bounded write this epic exists to ship) and ADR 0047's "`account_id` is a silver concept" clause; the parent spine's raw-layer append rule is likewise superseded at the raw layer. The handoff's encrypted-pre-dedup contract and 1039 MB peak baseline remain the measurement baseline.
- New file `pipeline/raw/retention.py` is the single source of truth for the per-broker retention key and VACUUM invocation. No new dependencies (deltalake 1.6.0 supplies merge/vacuum).

## Cross-Story Dependencies

- **Blocking:** 5-1 (schema) gates 5-2 (XTB merge key = `account_id`) and 5-5 (migration backfills `account_id` from `source_file`); 5-2 (bounded table) and 5-3 (single read) gate 5-6 (the handoff is removable only once the table is bounded and the single read replaces it).
- **Independent:** 5-1, 5-3, 5-4 run in parallel (file-disjoint except different `run.py` functions); 5-2 and 5-5 run in parallel after 5-1; 5-6 lands last (sequential).
- **Shared-file seams:** `run.py` fetch return shape (5-2 writes the merge inside, 5-4 adds per-broker fetch times, 5-6 removes the handoff dict — 5-6 defines the final signature); `raw/ingest.py` (5-2 keeps the pre-dedup encrypted-table return so the handoff tests keep passing; 5-6 removes it); `connectors/transform_utils.py` (5-1 drops `source_file`; 5-3 only verifies `dedup_events` stays).
- **Phase 4 (orchestrator):** full check suite, docs sweep, ADR via `manage-adr` recording the bounded-bronze + incremental-events decision (superseding the ADR 0116 no-merge clause and ADR 0047 `account_id` exclusion), and a single PR. Merge only after all checks green and the 5-5 migration is applied per environment with counts verified.
- Builds on Epic 1's events-named layer (`dedup_events`, `{broker}_events`), Epic 2's staging naming, and Epic 3's migration pattern.
