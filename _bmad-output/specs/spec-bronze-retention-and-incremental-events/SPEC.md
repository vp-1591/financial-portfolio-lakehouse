---
id: SPEC-bronze-retention-and-incremental-events
companions:
  - retention-and-events-contract.md
  - handoff-decision-matrix.md
sources: []
---

> **Canonical contract.** This SPEC and `retention-and-events-contract.md` define the bounded-bronze and incremental-events change. Unresolved implementation choices remain listed as open questions in the companion.

# Bounded Bronze Retention and Incremental Events

## Why

Each new fetch currently increases raw-table storage and the amount of data read and deduplicated. The change is primarily a major read/deduplication speed optimization and a bound on storage growth, while preserving broker semantics in one broker-scoped bronze table. Bronze deduplication must not make freshness appear stale, and incremental normalized event tables must preserve events that fall outside the latest broker report.

## Capabilities

- **CAP-1** — Broker-specific bronze retention.
  - **intent:** Bound raw storage and the read/deduplication work per fetch while retaining one bronze table per broker and the raw state each broker still requires.
  - **success:** Under the configured policy, repeated fetches do not make raw storage or raw read/deduplication work grow indefinitely; XTB retains the latest report per account, Trading 212 retains the latest response per endpoint, and IBKR retains only the latest report required per source while normalized event history remains durable in normalized storage.
- **CAP-2** — Incremental normalized events.
  - **intent:** Add or update normalized events without replacing previously retained events that are absent from the current broker response.
  - **success:** IBKR events missing from a later Flex response remain in normalized storage; repeated `event_id` values resolve to the latest version; moving the Flex query window does not remove existing events.
- **CAP-3** — Current-fetch freshness.
  - **intent:** Make freshness reflect a successful current fetch even when its payload is already represented in bronze.
  - **success:** A repeated identical fetch does not trigger a stale-data warning solely because its payload hash was previously stored.
- **CAP-4** — Explicit Delta retention policy.
  - **intent:** Enforce physical raw-data retention before each fetch rather than relying on Delta defaults or deferred maintenance.
  - **success:** Every fetch runs retention maintenance that removes or compacts out-of-policy raw data and then runs `VACUUM`; the behavior is documented and tested or verified for every environment.
- **CAP-5** — Single bronze read for both normalized outputs.
  - **intent:** Reuse one broker bronze read and decoded result to populate both snapshot and event tables.
  - **success:** Each broker run reads its bronze table once, routes the shared result to both transforms, and does not re-read or reparse the same bronze payload separately for the downstream tables.

## Constraints

- Keep one raw Delta table per broker; do not add a separate observability table unless an implementation constraint proves the single-table design insufficient.
- `RAW_SCHEMA` includes nullable `account_id` and removes `source_file`. XTB fetch populates `account_id` from the report filename; IBKR and Trading 212 store `NULL`. XTB transform groups by the raw account ID, recovers it by parsing the raw payload when it is null, and uses `fetched_at` plus `payload_hash` for any required deterministic tie-break.
- Do not treat `(source, account_id)` with null account IDs as one shared key across brokers. Broker-specific keys must avoid collapsing unrelated non-XTB rows.
- Normalized event history for every broker is append-preserved by `event_id`; event tables must not be overwritten from only the latest broker response.
- Snapshot and event normalization must consume the shared result of one bronze read per broker run; downstream tables must not independently re-read or reparse the same bronze payload.
- The Trading 212 in-memory fetch handoff is removed for a measurement experiment; existing handoff memory and runtime measurements are the baseline, and the handoff is restored if removal causes a material regression.
- Trading 212 endpoint responses are treated as complete according to the current connector contract. XTB account identity is the retention boundary. Partial-response and broker-correction handling is not added without evidence.
- Delta's defaults are not an application retention policy: deleted-file retention is one week and log retention is 30 days; VACUUM is not automatic. Retention implementation must distinguish logical table deletion, physical file deletion, and transaction-log history.
- Existing silver snapshot/event schemas and broker source vocabulary remain unchanged unless required by the incremental event contract.

## Non-goals

- No second metadata/observability table by default.
- No semantic canonicalization of broker payloads before hashing.
- No deletion or correction semantics for broker responses beyond the stated completeness assumptions.
- No guarantee of replaying historical broker payloads after the configured bronze retention window; normalized event rows are the durable history.

## Success signal

One implementation PR demonstrates broker-specific bronze retention, shows bounded raw storage and materially reduced read/deduplication work across repeated fetches, proves one shared bronze read feeds both normalized outputs, measures the memory and runtime impact of removing the T212 handoff against the existing baseline, preserves normalized events across a moving Flex window, prevents repeated identical fetches from producing a false stale warning, and verifies configured Delta retention/VACUUM behavior. Focused tests, Ruff, Pyright, and the full test suite pass.

## Open questions
