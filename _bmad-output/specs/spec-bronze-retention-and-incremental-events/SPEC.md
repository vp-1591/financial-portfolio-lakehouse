---
id: SPEC-bronze-retention-and-incremental-events
companions:
  - retention-and-events-contract.md
sources: []
---

> **Canonical contract.** This SPEC and `retention-and-events-contract.md` define the bounded-bronze and incremental-events change. Unresolved implementation choices remain listed as open questions in the companion.

# Bounded Bronze Retention and Incremental Events

## Why

One broker-scoped bronze table is sufficient, but its retention policy must match broker semantics. Bronze deduplication must not make freshness appear stale, and replacing normalized events must not erase IBKR events that fall outside the latest Flex query window.

## Capabilities

- **CAP-1** — Broker-specific bronze retention.
  - **intent:** Keep the raw payload history required by each broker while retaining one bronze table per broker.
  - **success:** XTB retains the latest report per account, Trading 212 retains the latest response per endpoint, and IBKR retains enough event coverage to rebuild every normalized event that is still retained.
- **CAP-2** — Incremental normalized events.
  - **intent:** Add or update normalized events without replacing previously retained events that are absent from the current broker response.
  - **success:** IBKR events missing from a later Flex response remain in normalized storage; repeated `event_id` values resolve to the latest version; moving the Flex query window does not remove existing events.
- **CAP-3** — Current-fetch freshness.
  - **intent:** Make freshness reflect a successful current fetch even when its payload is already represented in bronze.
  - **success:** A repeated identical fetch does not trigger a stale-data warning solely because its payload hash was previously stored.
- **CAP-4** — Explicit Delta retention policy.
  - **intent:** Make physical raw-data retention an intentional maintenance policy rather than relying on Delta defaults.
  - **success:** The configured policy and VACUUM behavior are documented and tested or verified for every environment; append-only writes are not mistaken for deletion.

## Constraints

- Keep one raw Delta table per broker; do not add a separate observability table unless an implementation constraint proves the single-table design insufficient.
- XTB `account_id` is not in `RAW_SCHEMA`; current code derives it from `source_file` in silver. Do not silently add it to bronze. The retention implementation must either preserve this boundary or introduce an explicitly reviewed ingestion identity.
- Do not treat `(source, account_id)` with null account IDs as one shared key across brokers. Broker-specific keys must avoid collapsing unrelated non-XTB rows.
- IBKR event history must be append-preserved by `event_id`; normalized events must not be overwritten from only the latest Flex response.
- Trading 212 endpoint responses are treated as complete according to the current connector contract. XTB account identity is the retention boundary. Partial-response and broker-correction handling is not added without evidence.
- Delta's defaults are not an application retention policy: deleted-file retention is one week and log retention is 30 days; VACUUM is not automatic. Retention implementation must distinguish logical table deletion, physical file deletion, and transaction-log history.
- Existing silver snapshot/event schemas and broker source vocabulary remain unchanged unless required by the incremental event contract.

## Non-goals

- No second metadata/observability table by default.
- No semantic canonicalization of broker payloads before hashing.
- No deletion or correction semantics for broker responses beyond the stated completeness assumptions.
- No guarantee of replaying IBKR payloads after the configured bronze retention window; normalized event rows are the durable history.

## Success signal

One implementation PR demonstrates broker-specific bronze retention, preserves IBKR normalized events across a moving Flex window, prevents repeated identical fetches from producing a false stale warning, and verifies configured Delta retention/VACUUM behavior. Focused tests, Ruff, Pyright, and the full test suite pass.

## Open questions

- How should ingestion obtain the XTB account retention key while keeping `account_id` out of `RAW_SCHEMA`?
- Does issue #155 require the existing Trading 212 handoff optimization to remain, or does the incremental-events design replace it with per-layer filtered reads/bounded scans?
- Should raw retention physically compact/delete rows, or should it only restrict newly admitted rows and rely on scheduled maintenance?
