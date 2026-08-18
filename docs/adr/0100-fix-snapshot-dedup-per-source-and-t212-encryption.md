# 0100: Fix Snapshot Dedup Per Source and T212 Snapshot Encryption

> **Partially superseded by [ADR 0108](./0108-xtb-new-format-connector-overhaul.md) for XTB only** — per-source `filter_latest_snapshot` is replaced by per-`account_id` latest (D18) because XTB raw lacks `account_id` and multiple accounts share one `source`. The per-source dedup (decision #1) and the T212 `security_value` encryption fix (decision #2) carry forward unchanged for T212 and IBKR, see 0108 §Constraints.

## Context

Two bugs in the Trading 212 snapshot pipeline:

1. **Per-source dedup**: ADR 0057 created `filter_latest_snapshot` to keep only
   rows with the global maximum `fetched_at` timestamp. This works for
   single-endpoint snapshots (IBKR Flex, XTB) but breaks Trading 212, which
   builds a snapshot from multiple API endpoints (`/equity/account/summary`,
   `/equity/positions`, `/metadata/instruments`). Raw tables accumulate via
   `mode="append"`, and the fetch layer dedups by `payload_hash`, so when an
   endpoint's payload is unchanged on a subsequent fetch its row keeps the old
   `fetched_at` while changed endpoints get newer timestamps. The global-max
   filter then drops the unchanged-but-still-current rows,
   `transform_snapshot` sees `summary_data`/`positions_data` missing, and
   produces an empty or incomplete normalized table.

2. **Snapshot encryption column name**: `transform_snapshot` passed
   `encrypt_columns=["value"]`, but the normalized schema column is
   `security_value`. Since no `value` column exists, `security_value` was
   silently stored plaintext in the normalized Delta table — violating the
   project invariant that all financial values are Fernet-encrypted at rest
   (CLAUDE.md; pattern established for the gold layer by ADR 0084).

## Decision

### Dedup per source

`filter_latest_snapshot` now groups rows by `source` and keeps the latest
`fetched_at` per source, instead of the single global maximum. This amends the
dedup decision in ADR 0057 (§Decision, point 1). For tables where every row
shares one `fetched_at` (a single fetch — the common case for IBKR and XTB),
the result is identical to the old behavior: per-source and global maximum
coincide. The filter remains snapshot-only; CDC transforms are not filtered
(unchanged since ADR 0057, §Decision).

### Correct snapshot encryption column

Define `_SNAPSHOT_ENCRYPT_COLUMNS = ["security_value"]` in
`pipeline/connectors/trading212/transform.py` and pass it to
`build_normalized_table(..., encrypt_columns=...)` so the snapshot value is
actually Fernet-encrypted at rest.

## Constraints

- Raw tables keep `mode="append"`; dedup stays at read time in the transform
  (unchanged since ADR 0057, §Constraints).
- CDC transforms remain unfiltered.
- No schema change — same columns, same types.
- IBKR and XTB snapshot transforms share `filter_latest_snapshot` and must not
  regress.
- Carried forward unchanged from ADR 0057, §Decision: the IBKR `BASE_SUMMARY`
  cash fallback (point 2) and the T212 nested `cash` dict handling (point 3)
  are not modified by this ADR.

## Consequences

- Trading 212 snapshots survive unchanged-endpoint dedup: every endpoint's
  latest payload is preserved, so `summary_data`, `positions_data`, and
  `instruments_data` are all present in the transform.
- `security_value` in the normalized T212 snapshot table is now encrypted.
  Rows already written to existing tables are not retroactively encrypted; a
  full pipeline run rewrites them.
- The shared helper change affects IBKR/XTB only in the (unexercised) case of
  multi-source tables with differing timestamps; single-fetch behavior is
  unchanged.
- Supersedes ADR 0057 for the dedup decision only; the cash-extraction fixes
  from 0057 remain in force.

## Validation

- `test_different_timestamps_per_source_keeps_latest_per_source` in
  `tests/test_transform_utils.py` — asserts latest-per-source filtering for a
  multi-endpoint table.
- Updated `TestFilterLatestSnapshot` case — keeps global-max semantics for a
  single source with differing timestamps.
- `test_transform_snapshot_with_mixed_endpoint_timestamps` in
  `tests/test_trading212_connector.py` — asserts the T212 transform produces
  both EQUITY and CASH rows when endpoint timestamps differ.
- Full test suite passes (683 tests).
