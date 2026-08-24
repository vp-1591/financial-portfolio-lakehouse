# 0118: Per-Account Staleness Flag and Account Purge Escape Hatch

> **Superseded by [ADR 0120](./0120-drop-unparseable-account-id-at-fetch.md)** —
> the NULL-keyed-row purge residual now concerns legacy rows only (new fetches
> cannot produce them); per-account staleness check and purge mechanics carry
> forward unchanged, see 0120 §Decision.

## Context

The data-quality freshness check (ADR 0072) is table-level: it flags a whole
table stale when its latest `fetched_at` is older than the window. With XTB's
multi-account raw table, one account can stop being re-uploaded while others
stay fresh — the table-level check cannot say *which* account is stale, so a
single departed account keeps the whole `xtb_snapshot` table flagged. There is
also no way to remove a departed account's data from the pipeline: raw rows,
silver snapshots, and silver events for that account accumulate forever.

## Decision

The goal is per-account visibility into staleness and a deliberate, guarded
way to remove a departed account's data. The mechanisms chosen:

1. **Per-account staleness flag.** `check_account_freshness(table_name,
   arrow_table, key_column, freshness_days)` groups rows by the key column,
   computes `max(fetched_at)` per key, and emits a WARN listing each stale key
   with its age in days. An empty table or a missing key column passes. A
   registry `ACCOUNT_STALENESS_KEYS` (`{"xtb_snapshot": "account_id"}`) sits
   next to `FRESHNESS_COLUMNS`; `run_validation` calls the check right after
   the table-level `check_freshness` for registered tables, reusing the
   already-loaded table and emitting metadata
   `(table_name, "account_freshness")`.

2. **Account purge escape hatch.** A `purge-account` subcommand
   (`--broker <broker> --account-id <id> [--yes]`) deletes rows for that
   account across `raw/{broker}`, `{broker}_snapshot`, and `{broker}_events`
   via `DeltaTable.delete(predicate)`, printing each predicate and the affected
   count. Without `--yes` it is a dry run: predicates are printed and it exits
   0. Brokers whose retention key is not `account_id` (trading212, ibkr) raise
   `RuntimeError` — the purge is only safe where the raw key is the account.
   After the deletes, a residual WARN reports any NULL-keyed raw rows that
   remain (they cannot be matched by an `account_id` predicate).

## Constraints

- `FRESHNESS_COLUMNS` and ADR 0072's table-level semantics are preserved; the
  per-account check is additive.
- The `data_quality` output schema, append/overwrite behavior, and print format
  are unchanged.
- No new silver columns; the source vocabulary is unchanged.
- The purge is destructive and requires an explicit `--yes`; the dry run is the
  default.

## Consequences

- A stale account is named in the DQ report instead of hiding behind a
  table-level WARN.
- Departed accounts can be removed end-to-end (raw + silver) with one command,
  instead of manual per-table deletes.
- The purge is an escape hatch, not a scheduled job — it stays manual and
  explicit.
- NULL-keyed raw rows (unparseable XTB filenames) cannot be purged by account
  and are surfaced as a residual WARN.

## Validation

- `tests/test_story_5_4.py` (10 tests): per-account staleness WARN lists stale
  keys with age; empty table / missing key column pass; purge deletes across
  raw + both silver tables with correct predicates and counts; dry run without
  `--yes` prints and exits 0; unsupported brokers raise `RuntimeError`.
- `tests/test_run_subcommands.py::test_main_dispatches_purge_account`: the
  `purge-account` subparser is wired into `main()`.
- Full suite: `pytest tests/ -q -rf` → 922 passed; `ruff` clean; `pyright`
  0 errors.
