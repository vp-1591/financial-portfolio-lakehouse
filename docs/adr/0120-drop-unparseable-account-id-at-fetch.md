# 0120: Drop Unparseable Account-id Rows at Fetch Time

## Context

ADR 0117 (AC-3) handles an XTB report whose filename does not match
`{CCY}_{account_id}_{from}_{to}.xlsx` by writing a raw row with a NULL
`account_id` (appended, never merged) and letting the transform's payload-parse
recovery rescue the account from the workbook's R1 "Account number". This is
silent and unactionable: the broken file re-fetches every run, the NULL row
re-appends, and nothing names the offending file. When the payload is not
parseable either, the account vanishes from silver with no operator signal.

The invariant this change builds on: on a single file, the raw `account_id`
(derived from the filename) and the silver account (parsed from the report)
must be identical. No code may assume they diverge; only the transform's
mismatch log may observe both.

## Decision

The goal is an operator-visible, actionable signal when a report cannot be
mapped to an account — the dashboard is missing exactly the affected account
and the fix (rename the file) is obvious. The mechanism chosen:

**Drop the row at fetch time instead of writing a NULL-keyed row.** When
`fetch_snapshot` cannot derive the account id from the artifact, it raises
`UnparseableAccountIdError` (`pipeline/connectors/base.py`) naming the file.
`fetch_connector` catches it before the generic error handler — the run
continues, `FetchResult` stays SUCCESS, the transform runs, exit 0 — and
records a synthetic `data_quality` WARN (`table_name="raw/xtb"`,
`check_name="account_id_unparseable"`, `threshold="{CCY}_{account_id}_{from}_{to}.xlsx"`,
`actual=<filename>`). The record is folded into the run's `data_quality` write
via a new optional `run_validation(extra_records=...)` parameter, because
`run_validation` is the sole dq writer and overwrites the table (a fetch-time
write would be wiped). Synthetic records share the run's single `checked_at`
— the builder never stamps time.

Alternatives rejected:
- *Write the NULL row and let the transform recover (status quo):* silent and
  unactionable; the account can still vanish when the payload is unparseable.
- *Return an empty table:* carries no signal — the caller cannot name the
  dropped file, record a WARN, or distinguish a drop from an empty fetch.
- *Error the whole run:* stops the pipeline for one bad file and starves every
  other account of that run's data.

The AC-3 NULL-append path and the transform payload-parse recovery REMAIN for
legacy NULL-keyed rows already in `raw/xtb` (pre-change rows and rows the
ADR 0117 AD-7 migration backfilled from unparseable `source_file` filenames).
Removing them would silently drop legacy accounts from silver — a data
regression. They become dead paths for new fetches only.

## Constraints

- New-format fetches never write NULL-keyed raw rows; a dropped file yields
  SUCCESS and the transform still runs.
- The `data_quality` schema (7 columns) and its overwrite write behavior are
  unchanged (ADR 0118 pins this); synthetic records share the run's single
  `checked_at`.
- `_account_id_from_filename` is unchanged — the AD-7 migration still depends
  on its None return.
- The AC-3 NULL-append and the transform payload-parse recovery stay intact
  for legacy rows (`TestNullKeyAppendBound`,
  `test_null_raw_account_id_payload_parse_recovery` keep passing).
- Only the snapshot fetch path raises; the events fetch is untouched (XTB has
  no events fetch; IBKR/T212 never raise this error).

## Consequences

- An unparseable filename is dropped before bronze: no NULL raw row, no
  payload-parse rescue, no silent silver loss. Silver is missing exactly that
  account — the intended, operator-visible outcome.
- The WARN is TRANSIENT: the next consolidate-time `run_validation` (no
  `extra_records`) overwrites the table, erasing it. Durable signals are the
  run-connector task's WARNING log and the WARN in the run-connector task's dq
  summary until the next consolidate; re-uploading the broken file re-emits
  the WARN. Persisting drops longer (append-only dq writes) is deliberately out
  of scope — ADR 0118 pins overwrite.
- The purge residual WARN (ADR 0118) now concerns legacy NULL-keyed rows only.
- Transform hardening (error on NULL/mismatch instead of recovery) is follow-up
  work, not part of this change.

## Validation

- `tests/test_xtb_connector.py`: fetch raises `UnparseableAccountIdError` with
  `.filename` for unparseable names (S3 URI and local path); valid-pattern
  files still yield a row with the parsed account id.
- `tests/test_run_subcommands.py::TestFetchConnectorDroppedFile`: a dropped
  file yields `FetchResult.SUCCESS`, `ingest_raw` is not called, and the dq
  record carries `("raw/xtb", "account_id_unparseable")` with the WARN fields;
  a valid-pattern file writes one bronze row via the real `ingest_raw`.
- `tests/test_quality.py::TestRunValidationExtraRecords`: extra records land
  in `data_quality`; multiple records share one `checked_at`.
- Unchanged legacy-path suites prove no regression: `test_raw_retention.py`
  (`TestNullKeyAppendBound`), `test_story_5_4.py`, `test_migrate_raw_account_id.py`.
- Full suite: `pytest tests/ -q -rf` → 928 passed; `ruff` clean; `pyright`
  0 errors.

Supersedes: ADR 0117 (the AC-3 NULL-keyed-row boundary for new XTB fetches)
and ADR 0118 (the NULL-row purge residual is now legacy-only). Carried forward
unchanged: merge-on-key retention, per-run VACUUM, append-preserving events
MERGE, single bronze read, and the AD-7 migration gate (ADR 0117 §Decision);
per-account staleness check and purge mechanics (ADR 0118 §Decision).
