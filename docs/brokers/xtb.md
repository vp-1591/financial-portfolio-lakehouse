# XTB Connector Setup

## Data Source

XTB does not provide a live API. Data is ingested from Excel report exports
provided via the `--xtb-file` CLI argument.

The report is a 3-sheet `.xlsx` workbook:

- **Open Positions** — current holdings, as of report generation time.
- **Cash Operations** — the cash ledger over the report window (deposits,
  withdrawals, interest, taxes, transfers, stock purchases/sells).
- **Closed Positions** — closed trades; used for fee enrichment only.

## Environment Variables

No API key or secret is required for XTB.

## Usage

XTB requires an explicit file path — it does not auto-discover files from any
directory.

**Local:**
```powershell
.venv\Scripts\python -m pipeline.run full --xtb-file path/to/report.xlsx --mode docker
```

**Docker:**
```bash
docker compose run --rm pipeline full --mode docker --xtb-file /path/to/report.xlsx
```

**Single connector:**
```powershell
.venv\Scripts\python -m pipeline.run run-connector xtb --xtb-file path/to/report.xlsx --mode docker
```

You can pass `--xtb-file` multiple times to process several reports in one run.

If `--xtb-file` is not provided, XTB is silently skipped during `full` runs.
The `run-connector xtb` subcommand skips gracefully (return 0) when no file is
provided. XTB is not part of the scheduled/CI run — it is driven solely by the
EventBridge file-arrival trigger, so a report arrives via `upload-xtb` and the
rule runs the connector with the real S3 key.

**Cloud upload (S3 + EventBridge):**
```bash
.venv\Scripts\python -m pipeline.run upload-xtb path/to/report.xlsx --mode staging
```
This uploads the file to S3 and triggers the Step Functions orchestrator
automatically. Requires S3 storage.

## File-arrival trigger (production)

In production, XTB is event-driven. EventBridge fires on S3 Object-Created for
the upload prefix (`xtb_uploads/` in both environments) and starts the Step
Functions orchestrator's `RunConnectors` Map with
`--xtb-file <s3-uri>` — one file per execution. The daily scheduled run
(`schedule_connectors = ["ibkr","trading212"]`) does **not** include XTB:
fetch+transform runs only on file arrival. `run-consolidate-analytics` still
reads `xtb_snapshot`/`xtb_events` silver on every run whenever present, and
`xtb_events` is not a required non-empty table. Multiple accounts accumulate
across triggers into the shared `raw/xtb` table and are unioned
per-account at transform time.

## Implementation

The XTB connector is implemented in `pipeline/connectors/xtb/`. The fetch step
stores the raw `.xlsx` file bytes (encrypted) with `source="XTB_REPORT"` — a
single raw row carries the full 3-sheet workbook (**shared bronze**: one raw
row feeds both the snapshot and events silver tables; there is no separate
`xtb_events` raw fetch). The transform step parses the workbook with
[openpyxl](https://openpyxl.sourceforge.org/) and builds normalized tables
with Polars.

Key behaviors of the new-format parser/transform:

- **3 sheets.** Open Positions → snapshot holdings; Cash Operations → all events;
  Closed Positions → fee-enrichment lookup only (keyed by Position ID,
  never emitted as its own event).
- **Dates** are decoded to timezone-aware UTC datetimes in the parser
  (openpyxl auto-converts date-formatted cells; raw numeric serials go through
  `openpyxl.utils.datetime.from_excel`). The transform emits ISO-8601 UTC
  strings, so downstream analytics is unchanged.
- **Account currency** is read from the Open Positions summary block's
  `Currency` column (not a hardcoded literal). All Cash Operations amounts are
  in this account currency.
- **Holdings** use the per-ticker aggregate row (real instrument name in
  `Instrument`, non-empty `Category`, empty `Type`); child lot rows under each
  aggregate are skipped (the snapshot schema is instrument-level). **Ticker**
  is the identifier — the new format exposes no ISIN.
- **A CASH holding** is emitted per account from the Cash Operations `Total`
  row. **Setup constraint:** reports must be exported with `Date from` set to
  the account opening date (full history from a zero opening balance); a
  partial-window export silently understates cash and cannot be detected
  in-file. Account equity = this CASH row + the sum of open positions.
- **Subaccount transfers** (internal moves between the trading and
  investment-plan subaccounts) are filtered out entirely. Currency-conversion
  `Transfer` rows are kept as TRANSFER events with `target_fx_rate` left null —
  `normalize_currency` fills it via `CurrencyConverter` (the broker's
  `Exchange rate:X` is the account-currency→destination rate, which is unsafe
  for non-EUR destinations, so it is not parsed).
- **Trades** carry quantity/price/side parsed from the cash-operation comment
  (`OPEN/CLOSE {side} {qty} @ {price}`). The closing (`Stock sell`) row is
  enriched from Closed Positions via Position ID: `fee_amount = Commission`,
  `settle_date = Close time`. The
  opening (`Stock purchase`) row gets no fee.
- **Multi-account.** Snapshot and events both keep the latest payload per
  `account_id`. Rows are grouped by the raw `account_id` column (AD-2 — the
  raw-schema migration backfills it from the legacy `source_file` filename
  pattern `{CCY}_{account_id}_{from}_{to}.xlsx`), and only the latest row per
  account is parsed — not `filter_latest_snapshot` (which keys on `source`
  alone and would collapse distinct accounts). The sort key is
  `(fetched_at, payload_hash)` descending, so ties on `fetched_at` break
  deterministically (AD-2). Each parse is guarded: a malformed latest row
  falls back to the previous good row for that account, and if all rows for
  an account fail the account is skipped with a warning (one bad historical
  row can no longer kill the connector). The report's R1 `account_id` is
  authoritative on a raw/R1 mismatch (logged). Rows whose raw `account_id` is
  NULL fall back to a guarded parse for account-id discovery. Raw retention
  is merge-on-key (AD-1): a re-fetched report for the same `account_id`
  replaces the stored row in place instead of accumulating duplicates. events
  dedups on `(event_type, event_id, account_id)` so same-ID events from
  different accounts coexist.