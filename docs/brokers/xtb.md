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
The `run-connector xtb` subcommand requires it and will error otherwise.

**Cloud upload (S3 + EventBridge):**
```bash
.venv\Scripts\python -m pipeline.run upload-xtb path/to/report.xlsx --mode staging
```
This uploads the file to S3 and triggers the Step Functions orchestrator
automatically. Requires S3 storage.

## File-arrival trigger (production)

In production, XTB is event-driven. EventBridge fires on S3 Object-Created for
the upload prefix (`pipeline/xtb_uploads/` in prod, `pipeline_demo/xtb_uploads/`
in demo) and starts the Step Functions orchestrator's `RunConnectors` Map with
`--xtb-file <s3-uri>` — one file per execution. The daily scheduled run
(`schedule_connectors = ["ibkr","trading212"]`) excludes XTB, so `xtb_cdc` may
legitimately be absent on that path. Multiple accounts accumulate across
triggers into the shared `xtb_snapshot` raw table and are unioned per-account
at transform time.

## Implementation

The XTB connector is implemented in `pipeline/connectors/xtb/`. The fetch step
stores the raw `.xlsx` file bytes (encrypted) with `source="XTB_REPORT"` — a
single raw row carries the full 3-sheet workbook (**shared bronze**: one raw
row feeds both the snapshot and CDC silver tables; there is no separate
`xtb_cdc` raw fetch). The transform step parses the workbook with
[openpyxl](https://openpyxl.sourceforge.org/) and builds normalized tables
with Polars.

Key behaviors of the new-format parser/transform:

- **3 sheets.** Open Positions → snapshot holdings; Cash Operations → all CDC
  events; Closed Positions → fee-enrichment lookup only (keyed by Position ID,
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
  `gross_amount = Sale value − Purchase value`, `settle_date = Close time`. The
  opening (`Stock purchase`) row gets no fee.
- **Multi-account.** Snapshot and CDC both keep the latest payload per
  `account_id` (keyed on `fetched_at`), not `filter_latest_snapshot` (which
  keys on `source` alone and would collapse distinct accounts). CDC dedups on
  `(event_type, event_id, account_id)` so same-ID events from different
  accounts coexist.