# XTB Connector Setup

## Data Source

XTB does not provide a live API. Data is ingested from Excel report exports
(Open Positions and Cash Operations) provided via the `--xtb-file` CLI
argument.

> **Warning:** This connector has not been tested with real account data. XTB
> does not allow downloading report files from demo accounts, so only sample
> data has been verified (as of 2026-07-16; this may be fixed in future
> versions).

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `XTB_ENABLED` | Enable/disable connector (default: enabled) |

No API key or secret is required for XTB.

## Usage

XTB requires an explicit file path — it does not auto-discover files from any
directory.

**Local:**
```powershell
.venv\Scripts\python -m pipeline.run full --xtb-file path/to/report.xlsx
```

**Docker:**
```bash
docker compose run --rm pipeline full --xtb-file /path/to/report.xlsx
```

**Single connector:**
```powershell
.venv\Scripts\python -m pipeline.run run-connector xtb --xtb-file path/to/report.xlsx
```

You can pass `--xtb-file` multiple times to process several reports in one run.

If `--xtb-file` is not provided, XTB is silently skipped during `full` and
`fetch` commands. The `run-connector xtb` subcommand requires it and will
error otherwise.

**Cloud upload (S3 + EventBridge):**
```bash
.venv\Scripts\python -m pipeline.run upload-xtb path/to/report.xlsx
```
This uploads the file to S3 and triggers the Step Functions orchestrator
automatically. Requires S3 storage.

## Implementation

The XTB connector is implemented in `pipeline/connectors/xtb/`. The fetch step
stores the raw .xlsx file bytes (encrypted). The transform step parses the
Excel ZIP/XML format using `xml.etree.ElementTree` and builds normalized
tables with Polars.