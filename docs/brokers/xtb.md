# XTB Connector Setup

## Data Source

XTB does not provide a live API. Data is ingested from Excel report exports
uploaded to the `xtb-report-sample/` directory.

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `XTB_ENABLED` | Enable/disable connector (default: enabled) |

No API key or secret is required for XTB.

## Usage

1. Export your XTB account report as an Excel file.
2. Place the file in the `xtb-report-sample/` directory.
3. Run the pipeline — the connector will parse and ingest the data.

## Implementation

The XTB connector is implemented in `pipeline/connectors/xtb/` and uses
Polars to parse the Excel report format.
