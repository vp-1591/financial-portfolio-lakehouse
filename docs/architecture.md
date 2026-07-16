# Architecture

## Medallion Pipeline

The `pipeline/` package implements a medallion architecture (raw → normalized →
analytics) with Delta tables and Fernet encryption for sensitive financial data.

### Data flow

Broker data flows through three layers:

1. **Raw** — Encrypted broker payloads stored as-is, with fetch metadata.
   Each connector writes snapshot and CDC (change data capture) payloads.
2. **Normalized** — Structured positions, cash, and CDC events parsed from
   raw payloads. Financial values remain Fernet-encrypted. Cross-broker
   holdings are consolidated into `consolidated_holdings`; CDC events are
   merged into `cdc_events` with currency conversion applied.
3. **Analytics** — Portfolio-level aggregations. Encrypted values are summed,
   percentages are calculated and stored in plaintext. CDC-derived tables
   provide dividend, interest, and cash flow breakdowns.

For the full Mermaid diagram showing every table, edge label, and report
chart connection, see [Table Lineage](table-lineage.md).

### Layers and tables

| Layer | Table | Contents |
|-------|-------|----------|
| 🔵 Sources | — | Broker APIs and files |
| 🟠 Raw | `raw/{broker}_snapshot` | Encrypted API payloads with fetch metadata |
| 🟠 Raw | `raw/{broker}_cdc` | Encrypted change-data-capture payloads |
| 🟢 Normalized | `normalized/{broker}_snapshot` | Structured positions & cash rows; financial values remain Fernet-encrypted |
| 🟢 Normalized | `normalized/{broker}_cdc` | Structured CDC events per broker |
| 🟢 Normalized | `normalized/consolidated_holdings` | Cross-broker holdings converted to target currency; financial values remain Fernet-encrypted |
| 🟢 Normalized | `normalized/cdc_events` | Merged CDC events with currency conversion applied |
| 🔵 Analytics | `analytics/portfolio_holdings` | Portfolio holdings with encrypted values and plaintext percentages |
| 🔵 Analytics | `analytics/dividend_income` | Dividends by period, broker, and security |
| 🔵 Analytics | `analytics/interest_income` | Interest by period and broker |
| 🔵 Analytics | `analytics/cash_flow_summary` | All CDC events aggregated by period and type |
| 🔵 Analytics | `analytics/data_quality` | Freshness and row-count validation badges |

### Table naming convention

Table names follow the `{name}_{layer}` convention:

| Table | Layer |
|-------|-------|
| `ibkr_snapshot_raw` | Raw |
| `ibkr_snapshot_normalized` | Normalized |
| `ibkr_cdc_raw` | Raw |
| `ibkr_cdc_normalized` | Normalized |
| `trading212_snapshot_raw` | Raw |
| `trading212_snapshot_normalized` | Normalized |
| `trading212_cdc_raw` | Raw |
| `trading212_cdc_normalized` | Normalized |
| `xtb_snapshot_raw` | Raw |
| `xtb_snapshot_normalized` | Normalized |
| `xtb_cdc_raw` | Raw |
| `xtb_cdc_normalized` | Normalized |
| `consolidated_holdings_normalized` | Normalized |
| `cdc_events_normalized` | Normalized |
| `portfolio_holdings_analytics` | Analytics |
| `dividend_income_analytics` | Analytics |
| `interest_income_analytics` | Analytics |
| `cash_flow_summary_analytics` | Analytics |
| `data_quality_analytics` | Analytics |

### Encryption

Financial values are encrypted at rest using Fernet (symmetric encryption) before
being stored in Delta tables. The encryption key is provided via the
`ENCRYPTION_KEY` environment variable and is **never stored in S3 or in config
files**. The `--decrypt` flag on query commands decrypts values for
human-readable output.

### Table lineage

For a comprehensive Mermaid diagram showing the full data flow from raw through
normalized to analytics and report charts, see [table-lineage.md](table-lineage.md).