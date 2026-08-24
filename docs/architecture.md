# Architecture

## Medallion Pipeline

The `pipeline/` package implements a medallion architecture (raw → normalized →
analytics) with Delta tables and Fernet encryption for sensitive financial data.

### Data flow

Broker data flows through three layers:

1. **Raw** — Encrypted broker payloads stored as-is, with fetch metadata.
   Each connector writes snapshot and events (change data capture) payloads
   into one table per broker (`raw/{broker}`); `source` discriminates them.
   Writes are merge-on-key (AD-1): a re-fetched payload for the same broker
   retention key (`account_id` for XTB, pagination-stripped `source` for
   Trading 212/IBKR) replaces the stored row in place, and each run VACUUMs
   the table (AD-3) so bronze stays bounded.
2. **Normalized** — Structured positions, cash, and events parsed from
   raw payloads. Financial values remain Fernet-encrypted. Cross-broker
   holdings are consolidated into `consolidated_holdings`; events are
   merged into `events` with currency conversion applied.
3. **Analytics** — Portfolio-level aggregations. Encrypted values are summed,
   percentages are calculated and stored in plaintext. events-derived tables
   provide dividend, interest, and cash flow breakdowns.

For the full Mermaid diagram showing every table, edge label, and report
chart connection, see [Table Lineage](table-lineage.md).

### Layers and tables

| Layer | Table | Contents |
|-------|-------|----------|
| 🔵 Sources | — | Broker APIs and files |
| 🟠 Raw | `raw/{broker}` | Encrypted API payloads, one table per broker; `source` discriminates snapshot/events |
| 🟢 Normalized | `normalized/{broker}_snapshot` | Structured positions & cash rows; financial values remain Fernet-encrypted |
| 🟢 Normalized | `normalized/{broker}_events` | Structured events per broker |
| 🟢 Normalized | `normalized/consolidated_holdings` | Cross-broker holdings converted to target currency; financial values remain Fernet-encrypted |
| 🟢 Normalized | `normalized/events` | Merged events with currency conversion applied |
| 🔵 Analytics | `analytics/portfolio_holdings` | Portfolio holdings with encrypted values and plaintext percentages |
| 🔵 Analytics | `analytics/dividend_income` | Dividends by period, broker, and security |
| 🔵 Analytics | `analytics/interest_income` | Interest by period and broker |
| 🔵 Analytics | `analytics/cash_flow_summary` | All events aggregated by period and type |
| 🔵 Analytics | `analytics/data_quality` | Freshness, per-account staleness, and row-count validation badges |

### Table naming convention

Table names follow the `{name}_{layer}` convention:

| Table | Layer |
|-------|-------|
| `ibkr_raw` | Raw |
| `ibkr_snapshot_normalized` | Normalized |
| `ibkr_events_normalized` | Normalized |
| `trading212_raw` | Raw |
| `trading212_snapshot_normalized` | Normalized |
| `trading212_events_normalized` | Normalized |
| `xtb_raw` | Raw |
| `xtb_snapshot_normalized` | Normalized |
| `xtb_events_normalized` | Normalized |
| `consolidated_holdings_normalized` | Normalized |
| `events_normalized` | Normalized |
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