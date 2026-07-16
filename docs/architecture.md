# Architecture

## Medallion Pipeline

The `pipeline/` package implements a medallion architecture (raw → normalized →
analytics) with Delta tables and Fernet encryption for sensitive financial data.

### Data flow

```mermaid
flowchart TD
    classDef source fill:#4472c4,stroke:#2f5496,color:#fff
    classDef raw fill:#ed7d31,stroke:#c55a11,color:#fff
    classDef norm fill:#70ad47,stroke:#4f7d28,color:#fff
    classDef fx fill:#7030a0,stroke:#5b2280,color:#fff
    classDef analytics fill:#5b9bd5,stroke:#2e75b6,color:#fff

    IBKR["IBKR Flex Web Service<br/>positions · cash"]:::source
    T212["Trading 212 API<br/>account · positions · instruments"]:::source
    XTB["XTB Excel Report<br/>open positions · cash ops"]:::source

    IBKR -->|"encrypt payloads"| RS["ibkr<br/>snapshot · cdc"]:::raw
    T212 -->|"encrypt payloads"| RT["trading212<br/>snapshot · cdc"]:::raw
    XTB -->|"parse & encrypt"| RX["xtb<br/>snapshot · cdc"]:::raw

    RS -->|"parse · normalize · re-encrypt values"| NS["ibkr_snapshot"]:::norm
    RT -->|"parse · normalize · re-encrypt values"| NT["trading212_snapshot"]:::norm
    RX -->|"parse · normalize · re-encrypt values"| NX["xtb_snapshot"]:::norm

    NS -->|"convert currency · normalize tickers · ISIN overrides"| CH["consolidated_holdings"]:::norm
    NT --> CH
    NX --> CH

    FX1["Frankfurter API"]:::fx -->|"FX rates"| CH
    FX2["Yahoo Finance"]:::fx -->|"fallback"| CH

    CH -->|"sum values · calculate %"| PH["portfolio_holdings<br/>(values encrypted, percentage plaintext)"]:::analytics
```

Each layer stores data in Delta tables under `data/`:

| Layer | Node color | Table | Contents |
|-------|-----------|-------|----------|
| 🔵 Sources | Blue | — | Broker APIs and files |
| 🟠 Raw | Orange | `raw/{broker}_snapshot` | Encrypted API payloads with fetch metadata |
| 🟠 Raw | Orange | `raw/{broker}_cdc` | Encrypted change-data-capture payloads |
| 🟢 Normalized | Green | `normalized/{broker}_snapshot` | Structured positions & cash rows; financial values remain Fernet-encrypted |
| 🟢 Normalized | Green | `normalized/consolidated_holdings` | Cross-broker holdings converted to target currency; financial values remain Fernet-encrypted |
| 🟣 FX Rates | Purple | — | Frankfurter API (primary) / Yahoo Finance (fallback) |
| 🔵 Analytics | Light blue | `analytics/portfolio_holdings` | Portfolio holdings with encrypted values and plaintext percentages |

### Table naming convention

Table names follow the `{name}_{layer}` convention:

| Table | Layer |
|-------|-------|
| `ibkr_snapshot_raw` | Raw |
| `ibkr_snapshot_normalized` | Normalized |
| `trading212_snapshot_raw` | Raw |
| `trading212_snapshot_normalized` | Normalized |
| `xtb_snapshot_raw` | Raw |
| `xtb_snapshot_normalized` | Normalized |
| `consolidated_holdings_normalized` | Normalized |
| `portfolio_holdings_analytics` | Analytics |

### Encryption

Financial values are encrypted at rest using Fernet (symmetric encryption) before
being stored in Delta tables. The encryption key is provided via the
`ENCRYPTION_KEY` environment variable and is **never stored in S3 or in config
files**. The `--decrypt` flag on query commands decrypts values for
human-readable output.

### Table lineage

For a comprehensive Mermaid diagram showing the full data flow from raw through
normalized to analytics and report charts, see [table-lineage.md](table-lineage.md).
