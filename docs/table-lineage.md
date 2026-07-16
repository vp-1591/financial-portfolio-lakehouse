# Table Lineage

This document describes how data moves through the pipeline. The lineage is split into two views:

1. **Storage lineage** – raw ingestion through normalized and analytics tables.
2. **Report lineage** – which analytics tables feed each report section.

---

# Storage Lineage

```mermaid
flowchart TD
  classDef raw fill:#fce4ec,stroke:#e57373,color:#333
  classDef norm fill:#e3f2fd,stroke:#64b5f6,color:#333
  classDef gold fill:#e8f5e9,stroke:#81c784,color:#333

  %% Raw
  r_ibkr_snap["ibkr_snapshot"]:::raw
  r_ibkr_cdc["ibkr_cdc"]:::raw
  r_t212_snap["trading212_snapshot"]:::raw
  r_t212_cdc["trading212_cdc"]:::raw
  r_xtb_snap["xtb_snapshot"]:::raw
  r_xtb_cdc["xtb_cdc"]:::raw

  %% Normalized
  n_ibkr_snap["ibkr_snapshot"]:::norm
  n_ibkr_cdc["ibkr_cdc"]:::norm
  n_t212_snap["trading212_snapshot"]:::norm
  n_t212_cdc["trading212_cdc"]:::norm
  n_xtb_snap["xtb_snapshot"]:::norm
  n_xtb_cdc["xtb_cdc"]:::norm
  n_consolidated["consolidated_holdings"]:::norm
  n_cdc_events["cdc_events"]:::norm

  %% Gold
  g_holdings["portfolio_holdings"]:::gold
  g_dividends["dividend_income"]:::gold
  g_interest["interest_income"]:::gold
  g_cashflow["cash_flow_summary"]:::gold
  g_quality["data_quality"]:::gold

  %% Raw → Normalized
  r_ibkr_snap -->|transform_snapshot| n_ibkr_snap
  r_ibkr_cdc -->|transform_cdc| n_ibkr_cdc

  r_t212_snap -->|transform_snapshot| n_t212_snap
  r_t212_cdc -->|transform_cdc| n_t212_cdc

  r_xtb_snap -->|transform_snapshot| n_xtb_snap
  r_xtb_cdc -->|transform_cdc| n_xtb_cdc

  %% Snapshot path
  n_ibkr_snap -->|extract_holdings| n_consolidated
  n_t212_snap -->|extract_holdings| n_consolidated
  n_xtb_snap -->|extract_holdings| n_consolidated

  %% CDC path
  n_ibkr_cdc -->|consolidate_cdc_events| n_cdc_events
  n_t212_cdc -->|consolidate_cdc_events| n_cdc_events
  n_xtb_cdc -->|consolidate_cdc_events| n_cdc_events

  n_cdc_events -.->|normalize_currency| n_cdc_events

  %% Gold
  n_consolidated -->|build_portfolio_holdings| g_holdings

  n_cdc_events -->|build_dividend_income| g_dividends
  n_cdc_events -->|build_interest_income| g_interest
  n_cdc_events -->|build_cash_flow_summary| g_cashflow
```

---

# Report Lineage

```mermaid
flowchart TD
  classDef gold fill:#e8f5e9,stroke:#81c784,color:#333
  classDef chart fill:#fff3e0,stroke:#ffb74d,color:#333

  g_holdings["portfolio_holdings"]:::gold
  g_dividends["dividend_income"]:::gold
  g_interest["interest_income"]:::gold
  g_cashflow["cash_flow_summary"]:::gold
  g_quality["data_quality"]:::gold

  c_alloc["Allocation by Broker"]:::chart
  c_positions["Positions"]:::chart
  c_ccy["Currency Exposure"]:::chart
  c_income["Passive Income Timeline"]:::chart
  c_cashflow["Cash Flow Breakdown"]:::chart
  c_dq["Data Quality Summary"]:::chart

  g_holdings -->|load_portfolio_holdings| c_alloc
  g_holdings -->|load_portfolio_holdings| c_positions
  g_holdings -->|load_portfolio_holdings| c_ccy

  g_dividends -->|load_dividend_income| c_income
  g_interest -->|load_interest_income| c_income

  g_cashflow -->|load_cash_flow_summary| c_cashflow

  g_quality -->|load_data_quality| c_dq
```

---

## Notes

* **Snapshot vs CDC tracks never merge.** `consolidated_holdings` is built from broker position snapshots, while `cdc_events` is built from transaction history.
* **`normalize_currency()` enriches `cdc_events` in place**, adding `target_fx_rate`, `target_value`, and `target_ccy`.
* **Gold value columns are Fernet-encrypted.** Monetary values (`security_value`, `target_value`, `cash_amount`) are stored as `pa.binary()`. Metadata columns remain plaintext.
* **Allocation charts** use the plaintext `percentage` column and therefore do not require decryption.
* **Data quality** is a validation stage that scans every normalized and gold table before producing the `data_quality` report. It is included in the storage lineage as a gold table, but its validation edges (reading all tables) are omitted for clarity.

