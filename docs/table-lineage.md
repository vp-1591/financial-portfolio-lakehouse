# Table Lineage

Pipeline data flow from raw ingestion through normalization to analytics and reports.

```mermaid
flowchart LR
  subgraph raw["Raw Layer (encrypted)"]
    r_ibkr_snap["ibkr_snapshot"]
    r_ibkr_cdc["ibkr_cdc"]
    r_t212_snap["trading212_snapshot"]
    r_t212_cdc["trading212_cdc"]
    r_xtb_snap["xtb_snapshot"]
    r_xtb_cdc["xtb_cdc"]
  end

  subgraph normalized["Normalized Layer (encrypted values)"]
    n_ibkr_snap["ibkr_snapshot"]
    n_ibkr_cdc["ibkr_cdc"]
    n_t212_snap["trading212_snapshot"]
    n_t212_cdc["trading212_cdc"]
    n_xtb_snap["xtb_snapshot"]
    n_xtb_cdc["xtb_cdc"]
    n_consolidated["consolidated_holdings"]
    n_cdc_events["cdc_events"]
  end

  subgraph gold["Analytics / Gold Layer (decrypted)"]
    g_holdings["portfolio_holdings<br/>(ticker, broker, security_ccy,<br/>security_value, target_value,<br/>target_ccy, percentage, position_type)"]
    g_dividends["dividend_income"]
    g_interest["interest_income"]
    g_cashflow["cash_flow_summary"]
    g_quality["data_quality"]
  end

  subgraph charts["Report Charts"]
    c_alloc["Allocation by Broker"]
    c_positions["Positions"]
    c_ccy["Currency Exposure"]
    c_income["Passive Income Timeline"]
    c_cashflow["Cash Flow Breakdown"]
    c_dq["Data Quality Summary"]
  end

  %% Raw → Normalized (1:1 per connector per layer)
  r_ibkr_snap -->|transform_snapshot| n_ibkr_snap
  r_ibkr_cdc -->|transform_cdc| n_ibkr_cdc
  r_t212_snap -->|transform_snapshot| n_t212_snap
  r_t212_cdc -->|transform_cdc| n_t212_cdc
  r_xtb_snap -->|transform_snapshot| n_xtb_snap
  r_xtb_cdc -->|transform_cdc| n_xtb_cdc

  %% Snapshot track: N:1 into consolidated_holdings
  n_ibkr_snap -->|extract_holdings| n_consolidated
  n_t212_snap -->|extract_holdings| n_consolidated
  n_xtb_snap -->|extract_holdings| n_consolidated

  %% CDC track: N:1 into cdc_events
  n_ibkr_cdc -->|consolidate_cdc_events| n_cdc_events
  n_t212_cdc -->|consolidate_cdc_events| n_cdc_events
  n_xtb_cdc -->|consolidate_cdc_events| n_cdc_events

  %% cdc_events self-loop: normalize_currency enriches in-place
  n_cdc_events -.->|normalize_currency| n_cdc_events

  %% Consolidated → Gold
  n_consolidated -->|build_portfolio_holdings| g_holdings

  %% cdc_events → Gold
  n_cdc_events -->|build_dividend_income| g_dividends
  n_cdc_events -->|build_interest_income| g_interest
  n_cdc_events -->|build_cash_flow_summary| g_cashflow

  %% Data quality reads all tables (dotted = validation, not data flow)
  n_ibkr_snap -.->|validate| g_quality
  n_ibkr_cdc -.->|validate| g_quality
  n_t212_snap -.->|validate| g_quality
  n_t212_cdc -.->|validate| g_quality
  n_xtb_snap -.->|validate| g_quality
  n_xtb_cdc -.->|validate| g_quality
  n_consolidated -.->|validate| g_quality
  n_cdc_events -.->|validate| g_quality
  g_holdings -.->|validate| g_quality
  g_dividends -.->|validate| g_quality
  g_interest -.->|validate| g_quality
  g_cashflow -.->|validate| g_quality

  %% Gold → Report charts
  g_holdings -->|load_portfolio_holdings| c_alloc
  g_holdings -->|load_portfolio_holdings| c_positions
  g_holdings -->|load_portfolio_holdings| c_ccy
  g_dividends -->|load_dividend_income| c_income
  g_interest -->|load_interest_income| c_income
  g_cashflow -->|load_cash_flow_summary| c_cashflow
  g_quality -->|load_data_quality| c_dq

  %% Styles
  classDef raw fill:#fce4ec,stroke:#e57373,color:#333
  classDef norm fill:#e3f2fd,stroke:#64b5f6,color:#333
  classDef gold fill:#e8f5e9,stroke:#81c784,color:#333
  classDef chart fill:#fff3e0,stroke:#ffb74d,color:#333
  class r_ibkr_snap,r_ibkr_cdc,r_t212_snap,r_t212_cdc,r_xtb_snap,r_xtb_cdc raw
  class n_ibkr_snap,n_ibkr_cdc,n_t212_snap,n_t212_cdc,n_xtb_snap,n_xtb_cdc,n_consolidated,n_cdc_events norm
  class g_holdings,g_dividends,g_interest,g_cashflow,g_quality gold
  class c_alloc,c_positions,c_ccy,c_income,c_cashflow,c_dq chart
```

## Notes

- **Snapshot vs CDC tracks never merge.** `consolidated_holdings` comes from broker
  position snapshots; `cdc_events` comes from transaction history. They feed separate
  gold tables.
- **`cdc_events` self-references.** `normalize_currency()` reads, enriches, and overwrites
  the same table (adds `target_fx_rate`, `target_value`, `target_ccy`).
- **Allocation charts and the positions chart** read from `portfolio_holdings`. The
  donut charts group by `broker` or `security_ccy` and sum `target_value`; the
  positions chart renders each row individually using the `percentage` column.
- **Data quality** (dotted lines) reads all normalized and gold tables but is not a
  data-flow dependency.