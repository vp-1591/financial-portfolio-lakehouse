# Roadmap Index

## Index

| # | Slug | Title | Created | Status | Notes |
|---|------|-------|---------|--------|-------|
| 0001 | productionization | Pipeline Architecture & Productionization | 2026-06-28 | active | Phases 1–2 done, 3–5 planned |
| 0002 | xtb-cloud-upload | XTB Cloud Upload | 2026-07-06 | active | Phase 1 done |
| 0003 | transform-cash-and-dedup | Fix Bronze–Silver Dedup and Cash Extraction | 2026-07-10 | done | All phases complete |
| 0004 | cdc-tables | CDC Tables | 2026-07-10 | active | Phase 1 planned |
| 0005 | standardize-silver-schema | Standardize Silver Schema | — | active | Note only, no phases |
| 0006 | reporting-baseline | Reporting Baseline with Current Data | 2026-07-12 | active | Current-state report from snapshots + CDC |
| 0007 | market-data-reporting | Market Data Integration and Performance Reporting | 2026-07-12 | active | Builds on reporting-baseline; adds performance charts |
| 0008 | pipeline-validation | Pipeline Validation in Step Functions | 2026-07-13 | active | Embed validation in pipeline steps; report on failure |
| 0009 | currency-column-clarity | Currency Column Clarity and Allocation Chart Fix | 2026-07-14 | active | Remove overloaded `currency` column; fix currency exposure chart |
| 0010 | currency-unification | Currency Unification — Store in Security Currency, Convert to Target | 2026-07-14 | active | Store values in security_ccy, add target_value/target_ccy, remove value_currency/base_currency/fx_rate_to_base |
| 0011 | gold-cleanup-positions-screenshots | Gold Table Cleanup, Positions Chart & README Screenshots | 2026-07-15 | active | Fold portfolio_allocation into portfolio_holdings, add positions chart, confirm gold encryption policy, add screenshots |
| 0012 | simplify-pipeline-execution | Simplify Pipeline Execution Model | 2026-07-22 | active | Replace DEMO+STORAGE_TYPE with --mode flag; make full trigger SFN in staging/prod |
| 0013 | draft | Polars migration draft notes | 2026-07-11 | draft | Out-of-scope notes from CDC work |