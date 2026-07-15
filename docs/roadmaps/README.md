# Roadmap Index

## Index

| Slug | Title | Created | Status | Notes |
|------|-------|---------|--------|-------|
| productionization | Pipeline Architecture & Productionization | 2026-06-28 | active | Phases 1–2 done, 3–5 planned |
| xtb-cloud-upload | XTB Cloud Upload | 2026-07-06 | active | Phase 1 done |
| transform-cash-and-dedup | Fix Bronze–Silver Dedup and Cash Extraction | 2026-07-10 | done | All phases complete |
| cdc-tables | CDC Tables | 2026-07-10 | active | Phase 1 planned |
| standardize-silver-schema | Standardize Silver Schema | — | active | Note only, no phases |
| reporting-baseline | Reporting Baseline with Current Data | 2026-07-12 | active | Current-state report from snapshots + CDC |
| market-data-reporting | Market Data Integration and Performance Reporting | 2026-07-12 | active | Builds on reporting-baseline; adds performance charts |
| pipeline-validation | Pipeline Validation in Step Functions | 2026-07-13 | active | Embed validation in pipeline steps; report on failure |
| currency-column-clarity | Currency Column Clarity and Allocation Chart Fix | 2026-07-14 | active | Remove overloaded `currency` column; fix currency exposure chart |
| currency-unification | Currency Unification — Store in Security Currency, Convert to Target | 2026-07-14 | active | Store values in security_ccy, add target_value/target_ccy, remove value_currency/base_currency/fx_rate_to_base |
| gold-cleanup-positions-screenshots | Gold Table Cleanup, Positions Chart & README Screenshots | 2026-07-15 | active | Fold portfolio_allocation into portfolio_holdings, add positions chart, confirm gold encryption policy, add screenshots |