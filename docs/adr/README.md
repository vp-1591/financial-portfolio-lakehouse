# Architecture Decision Records

This index tracks all ADRs in `docs/adr/`. Run `/optimize-adrs` to update it.

| Field | Value |
|-------|-------|
| last-indexed | 2026-07-13T00:00:00+02:00 |

## Index

| ADR | Title | Created | Status | Superseded by |
|-----|-------|---------|--------|---------------|
| 0001 | Disable Pytest Cache Provider | 2026-06-15 | active | — |
| 0002a | Add Consolidate Step and Fix Duplicates | 2026-06-26 | active | — |
| 0002b | Use Broker-Native Identifiers in Portfolio Report | 2026-06-15 | active | — |
| 0003 | Medallion Architecture Pipeline | 2026-06-19 | superseded | 0084 |
| 0004 | Pipeline End-to-End Bugfixes | 2026-06-21 | active | — |
| 0005 | Pipeline End-to-End Bugfixes (Round 2) | 2026-06-21 | active | — |
| 0006 | Replace IBKR Client Portal Gateway with Flex Web Service | 2026-06-25 | superseded | 0029 |
| 0007 | Make Trading 212 Account IDs Optional and Update Documentation | 2026-06-25 | superseded | 0029 |
| 0010 | Fix Trading 212 Authentication Method | 2026-06-25 | active | — |
| 0011 | Standardize Trading 212 CLI Args to t212 Prefix | 2026-06-25 | superseded | 0018 |
| 0012 | Add Pipeline Data Flow Diagram to README | 2026-06-26 | active | — |
| 0013a | Add IBKR Flex Web Service Connector to Pipeline | 2026-06-26 | active | — |
| 0013b | Replace Pandas with Polars in Transform Pipeline | 2026-06-27 | active | — |
| 0014 | Remove Pandas from Query Module, Use Polars Throughout | 2026-06-27 | active | — |
| 0015 | Configure Data Paths with Environment-Aware Storage | 2026-06-28 | active | — |
| 0016 | GitHub Actions CI | 2026-06-28 | superseded | 0056 |
| 0017 | Fixture Data for Transformation Tests | 2026-06-28 | active | — |
| 0018 | Bitwarden Secrets and YAML Config | 2026-06-28 | superseded | 0019, 0020 |
| 0019 | S3 Storage and GitHub Secrets | 2026-06-28 | active | — |
| 0020 | Remove YAML Config, Use Environment Variables | 2026-06-30 | active | — |
| 0021 | Fix Deltalake S3 URI Handling | 2026-06-30 | active | — |
| 0022 | Fix Review Findings in S3 Storage PR | 2026-06-30 | active | — |
| 0023 | Add Ruff Linter to CI and Clean Up Argparse | 2026-06-30 | active | — |
| 0024 | Fix DuckDB S3 Credential Propagation for delta_scan | 2026-06-30 | active | — |
| 0025 | Table Aliases, Auto-Discovery, and S3 Credential Fix | 2026-06-30 | active | — |
| 0026 | Remove Allocation Table and Row Counts from Pipeline Output | 2026-06-30 | active | — |
| 0027 | Query API Redesign — Native DuckDB Connection, Decrypt Utility, Drop Wrappers | 2026-07-01 | active | — |
| 0028 | Remove the scripts/ Folder | 2026-07-01 | active | — |
| 0029 | Remove Redundant Environment Variables and IBKR Gateway Dead Code | 2026-07-01 | active | — |
| 0030 | Require IBKR Flex Query ID and Rename PORTFOLIO_ENCRYPTION_KEY | 2026-07-01 | active | — |
| 0031 | Make decrypt_df Auto-Detect Encrypted Columns | 2026-07-01 | active | — |
| 0032a | ADR Index and Optimize-adrs Workflow | 2026-07-01 | active | — |
| 0032b | Docker Support and Query CLI Subcommand | 2026-07-01 | active | — |
| 0033 | Migrate Terraform State from Local to S3 Backend | 2026-07-01 | active | — |
| 0034 | Add required_version Constraint for Terraform 1.11+ | 2026-07-01 | active | — |
| 0035 | Remove dead cashBalance and startingCash fallbacks from IBKR transform | 2026-07-02 | active | — |
| 0036 | Remove conid and side from IBKR pipeline | 2026-07-02 | active | — |
| 0037 | Demo Mode with _DEMO Secrets and Isolated Storage | 2026-07-02 | active | — |
| 0038 | Demo Terraform Infrastructure in Separate Directory | 2026-07-02 | active | — |
| 0039 | STORAGE_TYPE Env Var and resolve_secret Credential Isolation | 2026-07-02 | active | — |
| 0040 | Consolidate AWS Credentials and Fix Demo Isolation Bugs | 2026-07-02 | active | — |
| 0041 | Step-Level CI Secrets and Explicit Empty Credentials | 2026-07-02 | superseded | 0055 |
| 0042 | Fix Demo Bucket Naming — Use Hyphen Instead of Underscore | 2026-07-02 | active | — |
| 0043 | Fix Empty-String Env Var Fallback and Broaden Demo IAM Policy | 2026-07-02 | active | — |
| 0044 | S3_BUCKET_DEMO Standalone — Demo Cloud Storage Without S3_BUCKET | 2026-07-02 | active | — |
| 0045 | Replace List-Append Pattern with Polars build_normalized_table | 2026-07-02 | active | — |
| 0046 | Fix Consolidated Holdings Currency Column | 2026-07-03 | active | — |
| 0047 | Move XLSX Parsing to Silver Layer and Remove account_id from Raw Schema | 2026-07-03 | active | — |
| 0048 | XTB Cloud Upload — S3 Staging + EventBridge | 2026-07-06 | active | — |
| 0049 | Deployment Model — Branch/Tag Environment Strategy | 2026-07-07 | superseded | 0063 |
| 0050 | Attach ECR Policy to Pipeline User in Terraform | 2026-07-07 | active | — |
| 0051 | Step Functions Orchestration | 2026-07-08 | superseded | 0052, 0054 |
| 0052 | Per-Environment State Machines and CI/CD Pipeline Trigger | 2026-07-08 | drifted | — |
| 0053 | Per-Environment CI/CD Credentials and IAM Permissions | 2026-07-09 | drifted | — |
| 0054 | Public-Subnet ECS — Eliminate Perma-VPC Charges | 2026-07-09 | active | — |
| 0055 | IAM Role Credential Fallback for ECS Tasks | 2026-07-09 | active | — |
| 0056 | Fix CI Push Branch Filter to Eliminate Duplicate Runs | 2026-07-09 | active | — |
| 0057 | Fix Bronze→Silver Dedup and Cash Extraction Bugs | 2026-07-10 | active | — |
| 0058 | Broker-Neutral CDC Events Schema | 2026-07-10 | superseded | 0077 |
| 0059 | Fix T212 CDC Paginated Response Bug and Test Isolation Leak | 2026-07-10 | active | — |
| 0060 | Fix T212 CDC Transform for Nested JSON Structures | 2026-07-11 | active | — |
| 0061 | Handle Missing Struct Fields in T212 CDC Transform | 2026-07-11 | active | — |
| 0062 | Track Step Function Execution Status in Deploy Workflow | 2026-07-11 | active | — |
| 0063 | Simplify ECR Tagging Strategy | 2026-07-12 | active | — |
| 0064 | Data Quality Framework | 2026-07-12 | superseded | 0070 |
| 0065 | CDC Analytics Tables and Unified Analytics Command | 2026-07-12 | active | — |
| 0066 | Portfolio Holdings Gold Table and Report Generation | 2026-07-13 | superseded | 0082 |
| 0067 | Fix Step Function Failure Logging in Deploy Workflow | 2026-07-13 | superseded | 0081 |
| 0068 | Add Gitleaks Secret Scanning to CI | 2026-07-13 | active | — |
| 0069 | Fix IBKR CDC Triplication and Date Parsing | 2026-07-13 | active | — |
| 0070 | Embed Validation in Pipeline and Selective Report Sections | 2026-07-13 | active | — |
| 0071 | IBKR Demo Initial Deposit Injection | 2026-07-14 | active | — |
| 0072 | DQ Table Overwrite and Empty-Table Freshness | 2026-07-14 | active | — |
| 0073 | Currency Exposure Donut Chart (Phase 1) | 2026-07-14 | active | — |
| 0074 | Remove Overloaded Currency Column, Rename to Explicit Names | 2026-07-14 | superseded | 0077 |
| 0075 | Cash Flow Breakdown Outlier Toggle | 2026-07-14 | active | — |
| 0076 | Fix T212 walletImpact.fxRate Usage (Phase 1: Currency Unification) | 2026-07-14 | superseded | 0077 |
| 0077 | Currency Unification Phase 2 — Schema Redesign | 2026-07-14 | active | — |
| 0078 | Currency Unification Phase 3: Data Quality Fixes | 2026-07-14 | active | — |
| 0079 | Fix IBKR Demo Deposit Currency After Phase 2 Rename | 2026-07-15 | active | — |
| 0080 | Store security_value and position_type in Consolidated Holdings; Add instrument_ccy to CDC Events | 2026-07-15 | active | — |
| 0081 | Fix Deploy Log Readability in GitHub Actions | 2026-07-15 | active | — |
| 0082 | Fold portfolio_allocation into portfolio_holdings | 2026-07-15 | active | — |
| 0083 | Replace "Allocation by Position Type" donut with "Positions" bar chart and EQUITY/CASH summary card | 2026-07-15 | active | — |
| 0084 | Encrypt Gold Value Columns | 2026-07-15 | active | — |
| 0085 | Gold Schema Migration and Analytics Error Propagation | 2026-07-16 | active | — |

<!-- Duplicate-number mapping
  0002a → 0002-add-consolidate-step-and-fix-duplicates.md
  0002b → 0002-use-broker-native-identifiers-in-portfolio-report.md
  0013a → 0013-add-ibkr-flex-connector-to-pipeline.md
  0013b → 0013-replace-pandas-with-polars-in-transform.md
  0032a → 0032-adr-index-and-optimize-workflow.md
  0032b → 0032-docker-and-query-cli.md
-->

<!-- Superseded ADRs without files (merged before creation):
  0008 → planned as "update-readme-trading212-requirements", merged into ADR 0007
  0009 → planned as "make-trading212-net-worth-account-id-optional", merged into ADR 0007
-->