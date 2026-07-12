# 0064: Data Quality Framework

## Context

The pipeline has no data quality validation. The productionization roadmap (step
3) requires quality gates before reporting, but there is no framework to define,
run, or persist quality checks. Without validation, schema mismatches, null
required fields, stale data, or sudden row-count drops can propagate silently
into analytics and reports.

The pipeline runs on Step Functions (ADR 0052). When a step fails, the Step
Function execution is marked FAILED and surfaced in CI via ADR 0062's status
tracking. This provides a natural communication path for FAIL-level quality
issues without adding email or SNS alerting.

## Decision

1. **Two-level severity model** — each quality check produces one of three
   statuses:

   | Status | When | Pipeline behavior |
   |--------|------|-------------------|
   | **FAIL** | Schema mismatch, nulls in required fields | `pipeline validate` exits non-zero → Step Function marks execution FAILED → visible in CI (ADR 0062) |
   | **WARN** | Row count drop >50%, stale data, structural reconciliation mismatch | Pipeline continues, logs warning |
   | **PASS** | Check succeeds | No action needed |

2. **Diagnostic-only checks** — checks report problems but never drop or filter
   data. Corrective action is a separate concern.

3. **Five checks per validated table:**
   - **schema** (FAIL) — column names and types match the expected PyArrow schema.
   - **required_nulls** (FAIL) — no nulls in non-nullable fields.
   - **row_count_stability** (WARN) — row count hasn't dropped >50% vs. previous
     run. First run → PASS (no previous count to compare).
   - **freshness** (WARN) — latest timestamp is within a configurable window
     (default 7 days).
   - **reconciliation** (WARN) — structural/coverage only: every broker in
     `consolidated_holdings` appears in `cdc_events`, and holdings currency set
     ⊆ CDC currency set. True value reconciliation (sum of positions ≈ net
     liquidation) is not achievable today because `netLiquidationValue` lives
     only in raw IBKR Flex XML, not in any normalized/gold table.

4. **`data_quality` Delta table** — results are stored in append mode at
   `analytics_path("data_quality")`. This is the only analytics table that
   accumulates history, making it load-bearing for the row-count stability
   check (which reads previous counts from this table).

5. **`pipeline validate` CLI subcommand** — runs all checks, prints a summary to
   stdout (PASS/WARN/FAIL counts per check), and exits non-zero on any FAIL.
   Optional `--fail-on-warn` flag escalates WARN to non-zero exit.

6. **Validated tables** — `consolidated_holdings`, `cdc_events`,
   `portfolio_allocation`. A missing table is logged "skipped, not found" — not
   a FAIL. The list will extend to new gold tables as they are added.

## Constraints

- Checks never modify, drop, or filter data — they are diagnostic only.
- No email or SNS alerting — deferred to productionization step 5.
- Reconciliation is structural only. Value reconciliation requires
  `netLiquidationValue` from raw IBKR data, which is not available in
  normalized/gold tables. Deferred to the market-data roadmap.
- `data_quality` is the only append-mode analytics table; all others use
  `mode="overwrite"`.
- The `pipeline validate` subcommand is standalone — it does not modify
  `pipeline full` (which is not used in cloud orchestration).

## Consequences

- **Positive**: Failed quality checks surface as red GitHub Actions runs via ADR
  0062's Step Function status tracking, making problems immediately visible
  without adding alerting infrastructure.
- **Positive**: Quality results are queryable via `pipeline query`, enabling
  ad-hoc inspection and future reporting.
- **Positive**: Row-count stability uses historical data from `data_quality`
  (append mode), giving trend visibility across runs.
- **Neutral**: First run of row-count stability always passes (no previous count
  to compare). This is a deliberate choice to avoid false positives on initial
  deployment.
- **Negative**: True value reconciliation is deferred to the market-data roadmap.
  The current structural check only verifies that all brokers in holdings
  appear in CDC events.
- **Negative**: The `data_quality` table grows unboundedly with each validation
  run. In practice, this is ~15–25 rows per run (3 tables × 5 checks, minus
  reconciliation for non-holdings tables) and is unlikely to be a concern.

## Validation

1. `pipeline validate --freshness-days 7` exits 0 when all checks pass on
   demo data.
2. `pipeline validate --fail-on-warn` exits 1 when any WARN exists (e.g., stale
   data).
3. `pipeline validate` exits 1 on schema mismatch or required nulls.
4. `pipeline query "SELECT * FROM data_quality_analytics"` returns persisted
   results.
5. `pytest tests/test_quality.py` passes — covering all five checks, exit codes,
   and `data_quality` round-trip.
6. Row-count stability: first run → PASS; >50% drop → WARN; stable count →
   PASS.