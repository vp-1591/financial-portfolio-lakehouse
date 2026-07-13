# Roadmap: Pipeline Validation in Step Functions

## Goal

Embed data quality validation into the Step Functions pipeline so that corrupted
silver data cannot silently propagate into gold tables, and both demo and prod
CI fail visibly when quality checks fail. Validation runs inside existing ECS
tasks (not as separate tasks) to avoid container startup overhead. When the
pipeline has failed, `pipeline report` still generates a usable report, hiding
only the sections whose data is broken or missing and always showing the Data
Quality section.

## Current state

The data quality framework exists (ADR 0064) with `pipeline validate` as a
standalone CLI command that checks 7 tables (2 silver, 5 gold) across 5 check
types (schema, required nulls, row-count stability, freshness, reconciliation).
Results persist to the `data_quality` Delta table in append mode. Note: after
Phase 1, the default scope expands to 13 tables (7 silver + 5 gold, excluding
`data_quality` which is the output table).

However, **validation is not wired into the Step Functions pipeline**. ADR 0051
explicitly deferred "Data quality gates (Phase 3)" and ADR 0064 deliberately
kept `pipeline validate` standalone. The state machine has two states:
`RunConnectors` (Map over connectors, each runs fetch+transform) and
`ConsolidateAllocate` (consolidate + CDC consolidate + analytics). Neither
calls validation.

Consequences of this gap:

- Demo CI (deploy-staging.yml) triggers the Step Function, waits for it to
  succeed, and goes green — even if silver data is corrupted. The report shows
  "No validation results available" and empty charts.
- Gold tables are built from silver data that has never been checked. A schema
  mismatch or null required field in `consolidated_holdings` silently propagates
  to `portfolio_allocation`, `dividend_income`, `interest_income`, and
  `cash_flow_summary`.
- `pipeline report` is run locally after the pipeline completes. When validation
  was never run, the Data Quality section is empty. When the pipeline has failed,
  the report may be incomplete or misleading because analytics tables are
  missing or corrupted.

Relevant ADRs: 0051 (Step Functions orchestration), 0052 (per-env state
machines), 0062 (SFN status tracking in CI), 0064 (data quality framework),
0065 (CDC analytics tables).

## Success criteria

- [ ] Running `cmd_run_connector` (as used by Step Functions) validates that
  connector's normalized tables after transform. A FAIL-level check causes the
  ECS task to exit non-zero, which fails the Step Function execution.
- [ ] Running `cmd_run_consolidate_analytics` validates silver tables
  (consolidated_holdings, cdc_events) after consolidate, before analytics runs.
  A FAIL-level check prevents analytics from running.
- [ ] Running `cmd_run_consolidate_analytics` validates gold tables
  (portfolio_allocation, portfolio_holdings, dividend_income, interest_income,
  cash_flow_summary) after analytics. A FAIL-level check causes non-zero exit.
- [ ] `pipeline validate` still works as a standalone command with no arguments
  (validate all tables) — no regression.
- [ ] Demo CI (deploy-staging.yml) shows a red run when any FAIL-level check
  fires, using the existing Step Function status tracking from ADR 0062.
- [ ] `pipeline report` generates a report even after a failed pipeline run. If
  analytics tables are missing or validation has failed for a specific table,
  only the sections that depend on that table are hidden. The Data Quality
  section is always shown.
- [ ] `pytest tests/test_quality.py` passes with scoped validation and
  per-connector table registrations.
- [ ] No changes to the Step Functions ASL definition — validation is embedded
  in existing commands, not a new state.

## Alternatives considered

| Approach | Why rejected |
|----------|-------------|
| Separate `Validate` ECS task in the state machine | Adds ~30-60s container cold start per validation point (3 extra cold starts: after connectors, after consolidate, after analytics). ADR 0051 discussed this — the current Map+Task design avoids cold starts by embedding fetch+transform in one task. |
| Single validation at the end of the pipeline | Silver problems propagate into gold tables before being caught. The whole point is early detection — you don't want to build analytics on top of corrupted consolidated data. |
| Keep validate as a standalone command (current state) | Demo CI goes green even when data is corrupted. The report shows empty Data Quality section because validation was never run. |
| Validate only gold tables | Misses the silver layer entirely. Schema drift or nulls in `consolidated_holdings` or `cdc_events` would propagate undetected. |

## Phases

### Phase 1 — Embed validation in pipeline and report on failure *[status: planned]*

Add scoped validation so each pipeline step validates its own outputs. Embed
validation calls into `cmd_run_connector` and `cmd_run_consolidate_analytics`
at the appropriate points, respecting the FAIL/WARN semantics from ADR 0064
(FAIL → non-zero exit, WARN → log warning). Make `pipeline report` handle
partial data gracefully by hiding only sections whose data is broken or
missing, always showing the Data Quality section.

**Scope:**
- [ ] Add `tables: list[str] | None = None` parameter to `run_validation()`.
  When `None`, validate all registered tables (current behavior). When
  specified, validate only the named tables.
- [ ] Add `--tables` flag to `pipeline validate` CLI subcommand that forwards
  to `run_validation(tables=...)`.
- [ ] Register per-connector normalized table schemas in `TABLE_SCHEMAS`,
  `FRESHNESS_COLUMNS`, and `REQUIRED_FIELDS` in `quality.py`. Tables:
  `ibkr_snapshot`, `ibkr_cdc`, `trading212_snapshot`, `trading212_cdc`,
  `xtb_snapshot`, `xtb_cdc`. Schemas come from `pipeline.normalized.models`.
- [ ] Embed validation in `cmd_run_connector`: after transform, call
  `run_validation(tables=[f"{connector.name}_snapshot", f"{connector.name}_cdc"])`.
  If exit code is non-zero, return it immediately (connector task fails).
- [ ] Embed validation in `cmd_run_consolidate_analytics`: after consolidate
  and CDC consolidation, call
  `run_validation(tables=["consolidated_holdings", "cdc_events"])`. If
  non-zero, return before running analytics. After analytics, call
  `run_validation(tables=["portfolio_allocation", "portfolio_holdings",
  "dividend_income", "interest_income", "cash_flow_summary"])`. If non-zero,
  return it.
- [ ] Make `pipeline report` handle partial or failed data: when analytics
  tables are missing or validation results show FAIL-level issues, render only
  the Data Quality section (with check details) instead of showing empty or
  misleading charts.
- [ ] Update `tests/test_quality.py` to cover scoped validation and
  per-connector table checks.
- [ ] Update `tests/test_run_subcommands.py` to verify that
  `cmd_run_connector` and `cmd_run_consolidate_analytics` call validation
  between steps.

**Out of scope:**
- Changing the Step Functions ASL definition (validation is embedded in
  existing commands, not a new state)
- Adding a report generation step to Step Functions (the report is generated
  locally; CI shows validation failures in task logs)
- Email or SNS alerting on validation failures (deferred to productionization
  step 5 per ADR 0051)
- Value reconciliation (requires `netLiquidationValue` from raw data, deferred
  per ADR 0064)

**Files:** `pipeline/analytics/quality.py`, `pipeline/run.py`,
`pipeline/report/renderer.py`, `pipeline/report/templates/report.html`,
`tests/test_quality.py`, `tests/test_run_subcommands.py`

**Links:** ADR 0051, ADR 0064