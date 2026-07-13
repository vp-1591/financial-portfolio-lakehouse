# ADR 0070: Embed Validation in Pipeline and Selective Report Sections

## Context

The data quality framework (ADR 0064) provides `pipeline validate` as a standalone CLI command, but validation is not wired into the Step Functions pipeline. Demo CI (`deploy-staging.yml`) goes green even when silver data is corrupted, and `pipeline report` shows empty or misleading charts after a failed pipeline run.

The roadmap (`docs/roadmaps/pipeline-validation.md`) plans to embed scoped validation into the existing ECS commands (`cmd_run_connector`, `cmd_run_consolidate_analytics`) so that FAIL-level checks fail the Step Function task via non-zero exit — without adding new ASL states or extra container cold starts.

Additionally, `pipeline report` needs to degrade gracefully: hiding only the sections whose underlying data is broken or missing, while always showing the Data Quality section.

## Decision

1. **Scoped validation**: Added `tables: list[str] | None = None` parameter to `run_validation()`. When `None`, validates all 13 registered tables (7 silver + 5 gold, excluding `data_quality` which is the output table). When specified, validates only the named tables; unknown names produce a WARN.

2. **Per-connector table registrations**: Registered 6 per-connector tables (`ibkr_snapshot`, `trading212_snapshot`, `xtb_snapshot`, `ibkr_cdc`, `trading212_cdc`, `xtb_cdc`) in `TABLE_SCHEMAS`, `FRESHNESS_COLUMNS`, and `REQUIRED_FIELDS`. Removed `data_quality` from all registries (it is the output, not an input).

3. **Embedded validation in pipeline commands**:
   - `cmd_run_connector`: after transform, calls `run_validation(tables=[f"{connector.name}_snapshot", f"{connector.name}_cdc"])`. FAIL exits 1 (fails ECS task → Step Function `States.TaskFailed`). WARN exits 0 (default `fail_on_warn=False`).
   - `cmd_run_consolidate_analytics`: validates silver tables after consolidate+CDC (`consolidated_holdings`, `cdc_events`), then gold tables after analytics (`portfolio_allocation`, `portfolio_holdings`, `dividend_income`, `interest_income`, `cash_flow_summary`). Silver FAIL prevents analytics from running.
   - `--tables` flag added to `pipeline validate` CLI for scoped runs.

4. **Selective section hiding in report**: Each report section is conditionally shown based on whether its dependency tables have data and no FAIL-level DQ checks. The Data Quality section is always shown. Sections hidden: portfolio-summary (depends on `portfolio_holdings`/`portfolio_allocation`), passive-income (depends on `dividend_income`/`interest_income`), cash-flow (depends on `cash_flow_summary`).

5. **13-table default scope**: `data_quality` is excluded from validation since it is the table where results are written (circular dependency). The 13 validated tables are: `consolidated_holdings`, `cdc_events`, `ibkr_snapshot`, `trading212_snapshot`, `xtb_snapshot`, `ibkr_cdc`, `trading212_cdc`, `xtb_cdc`, `portfolio_allocation`, `portfolio_holdings`, `dividend_income`, `interest_income`, `cash_flow_summary`.

## Constraints

- No changes to the Step Functions ASL definition — validation is embedded in existing commands, not a new state.
- `pipeline validate` with no arguments must still validate all tables (now 13, not 7) — no regression.
- Existing tests must continue to pass with `fail_on_warn=False` (missing per-connector tables only WARN).

## Consequences

- FAIL-level checks in connector tasks surface as `States.TaskFailed` in CI (ADR 0062).
- `pipeline report` after a failed run shows only the DQ section (if all analytics tables failed) or a mix of valid sections and DQ (if only some tables failed).
- Per-connector CDC tables that don't exist yet (e.g., `xtb_cdc`) produce WARNs on `pipeline validate` with no args — acceptable with default `fail_on_warn=False`.
- The `data_quality` table itself is no longer validated. If its schema drifts, it won't be caught by the framework (but schema changes to it are controlled by the codebase).

## Validation

- `pytest tests/test_quality.py` — scoped validation, per-connector tables, unknown table WARN
- `pytest tests/test_run_subcommands.py` — `cmd_run_connector` calls validation after transform; `cmd_run_consolidate_analytics` calls validation before and after analytics
- `pytest tests/test_report.py` — failed table hides its section; all tables failed shows only DQ
- `ruff check --fix . && ruff format .` — no lint issues
- `pytest tests/ -q` — all 560 tests pass