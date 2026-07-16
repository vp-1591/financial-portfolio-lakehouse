# ADR 0085: Gold Schema Migration and Analytics Error Propagation

## Context

PR #80 (ADR 0082 + ADR 0084) changed the gold-layer schemas to encrypt value columns as `pa.binary()` and added a `percentage` column to `portfolio_holdings`. After merging, the staging deploy failed because the quality checks flagged four schema mismatches:

- `portfolio_holdings`: `security_value` and `target_value` were `double` (expected `binary`); `percentage` column was missing
- `dividend_income`, `interest_income`, `cash_flow_summary`: `cash_amount` and `target_value` were `double` (expected `binary`)

The root cause: existing Delta tables on S3 still had the old schema. The `build_portfolio_holdings` function writes in `mode="overwrite"`, but the Step Function orchestrator calls `cmd_analytics`, which **catches all exceptions and returns 0**. This means that when `build_portfolio_holdings` fails (e.g., because `consolidated_holdings` is not yet populated), the pipeline continues to validate the stale gold tables — which then fail the schema check.

Two separate but related problems:
1. Pre-existing Delta tables need a one-time migration to match the new schemas
2. `cmd_analytics` swallows errors, making failures invisible to the orchestrator

## Decision

1. **Create a migration script** (`pipeline/migrations/migrate_001_encrypt_gold_values.py`) that reads each gold table, encrypts value columns from `float64` → `binary`, adds the `percentage` column where missing, and writes back with `mode="overwrite"`. The script is idempotent: tables that already match the target schema are skipped.

2. **Fix `cmd_analytics`** to return 1 when any build step fails, instead of always returning 0. Each builder (`build_portfolio_holdings`, `build_dividend_income`, etc.) is tracked individually so that one failure doesn't prevent others from running, but any failure causes the overall command to exit with status 1.

3. **Fix `percentage` null-vs-required contradiction**: change the division-by-zero guard in `build_portfolio_holdings` from `pl.lit(None)` to `pl.lit(0.0)`. A zero-value portfolio has 0% allocation for every position, which is semantically valid and avoids nulls in a `REQUIRED_FIELDS` column.

4. **Expose the migration as a CLI command** (`run-migration`) so it can be run manually or as a Step Function task.

## Constraints

- The migration must be idempotent: running it twice must be safe
- The migration must work with both S3 and local storage backends
- `cmd_analytics` must still attempt all builders even if some fail — we want partial success, not early termination
- The `percentage` column must never be null because it's in `REQUIRED_FIELDS` for `portfolio_holdings`

## Consequences

- **Positive**: Deploy can succeed against existing data; `cmd_analytics` failures are now visible to the orchestrator; no null percentage values
- **Negative**: The migration is a one-time manual step — future schema changes need new migration scripts (captured in CLAUDE.md)
- **Follow-up**: The Step Function definition should add a `run-migration` task that runs before the analytics step, so migrations are automatic on deploy

## Validation

- Run `pipeline/migrations/migrate_001_encrypt_gold_values.py` against the staging data
- Verify that all four gold tables pass `check_schema` and `check_required_nulls` after migration
- Confirm that `cmd_analytics` returns 1 when `build_portfolio_holdings` fails
- Confirm that `percentage` is `0.0` (not null) when `total_target == 0`