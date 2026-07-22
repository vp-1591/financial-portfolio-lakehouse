# TODO: Debug Prod Pipeline After Deploy

Context: `run-consolidate-analytics` was failing in prod. Root causes were:

1. **Fixed**: Step Functions was sending `run-consolidate-allocate` (old command name). Terraform was applied to use `run-consolidate-analytics`.
2. **Fixed**: `scripts/run_prod_pipeline.py` hardcoded task definition revision `:1`, which became inactive after Terraform created `:2`. Removed revision numbers so ECS uses latest active.
3. **NOT fixed in code — needs prod run to verify**: The `cmd_analytics` function returns exit code 1 when CDC tables fail. This is **correct behavior** — `cdc_events` is created by `_consolidate_cdc()` which runs before `cmd_analytics` in the same command. If CDC builders fail, that's a real error, not a missing-table situation.

## What to do

1. Deploy the code changes (Docker image build + push to ECR)
2. Re-run the prod pipeline: `python scripts/run_prod_pipeline.py`
3. Check the Step Functions execution result
4. If `run-consolidate-analytics` still fails, check CloudWatch logs for the specific CDC builder error — it's likely a real data/schema issue, not a missing table