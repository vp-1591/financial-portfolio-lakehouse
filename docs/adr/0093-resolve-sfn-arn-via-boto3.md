# 0093 — Resolve State Machine ARN via boto3 Instead of Env Var

## Context

ADR 0091 introduced `state_machine_arn()` which reads `STAGING_STATE_MACHINE_ARN` or `PROD_STATE_MACHINE_ARN` from environment variables. Before ADR 0091, `scripts/run_prod_pipeline.py` hardcoded the ARN, and the deploy workflow passed it via `${{ secrets.DEMO_STATE_MACHINE_ARN }}`. Both approaches required manual configuration: either editing a script or creating a GitHub secret from a Terraform output.

The state machine names are predictable and well-known — `portfolio-pipeline-orchestrator-demo` (staging) and `portfolio-pipeline-orchestrator` (prod) — defined in `terraform/demo/main.tf` and `terraform/prod/main.tf`. Since the code already creates a boto3 SFN client (`build_clients`), the ARN can be discovered at runtime via `list_state_machines` using the known name, eliminating the need for any env var or secret.

## Decision

Replace `state_machine_arn(mode)` (which reads `STAGING_STATE_MACHINE_ARN`/`PROD_STATE_MACHINE_ARN` env vars) with `resolve_state_machine_arn(sfn_client, mode)` (which calls `sfn.list_state_machines` and matches on `STATE_MACHINE_NAMES[mode]`). This removes the need to set env vars or GitHub secrets for the state machine ARN — the caller only needs AWS credentials with `states:ListStateMachines` permission.

Key changes:

1. **`pipeline/sfn.py`**: New `STATE_MACHINE_NAMES` constant maps mode → name. `state_machine_arn()` replaced by `resolve_state_machine_arn(sfn_client, mode)` using the paginator for `list_state_machines`. The `os` import is removed.
2. **`pipeline/run.py`**: `build_clients()` called before ARN resolution (the session is already available). `state_machine_arn(mode)` replaced by `resolve_state_machine_arn(sfn_client, mode)`.
3. **`.github/workflows/deploy-staging.yml`**: Removed `STAGING_STATE_MACHINE_ARN` env var — no longer needed.
4. **`.env.example`**: Removed `STAGING_STATE_MACHINE_ARN` and `PROD_STATE_MACHINE_ARN` comments.
5. **`terraform/demo/main.tf`**: Added `states:ListStateMachines` IAM permission (does not support resource-level ARNs, same as `ecs:DescribeTaskDefinition`).
6. **Tests**: `TestStateMachineArn` replaced with `TestResolveStateMachineArn` using mocked SFN paginator; `TestCmdFullSfnTrigger` uses `resolve_state_machine_arn` mock instead of env var monkeypatching.

## Constraints

- The state machine names must match Terraform exactly. If a name changes, `STATE_MACHINE_NAMES` must be updated in `pipeline/sfn.py`.
- `states:ListStateMachines` is a list-level permission scoped to `"*"` — it cannot be restricted to a specific state machine ARN. This is the same pattern used for `ecs:DescribeTaskDefinition`.
- `PROD_STATE_MACHINE_ARN` was never used in CI (only local/prod manual runs). The prod CI/CD policy does not include SFN permissions (prod deploys are schedule-driven, not CI-triggered). If `full --mode prod` is run locally, the caller's IAM user needs `states:ListStateMachines` added to their policy.

## Consequences

- **No env vars or GitHub secrets needed for the ARN.** `STAGING_STATE_MACHINE_ARN` and `PROD_STATE_MACHINE_ARN` are removed from the codebase. The deploy workflow and local runs discover the ARN automatically.
- **One additional IAM permission.** `states:ListStateMachines` on `"*"` is added to the demo CI/CD policy. This is a read-only permission that lists state machine names and ARNs — no execution or state data is exposed.
- **Terraform apply required.** The new `states:ListStateMachines` statement must be applied before the next deploy; otherwise the CI user will get an authorization error when trying to start the execution.
- **ADR 0091 partially superseded.** Point 4 of ADR 0091 ("State machine ARN via env var") is superseded. The validation section reference to `STAGING_STATE_MACHINE_ARN` is also updated.

## Validation

- `tests/test_sfn.py::TestResolveStateMachineArn` — covers staging lookup, prod lookup, not-found error, and unsupported mode error.
- `tests/test_run_subcommands.py::TestCmdFullSfnTrigger` — uses `resolve_state_machine_arn` mock instead of env vars; `test_state_machine_not_found_errors` replaces the old `test_state_machine_arn_missing_errors`.
- `terraform -chdir=terraform/demo validate` passes (validates the new IAM statement).
- `ruff check --fix . && ruff format .` and `.venv/Scripts/python -m pyright pipeline/ tests/` clean.
- Manual: `python -m pipeline.run full --mode staging --wait` resolves the ARN automatically without any env var.