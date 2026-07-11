# 0062: Track Step Function Execution Status in Deploy Workflow

## Context

The `deploy-staging.yml` workflow triggers a Step Function execution via `aws stepfunctions start-execution` after building and pushing a Docker image. Because `start-execution` is asynchronous, the workflow shows green (✅) as soon as the execution is started, regardless of whether the Step Function run succeeds or fails. A failed connector or consolidate-allocate task goes unnoticed until someone checks the Step Functions console manually.

When a Step Function execution does fail, the developer has to navigate to the AWS Console, find the execution, inspect the failure event, then locate the corresponding CloudWatch log group and stream — a multi-step manual process that delays debugging.

## Decision

1. **Poll for Step Function completion** — Split the "Trigger demo pipeline" step into two: one that starts the execution and captures the `executionArn`, and another that polls `describe-execution` in a loop until the execution reaches a terminal state. The step exits 0 on `SUCCEEDED`, and exits 1 on `FAILED`, `TIMED_OUT`, or `ABORTED`. A 15-minute timeout with 30-second polling intervals prevents indefinite waits.

2. **Print execution history on failure** — When the execution fails, the polling step dumps `get-execution-history` filtered to `TaskFailed` and `ExecutionFailed` events as a table, giving immediate visibility into which step failed and why.

3. **Print container logs on failure** — A separate `if: failure()` step fetches recent CloudWatch log events from each connector's log group (`/ecs/portfolio-pipeline-demo-{name}`) using `filter-log-events` with the execution start time. This provides the full container stdout/stderr output without leaving the GitHub Actions run page.

4. **Add IAM permissions** — Extend the `pipeline-demo-cicd` policy with:
   - `states:DescribeExecution` and `states:GetExecutionHistory` scoped to executions of the demo state machine (resource ARN derived by replacing `:stateMachine:` with `:execution:` in the state machine ARN plus `:*`).
   - `logs:FilterLogEvents` scoped to all demo task log groups (connector + consolidate-allocate).

5. **Add `log_group_arn` output to the ecs-task module** — The module now exposes its CloudWatch log group ARN so the demo CI/CD policy can reference log group ARNs without hardcoding.

## Constraints

- The deploy workflow must not hang indefinitely — the 15-minute timeout ensures it always terminates.
- IAM permissions remain least-privilege: `DescribeExecution` and `GetExecutionHistory` are scoped to the demo state machine's executions only, and `FilterLogEvents` is scoped to demo task log groups only.
- XTB connector logs are not fetched because XTB is not triggered by the CI/CD pipeline (it runs on S3 file arrival via EventBridge).

## Consequences

- **Positive**: Failed Step Function executions now surface as red (❌) GitHub Actions runs, making failures immediately visible.
- **Positive**: Execution history and container logs are printed directly in the GitHub Actions log, eliminating the need to navigate to the AWS Console for basic debugging.
- **Neutral**: The deploy workflow takes longer (up to 15 minutes) because it now waits for the Step Function to complete. This is acceptable for staging — the workflow already waits for the Docker build and push, and the pipeline typically completes in 2–5 minutes.
- **Negative**: Two new IAM permissions are added to the demo CI/CD policy (`states:DescribeExecution`, `states:GetExecutionHistory`, `logs:FilterLogEvents`). These are read-only and scoped to demo resources only.
- **Negative**: The log-printing step relies on the log group naming convention (`/ecs/portfolio-pipeline-demo-{name}`). If the naming convention changes, the step must be updated. This is unlikely because the naming is defined in Terraform (`terraform/modules/ecs-task/main.tf`).

## Validation

1. `terraform validate` in `terraform/demo/` passes.
2. `terraform plan` in `terraform/demo/` shows the new `SFNDescribeExecution` and `CloudWatchLogRead` statements in the `pipeline-demo-cicd` policy.
3. The `log_group_arn` output is available from the `ecs-task` module.
4. Push to `main` triggers the workflow; the "Wait for demo pipeline" step polls until the Step Function completes.
5. If the Step Function execution fails, the GitHub Actions run shows red (❌) with execution history and container logs in the log output.