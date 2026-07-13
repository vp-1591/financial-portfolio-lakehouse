# 0053: Per-Environment CI/CD Credentials and IAM Permissions

> **Drifted** — Demo CI/CD policy grants 3 extra permissions not described in the ADR (states:DescribeExecution, states:GetExecutionHistory, logs:FilterLogEvents); image tagging uses only `staging-latest`/`production-latest` without the `git-<sha>`/`<version>` tags described.

## Context

ADR 0052 added a Step Functions trigger to the deploy workflow that runs
`aws ecs describe-task-definition` and `aws stepfunctions start-execution` after
a merge to `main`. This trigger step authenticated as the prod IAM user
(`pipeline`) because the entire workflow used the same `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` secrets.

Two problems emerged:

1. **Missing IAM permissions**: The prod `pipeline` user did not have
   `ecs:DescribeTaskDefinition` or `states:StartExecution` permissions. The
   deploy workflow failed with `AccessDeniedException`.

2. **Cross-environment credential use**: The prod IAM user was used for demo
   operations (describing demo task definitions, starting the demo state
   machine). This violates the credential isolation principle established in
   ADR 0039 and ADR 0041 — the prod user should not need demo-specific
   permissions, and demo operations should be performed with demo credentials.

Additionally, the single `deploy.yml` workflow served both merge-to-main (staging)
and tag-push (production) triggers, which made credential isolation awkward
within a single job.

## Decision

### Per-environment CI/CD IAM policies

Add IAM policies to both environments that grant the deploy workflow the
permissions it needs:

1. **`pipeline-demo-cicd` policy** (in `terraform/demo/`): Grants
   `ecs:DescribeTaskDefinition` (on `*` — this action does not support
   resource-level permissions) and `states:StartExecution` (scoped to the
   demo state machine ARN only) to the `pipeline-demo` user.

2. **`pipeline-cicd` policy** (in `terraform/prod/`): Grants only
   `ecs:DescribeTaskDefinition` (on `*`). The prod deploy workflow does not
   trigger Step Functions — production runs on a daily EventBridge schedule —
   so `states:StartExecution` is not needed.

### ECR push/pull for the demo user

Attach the shared `pipeline-ecr-push-pull` policy (defined in
`terraform/shared/`) to the `pipeline-demo` user via a data source lookup.
This follows the same pattern used in `terraform/prod/` and makes both users
interchangeable for ECR operations, so either can push Docker images.

### Split deploy workflow into two files

- **`deploy-staging.yml`** — Triggered by push to `main`. Uses demo
  credentials (`AWS_ACCESS_KEY_ID_DEMO` / `AWS_SECRET_ACCESS_KEY_DEMO`)
  for all steps including ECR login, image push, and the demo pipeline
  trigger. Tags images `git-<sha>` + `staging-latest`.

- **`deploy-prod.yml`** — Triggered by `v*` tag push. Uses prod credentials
  (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) for ECR login and image
  push only. Tags images `<version>` + `production-latest`. Does not trigger
  the prod state machine — production runs on its daily EventBridge schedule.

This split ensures that each workflow loads only the credentials it needs.
The staging workflow never sees prod credentials, and the production workflow
never sees demo credentials.

## Constraints

- `ecs:DescribeTaskDefinition` does not support resource-level permissions,
  so it must be granted on `*`. This is a read-only action and poses minimal
  risk.
- `terraform/shared/` must be applied before `terraform/demo/` and
  `terraform/prod/` so the `pipeline-ecr-push-pull` policy exists for the
  data source lookup.
- The demo CI/CD policy references `module.orchestrator.state_machine_arn`,
  so it must be defined after the orchestrator module block.
- Production pipeline runs are triggered by the daily EventBridge schedule
  defined in Terraform, not by the deploy workflow. This matches the design
  from ADR 0049.

## Consequences

- **Positive**: The deploy trigger step no longer fails with
  `AccessDeniedException`. `ecs:DescribeTaskDefinition` is authorized for
  both environments' IAM users, and `states:StartExecution` is authorized
  for the demo user.
- **Positive**: Demo operations use demo credentials only. The staging workflow
  never loads prod credentials, and the production workflow never loads demo
  credentials.
- **Positive**: The prod CI/CD policy follows least-privilege — it grants only
  `ecs:DescribeTaskDefinition`, the single permission the prod workflow needs.
  A compromised prod key cannot trigger the production state machine.
- **Positive**: `states:StartExecution` is scoped to the demo state machine
  ARN only in the demo policy, following least-privilege.
- **Neutral**: Two additional IAM policies to maintain (one per environment).
- **Neutral**: `deploy.yml` is replaced by `deploy-staging.yml` and
  `deploy-prod.yml`. The old file is deleted.

## Validation

1. `terraform validate` passes in `demo/` and `prod/`.
2. `terraform plan` in `demo/` shows `pipeline-demo-cicd` policy and ECR
   push/pull attachment being created.
3. `terraform plan` in `prod/` shows `pipeline-cicd` policy being created.
4. After `terraform apply`, `aws iam list-attached-user-policies --user-name
   pipeline-demo` shows 3 policies: `pipeline-demo-s3-access`,
   `pipeline-ecr-push-pull`, `pipeline-demo-cicd`.
5. After `terraform apply`, `aws iam list-attached-user-policies --user-name
   pipeline` shows 3 policies: `pipeline-s3-access`,
   `pipeline-ecr-push-pull`, `pipeline-cicd`.
6. Push to `main` triggers `deploy-staging.yml` and the "Trigger demo
   pipeline" step succeeds with demo credentials.