# 0052 — Per-Environment State Machines and CI/CD Pipeline Trigger

## Context

The Step Functions state machine and EventBridge triggers were defined in `shared/main.tf`, parameterized via `terraform.tfvars`. This meant applying with prod values **overwrites** the demo state machine — both environments couldn't run simultaneously. The roadmap intended `DEMO` to be an environment selector, but the implementation bakes `subnet_ids`, `security_group_ids`, and `task_def_arns` into the ASL definition at `terraform apply` time, not at execution time.

Additionally, the CI/CD workflow (`deploy.yml`) only built and pushed Docker images — it didn't trigger the demo pipeline after merge, and there was no staging gate between demo and prod.

## Decision

1. **Extract the orchestrator into a reusable Terraform module** (`terraform/modules/orchestrator/`) that creates the state machine, EventBridge rules (S3 file arrival + daily schedule), and EventBridge IAM role for a single environment. The ASL definition is identical between environments — only the input values differ.

2. **Move state machine ownership into per-environment roots** (`demo/` and `prod/`). Each environment instantiates the orchestrator module with its own subnets, security groups, task definition ARNs, bucket name, and `demo` flag. This eliminates the two-phase apply: `shared/` is applied once (ECR + cluster + IAM), then each environment independently.

3. **Add a Step Functions trigger to `deploy.yml`** that starts a demo execution on every merge to `main`. Production runs on its daily schedule or via manual trigger — no auto-trigger on tag push.

4. **Simplify `shared/terraform.tfvars`** by removing all orchestrator variables (task_def_arns, subnet_ids, etc.). The shared root now only needs `ecs_cluster_arn`.

5. **Environment-specific naming**: EventBridge rules and IAM roles are suffixed with the environment (e.g., `portfolio-pipeline-daily-schedule-prod`, `pipeline-eventbridge-role-demo`) to avoid name collisions.

## Constraints

- The shared `pipeline-sfn-role` IAM role (with RunTask, PassRole, SyncCallback, DescribeExecution permissions) remains in `shared/` — it's referenced by both environments via a `data` source.
- The ASL definition stays identical across environments (same Map/Task pattern, same container name `pipeline`).
- Demo has `scheduled = false` — daily schedule only runs in prod.
- The demo state machine ARN changes (from `portfolio-pipeline-orchestrator` to `portfolio-pipeline-orchestrator-demo`), requiring a one-time `DEMO_STATE_MACHINE_ARN` GitHub Secret update.

## Consequences

- **Positive**: Demo and prod state machines coexist — no more overwriting one environment's state machine with another's config.
- **Positive**: Single apply for `shared/` instead of two-phase. Each environment is self-contained.
- **Positive**: Merges to `main` automatically trigger a demo pipeline run, giving immediate staging feedback.
- **Positive**: Per-environment EventBridge rules have unique names, avoiding conflicts.
- **Negative**: `shared/` apply will destroy the existing state machine and EventBridge rules. The demo/prod ARNs change — a one-time migration step is needed.
- **Negative**: `DEMO_STATE_MACHINE_ARN` must be added as a GitHub Secret before the deploy trigger works.

## Validation

1. `terraform validate` passes in `modules/orchestrator/`, `shared/`, `demo/`, and `prod/`.
2. `terraform plan` in `shared/` shows state machine + EventBridge rules being destroyed.
3. `terraform plan` in `demo/` shows state machine + EventBridge rules being created.
4. `terraform plan` in `prod/` shows state machine + EventBridge rules being created.
5. After apply: both `portfolio-pipeline-orchestrator-demo` and `portfolio-pipeline-orchestrator` exist in AWS Console.
6. After apply: demo EventBridge rule has XTB file arrival trigger only (no daily schedule).
7. After apply: prod EventBridge rule has both daily schedule and XTB file arrival.
8. Push to `main` triggers `deploy.yml` and the Step Function trigger step starts a demo execution.