# Plan: Per-Environment State Machines + CI/CD Pipeline

## Context

The current Terraform architecture puts the Step Functions state machine and EventBridge triggers in `shared/`, parameterized via `terraform.tfvars`. This means applying with prod values **overwrites** the demo state machine — you can't run both environments simultaneously. The roadmap intended `DEMO` to be an environment selector, but the implementation bakes `subnet_ids`, `security_group_ids`, and `task_def_arns` into the ASL definition at `terraform apply` time, not at execution time.

Additionally, the CI/CD workflow (`deploy.yml`) only builds and pushes Docker images — it doesn't trigger the demo pipeline after merge, and there's no staging gate between demo and prod.

This plan moves the state machine and EventBridge rules into per-environment Terraform roots (demo/ and prod/), and adds a Step Function trigger to the CI/CD pipeline so staging runs automatically on merge.

---

## Step 1: Extract orchestrator Terraform module

Create `terraform/modules/orchestrator/` containing the state machine and EventBridge resources currently in `shared/main.tf`:

- `aws_sfn_state_machine.orchestrator`
- `aws_cloudwatch_event_rule.xtb_file_arrival` + target
- `aws_cloudwatch_event_rule.daily_schedule` + target
- `aws_iam_role.eventbridge` + policy

**Module inputs:**
- `env` — environment label (`"demo"` or `"prod"`)
- `demo` — bool, passed into execution input
- `ecs_cluster_arn` — from shared/
- `subnet_ids` — from this environment's VPC
- `security_group_ids` — from this environment's security group
- `task_def_arns` — map of connector name → ARN (from this environment's ecs-task modules)
- `consolidate_allocate_task_def_arn` — from this environment's consolidate-allocate module
- `sfn_role_arn` — from shared/ (the `pipeline-sfn-role`)
- `xtb_staging_bucket_name` — this environment's S3 bucket
- `xtb_staging_prefix` — S3 prefix for XTB uploads
- `scheduled` — bool, whether to create daily schedule (prod: true, demo: false)
- `schedule_cron` — cron expression
- `schedule_connectors` — connector list for schedule trigger
- `file_arrival_connectors` — connector list for file arrival trigger
- `state_machine_name` — name suffix (e.g. `"portfolio-pipeline-orchestrator-demo"`)

**Key design point:** The ASL definition stays identical between environments — it's the same `Map` over `$.connectors` pattern. Only the tfvars values differ (subnets, task def ARNs, `demo` flag, bucket name).

**Files:** `terraform/modules/orchestrator/main.tf`, `terraform/modules/orchestrator/variables.tf`, `terraform/modules/orchestrator/outputs.tf`

---

## Step 2: Remove state machine and EventBridge from shared/

In `terraform/shared/main.tf`, remove:
- The `sfn_definition` local (lines 386–466)
- The `create_state_machine` local (line 469)
- The `aws_sfn_state_machine.orchestrator` resource (lines 472–496)
- The `aws_cloudwatch_event_rule.xtb_file_arrival` + target (lines 511–568)
- The `aws_cloudwatch_event_rule.daily_schedule` + target (lines 574–607)
- The `aws_iam_role.eventbridge` + policy (lines 613–655)
- All variables that only the orchestrator uses: `scheduled`, `xtb_enabled`, `schedule_connectors`, `file_arrival_connectors`, `task_def_arns`, `consolidate_allocate_task_def_arn`, `xtb_staging_bucket_name`, `xtb_staging_prefix`, `schedule_cron`, `subnet_ids`, `security_group_ids`, `state_machine_name`, `env`, `demo`

Keep in shared/:
- ECR repository + lifecycle policy
- ECR push/pull IAM policy
- ECS cluster
- SFN IAM role (`pipeline-sfn-role`) + policy (the RunTask, PassRole, SyncCallback, DescribeExecution statements)

Remove the `state_machine_arn` output (no longer in shared/).

---

## Step 3: Add orchestrator module call to demo/

In `terraform/demo/main.tf`, add:

```hcl
module "orchestrator" {
  source = "../modules/orchestrator"

  env                              = "demo"
  demo                             = true
  ecs_cluster_arn                  = var.ecs_cluster_arn
  subnet_ids                       = aws_subnet.private[*].id
  security_group_ids               = [aws_security_group.pipeline_demo.id]
  task_def_arns                    = { for k, v in module.connector_task : k => v.task_definition_arn }
  consolidate_allocate_task_def_arn = module.consolidate_allocate.task_definition_arn
  sfn_role_arn                     = aws_iam_role.sfn.arn  # from shared/ data source
  xtb_staging_bucket_name         = aws_s3_bucket.pipeline_demo.bucket
  xtb_staging_prefix              = "staging_demo/xtb/"
  scheduled                        = false    # no daily schedule for demo
  schedule_cron                    = "cron(0 6 * * ? *)"
  schedule_connectors              = ["ibkr", "trading212"]
  file_arrival_connectors          = ["ibkr", "trading212", "xtb"]
  state_machine_name               = "portfolio-pipeline-orchestrator-demo"
}
```

The `sfn_role_arn` needs a `data "aws_iam_role"` lookup since it's created in shared/:

```hcl
data "aws_iam_role" "sfn" {
  name = "pipeline-sfn-role"
}
```

**Important:** demo gets `scheduled = false`. Demo runs are triggered by:
1. Auto-trigger on merge (Step 7 — deploy.yml starts an execution)
2. XTB file arrival (manual upload)
3. Manual start from AWS Console/CLI

Add outputs:
- `state_machine_arn` — for deploy.yml to trigger

---

## Step 4: Add orchestrator module call to prod/

In `terraform/prod/main.tf`, same pattern as demo but with prod values:

```hcl
module "orchestrator" {
  source = "../modules/orchestrator"

  env                              = "prod"
  demo                             = false
  ecs_cluster_arn                  = var.ecs_cluster_arn
  subnet_ids                       = aws_subnet.private[*].id
  security_group_ids               = [aws_security_group.pipeline.id]
  task_def_arns                    = { for k, v in module.connector_task : k => v.task_definition_arn }
  consolidate_allocate_task_def_arn = module.consolidate_allocate.task_definition_arn
  sfn_role_arn                     = data.aws_iam_role.sfn.arn
  xtb_staging_bucket_name         = aws_s3_bucket.pipeline.bucket
  xtb_staging_prefix              = "staging/xtb/"
  scheduled                        = true     # daily schedule for prod
  schedule_cron                    = "cron(0 6 * * ? *)"
  schedule_connectors              = ["ibkr", "trading212"]
  file_arrival_connectors          = ["ibkr", "trading212", "xtb"]
  state_machine_name               = "portfolio-pipeline-orchestrator"
}
```

Prod gets `scheduled = true` — daily schedule at 6 AM UTC, plus XTB file arrival trigger.

Add `data "aws_iam_role" "sfn"` lookup and `state_machine_arn` output.

---

## Step 5: Simplify shared/ tfvars

Since shared/ no longer contains the state machine or EventBridge rules, `terraform.tfvars` for shared/ becomes much simpler. Remove all the orchestrator-related variables (task_def_arns, subnet_ids, security_group_ids, xtb_staging_bucket_name, etc.). shared/ tfvars only needs:

- `aws_region` (has default)
- `ecr_repository_name` (has default)

The two-phase apply (shared #1 → env → shared #2) is eliminated. Apply order becomes:

1. `shared/` apply — once, creates ECR + cluster + IAM (rarely changes)
2. `demo/` apply — creates everything demo needs including its own state machine
3. `prod/` apply — creates everything prod needs including its own state machine

No more shared #2. Each environment is self-contained.

Update `terraform/shared/terraform.tfvars.example` accordingly.

---

## Step 6: Destroy the existing shared/ orchestrator resources

After the module is moved and per-environment state machines are created:

1. Remove the orchestrator resources from shared/ state (Step 2 removes them from config; `terraform apply` in shared/ will destroy the existing state machine and EventBridge rules)
2. Apply demo/ and prod/ — this creates the new per-environment state machines
3. Verify both state machines exist in the AWS Console

**Migration note:** This is a one-time migration. The demo state machine ARN will change (from `portfolio-pipeline-orchestrator` to `portfolio-pipeline-orchestrator-demo`). Any existing EventBridge rules in shared/ will be destroyed and recreated in demo/ and prod/.

---

## Step 7: Add demo state machine trigger to deploy.yml

In `.github/workflows/deploy.yml`, add a step after "Verify image starts" that triggers the demo state machine when merging to main:

```yaml
- name: Trigger demo pipeline
  if: github.ref == 'refs/heads/main'
  run: |
    aws stepfunctions start-execution \
      --state-machine-arn ${{ secrets.DEMO_STATE_MACHINE_ARN }} \
      --name "staging-$(echo ${{ github.sha }} | cut -c1-7)-$(date +%Y%m%d%H%M%S)"
```

This requires adding `DEMO_STATE_MACHINE_ARN` to GitHub Secrets. The ARN is an output from `terraform demo/` (from Step 3).

For production (tag pushes), **do not auto-trigger**. The prod pipeline runs on its daily schedule or via manual trigger.

---

## Step 8: Add pipeline.yml trigger for demo runs

The existing `.github/workflows/pipeline.yml` has a `workflow_dispatch` that runs the pipeline directly on the GitHub runner (not via ECS). This should remain for manual testing, but it's not the primary way to trigger cloud runs. The Step Function trigger in deploy.yml replaces it for automated staging runs.

No changes needed to pipeline.yml — it's still useful for manual testing.

---

## Files to modify

| File | Action |
|------|--------|
| `terraform/modules/orchestrator/main.tf` | **Create** — state machine + EventBridge module |
| `terraform/modules/orchestrator/variables.tf` | **Create** — module input variables |
| `terraform/modules/orchestrator/outputs.tf` | **Create** — state_machine_arn output |
| `terraform/shared/main.tf` | **Edit** — remove orchestrator resources and variables |
| `terraform/shared/variables.tf` | No change (already empty) |
| `terraform/shared/terraform.tfvars.example` | **Edit** — simplify (remove orchestrator vars) |
| `terraform/demo/main.tf` | **Edit** — add orchestrator module call + sfn data source |
| `terraform/demo/outputs.tf` | **Edit** — add state_machine_arn output |
| `terraform/prod/main.tf` | **Edit** — add orchestrator module call + sfn data source |
| `terraform/prod/outputs.tf` | **Edit** — add state_machine_arn output |
| `.github/workflows/deploy.yml` | **Edit** — add Step Function trigger step |
| `docs/adr/0052-per-env-state-machines.md` | **Create** — ADR for this change |

---

## Verification

1. `terraform validate` in `modules/orchestrator/`, `shared/`, `demo/`, `prod/`
2. `terraform plan` in `shared/` — should show state machine + EventBridge rules being destroyed
3. `terraform plan` in `demo/` — should show state machine + EventBridge rules being created (plus `data.aws_iam_role.sfn` read)
4. `terraform plan` in `prod/` — same as demo
5. After apply: verify both state machines exist in AWS Console (`portfolio-pipeline-orchestrator-demo` and `portfolio-pipeline-orchestrator`)
6. After apply: verify demo EventBridge rule has no schedule (only XTB file arrival)
7. After apply: verify prod EventBridge rule has both daily schedule and XTB file arrival
8. Push to main → verify deploy.yml pushes image and triggers demo state machine execution
9. Check CloudWatch Logs for the demo execution showing new container image running