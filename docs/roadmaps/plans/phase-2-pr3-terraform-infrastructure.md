# Plan: Phase 2 PR 3 — Terraform orchestration infrastructure

> Implementation plan for **PR 3 of 3** of `docs/roadmaps/phase-2-step-functions-orchestration.md`.
> Workflow stage: `implement` → `ADR` → `review`.

## Context

Phase 2 moves the portfolio pipeline to a Step Functions orchestrator on AWS. PR 1 made connectors
self-describing and PR 2 added the `run-connector <name>` / `run-consolidate-allocate` CLI
subcommands. This PR builds the AWS infrastructure that runs them: a single orchestrator state
machine that `Map`s over a connector list from execution input, an ECS task definition per
connector per env, EventBridge triggers (S3 file arrival + daily schedule), per-environment VPCs,
SSM-backed secrets, and IAM roles. It also records ADR 0051 and marks Phase 2 done.

**Why registry-driven Terraform (not the first draft):** an earlier design baked the three
connector names into ~10 places (CLI subcommands, Terraform vars, literal ASL `Parallel` branches,
`iam:PassRole` grants) — adding a 4th connector would be *more* expensive than today. It also had a
real correctness bug (daily-schedule + XTB sharing one state machine with a literal XTB branch). This
plan drives connector inclusion from execution input lists, so adding a connector touches only:
(1) new connector package, (2) secrets in `DEMO_SECRET_MAP`/`REQUIRED_SECRETS`, (3) one `ecs-task`
module `for_each` map entry per env + SSM params. No ASL edit, no new Terraform var, no new
PassRole grant.

**User-confirmed decisions:**
- **Secrets:** SSM Parameter Store `SecureString` per env, wired now (KMS-encrypted, free Standard
  tier, end-to-end runnable). Values seeded out-of-band (never in Terraform state).
- **Networking:** private subnets + S3/ECR/CloudWatch VPC interface endpoints, no public IP.
  **Separate VPC per environment** (one in `prod/`, one in `demo/`).
- **EventBridge input_transformer:** static constant templates (no Lambda).
- **SSM naming:** `/portfolio/prod/<SECRET>` for prod, `/portfolio/demo/<SECRET>_DEMO` for demo,
  mirroring `DEMO_SECRET_MAP` in `secrets.py`.
- **CloudWatch Logs:** Terraform creates one log group per task def,
  `/ecs/portfolio-pipeline-<env>-<connector>`, 7-day retention.

**Depends on PR 1 and PR 2** being merged — the orchestrator runs the `run-connector` subcommand
from PR 2 over the self-describing connectors from PR 1.

## Reused, not rebuilt

- `pipeline/run.py` — `cmd_upload_xtb` (428) stages XTB files to S3; EventBridge fires on arrival.
- `pipeline/storage.py` `staging_path("xtb", name)` → `staging/xtb/` / `staging_demo/xtb/`.
- `terraform/shared/main.tf` ECR + `pipeline-ecr-push-pull` policy (data-source reuse, ADR 0049).
- `terraform/prod/` and `terraform/demo/` existing S3 buckets + IAM users (ADRs 0037–0044, 0049).
- State files stay independent (ADR 0049): `shared/`, `prod/`, `demo/` each have their own backend.

## Step 1 — Terraform local module `terraform/modules/ecs-task/`

Produces one `aws_ecs_task_definition` (Fargate, `awsvpc`) + task execution role + task role +
policies. Variables: `name`, `image`, `demo` (bool), `cpu`, `memory`, `command` (list),
`environment` (map), `secrets` (map of SSM ARNs), `bucket_name`, `s3_prefix`, `subnet_ids`,
`security_group_id`, `ecr_policy_arn` (reuse `pipeline-ecr-push-pull`), `kms_key_arn`, `region`,
`task_role_arn` (optional shared role; default creates one per task def for S3-scoped isolation).

- **Task execution role:** ECR pull (attach `ecr_policy_arn`), CloudWatch Logs put,
  `ssm:GetParameters` + `kms:Decrypt` on the env KMS key for `secrets`.
- **Task role:** S3 read/write scoped to this env's `bucket_name`/`s3_prefix` only — no cross-env
  access.
- **CloudWatch Logs:** Terraform creates one log group per task definition with naming convention
  `/ecs/portfolio-pipeline-<env>-<connector>` (e.g. `/ecs/portfolio-pipeline-prod-ibkr`).
  Retention: 7 days. ECS `logConfiguration` points to the corresponding log group.

## Step 2 — Terraform `terraform/shared/` (orchestration + triggers)

Variables: `scheduled` (bool, default `false`), `xtb_enabled` (bool, default `true` — also creates
the S3 file-arrival rule), `schedule_connectors` (list, default `["ibkr","t212"]`),
`file_arrival_connectors` (list, default `["ibkr","t212","xtb"]`), `task_def_arns` (map
`name→arn`, default `{}` — fed from prod/demo outputs via tfvars; **keys are connector registry
names** like `ibkr`, `trading212`, `xtb` matching the `for_each` keys in per-env modules),
`consolidate_allocate_task_def_arn` (string), `xtb_staging_bucket_name`, `xtb_staging_prefix`
(default `staging/xtb/`), `schedule_cron` (default `0 6 * * ? *`), `ecs_cluster_arn`, `subnet_ids`,
`security_group_id`, `state_machine_name` (default `portfolio-pipeline-orchestrator`).

These replace per-connector `ibkr_enabled`/`t212_enabled` ASL-branch vars: connector inclusion is a
**list in execution input**, not ASL structure. `scheduled` + `xtb_enabled` remain as
**trigger-creation** flags (EventBridge rules are genuinely Terraform resources).

Resources (gated by `count`):
- `aws_ecs_cluster` `portfolio-pipeline-cluster` (single, env-agnostic — passed into env modules).
- `aws_iam_role` `pipeline-sfn-role` + policy: `ecs:RunTask/StopTask/DescribeTasks` +
  `iam:PassRole` scoped to a **role-name prefix** (`pipeline-task-*-prod` / `-demo-*`) rather than
  enumerated ARNs, so a new connector task role needs no policy edit.
- `aws_sfn_state_machine` `orchestrator`:
  `count = var.scheduled || var.xtb_enabled ? 1 : 0`. **Map over `$.connectors`**, not literal
  `Parallel` branches. Each item = `{name, task_def_arn, command (list)}`. The Map's `RunTask.sync`
  uses `TaskDefinition.$: "$.task_def_arn"` and `ContainerOverrides.Command.$: "$.command"`
  (command pre-built in execution input, e.g. `["run-connector","xtb","--xtb-file","s3://..."]`).
  Per-item `Retry` on `States.TaskFailed` (connector-level isolation). After the Map,
  `ConsolidateAllocate` `RunTask.sync` using `$.consolidate_allocate_task_def_arn` + `DEMO` from
  `$.demo`. **One generic definition — no per-connector branches.**
- `aws_cloudwatch_event_rule` `xtb_file_arrival` (`count = var.xtb_enabled ? 1 : 0`):
  `source=["aws.s3"]`, `detail-type=["Object Created"]`, `detail.bucket.name`/`detail.object.key`
  prefix from `var.xtb_staging_prefix`. Target → state machine; `input_transformer` builds the
  execution input using **static constant templates** (EventBridge input transformers support
  constant values and `$.detail` JSON path extraction, but cannot dynamically construct lists).
  The `connectors` array is a constant in the input_transformer template — adding a connector
  requires updating this template in Terraform, which is acceptable since it's configuration-only
  (no ASL or CLI edit). The template specifies each connector item's `name`, `task_def_arn` (looked
  up from `var.task_def_arns`), and `command` (xtb item gets `--xtb-file <xtb_file_uri>`).
- `aws_cloudwatch_event_rule` `daily_schedule` (`count = var.scheduled ? 1 : 0`):
  `schedule_expression = var.schedule_cron`. Target → state machine; input lists
  `var.schedule_connectors` (no xtb_file). **No XTB item, no broken `States.Format`.** Like the
  file-arrival rule, the input_transformer uses static constant templates for the `connectors`
  array.

### Daily-schedule + XTB correctness (bug avoided)

An earlier design put both triggers into one state machine with a literal XTB branch reading
`$.detail.object.key` — on schedule input there is no object key, so the XTB branch errored,
exhausted retries, failed the `Parallel`, and consolidate-allocate never ran. The Map-over-input
design fixes this: the schedule target's input simply omits the xtb item (no xtb_file), so the Map
runs only the API connectors then consolidate-allocate. The file-arrival target's input includes
the xtb item with its s3 URI. One state machine, correct for both triggers.

## Step 3 — Terraform `terraform/prod/` and `terraform/demo/` (per env)

For each env (prod: `DEMO=false`, image `production-latest`; demo: `DEMO=true`, image
`staging-latest`):

- `locals.connectors` map: `{ibkr={command=["run-connector","ibkr","--target-currency","EUR"],
  secrets=[...]}, t212={...}, xtb={...}}`. Call `ecs-task` module with `for_each =
  local.connectors` → 3 connector task defs per env, each mounting only its own SSM secrets
  (secret isolation — narrower blast radius than one task def with all secrets). Plus one
  standalone `consolidate-allocate` module call (`["run-consolidate-allocate","--target-currency",
  "EUR"]`). Adding a connector = one `locals.connectors` entry (per env) + SSM params.
- All task `environment`: `DEMO`, `STORAGE_TYPE=cloud`, `S3_BUCKET`, `AWS_REGION`, and the
  `*_ENABLED` flags (runtime gate). `secrets`: `valueFrom` SSM param ARNs for that connector's
  secrets (5 total across connectors; `_DEMO`-suffixed params for demo).
- `aws_s3_bucket_notification` with empty `eventbridge {}` block (enables EventBridge on the
  bucket so the XTB file-arrival rule fires).
- SSM `SecureString` parameter *names* + per-env `aws_kms_key` + grants (Terraform creates names +
  KMS + grants; **values seeded out-of-band** — see Step 4). SSM naming convention mirrors
  `DEMO_SECRET_MAP` in `secrets.py`:
  - Prod: `/portfolio/prod/<SECRET>` (e.g. `/portfolio/prod/IBKR_FLEX_TOKEN`)
  - Demo: `/portfolio/demo/<SECRET>_DEMO` (e.g. `/portfolio/demo/IBKR_FLEX_TOKEN_DEMO`)
- VPC: `aws_vpc` + **private** subnets + `aws_security_group` (egress to VPC endpoints) + **S3,
  ECR, and CloudWatch Logs VPC interface endpoints** (`aws_vpc_endpoint`, `Interface`) with
  route-table/private-DNS. No public IP. **Separate VPC per environment** (one in `prod/`, one in
  `demo/`) — full data isolation matches the existing S3/IAM model. `cluster_arn` passed in from
  `shared/`.
- Outputs: map of connector task-def ARNs + `consolidate_allocate_task_def_arn` + bucket name +
  subnet ids + security group id (consumed by `shared/` via tfvars).

## Step 4 — Secrets seeding runbook (documented in ADR 0051)

Terraform creates KMS keys + SSM parameter *names* + IAM grants. **Values** are seeded out-of-band
(never in Terraform state):

```
aws ssm put-parameter --name /portfolio/prod/IBKR_FLEX_TOKEN --value "..." --type SecureString --key-id <prod-kms-id>
# IBKR_FLEX_QUERY_ID, T212_API_KEY, T212_API_SECRET, ENCRYPTION_KEY (prod)
# repeat under /portfolio/demo/* with _DEMO-suffixed logical names for demo env
```

The task-def `secrets` block references `/portfolio/<env>/<SECRET>`. **Hard requirement:**
`ENCRYPTION_KEY` (prod and `_DEMO`) MUST equal the key used to write existing raw Delta tables —
otherwise stored data is unreadable. Verify before the first cloud run.

## Step 5 — Apply / deploy sequencing (variable-based decoupling)

State files stay independent (ADR 0049). Apply order:
1. `terraform/shared/` apply #1 — ECR + IAM policy + cluster (state machine/EventBridge
   `count=0` while `task_def_arns` empty).
2. `terraform/prod/` and `terraform/demo/` apply — S3, IAM users, ECS task defs (module `for_each`),
   roles, SSM params + KMS, VPC + endpoints, bucket EventBridge notification.
3. `terraform/shared/` apply #2 — state machine + EventBridge rules (connector ARN map + bucket
   name + subnet ids + cluster_arn in `shared/terraform.tfvars`).

## Step 6 — ADR 0051 + roadmap updates

Create `docs/adr/0051-step-functions-orchestration.md`:
- **Context** — Phase 2 need; the first design's modularity regression + daily/XTB bug as the
  trigger for the registry-driven revision.
- **Decision** — orchestrator `Map` over a connector list from execution input (not literal
  branches); one `run-connector <name>` subcommand; connector self-description via
  `fetch_kwargs`/`required_secrets`/`extract_holdings`/`enabled_env_var`; SSM SecureString secrets
  + per-env KMS; VPC endpoints (no public IP); cluster-in-shared/networking-in-env;
  variable-based apply decoupling; PassRole scoped to a role-name prefix; per-connector task defs
  via module `for_each` for secret isolation; CloudWatch Logs with 7-day retention per task;
  separate VPC per environment.
- **Constraints** — existing connectors keep working; `DEMO` isolation intact; state files
  independent; `ENCRYPTION_KEY` continuity; no `boto3` in app (EventBridge triggers the state
  machine; app uses PyArrow S3 — corrects ADR 0048's speculative "Phase 2 will add boto3").
- **Out of scope** — (a) CD of the Step Functions/Terraform via GitHub Actions: applies are
  manual (`terraform apply`) following the apply order above; wiring `terraform apply` into a GHA
  workflow on merge/tag is deferred (a future CD phase will add plan/apply workflows with the IAM
  and state-locking guardrails). (b) Per-connector S3-prefix scoping within an env (one task role
  per env shared across connector task defs — note cross-connector read/write blast radius within
  an env). (c) XTB report extraction automation. (d) Data quality gates (Phase 3). (e) Email
  delivery (Phase 5).
- **Consequences** — adding a connector ≈ new package + secrets entries + one `locals` map entry
  per env + SSM params; no CLI/ASL/Terraform-var/PassRole edit. Trade-off: execution input is
  richer (connector list + ARN map + per-item command), built in the EventBridge input_transformer.
- **Validation** — the verification steps below.

Update `docs/adr/README.md` index: append `| 0051 | Step Functions Orchestration | 2026-07-08 |
active | — |`. Update `docs/roadmaps/roadmap-productionization.md` Phase 2 heading →
`*[status: done]*`.

## Verification

1. `.venv/Scripts/python -m pytest tests/ -v` — existing + new tests pass (no app code changes in
   this PR, but run to confirm nothing regressed).
2. `ruff check .` and `ruff format --check .` clean.
3. `terraform validate` + `terraform plan` (with vars) in `shared/`, `prod/`, `demo/`.
4. Apply order: shared (ECR/cluster) → prod/demo → shared (orchestration with ARN map in tfvars).
5. Seed SSM values per Step 4 (verify `ENCRYPTION_KEY` matches existing raw data).
6. End-to-end cloud smoke: `python -m pipeline.run upload-xtb <file>` → EventBridge fires the
   orchestrator → Map runs the enabled connector tasks in parallel + consolidate-allocate →
   CloudWatch Logs show each step → analytics `portfolio_allocation` updated in S3. Also trigger a
   manual daily-schedule execution (no xtb_file) and confirm it completes without a broken XTB
   branch.