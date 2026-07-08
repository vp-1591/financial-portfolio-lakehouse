# 0051: Step Functions Orchestration

## Context

Phase 2 of the productionization roadmap moves the portfolio pipeline to a Step Functions orchestrator on AWS. PR 1 made connectors self-describing via the `BrokerConnector` protocol (`fetch_kwargs`, `required_secrets`, `extract_holdings`, `enabled_env_var`), and PR 2 added the `run-connector <name>` and `run-consolidate-allocate` CLI subcommands. This PR (PR 3 of 3) builds the AWS infrastructure that runs them.

An earlier design baked the three connector names into ~10 places (CLI subcommands, Terraform vars, literal ASL `Parallel` branches, `iam:PassRole` grants) — adding a 4th connector would be more expensive than today. It also had a correctness bug: a daily-schedule + XTB sharing one state machine with a literal XTB branch would fail because `$.detail.object.key` doesn't exist on schedule input, exhausting retries and preventing consolidate-allocate from running.

The registry-driven revision fixes both problems: connector inclusion is driven by execution input lists, so adding a connector touches only (1) new connector package, (2) secrets entries in `DEMO_SECRET_MAP`/`REQUIRED_SECRETS`, (3) one `locals` map entry per env + SSM params. No ASL edit, no new Terraform variable, no new PassRole grant.

## Decision

1. **Orchestrator `Map` over connector list from execution input** — the Step Functions state machine uses a `Map` state over `$.connectors` from execution input, not literal `Parallel` branches. Each item specifies `{name, task_def_arn, command}`. After the Map completes, a single `ConsolidateAllocate` step runs using `$.consolidate_allocate_task_def_arn` and `$.demo`.

2. **One generic `run-connector <name>` subcommand** — the CLI has one connector subcommand and one consolidation subcommand. Adding a connector requires zero CLI changes.

3. **Connector self-description via `BrokerConnector` protocol** — each connector declares `fetch_kwargs`, `required_secrets`, `extract_holdings`, and `enabled_env_var`. This replaces per-connector `if/elif` branching.

4. **SSM SecureString secrets + per-env KMS** — secrets stored in SSM Parameter Store as `SecureString`, encrypted with per-environment KMS keys. Naming convention mirrors `DEMO_SECRET_MAP` in `secrets.py`: `/portfolio/prod/<SECRET>` for prod, `/portfolio/demo/<SECRET>_DEMO` for demo. Terraform creates parameter names and KMS keys; values are seeded out-of-band (never in Terraform state).

5. **VPC endpoints (no public IP)** — each environment has its own VPC with private subnets and S3/ECR/CloudWatch/SSM VPC interface endpoints. Separate VPC per environment matches the existing S3/IAM isolation model.

6. **Cluster in shared, networking in env** — the ECS cluster is shared (env-agnostic), while VPC, subnets, security groups, and VPC endpoints are per-environment. `DEMO` selects the environment.

7. **Variable-based apply decoupling** — state files stay independent (ADR 0049). Apply order: shared #1 (ECR + cluster, state machine `count=0`) → prod/demo (task defs, VPCs, SSM) → shared #2 (state machine + EventBridge with ARN map in tfvars).

8. **PassRole scoped to role-name prefix** — the Step Functions IAM role has `iam:PassRole` scoped to `pipeline-task-*-prod` / `-demo-*` role-name patterns rather than enumerated ARNs, so a new connector task role needs no policy edit.

9. **Per-connector task definitions via module `for_each`** — each connector gets its own ECS task definition (via the `ecs-task` module with `for_each = local.connectors`) for secret isolation: each task def mounts only its own SSM secrets, not all of them.

10. **CloudWatch Logs with 7-day retention per task** — Terraform creates one log group per task definition with naming convention `/ecs/portfolio-pipeline-<env>-<connector>`.

11. **EventBridge input transformers with static constant templates** — the `connectors` array in the execution input is a constant in the input_transformer template. Adding a connector requires updating this template in Terraform (configuration-only, no ASL or CLI edit).

## Constraints

- Existing connectors keep working; no changes to connector packages.
- `DEMO` isolation intact: prod and demo have separate VPCs, S3 buckets, IAM users, KMS keys, and SSM parameters.
- State files remain independent (ADR 0049): `shared/`, `prod/`, `demo/` each have their own backend.
- `ENCRYPTION_KEY` continuity: the Fernet key seeded into SSM must match the key used to write existing raw Delta tables, otherwise stored data is unreadable. Verify before the first cloud run.
- No `boto3` in the application (EventBridge triggers the state machine; app uses PyArrow S3). Corrects ADR 0048's speculative "Phase 2 will add boto3."

### Out of scope

- CD of Step Functions/Terraform via GitHub Actions. Applies are manual (`terraform apply`) following the apply order above. Wiring `terraform apply` into a GHA workflow on merge/tag is deferred to a future CD phase.
- Per-connector S3-prefix scoping within an env. One task role per connector task def shares S3 read/write within the env bucket/prefix. Cross-connector blast radius within an env is accepted.
- XTB report extraction automation.
- Data quality gates (Phase 3).
- Email delivery (Phase 5).

## Consequences

- **Positive**: Adding a connector ≈ new package + secrets entries + one `locals` map entry per env + SSM params. No CLI/ASL/Terraform-var/PassRole edit. The Map-over-input design avoids the daily-schedule + XTB correctness bug.
- **Positive**: Per-connector task definitions provide secret isolation — a connector task only sees its own secrets, not all 5.
- **Positive**: Separate VPC per environment provides full data isolation matching the existing S3/IAM model.
- **Neutral**: Execution input is richer (connector list + ARN map + per-item command), built in the EventBridge input transformer. This is configuration-only but must be updated in Terraform when connectors change.
- **Neutral**: Two-phase apply for `shared/` (first ECR/cluster, then orchestration after task defs exist). Requires passing ARN maps via tfvars.
- **Negative**: SSM values are seeded out-of-band (manual `aws ssm put-parameter`). This is intentional — secrets must never be in Terraform state.
- **Negative**: Per-connector task definitions mean more ECS task definitions to manage (4 per env × 2 envs = 8 total), but the `for_each` module pattern keeps the Terraform manageable.

## Validation

1. `.venv/Scripts/python -m pytest tests/ -v` — existing tests pass (no app code changes in this PR).
2. `ruff check .` and `ruff format --check .` clean.
3. `terraform validate` in `modules/ecs-task/`, `shared/`, `prod/`, `demo/`.
4. `terraform plan` (with vars) in `shared/`, `prod/`, `demo/`.
5. Apply order: shared (ECR/cluster) → prod/demo → shared (orchestration with ARN map in tfvars).
6. Seed SSM values per the secrets runbook (verify `ENCRYPTION_KEY` matches existing raw data).
7. End-to-end cloud smoke: `python -m pipeline.run upload-xtb <file>` → EventBridge fires the orchestrator → Map runs the enabled connector tasks in parallel + consolidate-allocate → CloudWatch Logs show each step → analytics `portfolio_allocation` updated in S3.
8. Manual daily-schedule execution (no `xtb_file`) completes without a broken XTB branch.