# 0091 — Trigger Step Functions in `cmd_full` for staging/prod

## Context

ADR 0090 (roadmap 0012 Phase 2) replaced `DEMO`/`STORAGE_TYPE` with the `--mode` flag and made
`cmd_full` run the pipeline locally in docker mode. It left `cmd_full --mode staging` and
`--mode prod` as a "not yet implemented" stub — the Step Functions trigger was deferred to Phase 3.

Before this ADR, triggering the orchestrator in AWS required one of three separate, inconsistent
paths:

- **`scripts/run_prod_pipeline.py`** — a manual prod-only script with hardcoded state-machine and
  task-definition ARNs, no staging support, and no wait/poll. Undocumented in CI; not used by any
  workflow.
- **`.github/workflows/deploy-staging.yml`** — ~160 lines of bash that resolved ECS task-def ARNs
  via the AWS CLI, built the SFN input JSON with `jq`, called `aws stepfunctions start-execution`,
  polled `describe-execution` in a bash loop, and on failure invoked two helper scripts
  (`.github/scripts/parse_stepfunctions_event.py` and `.github/scripts/format_log_events.py`) to
  print execution history and CloudWatch container logs.
- **EventBridge** — the daily schedule and S3 file-arrival rule, which build the input in Terraform.

The bash workflow duplicated the input-building logic that lived in `run_prod_pipeline.py` and in
Terraform, with no shared schema enforcement. The consolidate command was hardcoded in the ASL
(`Command = ["run-consolidate-analytics", "--target-currency", "EUR"]`), so `--mode` could not be
propagated to the consolidate step. The vestigial `demo` field was emitted by every caller even
though the ASL never referenced `$.demo`.

The goal of Phase 3: one entry point (`pipeline run full --mode staging|prod`) for all AWS
execution contexts, with the caller needing only AWS credentials with `states:StartExecution`.

## Decision

1. **`cmd_full` triggers Step Functions in staging/prod.** In staging/prod mode, `cmd_full` no
   longer errors — it starts a Step Functions execution and returns the execution ARN plus a
   console URL. The docker-mode local orchestrator is unchanged. The logic lives in a new
   `pipeline/sfn.py` module; `cmd_full`'s staging/prod branch is a thin caller (`_trigger_sfn_execution`).

2. **Credential model: boto3 default chain, base env vars.** The SFN/ECS/CloudWatch boto3 clients
   use boto3's default credential chain (the `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars
   that `configure-aws-credentials` exports), NOT `pipeline.secrets.resolve_aws_credentials()`.
   `resolve_aws_credentials()` swaps to `_DEMO` variants in staging mode, which the
   `configure-aws-credentials` GitHub Action does not set — routing the SFN client through it would
   require the workflow to additionally export `_DEMO` env vars. The SFN trigger only needs IAM
   `states:StartExecution` / `ecs:DescribeTaskDefinition` / `logs:FilterLogEvents` permissions; the
   `_DEMO` swap exists for broker/S3 data-plane isolation, which is irrelevant here. Credentials are
   pre-validated with `boto3.Session().get_credentials() is None` before any AWS call.

3. **`main()` skips `resolve_storage()` for staging/prod `full`.** The SFN-trigger path needs no
   S3 data-plane config on the caller's machine, so `resolve_storage()` is skipped for
   `full --mode staging|prod`. This avoids requiring the local machine to know the demo S3 bucket
   name just to trigger SFN. `set_mode()` still runs.

4. **State machine ARN via env var.** `STAGING_STATE_MACHINE_ARN` / `PROD_STATE_MACHINE_ARN` env
   vars hold the ARN (from the `state_machine_arn` Terraform output). Not hardcoded. Missing ARN →
   actionable error naming the Terraform source.

5. **ECS task-def ARNs resolved at runtime.** `boto3 ecs.describe_task_definition` resolves the
   latest active revision for each family (`portfolio-pipeline-{env}-{connector}` and
   `portfolio-pipeline-{env}-consolidate-allocate`), so a Terraform revision bump doesn't break the
   trigger. `staging` mode maps to the `demo` env label; `prod` to `prod`.

6. **Execution input schema: drop `demo`, add `consolidate_command`.** The input is
   `{connectors: [{name, task_def_arn, command}], consolidate_allocate_task_def_arn,
   consolidate_command}`. The `demo` field is dropped (vestigial — ASL never read `$.demo`).
   `consolidate_command` is new and consumed by the ASL change below. Default connectors: `ibkr`,
   `trading212` (XTB excluded — driven by the EventBridge file-arrival trigger).

7. **ASL parameterizes the consolidate command.** `terraform/modules/orchestrator/main.tf` changes
   the ConsolidateAllocate `Command = [...]` literal to `"Command.$" = "$.consolidate_command"`.
   This couples the ASL to every input source emitting `consolidate_command` (see Constraints).

8. **`--mode` propagated through ECS and EventBridge.** A `local.mode_flag = var.demo ? "staging" :
   "prod"` drives `--mode` in the EventBridge XTB file-arrival template, the daily schedule input,
   and the ECS task definition commands (connector and consolidate-allocate). `DEMO`, `STORAGE_TYPE`,
   `IBKR_ENABLED`, `T212_ENABLED`, and `XTB_ENABLED` are removed from ECS `environment` blocks —
   each ECS task runs exactly one connector via `run-connector <name>`, so the enabled flags are
   redundant (only docker-mode `_run_connectors_parallel` consults them), and `DEMO`/`STORAGE_TYPE`
   are dead since Phase 2. `S3_BUCKET`, `S3_BUCKET_DEMO`, `S3_PREFIX_DEMO`, `AWS_REGION` stay.
   `_DEMO`-suffixed secret env var names stay until Phase 4.

9. **`--wait` absorbs the failure-detail scripts.** A `--wait` flag polls `describe_execution`
   (timeout 900s, interval 30s). On `SUCCEEDED` → exit 0. On `FAILED`/`TIMED_OUT`/`ABORTED` → exit 1
   and print failure details: SFN execution history (`TaskFailed`/`TaskTimedOut`/`ExecutionFailed`
   parsed for exit code, task def, stopped reason) and CloudWatch container logs for each connector
   + consolidate-allocate task (scoped to the execution start time). This absorbs
   `parse_stepfunctions_event.py` and `format_log_events.py`, which are deleted.

10. **`--with-xtb` / `--xtb-file` rejected in staging/prod.** They print guidance to use
    `upload-xtb` + the EventBridge file-arrival trigger. `--with-xtb` is ignored in docker mode
    (where XTB runs via `--xtb-file`).

11. **`scripts/run_prod_pipeline.py` deleted.** Its functionality is absorbed into `cmd_full`.
    `.github/workflows/deploy-staging.yml` is simplified: the trigger/wait/log-print bash steps
    become `python -m pipeline.run full --mode staging --wait` (with `setup-python` and
    `pip install -e .[pipeline]` steps added).

12. **boto3 added as a pipeline dependency.** The roadmap claimed boto3 was already a dependency
    ("`pipeline/s3.py` imports it") — this was wrong; `pipeline/s3.py` uses `pyarrow.fs`. boto3 is
    added to `[project.optional-dependencies] pipeline` as `boto3==1.37.0`.

## Constraints

- The ASL `consolidate_command` change is load-bearing: every input source (`cmd_full`, the
  EventBridge XTB template, the EventBridge daily schedule) must emit `consolidate_command` or the
  consolidate step fails at runtime. All three are updated atomically in this ADR. Because `--mode`
  is a hard two-sided incompatibility (see Consequences), the Terraform apply must land before the
  new image and before the first `--wait` run with the new code.
- `_DEMO`-suffixed secret env var names and the `var.demo` Terraform variable are NOT removed —
  Phase 4 removes the suffix and renames `DEMO_STATE_MACHINE_ARN` → `STAGING_STATE_MACHINE_ARN`.
- The SFN state machine structure (Map → ConsolidateAllocate) is unchanged; only the consolidate
  command is parameterized.
- XTB is not added to staging/prod `full`.
- `deploy-prod.yml` is unchanged (build/push only; prod runs via the daily EventBridge schedule or
  manual `full --mode prod`).

## Consequences

- **One entry point.** `pipeline run full --mode <env>` covers docker (local), staging (SFN), and
  prod (SFN). `scripts/run_prod_pipeline.py` and its hardcoded ARNs are gone.
- **Smaller, declarative staging workflow.** `deploy-staging.yml` drops ~160 lines of bash and two
  helper scripts; failure diagnostics now live in `pipeline/sfn.py` and run cross-platform.
- **Coupled deploy, Terraform-first.** `--mode` is a hard two-sided incompatibility, not a
  backward-compatible field addition: the new image **requires** `--mode` on
  `run-connector`/`run-consolidate-analytics` (added in this ADR), while the old image does not
  recognize `--mode` at all; the old ASL/templates pass no `--mode`, the new ones do. Neither
  artifact tolerates the other, so there is a break window for any run that fires between the two
  deploys. Order it **Terraform apply first, then push the image, then run `full --mode staging
  --wait`**: by the time the smoke `--wait` runs, new Terraform (ASL reads `consolidate_command`
  with `--mode`; EventBridge templates and ECS commands carry `--mode`) and the new image are both
  live, so it passes. Pushing the image first would make the deploy's own `--wait` fail — the old
  ASL hardcodes the consolidate command without `--mode`, which the new image rejects with
  "the following arguments are required: --mode". To keep a scheduled EventBridge run from failing
  in the window between the apply and the image push, apply Terraform and deploy the image
  back-to-back and/or pause the daily schedule across the window. (A follow-up could make `--mode`
  optional on those subcommands with an env-backed default so the new image tolerates old Terraform
  and the window disappears entirely.)
- **New boto3 dependency.** Every `.[pipeline]` install pulls boto3 (~80 MB with botocore). Docker
  dev installs boto3 but never imports it in docker mode (the `import boto3` in `cmd_full` is
  deferred to the staging/prod branch).
- **`STAGING_STATE_MACHINE_ARN` bridging.** Phase 3 reads `STAGING_STATE_MACHINE_ARN` but the
  GitHub secret is still named `DEMO_STATE_MACHINE_ARN`; the workflow bridges this with an env
  export. Phase 4 cleans up the rename.
- **`--wait` is a long-running local process.** It holds a CI runner (or terminal) for up to 15
  minutes. Acceptable for the staging deploy; prod remains schedule-driven.

## Validation

- `tests/test_sfn.py` — pure functions (`task_def_family`, `build_connector_command`,
  `build_consolidate_command`, `build_execution_input` asserts no `demo` key and `consolidate_command`
  present, `console_url`, `execution_name`, `state_machine_arn`), failure parsers
  (`parse_task_failed` extracts exitCode/task/reason; `parse_generic_failure` truncates at 500),
  `format_log_messages`, and boto3 wrappers via `MagicMock` (`resolve_task_def_arn`,
  `resolve_all_arns`, `start_execution`, `wait_for_execution` RUNNING→SUCCEEDED/FAILED/timeout,
  `fetch_failure_details` queries history + each `/ecs/portfolio-pipeline-demo-{name}` log group
  with start-time scoping, `build_clients`).
- `tests/test_run_subcommands.py::TestCmdFullSfnTrigger` — staging/prod start execution with the
  correct state-machine ARN per mode; `--with-xtb`/`--xtb-file` error; missing AWS creds error;
  missing `STAGING_STATE_MACHINE_ARN` error; `--wait` SUCCEEDED→0, FAILED→1 + details, timeout→1.
  The Phase 2 "not yet implemented" stub tests are deleted.
- `terraform -chdir=terraform/demo validate` and `terraform -chdir=terraform/prod validate` pass
  (validates the ASL `Command.$` change, `local.mode_flag` heredoc interpolation, and the
  `environment` block cleanup).
- `.venv/Scripts/python -m pytest tests/ -v` — full suite green.
- `ruff check --fix . && ruff format .` clean; tests re-run after auto-fixes.
- Manual: with AWS creds + `STAGING_STATE_MACHINE_ARN` set, `python -m pipeline.run full --mode
  staging` prints an execution ARN + console URL and exits 0; `... --wait` polls and exits 0 on
  success or 1 with TaskFailed/CloudWatch detail on failure.
- `grep -r run_prod_pipeline`, `grep -r parse_stepfunctions_event`, `grep -r format_log_events`
  return no references outside `docs/roadmaps/0012-*.md`.