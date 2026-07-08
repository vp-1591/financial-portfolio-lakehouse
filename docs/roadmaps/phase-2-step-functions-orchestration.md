# Plan: Phase 2 — Step Functions Orchestration (registry-driven)

> Plans the implementation of **Phase 2** of `docs/roadmaps/roadmap-productionization.md`.
> Workflow stage: `plan` (follows `roadmap`, precedes `implement` → `ADR` → `review`).

## Context

`docs/roadmaps/roadmap-productionization.md` Phase 1 (ADR 0049: branch/tag deploy, `DEMO` env
selector, `terraform/shared/` ECR, `deploy.yml` pushing `staging-latest`/`production-latest`) is
done. Phase 2 moves core pipeline execution to a single orchestrator Step Function that runs the
enabled connectors in parallel, waits for all, then runs `consolidate+allocate` once for a
consistent snapshot. Today the pipeline only runs via `python -m pipeline.run full` locally or a
GitHub-runner `workflow_dispatch` — no AWS orchestration, ECS, scheduling, or event trigger.

**Why this design (not the first draft):** an earlier plan baked the three connector names into
~10 places (CLI subcommands, Terraform vars, literal ASL `Parallel` branches, `iam:PassRole`
grants) — adding a 4th connector would be *more* expensive than today, defeating the connector
registry pattern. It also had a real correctness bug (daily-schedule + XTB sharing one state
machine with a literal XTB branch). This plan drives everything from the connector registry so
adding a connector touches only: (1) new connector package, (2) secrets in
`DEMO_SECRET_MAP`/`REQUIRED_SECRETS`, (3) one `ecs-task` module `for_each` map entry per env + SSM
params. No CLI edit, no ASL edit, no new Terraform var, no new PassRole grant.

**User-confirmed decisions:**
- **Secrets:** SSM Parameter Store `SecureString` per env, wired now (KMS-encrypted, free Standard
  tier, end-to-end runnable). Values seeded out-of-band (never in Terraform state).
- **Terraform:** local in-repo module `terraform/modules/ecs-task/` (called via `for_each`).
- **Networking:** private subnets + S3/ECR/CloudWatch VPC interface endpoints, no public IP.

---

## Reused, not rebuilt

- `pipeline/run.py` — `cmd_fetch` (107), `cmd_transform` (230), `cmd_consolidate` (288),
  `cmd_allocate` (366), `cmd_full` (414), `cmd_upload_xtb` (428).
- `pipeline/connectors/registry.py` — `get(name)`, `all()`; `pipeline/connectors/base.py`
  `BrokerConnector` Protocol.
- `pipeline/connectors/xtb/fetch.py` `_read_file_bytes` — already accepts `s3://`.
- `pipeline/secrets.py` — `is_enabled`, `is_demo`, `resolve_secret`, `DEMO_SECRET_MAP`.
- `pipeline/storage.py` `staging_path("xtb", name)` → `staging/xtb/` / `staging_demo/xtb/`.
- `Dockerfile` — `ENTRYPOINT ["python","-m","pipeline.run"]`, already supports subcommand args.
- `terraform/shared/main.tf` ECR + `pipeline-ecr-push-pull` policy (data-source reuse).
- `tests/` patterns: `conftest.py` `tmp_data_dir`/`fernet_key`/`env_key`; in-process monkeypatch
  (see `test_pipeline_integration.py`).

---

## Step 1 — Connector protocol: make connectors self-describing

The modularity foundation. Add three methods to `BrokerConnector` (base.py) and implement in each
connector (ibkr, trading212, xtb):

1. `fetch_kwargs(self, args: argparse.Namespace) -> dict` — builds the connector-specific
   snapshot kwargs currently hardcoded as the `if connector.name == "ibkr"/elif "trading212"/elif "xtb"`
   block in `cmd_fetch` (run.py:128-196). `args` is the argparse `Namespace` from the CLI parser.
   IBKR resolves `IBKR_FLEX_TOKEN`/`IBKR_FLEX_QUERY_ID`/`IBKR_FLEX_BASE_URL`; T212 resolves
   `T212_API_KEY`/`T212_API_SECRET` + `is_demo()` base URL; XTB reads `args.xtb_file`. Each
   connector imports `resolve_secret`/`get_env`/`is_demo` itself. Also `fetch_cdc_kwargs(self)
   -> dict` (T212 returns its snapshot kwargs; others `{}`).
2. `required_secrets(self) -> list[str]` — the base secret env-var names (e.g. IBKR returns
   `["IBKR_FLEX_TOKEN","IBKR_FLEX_QUERY_ID"]`). Used by a future validate step and to document SSM
   param names; cheap to add.
3. `extract_holdings(self, df: pl.DataFrame, fernet_key: bytes) -> list[Holding]` — moves the
   per-broker branch ladder from `pipeline/normalized/extract.py:86-134` onto the connector. Each
   connector knows its display name, description column, and security_currency source.
4. `enabled_env_var` — a class/instance attribute on each connector declaring the `*_ENABLED`
   environment variable name (e.g. `IbkrConnector.enabled_env_var = "IBKR_ENABLED"`,
   `Trading212Connector.enabled_env_var = "T212_ENABLED"`, `XtbConnector.enabled_env_var =
   "XTB_ENABLED"`). Avoids deriving the env var name from the connector registry name (the
   mapping is not 1:1 — `trading212` → `T212_ENABLED`, not `TRADING212_ENABLED`).

`pipeline/normalized/extract.py` `extract_holdings(broker, table_path, fernet_key)` becomes a thin
shim that reads the normalized table to a DataFrame and calls `get(broker).extract_holdings(df,
fernet_key)` — **public signature preserved** so `cmd_consolidate` and existing tests
(`test_consolidate.py`, `test_consolidate_pipeline.py`) keep working unchanged.

`pipeline/secrets.py`: any new connector adds its secrets to `DEMO_SECRET_MAP` (and
`REQUIRED_SECRETS` if that list exists — verify during impl). No other central registry edit.

## Step 2 — CLI: one generic `run-connector <name>` subcommand

Refactor `pipeline/run.py` (no behavior change to existing commands):
1. Extract `fetch_connector(connector, args: argparse.Namespace, fernet_key) -> int` from
   `cmd_fetch`'s loop body (114–226) — now calls `connector.fetch_kwargs(args)` (no `if/elif`),
   then `connector.fetch_snapshot(**kwargs)`, ingests raw, tries CDC. The XTB multi-file append
   loop moves inside `XtbConnector.fetch_kwargs`/`fetch_snapshot` (or the helper handles
   `args.xtb_file` list — pick the cleaner spot during impl). Preserve error-to-stderr behavior.
2. Extract `transform_connector(connector, fernet_key) -> int` from `cmd_transform`'s loop body
   (240–284).
3. `cmd_fetch`/`cmd_transform` iterate `all()` and call the helpers — unchanged behavior.
4. **One** new subcommand `run-connector` with positional `connector` (connector name) + the
   `--xtb-file` (append) and `common_parser` args. `cmd_run_connector(args)`:
   `connector = get(args.connector)`; if `not is_enabled(connector.enabled_env_var)`: log + return
   0 (runtime gate, matches existing behavior — uses the connector's `enabled_env_var` attribute
   so `trading212` maps to `T212_ENABLED`, not `TRADING212_ENABLED`); `rc =
   fetch_connector(...)`; `return rc if rc else transform_connector(...)`. For XTB without
   `--xtb-file`: print error, return **1** (dedicated subcommand fails loudly, unlike
   `cmd_fetch`'s silent skip).
5. `cmd_run_consolidate_allocate(args)` — `cmd_consolidate(args)` then `cmd_allocate(args)` (both
   idempotent full-overwrite; reuse unchanged). Register subcommand `run-consolidate-allocate`
   with `common_parser` + `--fx-rate`/`--isin`/`--isin-map-file`.
6. Add both to the `commands` dict. **No per-connector subcommands** — a 4th connector needs zero
   CLI changes. `full` stays for local dev.
7. Update `cmd_upload_xtb` print (run.py:457): replace "future phase" with "EventBridge will
   trigger the orchestrator on this file's arrival."

---

## Step 3 — Terraform local module `terraform/modules/ecs-task/`

Produces one `aws_ecs_task_definition` (Fargate, `awsvpc`) + task execution role + task role +
policies. Variables: `name`, `image`, `demo` (bool), `cpu`, `memory`, `command` (list),
`environment` (map), `secrets` (map of SSM ARNs), `bucket_name`, `s3_prefix`, `subnet_ids`,
`security_group_id`, `ecr_policy_arn` (reuse `pipeline-ecr-push-pull`), `kms_key_arn`, `region`,
`task_role_arn` (optional shared role; default creates one per task def for S3-scoped isolation).

Task execution role: ECR pull (attach `ecr_policy_arn`), CloudWatch Logs put,
`ssm:GetParameters` + `kms:Decrypt` on the env KMS key for `secrets`.
Task role: S3 read/write scoped to this env's `bucket_name`/`s3_prefix` only — no cross-env access.

CloudWatch Logs: Terraform creates one log group per task definition with naming convention
`/ecs/portfolio-pipeline-<env>-<connector>` (e.g. `/ecs/portfolio-pipeline-prod-ibkr`).
Retention: 7 days (personal-use pipeline; adjust if needed later). ECS `logConfiguration` in the
task definition points to the corresponding log group.

---

## Step 4 — Terraform `terraform/shared/` (orchestration + triggers)

Variables: `scheduled` (bool, default `false`), `xtb_enabled` (bool, default `true` — also creates
the S3 file-arrival rule), `schedule_connectors` (list, default `["ibkr","t212"]`),
`file_arrival_connectors` (list, default `["ibkr","t212","xtb"]`), `task_def_arns` (map
`name→arn`, default `{}` — fed from prod/demo outputs via tfvars; **keys are connector registry
names** like `ibkr`, `trading212`, `xtb` matching the `for_each` keys in per-env modules),
`consolidate_allocate_task_def_arn` (string), `xtb_staging_bucket_name`, `xtb_staging_prefix`
(default `staging/xtb/`), `schedule_cron` (default `0 6 * * ? *`), `ecs_cluster_arn`, `subnet_ids`,
`security_group_id`, `state_machine_name` (default `portfolio-pipeline-orchestrator`).

These replace the original per-connector `ibkr_enabled`/`t212_enabled` ASL-branch vars: connector
inclusion is now a **list in execution input**, not ASL structure. `scheduled` + `xtb_enabled`
remain as **trigger-creation** flags (EventBridge rules are genuinely Terraform resources). Adding
a connector = add to the two connector lists (or rely on defaults) — no ASL edit.

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
  `$.demo`. **One generic definition — no per-connector branches.** This also fixes the
  daily/XTB bug (see below).
- `aws_cloudwatch_event_rule` `xtb_file_arrival` (`count = var.xtb_enabled ? 1 : 0`):
  `source=["aws.s3"]`, `detail-type=["Object Created"]`, `detail.bucket.name`/`detail.object.key`
  prefix from `var.xtb_staging_prefix`. Target → state machine; `input_transformer` builds the
  execution input using **static constant templates** (EventBridge input transformers support
  constant values and `$.detail` JSON path extraction, but cannot dynamically construct lists).
  The `connectors` array is defined as a constant in the input_transformer template — adding a
  connector requires updating this template in Terraform, which is acceptable since it's
  configuration-only (no ASL or CLI edit). The template specifies each connector item's `name`,
  `task_def_arn` (looked up from `var.task_def_arns`), and `command` (e.g.
  `["run-connector","xtb","--xtb-file","s3://..."]` for the xtb item).
- `aws_cloudwatch_event_rule` `daily_schedule` (`count = var.scheduled ? 1 : 0`):
  `schedule_expression = var.schedule_cron`. Target → state machine; input lists
  `var.schedule_connectors` (no xtb_file). **No XTB item, no broken `States.Format`.** Like
  the file-arrival rule, the input_transformer uses static constant templates for the
  `connectors` array.

### Daily-schedule + XTB correctness (bug avoided)
An earlier design put both triggers into one state machine with a literal XTB branch reading
`$.detail.object.key` — on schedule input there is no object key, so the XTB branch errored,
exhausted retries, failed the `Parallel`, and consolidate-allocate never ran. The Map-over-input
design fixes this: the schedule target's input simply omits the xtb item (no xtb_file), so the
Map runs only the API connectors then consolidate-allocate. The file-arrival target's input
includes the xtb item with its s3 URI. One state machine, correct for both triggers.

---

## Step 5 — Terraform `terraform/prod/` and `terraform/demo/` (per env)

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
  KMS + grants; **values seeded out-of-band** — see Step 7). SSM naming convention mirrors
  `DEMO_SECRET_MAP` in `secrets.py`:
  - Prod: `/portfolio/prod/<SECRET>` (e.g. `/portfolio/prod/IBKR_FLEX_TOKEN`)
  - Demo: `/portfolio/demo/<SECRET>_DEMO` (e.g. `/portfolio/demo/IBKR_FLEX_TOKEN_DEMO`)
  This matches the existing convention where demo variants use `_DEMO` suffix.
- VPC: `aws_vpc` + **private** subnets + `aws_security_group` (egress to VPC endpoints) + **S3,
  ECR, and CloudWatch Logs VPC interface endpoints** (`aws_vpc_endpoint`, `Interface`) with
  route-table/private-DNS. No public IP. **Separate VPC per environment** (one in `prod/`, one in
  `demo/`) — full data isolation matches the existing S3/IAM model. `cluster_arn` passed in from
  `shared/`.
- Outputs: map of connector task-def ARNs + `consolidate_allocate_task_def_arn` + bucket name +
  subnet ids + security group id (consumed by `shared/` via tfvars).

---

## Step 6 — S3 key decoding (at the XTB boundary, not shared helpers)

EventBridge object keys arrive percent-encoded; `parse_s3_uri` (s3.py:19) does NOT unquote and is
shared with `upload_to_staging`/`read_s3_bytes` (locally-typed keys are already decoded → naive
unquote there risks double-decoding literal `%` sequences). **Decode once in the XTB fetch path:**
in `pipeline/connectors/xtb/fetch.py` `_read_file_bytes`, for the `s3://` branch,
`urllib.parse.unquote` the key before reading. `parse_s3_uri`/`read_s3_bytes` stay untouched.
Document the caveat (XTB report filenames should not contain literal `%`). This is the
single-decode point recommended in review.

---

## Step 7 — Secrets seeding runbook (documented in ADR 0051)

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

---

## Step 8 — Apply / deploy sequencing (variable-based decoupling)

State files stay independent (ADR 0049). Apply order:
1. `terraform/shared/` apply #1 — ECR + IAM policy + cluster (state machine/EventBridge
   `count=0` while `task_def_arns` empty).
2. `terraform/prod/` and `terraform/demo/` apply — S3, IAM users, ECS task defs (module `for_each`),
   roles, SSM params + KMS, VPC + endpoints, bucket EventBridge notification.
3. `terraform/shared/` apply #2 — state machine + EventBridge rules (connector ARN map + bucket
   name + subnet ids + cluster_arn in `shared/terraform.tfvars`).

---

## Tests (`tests/test_run_subcommands.py`, new + update connector tests)

In-process monkeypatch, reuse `tmp_data_dir`/`fernet_key`/`env_key`:
1. Argparse dispatch — `run-connector` + `run-consolidate-allocate` present in `commands` dict;
   `run-connector ibkr` resolves via `get("ibkr")`.
2. `fetch_connector`/`transform_connector` isolation — monkeypatch `get("ibkr")` methods; assert
   only `ibkr_*` raw/normalized written; `trading212_*` untouched. Verify `fetch_kwargs` is called
   on the connector (no if/elif in the helper).
3. `cmd_run_connector` for each connector — set `*_ENABLED=1` + mock secrets; fetch+transform that
   connector only. XTB without `--xtb-file` returns 1.
4. `connector.fetch_kwargs()` / `extract_holdings()` — unit tests per connector (new methods from
   Step 1); assert IBKR/T212/XTB each build correct kwargs and produce correct `Holding` lists
   (port the expectations from existing `extract_holdings` behavior).
5. `cmd_run_consolidate_allocate` — seed normalized fixtures for two connectors; assert
   `consolidated_holdings` + `portfolio_allocation` exist.
6. `cmd_full` + existing `cmd_fetch`/`cmd_transform` regression — unchanged behavior (helpers
   iterate `all()`).
7. XTB s3 key decoding — unit test `_read_file_bytes` with a percent-encoded s3 key unquotes
   correctly (mock `read_s3_bytes`).

Run `ruff check --fix . && ruff format .`, then `.venv/Scripts/python -m pytest tests/ -v`.

---

## ADR 0051 + roadmap updates

Create `docs/adr/0051-step-functions-orchestration.md`:
- **Context** — Phase 2 need; the first design's modularity regression + daily/XTB bug as the
  trigger for the registry-driven revision.
- **Decision** — orchestrator `Map` over a connector list from execution input (not literal
  branches); one `run-connector <name>` subcommand; connector self-description via
  `fetch_kwargs`/`required_secrets`/`extract_holdings`/`enabled_env_var`; SSM SecureString secrets
  + per-env KMS; VPC endpoints (no public IP); cluster-in-shared/networking-in-env;
  variable-based apply decoupling; PassRole scoped to a role-name prefix; per-connector task defs
  via module `for_each` for secret isolation; CloudWatch Logs with 7-day retention per task.
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

---

## Implementation sharding

The plan is split into three PRs for focused review:

### PR 1: Connector protocol (Steps 1)

Connector self-description: `fetch_kwargs`, `fetch_cdc_kwargs`, `required_secrets`,
`extract_holdings`, `enabled_env_var` on the `BrokerConnector` protocol. Implement in all three
connectors. Refactor `extract_holdings` in `pipeline/normalized/extract.py` to delegate to
connectors. **No behavior change** — existing `full`/`fetch`/`transform`/`consolidate`/`allocate`
commands produce identical results.

**Verification:** all existing tests pass; new unit tests for each connector's protocol methods.

### PR 2: CLI subcommands (Step 2 + Step 6)

Extract `fetch_connector`/`transform_connector` helpers from `cmd_fetch`/`cmd_transform`. Add
`run-connector <name>` and `run-consolidate-allocate` subcommands. XTB S3 key percent-decoding in
`_read_file_bytes`. Update `cmd_upload_xtb` print message. **No behavior change to existing
commands** — `full`/`fetch`/`transform` still work identically.

**Verification:** existing + new tests pass; `docker build` succeeds; `run-connector --help` and
`run-consolidate-allocate --help` print usage.

### PR 3: Terraform infrastructure (Steps 3–5 + Steps 7–8)

ECS task module, shared orchestrator state machine + EventBridge triggers, per-env task defs + VPC
+ SSM + IAM. Secrets seeding runbook. Apply sequencing. ADR 0051 + roadmap status update.

**Verification:** `terraform validate` + `terraform plan` in `shared/`, `prod/`, `demo/`; end-to-end
cloud smoke test.

---

## Verification

1. `.venv/Scripts/python -m pytest tests/ -v` — existing + new tests pass.
2. `ruff check .` and `ruff format --check .` clean.
3. `docker build -t portfolio-pipeline:phase2 .` succeeds.
4. `docker run --rm portfolio-pipeline:phase2 run-connector --help` and
   `run-consolidate-allocate --help` print usage; `run-connector ibkr` (with mocked env) runs.
5. `terraform validate` + `terraform plan` (with vars) in `shared/`, `prod/`, `demo/`.
6. Apply order: shared (ECR/cluster) → prod/demo → shared (orchestration with ARN map in tfvars).
7. Seed SSM values per Step 7 (verify `ENCRYPTION_KEY` matches existing raw data).
8. End-to-end cloud smoke: `python -m pipeline.run upload-xtb <file>` → EventBridge fires the
   orchestrator → Map runs the enabled connector tasks in parallel + consolidate-allocate →
   CloudWatch Logs show each step → analytics `portfolio_allocation` updated in S3. Also trigger a
   manual daily-schedule execution (no xtb_file) and confirm it completes without a broken XTB
   branch.