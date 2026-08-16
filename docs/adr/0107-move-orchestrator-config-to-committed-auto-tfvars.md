# 0107: Move orchestrator connector/schedule config to committed connectors.auto.tfvars

## Context

The Step Functions orchestrator (ADR 0091; the per-environment state machines of ADR 0052/0054 superseded the original ADR 0051 design) is driven by four operational variables on the `terraform/modules/orchestrator` module:

- `scheduled` (bool) — whether the EventBridge daily-schedule trigger fires the orchestrator.
- `schedule_cron` (string) — cron expression for that schedule.
- `schedule_connectors` (list) — connectors included in the daily-schedule execution input.
- `file_arrival_connectors` (list) — connectors included in the XTB S3 file-arrival execution input.

These four values were set as **literals inside the `module "orchestrator" { ... }` block** of `terraform/prod/main.tf` and `terraform/demo/main.tf`, buried among ~15 infrastructure arguments (cluster ARN, subnets, security groups, IAM, S3). They were **also duplicated as module-level defaults** in `terraform/modules/orchestrator/variables.tf` — three copies of each list, all identical, so the per-env pass was redundant and it was unclear which copy was authoritative.

After ADR 0094 removed the Python `*_ENABLED` env-var toggles, the Terraform trigger lists (`schedule_connectors` / `file_arrival_connectors`) became the **sole mechanism** for enabling or disabling a connector in staging/prod. Yet toggling a connector still required diving into the deep module block of `main.tf` — exactly the "buried in infrastructure wiring" problem that motivated this change.

`terraform.tfvars` is gitignored (`terraform/{prod,demo}/.gitignore`: `*.tfvars`) because it holds account-specific ARNs (ECR, ECS cluster) that may contain identifiers. So the existing auto-loaded tfvars surface is **not committed** and cannot serve as a visible source of truth; only `terraform.tfvars.example` (a template, not auto-loaded) is committed. ADR 0020 had already established the project's caution toward config surfaces that do not reach the runtime that matters.

## Decision

Lift the four operational values into a **committed, auto-loaded `connectors.auto.tfvars`** file per environment, and make the module variables required:

1. Create `terraform/prod/connectors.auto.tfvars` and `terraform/demo/connectors.auto.tfvars` holding the four values. Terraform auto-loads `*.auto.tfvars`, so no `-var-file` flag and no deploy-workflow change is required.
2. Declare the four variables at the env `main.tf` level (alongside the existing `aws_region` / `bucket_name` / etc.) and reference them as `var.*` in the module block — the same pattern already used for `ecr_repository_url` / `ecs_cluster_arn` (declared in `main.tf`, valued in tfvars).
3. **Remove the module-level defaults** for all four variables in `terraform/modules/orchestrator/variables.tf`, making them required. Each env sets them explicitly in its committed `connectors.auto.tfvars`, which becomes the single source of truth per environment — no silent stale default.
4. Add a scoped gitignore negation `!connectors.auto.tfvars` to `terraform/{prod,demo}/.gitignore` so this one non-secret tfvars file is committed despite the blanket `*.tfvars` ignore.
5. Keep the gitignored `terraform.tfvars` for the secret-ish ARNs only; add a pointer comment in `terraform.tfvars.example` directing operators to `connectors.auto.tfvars` for connector toggles.

Alternatives rejected:

- **Keep values as literals in `main.tf`** (status quo): fails the goal — toggling requires editing the deep module block.
- **Put the values in the gitignored `terraform.tfvars`**: not committed, so not visible in the repo/PR — defeats the "visible source of truth" goal (the ADR 0020 reachability problem, in repo form).
- **A non-Terraform config file (YAML/JSON) read via `yamldecode`/`jsondecode`**: couples Terraform to a repo-relative file path and is non-idiomatic; `*.auto.tfvars` achieves the same visibility with zero coupling and no extra parsing.
- **Keep the module defaults and just override them in tfvars**: leaves three copies of each value and a silent stale default — the parallel encoding this ADR removes.

## Constraints

- `connectors.auto.tfvars` may contain **only non-secret operational config** (the four variables above). Any secret or account-identifier value stays in the gitignored `terraform.tfvars`. The gitignore negation is scoped by exact filename (`!connectors.auto.tfvars`), not a broad un-ignore of all tfvars.
- Both prod and demo must set all four variables (they are now required). A new environment that forgets to provide them fails loudly at `terraform plan` with a missing-required-variable error rather than silently inheriting a stale default.
- **No behavior change**: the committed values are identical to the previous literals, so `terraform plan` shows no resource diff.

## Consequences

- Toggling a connector in staging/prod = editing one flat, committed, visible file (`connectors.auto.tfvars`) — no `main.tf` or module-block edit, no diving into infrastructure wiring.
- Single source of truth per environment for the connector lists and schedule: the duplicate module defaults and the redundant env-level literals are gone (12 literal occurrences reduced to 8; each value lives in exactly one place).
- A deliberate, **scoped policy exception** to the "tfvars files may contain secrets → gitignore" rule: one named non-secret tfvars file is committed. Future non-secret operational config can follow this pattern; secret config must not. This is the main risk — a future contributor must understand the negation is intentional and scoped, not a mistake to "clean up" (hence the ADR-reference comment on the gitignore line).
- Slightly more files per env (one new `connectors.auto.tfvars`), offset by removing the buried literals and the redundant module defaults.

## Validation

- `terraform validate` passes for `terraform/modules/orchestrator`, `terraform/prod`, and `terraform/demo` (each with `terraform init -backend=false`).
- `terraform fmt` passes on the new `connectors.auto.tfvars` files and the module `variables.tf`.
- `git check-ignore terraform/prod/connectors.auto.tfvars` returns nothing (the file is tracked, not ignored).
- `git diff --stat` confirms a minimal, focused diff (7 files changed, ~69 insertions / 12 deletions, plus 2 new files) with no unrelated formatting churn in `main.tf` — `terraform fmt` was deliberately not run on `main.tf` to avoid reformatting pre-existing code.
- No `terraform plan` resource diff is expected because the committed values equal the previous literals (not run here — requires AWS backend access; recommended to confirm in the deploy environment).