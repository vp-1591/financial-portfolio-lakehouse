# ADR 0050: Fix Deploy Workflow — ECR Policy Attachment and Buildx Driver

## Context

ADR 0049 introduced a deploy workflow that pushes Docker images to ECR. Two
issues prevented it from working:

1. **IAM permissions missing**: The `terraform/shared/` module creates the
   `pipeline-ecr-push-pull` IAM policy, and `terraform/prod/` creates the
   `pipeline` IAM user, but ADR 0049 documented the policy attachment as a
   manual console step that was never performed. The Deploy workflow failed
   with `ecr:GetAuthorizationToken` denied.

2. **Buildx cache export unsupported**: The workflow used `cache-from:
   type=gha` and `cache-to: type=gha,mode=max`, which require the
   `docker-container` buildx driver. The default `docker` driver does not
   support cache export, causing the build to fail with "Cache export is
   not supported for the docker driver."

## Decision

### IAM policy attachment in Terraform

Automate the policy attachment in Terraform. The `terraform/prod/` module
now uses a `data "aws_iam_policy"` lookup by the well-known policy name
`pipeline-ecr-push-pull` and attaches it to the `pipeline` user with an
`aws_iam_user_policy_attachment` resource.

Alternatives considered:

1. **Manual console step** (rejected) — already proved fragile; the step
   was skipped and CI broke silently.
2. **`terraform_remote_state` data source** (rejected) — would couple the
   `prod` and `shared` state files, making state migration and refactoring
   harder. A policy name is a stable, well-known identifier; a data source
   lookup by name is sufficient and keeps the state files independent.
3. **Merge `shared/` into `prod/`** (rejected) — ECR is shared between
   staging and production environments. Keeping it in its own state file
   avoids environment-specific drift and supports future `demo/` attachment
   without cross-environment state coupling.

### Docker Buildx setup

Add `docker/setup-buildx-action@v3` before the build-push step. This
creates a `docker-container` driver instance that supports GHA cache
export, which is required by the existing `cache-from`/`cache-to`
configuration.

## Constraints

- `terraform/shared/` must be applied before `terraform/prod/` so the
  `pipeline-ecr-push-pull` policy exists when the data source reads it.
- The policy name `pipeline-ecr-push-pull` is now an implicit contract
  between the two modules. Renaming it in `shared/` requires updating the
  data source in `prod/`.
- No changes to `ci.yml` or `pipeline.yml`.

## Consequences

- **Positive**: CI no longer depends on a manual console step. Applying
  `terraform/prod/` after `terraform/shared/` is sufficient for the Deploy
  workflow to authenticate with ECR.
- **Positive**: Docker layer caching via GHA works correctly with the
  `docker-container` buildx driver.
- **Positive**: State files remain independent — no `terraform_remote_state`
  coupling.
- **Neutral**: Apply order matters (`shared` before `prod`). This is already
  the natural order since `shared/` creates the ECR repository the deploy
  workflow targets.

## Validation

- `cd terraform/shared && terraform plan` shows no changes (policy already
  exists).
- `cd terraform/prod && terraform plan` shows the new `data.aws_iam_policy`
  read and `aws_iam_user_policy_attachment` resource.
- After `terraform apply`, re-running the Deploy workflow succeeds past the
  ECR login step.
- Deploy workflow builds and pushes the Docker image with GHA caching.