# ADR 0049: Deployment Model — Branch/Tag Environment Strategy

> **Superseded by [ADR 0063](./0063-simplify-ecr-tagging-strategy.md)** — The `git-<sha>` and `<version>` ECR tags have been removed. Only `staging-latest` and `production-latest` remain.

## Context

The roadmap (`docs/roadmaps/0001-productionization.md`) defines a branch/tag-based
deployment model where `main` is the staging branch and version tags trigger
production deployments. Deploy and run are separate concerns: a deployment
makes a new container image available; a run executes the pipeline steps.

The pipeline already has demo mode (`DEMO=true`) for credential and storage
isolation, Terraform infrastructure for S3 buckets in both `prod/` and
`demo/`, a Docker build verified in CI, and a manual `workflow_dispatch`
for running the pipeline. What is missing is the container registry and the
deploy workflow that pushes images on merge to `main` (staging) and on
version tags (production).

## Decision

### Single ECR repository with tag-based environment distinction

One ECR repository (`investment-portfolio-pipeline`) serves both
environments. Images are tagged:

| Trigger | Tags |
|---|---|
| Merge to `main` | `git-<short-sha>` (immutable) + `staging-latest` (mutable) |
| Version tag (`v*`) | `<tag-name>` (immutable) + `production-latest` (mutable) |

This avoids duplicating ECR infrastructure for a single-tenant pipeline
where the isolation boundary is S3 buckets and credentials, not the
container registry.

### `DEMO` flag as environment selector

No new `PIPELINE_ENV` variable. The existing `DEMO` flag already provides
the needed isolation:

| `DEMO` | Environment | Credentials | Storage |
|---|---|---|---|
| `true` | staging | `_DEMO` suffixed secrets | `S3_BUCKET_DEMO` / `pipeline_demo` |
| `false` or unset | production | base secrets | `S3_BUCKET` / `pipeline` |

A `PIPELINE_ENV` variable would create a confusing matrix (`DEMO=true,
PIPELINE_ENV=production`?) that requires cross-validation logic with no
benefit. If a third environment is needed later, `PIPELINE_ENV` can be
added without breaking existing behavior.

### `terraform/shared/` directory for ECR

ECR is shared infrastructure used by both staging and production
deployments. A `terraform/shared/` directory (with its own state file)
keeps environment-specific resources (`prod/`, `demo/`) separate from
shared resources.

### Deploy workflow on merge and tag

A new `.github/workflows/deploy.yml` triggers on:
- **Push to `main`** — build, tag `git-<sha>` + `staging-latest`, push to ECR
- **Version tag `v*`** — build, tag `<version>` + `production-latest`, push to ECR

The existing `ci.yml` (lint, test, docker build) runs on every PR and
push. The existing `pipeline.yml` (manual pipeline dispatch) remains
unchanged. Deploy and run are separate workflows.

### Staging pipeline runs deferred to Phase 3

The roadmap says merge to `main` triggers "run pipeline with demo creds,
data quality checks." Data quality checks are Phase 3 work. Running the
pipeline without validation provides limited value — the manual
`workflow_dispatch` already allows on-demand runs. Phase 3 will add an
automated run with quality gates after deploy.

## Constraints

- Must not break existing `DEMO` mode behavior or require new GitHub
  Secrets.
- Must work with the existing Terraform state layout (separate state
  files per directory).
- The `pipeline` IAM user (from `terraform/prod/`) is granted ECR push/pull
  permissions via a data source lookup of the `pipeline-ecr-push-pull` IAM
  policy (defined in `terraform/shared/`). The `shared` module must be
  applied before `prod` so the policy exists when the attachment is created.
- No changes to `ci.yml` or `pipeline.yml`.

## Consequences

- **Positive**: Clear separation between deploy (push to ECR) and run
  (execute pipeline steps). No new environment variable — `DEMO` already
  provides the needed isolation. ECR lifecycle policy prevents unbounded
  storage costs. Docker layer caching via GitHub Actions keeps build times
  low.
- **Positive**: Phase 2 Step Functions can reference the `staging-latest`
  or `production-latest` tag for ECS task definitions, enabling zero-downtime
  updates by pushing a new image.
- **Neutral**: The ECR push IAM policy lives in `shared/` state while the
  pipeline IAM user lives in `prod/` state. The attachment uses a data
  source lookup by policy name, so the state files remain independent but
  `terraform/shared/` must be applied before `terraform/prod/`.
- **Neutral**: Staging pipeline runs are manual until Phase 3.
- **Negative**: The `staging-latest` and `production-latest` tags are
  mutable. If two tags are pushed in quick succession, the mutable tag
  briefly points to the older image. This is not a practical concern for
  a personal project with infrequent deploys.

## Validation

- `cd terraform/shared && terraform plan` shows ECR repository, lifecycle
  policy, and IAM policy — no errors.
- Push to `main` triggers `deploy.yml`; image appears in ECR with
  `git-<sha>` and `staging-latest` tags.
- Push of `v1.0.0` tag triggers `deploy.yml`; image appears in ECR with
  `v1.0.0` and `production-latest` tags.
- `docker pull <ecr-url>:staging-latest && docker run --rm <ecr-url>:staging-latest --help`
  succeeds.
- `ci.yml` and `pipeline.yml` are unchanged and still pass.