# 0038: Demo Terraform Infrastructure in Separate Directory

## Context

The pipeline already supports a `DEMO` mode that routes to `_DEMO`-suffixed
secrets and separate S3 storage (ADR 0037). However, there was no Terraform to
provision the demo S3 bucket and IAM user — the user had to create these
resources manually.

The production Terraform lived in the root `terraform/` directory. Adding demo
infrastructure alongside it in the same directory (or via workspaces) would
mix concerns and make it easy to accidentally apply demo changes to prod or
vice versa.

## Decision

Create a separate `terraform/demo/` directory with its own Terraform config
for the demo S3 bucket and IAM user, and move the production config into
`terraform/prod/` for a symmetric layout:

```
terraform/
  prod/    ← production S3 bucket and IAM user
  demo/    ← demo S3 bucket and IAM user
```

Each directory is self-contained with its own `main.tf`, `outputs.tf`,
`variables.tf`, `backend.tf.sample`, and `.gitignore`. State is fully isolated:

- Prod state key: `investment-portfolio-dashboard/terraform.tfstate`
- Demo state key: `investment-portfolio-dashboard-demo/terraform.tfstate`

Both use the same S3 backend bucket but different keys, so `terraform apply`
in one directory cannot affect the other.

The demo config creates:

- S3 bucket `investment-portfolio-pipeline-demo` (with versioning, SSE,
  public access block)
- IAM user `pipeline-demo` with least-privilege access scoped to the demo
  bucket and `pipeline_demo` prefix only
- IAM access key for `AWS_ACCESS_KEY_ID_DEMO` / `AWS_SECRET_ACCESS_KEY_DEMO`

No shared Terraform modules — the duplication (~20 lines of S3 security
configuration) is acceptable for this small project. If a third environment
(e.g., staging) is needed later, modules can be extracted at that point.

## Consequences

- **Positive**: Production and demo infrastructure are fully isolated in
  separate directories with separate state files. `terraform apply` in one
  cannot affect the other.
- **Positive**: Symmetric layout (`terraform/prod/` and `terraform/demo/`)
  makes it obvious where to find each environment's config.
- **Positive**: Demo IAM user is scoped to the demo bucket only — no
  accidental access to production data.
- **Positive**: Adding more environments (staging, etc.) is just another
  subdirectory.
- **Negative**: Some duplication between prod and demo configs. If the S3
  security settings change, they must be updated in both places.
- **Negative**: Users must run `terraform init` in the new `terraform/prod/`
  directory since it moved from `terraform/`.

## Validation

- `terraform plan` in `terraform/prod/` shows no changes (existing resources
  are adopted)
- `terraform plan` in `terraform/demo/` shows the new S3 bucket, IAM user,
  policy, and access key
- Demo IAM policy only grants access to the demo bucket and prefix
- All existing tests pass (no Python changes)