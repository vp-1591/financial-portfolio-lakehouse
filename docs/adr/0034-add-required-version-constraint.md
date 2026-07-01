# 0034: Add required_version Constraint for Terraform 1.11+

## Context

PR #12 migrated Terraform state to an S3 backend using `use_lockfile = true`, which requires Terraform 1.11+. However, the `terraform` block in `main.tf` had no `required_version` constraint, meaning users running older Terraform versions would encounter a confusing `argument-not-expected` error at `terraform init` rather than a clear version mismatch message.

## Decision

Add `required_version = ">= 1.11"` to the `terraform` block in `terraform/main.tf`. This enforces the minimum version at the code level and provides a clear error message when an incompatible version is used.

## Consequences

- **Positive**: Users with Terraform < 1.11 will get an immediate, clear error message about version incompatibility.
- **Positive**: The version requirement documented in ADR 0033 is now enforced programmatically.
- **Negative**: Contributors must have Terraform 1.11+ installed, though this was already a de facto requirement due to `use_lockfile`.

## Validation

- `terraform init` succeeds with the constraint in place.
- `terraform validate` passes.