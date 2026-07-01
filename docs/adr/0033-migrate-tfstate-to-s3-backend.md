# 0033: Migrate Terraform State from Local to S3 Backend

## Context

Terraform state was stored locally in `terraform/terraform.tfstate`. This creates several problems:

- **Single point of failure**: The state file only exists on one machine. If the disk is lost, the state is gone.
- **No collaboration**: No other developer or CI system can run Terraform without the local state.
- **No locking**: Concurrent `terraform apply` runs could corrupt the state.

The project uses AWS (eu-west-1) for all infrastructure.

## Decision

Migrate Terraform state to an S3 backend with native S3 locking (`use_lockfile = true`), eliminating the need for a separate DynamoDB table.

The state bucket was created outside Terraform (bootstrap resource) with:

- Versioning enabled (for state recovery)
- AES256 server-side encryption
- All public access blocked

The backend configuration lives in a gitignored `backend.tf` file (the repo will be public and should not contain account identifiers). Terraform auto-loads `.tf` files in the working directory, so `terraform init` works without extra flags:

```hcl
# backend.tf (gitignored — copy from backend.tf.sample)
terraform {
  backend "s3" {
    bucket       = "<your-s3-bucket-name>"
    key          = "investment-portfolio-dashboard/terraform.tfstate"
    region       = "eu-west-1"
    use_lockfile = true
  }
}
```

The committed `backend.tf.sample` serves as a template.

## Consequences

- **Positive**: State is durable, versioned, and accessible from any machine with AWS credentials.
- **Positive**: Native S3 locking (`use_lockfile = true`) prevents concurrent state corruption without requiring a DynamoDB table. This feature was introduced in Terraform 1.11+; the project uses Terraform 1.14.9.
- **Positive**: AES256 encryption and blocked public access protect the state file (which contains sensitive data like IAM access keys).
- **Positive**: No account identifiers are committed to the repository — the bucket name lives in a gitignored `backend.tf`.
- **Positive**: `terraform init` works without extra flags — Terraform auto-loads `backend.tf`.
- **Negative**: The state bucket is a bootstrap resource not managed by Terraform. It must be maintained manually (e.g., if the bucket policy or lifecycle rules need changes).
- **Negative**: AWS credentials are now required for all Terraform operations, including `terraform plan`.

## Validation

- `terraform init` (no flags) successfully configures the S3 backend via auto-loaded `backend.tf`.
- `terraform state list` shows all 9 resources intact after migration.
- `terraform plan` reports "No changes" — the infrastructure matches the configuration with no drift.
- The local `terraform.tfstate` file is gone, confirming state has been fully migrated to S3.