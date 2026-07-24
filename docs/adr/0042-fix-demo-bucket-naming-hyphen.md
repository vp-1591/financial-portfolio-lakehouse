# 0042: Fix Demo Bucket Naming — Use Hyphen Instead of Underscore

> **Superseded by [ADR 0092](./0092-remove-demo-env-var-suffix.md)** — `S3_BUCKET_DEMO` is removed. The staging bucket is now set via `S3_BUCKET` in the demo ECS task environment.

## Context

The pipeline's demo mode constructs a default S3 bucket name by appending `_demo` (underscore) to the production bucket name via `f"{s3_bucket}_demo"` in `pipeline/storage.py`. However, S3 bucket names **cannot contain underscores** — only lowercase letters, numbers, and hyphens are allowed.

The Terraform demo infrastructure (`terraform/demo/main.tf`) correctly defines the bucket as `investment-portfolio-pipeline-demo` (hyphen). The mismatch means that when `S3_BUCKET_DEMO` is not explicitly set, the pipeline derives a bucket name like `investment-portfolio-pipeline_demo` which is invalid for S3 and will never match the Terraform-created bucket, resulting in `NoSuchBucket` errors.

## Decision

Change the default demo bucket suffix from `_demo` to `-demo`:

- `pipeline/storage.py` line 294: `f"{s3_bucket}_demo"` → `f"{s3_bucket}-demo"`
- Updated docstring from `{S3_BUCKET}_demo` to `{S3_BUCKET}-demo`
- Updated all test assertions, `.env.example`, `README.md`, and ADR 0037 to reflect the hyphen suffix

The `S3_PREFIX_DEMO` default (`pipeline_demo`) is unaffected — underscores are valid in S3 key prefixes.

## Consequences

- **Bug fix**: Demo pipeline runs that rely on the default bucket name will now correctly resolve to the Terraform-created bucket.
- **No breaking change for explicit config**: Users who set `S3_BUCKET_DEMO` explicitly (as recommended) are unaffected.
- **Breaking change for code default**: Any code or script that relied on the `_demo` suffix pattern must be updated to use `-demo`.

## Validation

- `tests/test_storage_config.py`: `TestDemoStorage` assertions updated to expect `-demo` suffix
- All existing tests pass with `ruff check` and `pytest`