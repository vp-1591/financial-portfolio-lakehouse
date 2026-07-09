# 0055: IAM Role Credential Fallback for ECS Tasks

> **Supersedes [ADR 0041](./0041-step-level-ci-secrets-and-explicit-empty-credentials.md)** — Decision #2 (explicit empty credentials) is superseded. When both AWS credentials are `None`, credential keys are now omitted (allowing SDK fallback) instead of set to empty strings (blocking fallback). ADR 0041 Decision #1 (step-level CI secrets) and Decision #3 (demo-aware DuckDB SECRET creation) remain in force.

## Context

ADR 0041 introduced explicit empty strings for missing AWS credentials to prevent SDK fallback to environment variables that might contain production credentials in a demo CI run. This was a correct isolation measure for GitHub Actions, where both production and `_DEMO` secrets are injected at the job level.

However, ADR 0054 switched ECS Fargate tasks from private subnets (with VPC endpoints) to public subnets (with Internet Gateway). In the old architecture, S3 access worked through the VPC S3 Gateway Endpoint using the ECS task IAM role — no explicit credentials were needed. The `object_store` and PyArrow SDKs never reached the authentication layer because the VPC endpoint handled it.

On public subnets with no VPC endpoints, S3 requests go through the Internet Gateway and require authentication. The ECS task IAM role provides this authentication via the instance metadata endpoint. But the application code passes empty strings for `aws_access_key_id` and `aws_secret_access_key` to both `object_store` (deltalake) and PyArrow, which explicitly blocks their credential chain from reaching the IAM role provider.

The result: ECS tasks fail with `AuthorizationHeaderMalformed` because the SDKs try to authenticate with empty credentials instead of falling through to the IAM task role.

DuckDB was already handled correctly (ADR 0041 Decision #3): in production mode with both credentials `None`, no SECRET is created, allowing DuckDB to fall back to IAM role metadata. This pattern needs to be extended to `object_store` and PyArrow.

## Decision

When both `key_id` and `secret_key` are `None`, `AwsCredentials.to_storage_options()` and `AwsCredentials.to_pyarrow_kwargs()` now **omit** credential keys entirely instead of setting them to empty strings. This allows the underlying SDKs to fall through their default credential chains (which include ECS IAM task role metadata).

When either credential is set, both keys are included (with the missing one as an empty string). This preserves the existing isolation behavior: a partial credential set still blocks fallback to environment variables.

This matches the existing DuckDB pattern in `_configure_s3()` (ADR 0041 Decision #3).

### Why this is safe for demo isolation

- In **demo CI runs** (GitHub Actions), `_DEMO` secrets are always set as environment variables by the workflow. `resolve_secret()` resolves them, so `key_id` and `secret_key` are never both `None`. The SDKs receive real credentials and demo isolation is preserved.
- In **demo ECS runs**, `_DEMO` secrets are injected via SSM parameters. If they're missing, the task should fail — and it will, because the IAM task role is scoped to the demo bucket only, and the `_DEMO` suffixed environment variables (`S3_BUCKET_DEMO`, `DEMO=true`) still point to demo resources. A missing `_DEMO` secret will cause a failure at the application level (e.g., `resolve_secret()` returns `None`), not a silent fallback to production.
- In **production ECS runs**, there are no `_DEMO` secrets, and the IAM task role is scoped to the production bucket. IAM role fallback is the correct behavior.

## Constraints

- ECS task IAM roles must have S3 read/write permissions scoped to the correct bucket. The existing Terraform module (`terraform/modules/ecs-task/main.tf`) already provides this.
- `to_duckdb_secret_parts()` is unchanged — it still returns `KEY_ID ''` and `SECRET ''` when credentials are `None`, because the caller (`_configure_s3()`) handles the `None`/`None` case with a separate branch that skips SECRET creation entirely.
- GitHub Actions CI workflows still use step-level secret injection (ADR 0041 Decision #1) to prevent production secrets from appearing in a demo run's environment. This is a separate layer of isolation that is not affected by this change.

## Consequences

- **Positive**: ECS tasks on public subnets can access S3 via the IAM task role without explicit credentials. This eliminates the `AuthorizationHeaderMalformed` error seen after ADR 0054.
- **Positive**: Simpler ECS task definitions — no need to set `AWS_ACCESS_KEY_ID_DEMO` / `AWS_SECRET_ACCESS_KEY_DEMO` SSM parameters or environment variables for the IAM role path.
- **Positive**: Consistent with the existing DuckDB pattern and the general AWS SDK credential chain behavior.
- **Neutral**: In local development with `STORAGE_TYPE=cloud` and no explicit credentials, the SDK will fall back to the default credential chain (environment variables, `~/.aws/credentials`, instance metadata). This is the expected behavior — developers who need explicit isolation should set the appropriate environment variables.
- **Risk**: If a demo CI run somehow has production credentials in its environment but no `_DEMO` credentials, the SDK could fall back to production credentials. This risk is mitigated by ADR 0041 Decision #1 (step-level secrets), which ensures production secrets are never present in a demo run's environment.

## Validation

1. Updated `test_to_storage_options_none_credentials_omitted` asserts credential keys are absent when both are `None`.
2. Updated `test_to_pyarrow_kwargs_none_credentials_omitted` asserts credential keys are absent when both are `None`.
3. New `test_to_storage_options_partial_credentials_includes_empty` verifies that partial credentials still include both keys with empty string for the missing one.
4. New `test_to_pyarrow_kwargs_partial_credentials_includes_empty` verifies the same for PyArrow kwargs.
5. All 89 tests in `tests/test_secrets.py` pass.
6. Deploy to demo and trigger a pipeline run to confirm S3 access via IAM task role works.