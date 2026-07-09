# ADR 0041: Step-Level CI Secrets and Explicit Empty Credentials

> **Superseded by [ADR 0055](./0055-iam-role-credential-fallback.md)** — Decision #2 (explicit empty credentials) is superseded. When both AWS credentials are `None`, credential keys are now omitted to allow IAM role fallback instead of being set to empty strings.

## Context

PR #16 introduced DEMO mode with isolated secrets. Code review (PR #16 review) identified two isolation gaps:

1. **GitHub Actions injects all secrets at the job level** — both production and `_DEMO` secrets are set as environment variables for every workflow run, regardless of whether demo mode is selected. In a demo run, production credentials are present in the process environment even though `resolve_secret()` never reads them. If code bypasses `resolve_secret()` and reads environment variables directly, production credentials would be accessible in demo mode.

2. **`AwsCredentials` helper methods omit credential keys when `None`** — `to_storage_options()`, `to_pyarrow_kwargs()`, and `to_duckdb_secret_parts()` omit `aws_access_key_id`/`aws_secret_access_key` keys from their output dicts when the corresponding fields are `None`. This allows object_store, PyArrow, and DuckDB to fall back to their default credential chains, which check `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` environment variables. In a demo run where production credentials are in the environment, these SDKs would silently use production credentials for S3 operations.

## Decision

1. **Step-level conditionals for CI secrets** — Split the "Run pipeline" step into two conditional steps in `.github/workflows/pipeline.yml`: one for production (injects only production secrets) and one for demo (injects only `_DEMO` secrets). Non-secret configuration (`DEMO`, `STORAGE_TYPE`, `S3_BUCKET`, etc.) remains at the job level. Production secrets are never present in a demo run's environment, and `_DEMO` secrets are never present in a production run's environment.

2. **Explicit empty credentials instead of omitted keys** — Change `AwsCredentials.to_storage_options()`, `to_pyarrow_kwargs()`, and `to_duckdb_secret_parts()` to always include credential keys, using empty strings when the corresponding fields are `None`. This prevents SDKs from falling back to environment variables even if production credentials are somehow present in the environment.

3. **Demo-aware DuckDB SECRET creation** — In `_configure_s3()`, when both `key_id` and `secret_key` are `None` and demo mode is active, create a DuckDB SECRET with empty `KEY_ID`/`SECRET` to prevent DuckDB from falling back to environment variables. In production mode, skip SECRET creation to allow IAM role fallback.

## Consequences

- **Production secrets are never in a demo run's environment** — The GitHub Actions workflow only injects the secrets needed for the active mode.
- **SDK fallback is blocked at the application level** — Even if production credentials are somehow present in the environment (e.g., local development), deltalake and PyArrow receive empty strings for missing credentials, preventing silent fallback.
- **DuckDB fallback is blocked in demo mode** — A SECRET with empty credentials prevents DuckDB from reading production credentials from environment variables. In production mode without explicit credentials, IAM role fallback still works.
- **IAM role fallback is preserved** — In production mode on EC2, when both credentials are `None`, no DuckDB SECRET is created, and no empty strings are passed to deltalake/PyArrow. The `to_storage_options()` and `to_pyarrow_kwargs()` methods still include empty strings, which will cause authentication failures on EC2. This is an acceptable trade-off: EC2 deployments should set `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` explicitly or use a MinIO endpoint where empty credentials are valid.

## Validation

- All 157 tests pass, including updated tests for `AwsCredentials` helper methods.
- `to_storage_options()` now includes `"aws_access_key_id": ""` and `"aws_secret_access_key": ""` when credentials are `None`.
- `to_pyarrow_kwargs()` now includes `"access_key": ""` and `"secret_key": ""` when credentials are `None`.
- `to_duckdb_secret_parts()` now includes `KEY_ID ''` and `SECRET ''` when credentials are `None`.
- `_configure_s3()` creates a SECRET with empty credentials in demo mode when both credentials are `None`.
- GitHub Actions workflow uses step-level conditionals to inject only the relevant secrets.