# ADR 0040: Consolidate AWS Credentials and Fix Demo Isolation Bugs

> **Superseded by [ADR 0092](./0092-remove-demo-env-var-suffix.md)** — `REQUIRED_SECRETS_DEMO_NON_AWS` and `_DEMO`-specific credential handling are removed. All secrets use base names; environment isolation is via SSM path prefix.

## Context

PR #16 introduced DEMO mode with isolated secrets and Terraform infrastructure. Code review found three bugs:

1. **Spurious/duplicate AWS warnings in demo mode**: `inject_secrets()` validated all `REQUIRED_SECRETS_DEMO` (including AWS credentials) regardless of storage type. In demo+local mode, this produced spurious warnings about missing `AWS_ACCESS_KEY_ID_DEMO`. In demo+cloud mode, AWS credentials were warned about twice — once in the general loop and once in the S3-specific section. The production path correctly excluded AWS credentials from `REQUIRED_SECRETS`, keeping them only in `REQUIRED_SECRETS_S3`.

2. **`load_key()` falls through to production key in demo mode**: The docstring stated "no cross-mode fallback" but the implementation fell through from `resolve_secret("ENCRYPTION_KEY")` returning `None` to a file-based lookup at `.secrets/encryption.key`, which always contains the production key. This violated the isolation guarantee from ADR 0037 and ADR 0039.

3. **`or ""` fallback breaks demo/production isolation**: At three call sites (`storage.py`, `query.py` lines 107 and 347), `resolve_secret("AWS_ACCESS_KEY_ID") or ""` converted `resolve_secret()`'s `None` return into an empty string. When both `key_id` and `secret` were empty strings, the `if key_id:` / `if secret:` guards evaluated to `False`, so no explicit credentials were passed to deltalake/object_store or DuckDB. Both libraries then fell back to their own credential chains, which check environment variables. In GitHub Actions, production AWS credentials are always present, so the pipeline silently used production credentials for demo storage operations.

Additionally, AWS credential resolution was duplicated across three call sites (`S3Backend.storage_options`, `_discover_tables_s3`, `_configure_s3`), each with the same `or ""` pattern. A roadmap existed at `docs/roadmap-consolidate-aws-credentials.md` proposing consolidation, but it perpetuated bug #3 by using `os.environ.get()` instead of `resolve_secret()` and using `or ""` instead of `None`.

## Decision

1. **Fix #1**: Add `REQUIRED_SECRETS_DEMO_NON_AWS` constant that excludes AWS credential names. Use it in `inject_secrets()` general demo loop instead of `REQUIRED_SECRETS_DEMO`. AWS demo credentials are now only validated in the S3-specific section (when `STORAGE_TYPE=cloud`), matching the production path.

2. **Fix #2**: In `load_key()`, when `resolve_secret("ENCRYPTION_KEY")` returns `None` and `is_demo()` is `True`, raise `EnvironmentError` immediately. The file-based fallback only works in production mode, since the file is shared between modes.

3. **Fix #3 + Consolidation**: Implement the AWS credential consolidation from the roadmap, but with the `or ""` bug fixed:
   - Add `AwsCredentials` dataclass to `pipeline/secrets.py` with `key_id: str | None` and `secret_key: str | None`. When `resolve_secret()` returns `None` or `""`, these are stored as `None` — never as empty strings.
   - Add `resolve_aws_credentials()` that uses `resolve_secret()` (not `os.environ.get()`) and normalizes empty strings to `None`.
   - Add three helper methods: `to_storage_options()`, `to_pyarrow_kwargs()`, `to_duckdb_secret_parts()`. When credentials are `None`, the helpers omit those keys entirely, preventing SDK fallback to environment variables.
   - Replace the three duplication sites with calls to `resolve_aws_credentials()`.

4. **Delete roadmap**: The consolidation is now implemented, so `docs/roadmap-consolidate-aws-credentials.md` is deleted.

## Consequences

- **Demo isolation is now complete**: Missing `_DEMO` credentials produce hard errors (encryption key) or `None` values (AWS credentials) that prevent any fallback to production data or credentials.
- **No more spurious/duplicate warnings**: Demo+local mode no longer warns about missing AWS credentials. Demo+cloud mode warns about each AWS credential exactly once.
- **Single source of truth for AWS credentials**: All three consumers (deltalake, PyArrow, DuckDB) use `resolve_aws_credentials()` instead of duplicating the resolution logic. Future changes to credential handling need only modify one place.
- **Empty-string credentials are treated as absent**: If `AWS_ACCESS_KEY_ID=""` is set, `resolve_aws_credentials()` normalizes it to `None`, preventing the SDK from receiving an empty string that would override IAM role fallback.

## Validation

- All 321 tests pass, including new tests for:
  - `TestInjectSecretsS3Validation.test_demo_local_no_aws_warnings`
  - `TestInjectSecretsS3Validation.test_demo_cloud_single_aws_warning`
  - `TestResolveAwsCredentials` (8 tests)
  - `TestAwsCredentialsDataclass` (10 tests)
  - `TestLoadKey.test_demo_mode_raises_when_demo_key_missing`
  - `TestLoadKey.test_demo_mode_uses_demo_key_when_set`
  - `TestLoadKey.test_production_mode_falls_back_to_file`
  - `TestConfigureS3.test_demo_mode_uses_demo_credentials`
  - `TestConfigureS3.test_demo_mode_no_fallback_to_prod`
- `grep -r "or \"\"" pipeline/` returns no AWS-credential-related hits.
- `grep -r "AWS_ACCESS_KEY_ID" pipeline/` only hits `secrets.py`.