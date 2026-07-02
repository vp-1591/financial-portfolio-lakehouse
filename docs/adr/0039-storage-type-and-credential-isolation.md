# 0039: STORAGE_TYPE Env Var and resolve_secret Credential Isolation

## Context

PR #16 introduced DEMO mode with `_DEMO`-suffixed secrets and isolated storage. Code review identified two bugs:

1. **`resolve_secret()` crashed the pipeline in demo mode.** When a `_DEMO` secret was missing, `resolve_secret()` raised `EnvironmentError`, which crashed `run.py`'s connector-skipping logic (`if not resolve_secret(...): continue`). In production mode, missing secrets return `None` and the connector is gracefully skipped. In demo mode, the exception meant a user who hadn't set all demo secrets for all brokers would get a crash instead of the broker being skipped.

2. **AWS credentials silently fell back to production.** In `storage.py` and `query.py`, AWS credentials used raw `os.environ.get("AWS_ACCESS_KEY_ID_DEMO" if demo else "AWS_ACCESS_KEY_ID", "")`. When the `_DEMO` variant was missing, this returned `""`, which meant the `if key_id:` guard skipped setting explicit credentials and the AWS SDK walked its default credential chain — potentially resolving to production credentials. This contradicted the "no cross-mode fallback" design.

Additionally, the user requested a `STORAGE_TYPE` env var defaulting to `cloud` with `minio` and `local` options, where AWS credentials are only required when using cloud storage (not minio or local).

## Decision

### 1. `resolve_secret()` returns `None` with warning instead of raising `EnvironmentError`

Changed `resolve_secret()` to return `None` and log a warning when a `_DEMO` secret is missing in demo mode, instead of raising `EnvironmentError`. This restores the graceful degradation pattern where missing credentials cause the connector to be skipped rather than the pipeline to crash.

Added info-level logging when a secret IS resolved (logging which variant was used, but not the value) and debug-level logging when a secret is missing in non-demo mode.

### 2. Added `STORAGE_TYPE` env var (`cloud`/`minio`/`local`)

Added `get_storage_type()` to `pipeline/secrets.py` that reads `STORAGE_TYPE` from the environment. Valid values are `cloud`, `minio`, and `local`. Defaults to `cloud` when `S3_BUCKET` is set (backward compatibility) and `local` otherwise. Raises `ValueError` for invalid values.

Modified `resolve_storage()` in `pipeline/storage.py` to use `get_storage_type()` instead of checking `S3_BUCKET` presence. When `STORAGE_TYPE=cloud`, `S3_BUCKET` is required (raises `ValueError` if missing). When `STORAGE_TYPE=local`, `S3_BUCKET` is ignored even if set. When `STORAGE_TYPE=minio`, a warning is logged if `S3_ENDPOINT_URL` is not set.

### 3. AWS credentials routed through `resolve_secret()` with no silent fallback

Added `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to `DEMO_SECRET_MAP`, so `resolve_secret()` handles demo isolation for AWS credentials the same way as broker secrets. Replaced all direct `os.environ.get()` calls for AWS credentials in `storage.py` and `query.py` with `resolve_secret("AWS_ACCESS_KEY_ID")` and `resolve_secret("AWS_SECRET_ACCESS_KEY")`.

Added `REQUIRED_SECRETS_S3` and `REQUIRED_SECRETS_S3_DEMO` lists for AWS credentials, validated in `inject_secrets()` only when `STORAGE_TYPE=cloud`. AWS credentials are optional for `minio` and `local` storage.

## Consequences

- **Positive**: Demo mode no longer crashes when a `_DEMO` secret is missing — the connector is skipped with a warning log, matching production behavior.
- **Positive**: AWS credentials are fully isolated in demo mode with no silent fallback to production. Missing demo AWS credentials return `None` with a warning, not production credentials.
- **Positive**: `STORAGE_TYPE=local` explicitly uses local storage even if `S3_BUCKET` is set, enabling easier local development.
- **Positive**: `STORAGE_TYPE=minio` supports MinIO with a warning for missing endpoint URL.
- **Positive**: AWS credentials are only required for `STORAGE_TYPE=cloud`, not for `minio` or `local`.
- **Positive**: `resolve_secret()` now logs which variant was used, giving visibility into which secrets are active.
- **Breaking**: The `EnvironmentError` from `resolve_secret()` in demo mode is removed. Code that relied on catching this exception will need to check for `None` instead.
- **Breaking**: `STORAGE_TYPE` defaults to `cloud` when `S3_BUCKET` is set (backward compatible), but existing deployments that set `S3_BUCKET` and don't set `STORAGE_TYPE` will now get `cloud` behavior explicitly. No functional change.
- **Change**: The `resolve_storage()` function now raises `ValueError` when `STORAGE_TYPE=cloud` but `S3_BUCKET` is not set, whereas before it silently defaulted to local storage.

## Validation

- `tests/test_secrets.py`: `TestResolveSecret.test_returns_none_when_demo_secret_missing` (was `test_raises_error_when_demo_secret_missing`), `TestResolveSecretLogging`, `TestGetStorageType`, `TestAWSSecretsInDemoMap`, `TestInjectSecretsS3Validation`
- `tests/test_storage_config.py`: `TestStorageType` (cloud/minio/local/invalid/backward compat), `TestS3Backend.test_storage_options_demo_mode_uses_demo_creds`, `TestS3Backend.test_storage_options_demo_mode_no_fallback`
- All 292 tests pass