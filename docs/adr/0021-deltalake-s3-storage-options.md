# 0021: Pass storage_options to deltalake for S3 support

## Context

The GitHub Actions pipeline failed with `Generic error: Unable to recognise URL "s3://bucket/pipeline/raw/ibkr_snapshot"`. The `deltalake` library could not handle S3 URLs because no `storage_options` were passed to `write_deltalake` and `DeltaTable` calls.

## Decision

- Add a `storage_options` property to `StorageBackend` protocol, `LocalBackend`, and `S3Backend`.
- `S3Backend.storage_options` returns a dict with `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION` from environment variables.
- `LocalBackend.storage_options` returns `None` (no cloud config needed).
- `StorageConfig.storage_options` delegates to the backend.
- All `write_deltalake()` and `DeltaTable()` calls throughout the pipeline now receive `storage_options` from `get_storage().storage_options`.
- `extract_holdings()` in `normalized/extract.py` skips the local `Path.exists()` check when `storage_options` is set (S3 paths are not local files).

## Consequences

- S3 paths now work correctly with `deltalake` in CI.
- Local development is unaffected — `storage_options=None` means deltalake uses local filesystem defaults.
- AWS credentials are explicitly passed to deltalake rather than relying on implicit env var discovery, which is more reliable.

## Validation

- All 233 existing tests pass.
- S3 pipeline runs in GitHub Actions will now correctly write to and read from S3 Delta tables.