# 0021: Pass storage_options to deltalake for S3 support

## Context

The GitHub Actions pipeline failed with `Generic error: Unable to recognise URL "s3://..."` when writing to S3. The `deltalake` library requires `storage_options` (AWS credentials) to be passed explicitly when reading/writing Delta tables on S3.

Additionally, the `S3_BUCKET` environment variable was set to a full `s3://` URI (e.g., `s3://bucket-name`) instead of just the bucket name. The `S3Backend` constructor prepends `s3://` to the bucket, resulting in malformed URLs like `s3://s3://bucket-name/...`.

## Decision

1. Add a `storage_options` property to `StorageBackend` protocol, `LocalBackend`, and `S3Backend`:
   - `S3Backend.storage_options` returns `{"AWS_ACCESS_KEY_ID": ..., "AWS_SECRET_ACCESS_KEY": ..., "AWS_REGION": ...}` from environment variables
   - `LocalBackend.storage_options` returns `None`
2. Add `StorageConfig.storage_options` property that delegates to the backend.
3. Pass `storage_options` to all `write_deltalake()` and `DeltaTable()` calls throughout the pipeline.
4. Strip `s3://` and `s3a://` prefixes from the `S3_BUCKET` value in `S3Backend.__init__` so that `s3://` is not doubled.
5. Fix `resolve_storage()` to use `backend.bucket` (stripped) instead of the raw `s3_bucket` env var when constructing the base URI.

## Consequences

- S3 paths now work correctly with `deltalake` in CI.
- The `S3_BUCKET` secret can be set to either `bucket-name` or `s3://bucket-name` — both are handled.
- Local development is unaffected — `storage_options=None` means deltalake uses local filesystem defaults.
- AWS credentials are explicitly passed to deltalake rather than relying on implicit env var discovery.

## Validation

- All 233 existing tests pass.
- Diagnostic CI step confirmed `DeltaTable` and `write_deltalake` both work with `storage_options` on S3.
- The `S3_BUCKET` prefix-stripping fix resolves the `path_starts_s3://False` issue.