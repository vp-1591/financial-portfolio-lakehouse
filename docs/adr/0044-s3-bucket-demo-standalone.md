# 0044: S3_BUCKET_DEMO Standalone — Demo Cloud Storage Without S3_BUCKET

## Context

When running in demo mode with only `DEMO=true` and `S3_BUCKET_DEMO` set (no `S3_BUCKET`), `list_tables()` returned an empty list even though Delta tables existed in the S3 demo bucket. The root cause was that `get_storage_type()` only checked `S3_BUCKET` to decide between cloud and local storage. Without `S3_BUCKET`, it defaulted to `"local"`, causing `resolve_storage()` to create a `LocalBackend` that looked at a non-existent local `data_demo/` directory instead of the S3 demo bucket.

This made the demo exploration notebook (`exploration/query-s3-demo.ipynb`) unusable without manually setting `STORAGE_TYPE=cloud` — a non-obvious workaround that contradicts the demo isolation principle (all configuration should be possible via `_DEMO` env vars alone).

## Decision

1. **`get_storage_type()`** now checks `S3_BUCKET_DEMO` when `DEMO=true` and `S3_BUCKET` is not set. If `S3_BUCKET_DEMO` has a non-empty value, it returns `"cloud"`. This mirrors the existing pattern where `S3_BUCKET` triggers cloud storage in production mode.

2. **`resolve_storage()`** now allows `S3_BUCKET_DEMO` to standalone without `S3_BUCKET` in demo mode. The bucket name is taken directly from `S3_BUCKET_DEMO`. When both are set, `S3_BUCKET_DEMO` takes precedence (existing behavior). When neither is set in demo mode with `STORAGE_TYPE=cloud`, a clear `ValueError` is raised indicating `S3_BUCKET_DEMO` is required.

## Consequences

- Demo notebooks and scripts can configure S3 storage using only `DEMO=true` and `S3_BUCKET_DEMO` — no need to set `S3_BUCKET` or `STORAGE_TYPE=cloud`.
- Production behavior is unchanged: `S3_BUCKET_DEMO` alone (without `DEMO=true`) does **not** trigger cloud storage.
- The existing `S3_BUCKET` → `{bucket}-demo` derivation still works when `S3_BUCKET_DEMO` is not set.
- Empty string `S3_BUCKET_DEMO=""` is treated as unset (via `get_env()`), consistent with the empty-string fallback pattern established in ADR 0043.

## Validation

- Added `TestGetStorageType` tests: demo mode with `S3_BUCKET_DEMO` triggers cloud, without demo it does not, empty string does not trigger cloud.
- Added `TestDemoStorage` tests: `S3_BUCKET_DEMO` standalone creates `S3Backend`, missing both raises `ValueError`.
- Added `TestStorageType` test: `STORAGE_TYPE=cloud` with `S3_BUCKET_DEMO` in demo mode works without `S3_BUCKET`.