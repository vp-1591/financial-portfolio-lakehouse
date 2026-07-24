# ADR 0044: S3_BUCKET_DEMO Standalone — Demo Cloud Storage Without S3_BUCKET

> **Superseded by [ADR 0092](./0092-remove-demo-env-var-suffix.md)** — `S3_BUCKET_DEMO` is removed. Staging mode reads `S3_BUCKET` directly; the demo ECS task sets it to the demo bucket name.

## Context

When running the demo notebook (`exploration/query-s3-demo.ipynb`) with only `DEMO=true` and `S3_BUCKET_DEMO=investment-portfolio-pipeline-demo` set (no `S3_BUCKET`), `list_tables()` returns an empty list. The user can see tables in S3 from the demo pipeline, but `list_tables()` discovers no Delta tables.

**Root cause:** `get_storage_type()` only checks `S3_BUCKET` to decide between cloud and local storage. In the notebook, only `DEMO=true` and `S3_BUCKET_DEMO` are set — `S3_BUCKET` is not set — so `get_storage_type()` returns `"local"`. Then `resolve_storage()` creates a `LocalBackend` pointing at a local `data_demo/` directory, and `list_tables()` discovers no Delta tables there.

Additionally, `resolve_storage()` required `S3_BUCKET` to be set whenever `STORAGE_TYPE` is `"cloud"` or `"minio"`, even in demo mode where `S3_BUCKET_DEMO` is the intended bucket name.

## Decision

1. **`get_storage_type()`** now checks `S3_BUCKET_DEMO` as a fallback in demo mode: when `S3_BUCKET` is not set but `S3_BUCKET_DEMO` is set and `is_demo()` is true, return `"cloud"`. This means demo users no longer need to set `STORAGE_TYPE=cloud` explicitly — just `DEMO=true` and `S3_BUCKET_DEMO` suffice.

2. **`resolve_storage()`** no longer requires `S3_BUCKET` in demo mode. When `S3_BUCKET` is not set but `S3_BUCKET_DEMO` is available, the demo bucket name is used directly. When both `S3_BUCKET` and `S3_BUCKET_DEMO` are missing in demo+cloud mode, a clear `ValueError` is raised mentioning `S3_BUCKET_DEMO`.

## Consequences

- Demo notebooks and scripts only need `DEMO=true` and `S3_BUCKET_DEMO` — no need for `STORAGE_TYPE=cloud` or `S3_BUCKET`.
- `get_env()` (ADR 0043) correctly treats empty-string env vars as unset, so `S3_BUCKET_DEMO=""` does not trigger cloud storage.
- Production (non-demo) behavior is unchanged: `S3_BUCKET_DEMO` is ignored without `DEMO=true`.
- The `resolve_storage()` restructure moves the `ValueError` for missing bucket into the non-demo branch, so demo mode has its own error message.

## Validation

- Added 6 new tests covering `get_storage_type()` fallback, empty-string handling, `resolve_storage()` with `S3_BUCKET_DEMO` standalone, and the `ValueError` when both `S3_BUCKET` and `S3_BUCKET_DEMO` are missing.
- All 144 tests pass including the new ones.