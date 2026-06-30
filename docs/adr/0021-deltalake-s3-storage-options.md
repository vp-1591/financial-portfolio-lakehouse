# 0021: Fix deltalake S3 URI handling

## Context

The GitHub Actions pipeline failed with `Generic error: Unable to recognise URL "s3:/bucket/pipeline/raw/ibkr_snapshot"` when writing to S3. The error showed a single slash (`s3:/`) instead of the required double slash (`s3://`).

Two root causes were identified:

1. **`pathlib.Path` collapses `s3://` to `s3:/`** — `Path()` is for local filesystem paths and normalizes repeated slashes. Code that wrapped S3 URIs in `Path()` (or `str(Path(...))`) produced corrupted URIs. On Windows, `Path()` also flips `/` to `\`.

2. **Missing `storage_options`** — `deltalake` requires explicit AWS credentials in `storage_options` for S3 operations. Without them, S3 URLs are not recognized.

3. **`S3_BUCKET` could contain a `s3://` prefix or leading `/`** — GitHub Secret values are not validated. If `S3_BUCKET` is set to `s3://bucket-name` or `/bucket-name`, the resulting path would be malformed.

## Decision

1. **Remove all `Path()` wrapping of S3 URIs.** `get_raw_path()` now returns `str` instead of `Path`. `extract.py` passes `table_path` directly to `DeltaTable()` without wrapping in `Path()`. All `str(get_raw_path(...))` calls simplified since the function already returns `str`.

2. **Pass `storage_options` to all `write_deltalake()` and `DeltaTable()` calls** throughout the pipeline (`run.py`, `ingest.py`, `consolidate.py`, `allocation.py`, `extract.py`).

3. **Strip `s3://`, `s3a://`, and leading `/` from `S3_BUCKET`** in `S3Backend.__init__` so the bucket name is always bare.

4. **Move `S3_BUCKET` from GitHub Secrets to GitHub Variables** — bucket names are not sensitive, and Secrets mask values in logs making debugging impossible.

## Consequences

- S3 paths are now plain strings end-to-end, never passed through `pathlib.Path`.
- `get_raw_path()` returns `str` — callers no longer need `str()` wrappers.
- The `S3_BUCKET` variable can be set to `bucket-name`, `s3://bucket-name`, or `/bucket-name` — all are normalized to `bucket-name`.
- Local development is unaffected — `storage_options=None` means deltalake uses local filesystem defaults.
- AWS credentials are explicitly passed to deltalake rather than relying on implicit env var discovery.

## Validation

- All 233 existing tests pass.
- Local test with `DeltaTable("s3://bucket/...", storage_options=...)` correctly connects to S3 (403 with fake creds = URL recognized).
- Local test with `DeltaTable(str(Path("s3://bucket/...")), ...)` reproduces the `s3:/` single-slash bug.