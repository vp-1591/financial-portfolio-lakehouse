# 0024 Fix DuckDB S3 credential propagation for delta_scan

## Context

When querying S3-hosted Delta Lake tables via `pipeline.query.decrypted_df()` or `pipeline.query.query()` in a notebook, DuckDB threw an `IOException`:

```
DeltaKernel ObjectStoreError (8): Error interacting with object store:
Generic S3 error: Error performing PUT http://169.254.169.254/latest/api/token
in 2.365856s, after 10 retries, max_retries: 10, retry_timeout: 180s
```

The `_configure_s3()` function used DuckDB's legacy `SET s3_access_key_id` / `SET s3_secret_access_key` / `SET s3_region` variables. These `SET` variables only configure DuckDB's built-in httpfs extension and are **not** propagated to the Delta Kernel extension used by `delta_scan()`. Without credentials, the Delta Kernel's `object_store` crate falls back to the EC2 instance metadata service (`169.254.169.254`), which times out on non-EC2 machines.

## Decision

Replace the legacy `SET s3_*` variables with DuckDB's `CREATE SECRET (TYPE S3, ...)` mechanism. DuckDB secrets are stored in the secret manager and are accessible to all extensions, including `delta_scan()`.

Changed `pipeline/query.py` `_configure_s3()` from:

```python
conn.execute(f"SET s3_access_key_id='{...}'")
conn.execute(f"SET s3_secret_access_key='{...}'")
conn.execute(f"SET s3_region='{...}'")
```

to:

```python
conn.execute(f"CREATE SECRET (TYPE S3, KEY_ID '{...}', SECRET '{...}', REGION '{...}')")
```

## Consequences

- **Fixed**: S3 queries via `delta_scan()` now receive proper credentials and work on non-EC2 machines.
- **Compatible**: DuckDB v0.10+ supports `CREATE SECRET`. Our pinned version (1.5.4) is well past this threshold.
- **Backward compatible**: The `CREATE SECRET` approach also propagates to the `s3_*` settings, so httpfs-based queries continue to work.
- **Added tests**: `tests/test_query_s3.py` verifies secret creation, region handling, default region, and credential propagation.

## Validation

- All 241 existing tests pass.
- 4 new tests in `test_query_s3.py` verify `CREATE SECRET` behavior.
- `ruff check` and `ruff format` pass cleanly.