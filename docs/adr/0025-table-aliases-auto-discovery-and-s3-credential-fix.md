# 0025 Table aliases, auto-discovery, and S3 credential fix

## Context

Three issues motivated this change:

1. **`list_tables()` returned empty on S3** — `S3Backend.storage_options` returned credential keys in UPPERCASE (`AWS_ACCESS_KEY_ID`) but `deltalake`'s Rust `object_store` crate only recognizes lowercase keys (`aws_access_key_id`). All `DeltaTable()` calls failed silently.

2. **No layer-qualified aliases** — Users had to know full S3 paths or bare canonical names (which defaulted to `normalized`). They wanted `decrypted_df("ibkr_snapshot_raw")` with explicit layer specification.

3. **Hardcoded `KNOWN_TABLES` was brittle** — Adding a new broker required updating `query.py`. The storage backend should be the source of truth.

## Decision

### Fix S3 credential key casing

Changed `S3Backend.storage_options` from UPPERCASE keys to lowercase:

```python
# Before (broken — uppercase keys ignored by deltalake):
"AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),

# After (fixed — lowercase keys recognized by deltalake):
"aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", ""),
```

Also: omitted empty credentials instead of passing `""`, so `object_store` can fall back to its credential chain (IAM roles, etc.) rather than being overridden with empty strings.

### Replace `KNOWN_TABLES` with auto-discovery

Removed the hardcoded `KNOWN_TABLES` dict. `list_tables()` now scans the storage backend for Delta tables:

- **Local storage**: `pathlib.Path` scans `data_dir/{layer}/` for directories containing `_delta_log/`
- **S3 storage**: `pyarrow.fs.S3FileSystem` lists directories under each layer prefix and checks for `_delta_log/` objects

Each discovered table gets a layer-qualified alias: `{name}_{layer}` (e.g. `ibkr_snapshot_raw`, `portfolio_allocation_analytics`).

### Convention-based alias resolution

Added `parse_alias()` to extract layer from the suffix. Updated `_resolve_path()` to check for layer suffixes (`_raw`, `_normalized`, `_analytics`) before falling back to bare names or full paths.

No separate `TABLE_ALIASES` dict — the naming convention `{name}_{layer}` is parsed at resolve time.

### Add `get_connection()` query helper

Added `get_connection()` that returns a cached DuckDB connection with S3 credentials configured and all discovered Delta tables registered as views. Users query using native DuckDB API (`db.sql("SELECT ...").pl()`). Added `decrypt_df()` utility for decrypting Fernet-encrypted columns. Added `refresh()` to re-discover tables and recreate the connection.

### Remove `dt.files()` / `dt.file_uris()` filter from `list_tables()`

Removed the empty-table filter that opened a `DeltaTable` for each discovered table and checked `file_uris()`. This was the primary performance bottleneck on S3 (7+ round-trips per call). Empty-table filtering is now the caller's responsibility.

## Consequences

- **S3 queries now work** — `deltalake.DeltaTable()` receives credentials in the correct format
- **Auto-discovery replaces hardcoded registry** — new brokers are automatically found
- **Layer-qualified aliases** — every table name includes its layer, eliminating ambiguity
- **`get_connection()` function** — pre-configured DuckDB connection with views registered; users query with native DuckDB API
- **Backward compatible** — bare names like `ibkr_snapshot` still resolve to `normalized/ibkr_snapshot` via the fallback path

## Validation

- All 280 tests pass (1 skipped — live FX test)
- `test_storage_config.py`: 2 new tests for lowercase `storage_options` keys
- `test_query_list_tables.py`: completely rewritten for auto-discovery, alias parsing, Result/DecryptedResult chaining, and table caching
- `ruff check` and `ruff format` pass cleanly