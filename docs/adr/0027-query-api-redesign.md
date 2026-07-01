# 0027 Query API redesign — native DuckDB connection, decrypt utility, drop wrappers

## Context

S3 queries took 10–12 seconds. The primary bottleneck was `list_tables()`, which opened a `DeltaTable` for every discovered table (7+ S3 round-trips per call) to check `file_uris()` for empty-table filtering. There was no caching — every call re-discovered everything.

The query API also had overengineered wrapper classes (`Result`, `DecryptedResult`) that reimplemented DuckDB's own chaining API (`.pl()`, `.df()`, `.filter()`, etc.). For a portfolio project, this custom wrapper surface is a red flag — it creates maintenance burden and obscures the fact that we're just using DuckDB.

## Decision

### 1. Cache `list_tables()` results

Added a module-level `_TABLE_CACHE` variable. `list_tables()` returns the cached result on subsequent calls. Added `refresh=True` parameter to force re-discovery and `clear_table_cache()` to reset the cache. This eliminates repeated S3 discovery overhead.

### 2. Remove empty-table filter from `list_tables()`

Removed the `DeltaTable` / `file_uris()` loop that filtered out empty tables. Empty-table filtering is not `list_tables()`'s concern — the `_discover_tables_*()` functions already verify `_delta_log/` existence, which is sufficient to identify Delta tables. This was the single biggest performance improvement for S3 queries.

### 3. Replace `Result`/`DecryptedResult`/`sql()`/`table()` with `get_connection()` and `decrypt_df()`

Removed the custom `Result` and `DecryptedResult` classes, the `sql()` and `table()` functions, and all associated wrapper code (`_extract_table_names`, `_resolve_path`, `_create_connection`, `_IDENTIFIER_RE`). Replaced with:

- **`get_connection()`** — returns a cached DuckDB connection with S3 credentials configured and all discovered Delta tables registered as views. Users query using native DuckDB API:
  ```python
  db = get_connection()
  db.sql("SELECT * FROM ibkr_snapshot_raw LIMIT 5").pl()   # Polars DataFrame
  db.sql("SELECT * FROM ibkr_snapshot_raw").filter("value > 100")  # native chaining
  ```

- **`decrypt_df(df, columns=None, key=None)`** — simple function that decrypts Fernet-encrypted columns in a Polars DataFrame:
  ```python
  df = db.sql("SELECT * FROM ibkr_snapshot_raw").pl()
  decrypt_df(df)
  ```

- **`refresh()`** — closes the connection, clears the table cache, so the next `get_connection()` call re-discovers and re-registers everything.

This uses DuckDB's native API instead of wrapping it. No custom classes, no method-chaining protocol to maintain.

### 4. Register all views upfront (no lazy extraction)

The old `sql()` extracted table names from the SQL string with a regex and only registered referenced tables. With a persistent connection from `get_connection()`, all tables are registered as views upfront. `CREATE OR REPLACE VIEW ... AS SELECT * FROM delta_scan(...)` is just DDL — no data is read until the view is queried, so registering all tables is cheap.

### 5. Drop dead code

Removed entirely (no deprecation wrappers):

- `Result` class — replaced by native DuckDB API
- `DecryptedResult` class — replaced by `decrypt_df()`
- `sql()` function — replaced by `db.sql()` (native DuckDB)
- `table()` function — all tables pre-registered as views
- `_extract_table_names()` — no longer needed (no lazy registration)
- `_resolve_path()` — no longer needed (no `table()` function)
- `_create_connection()` — inlined into `get_connection()`
- `_IDENTIFIER_RE` — no longer needed

## Consequences

- **Faster S3 queries** — `list_tables()` is cached after first call; empty-table filter removed (7+ fewer S3 round-trips)
- **Reduces maintenance surface** — no custom wrapper classes to keep in sync with DuckDB/Polars API changes. Native DuckDB API is stable and well-documented; our thin layer (connection setup, view registration, decryption utility) is small enough to audit at a glance.
- **Familiar API** — users get a standard `duckdb.DuckDBPyConnection` with all native methods (`.sql()`, `.execute()`, `.pl()`, `.df()`, `.filter()`, etc.)
- **Breaking change** — `Result`, `DecryptedResult`, `sql()`, `table()` are removed. Users must migrate to `get_connection()` + native DuckDB API, and `decrypt_df()` for decryption.

## Validation

- All tests pass (1 skipped — live FX test)
- New tests: `TestGetConnection` (5 tests), `TestDecryptDf` (6 tests)
- Removed tests: `TestExtractTableNames` (5 tests), `TestSqlAndTable` (13 tests)
- `ruff check` and `ruff format` pass cleanly