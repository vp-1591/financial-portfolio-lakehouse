# 0015: Configure Data Paths with Environment-Aware Storage

## Context

All data paths in the pipeline were hardcoded as module-level constants in `pipeline/paths.py`, pointing to `data/` relative to the project root. There was no way to:
- Switch between environments (e.g., `prod` vs `dev`) without modifying code
- Run tests in isolation without risking writes to real data
- Prepare for cloud storage backends (S3, GCS) which use URI-based paths

Additionally, coding agents working on the project could accidentally modify production data tables because everything pointed at the same `data/` directory.

## Decision

1. Introduce `pipeline/storage.py` with:
   - `StorageConfig` dataclass holding resolved path objects and a `StorageBackend`
   - `StorageBackend` protocol with `table_path()` and `ensure_parent()` methods
   - `LocalBackend` as the initial implementation (filesystem paths)
   - `resolve_storage(env)` function that maps `"prod"` → `data/`, `"dev"` → `data-dev/`
   - `use_storage(config)` for explicit injection (used by tests)
   - `get_storage()` returning the active singleton

2. Rewrite `pipeline/paths.py` as a `__getattr__`-based shim that delegates to `get_storage()`. All existing `from pipeline.paths import RAW_DIR` calls continue to work but now resolve dynamically.

3. Add `--env` CLI flag (choices: `prod`, `dev`) and `PIPELINE_ENV` env var support. Unknown environments raise `ValueError`. The `"test"` env is intentionally blocked — tests must call `use_storage()` explicitly with a `tmp_path`-based config.

4. Update all modules that imported from `pipeline.paths` to use `pipeline.storage.get_storage()` directly.

5. Update `tests/conftest.py` to call `use_storage()` in `tmp_data_dir` fixture, ensuring tests never write to `data/`.

## Consequences

- `--env dev` writes to `data-dev/`, keeping production data safe
- Tests use `tmp_path` directories via `use_storage()`, never touching `data/`
- `StorageBackend` protocol provides a clean extension point for S3/GCS backends
- `deltalake` already supports `s3://` URIs natively, so `S3Backend.table_path()` can return `s3://bucket/prefix/layer/table` without additional changes in the Delta write path
- The `pipeline.paths` module is a compatibility shim — new code should use `pipeline.storage` directly

## Validation

- All 214 existing and new tests pass
- `test_storage_config.py` covers env resolution, `use_storage()` injection, `paths` delegation, and `LocalBackend`