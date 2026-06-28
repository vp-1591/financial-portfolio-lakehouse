# 0017: Fixture Data for Transformation Tests

## Context

Existing tests used inline mock data built in `_build_raw_table()` helper methods within each test file. There was no shared fixture data across tests, and no end-to-end tests for the consolidate and allocate pipeline stages. This made it difficult to verify that the full transformation pipeline works correctly from raw data through to portfolio allocation.

## Decision

1. Create `tests/fixtures/` package with per-broker factory functions:
   - `ibkr.py`: `ibkr_raw_positions()` and `ibkr_normalized_snapshot()`
   - `trading212.py`: `t212_raw_snapshot()` and `t212_normalized_snapshot()`
   - `xtb.py`: `xtb_raw_snapshot()` and `xtb_normalized_snapshot()`
   - `pipeline.py`: `setup_pipeline_env()` for full end-to-end setup

2. Each factory function returns realistic `pa.Table` objects matching the actual schemas, with Fernet-encrypted values using a test key.

3. Add three new test files:
   - `test_transform_pipeline.py`: Tests each broker's transform with fixture data (schema validation, row count, position types)
   - `test_consolidate_pipeline.py`: Tests extract_holdings() and consolidate_holdings() across multiple brokers
   - `test_allocate_pipeline.py`: Tests the full allocate pipeline from consolidated holdings

4. All new tests use the `use_storage()` pattern from `conftest.py` to inject `tmp_path`-based `StorageConfig`, ensuring no writes to `data/`.

5. The `test_storage_config.py` file tests the storage configuration system (env resolution, `use_storage()` injection, `paths` module delegation, `LocalBackend`).

## Consequences

- Each pipeline stage (transform, consolidate, allocate) has dedicated end-to-end tests with realistic fixture data
- Fixture factory functions are reusable across test files
- Tests are isolated from production data via `use_storage()` + `tmp_path`
- The XTB fixture now matches the connector's expected `"OPEN POSITION"` source string and includes `label`, `name`, `asset_class` fields
- Parametrized tests verify consistent behavior across brokers where schemas overlap

## Validation

- 214 tests pass (176 existing + 38 new)
- New tests cover: storage config resolution, paths delegation, transform per broker, consolidate multi-broker, allocate percentages sum to ~100%