# 0044: Replace List-Append Pattern with Polars build_normalized_table

## Context

All three broker connectors (IBKR, Trading 212, XTB) used a manual list-append pattern for building normalized PyArrow tables:

1. Initialize N empty lists (one per output column)
2. Loop over rows, calling `.append()` on each list
3. Encrypt values inline with `encrypt_float()` per row
4. Assemble `pa.table({...}, schema=...)` at the end

This pattern was repeated across 5 transform functions (~170 lines of boilerplate total). ADR 0013b replaced pandas with Polars for downstream operations (allocation, extract), but the transform functions themselves still used the imperative list-append approach. The pattern has three problems:

- **No type safety**: Column names are repeated in 3 places (list init, append, table assembly), and typos fail at runtime only.
- **Scattered encryption**: `encrypt_float()` is called per-row inside loops, making it hard to test encryption separately from data extraction.
- **Boilerplate**: Each transform follows the same pattern, with column-specific append calls that obscure the actual business logic.

## Decision

Introduce a shared `build_normalized_table()` helper in `pipeline/connectors/transform_utils.py` that:

1. Accepts a list of row-dicts (plain Python values, no encryption)
2. Builds a Polars DataFrame from the dicts
3. Encrypts specified float columns via `pl.col(name).map_elements(encrypt_float, return_dtype=pl.Binary)`
4. Reorders columns to match the target schema
5. Converts to PyArrow and casts to the exact target schema

Each transform function now:
- Collects output rows as `dict[str, Any]` instead of N separate lists
- Passes plain float values (not encrypted) in the dicts
- Delegates encryption and table assembly to `build_normalized_table()`

Additionally, `pipeline/normalized/extract.py` was updated to use `pl.col("value").map_elements(decrypt_float, ...)` for batch decryption instead of per-row `decrypt_float()` calls inside the `iter_rows` loop.

### Files changed

- **pipeline/connectors/transform_utils.py**: Added `build_normalized_table()` helper with `encrypt_columns` parameter; added `import polars as pl` and `from pipeline.crypto import encrypt_float`
- **pipeline/connectors/xtb/transform.py**: Replaced list-append in `transform_snapshot` and `transform_cdc` with dict collection → `build_normalized_table()`
- **pipeline/connectors/trading212/transform.py**: Same pattern for `transform_snapshot` and `transform_cdc`; CDC uses `encrypt_columns=["value", "quantity"]`
- **pipeline/connectors/ibkr/transform.py**: Same pattern for `transform_snapshot`; XML parsing and FX rate logic unchanged
- **pipeline/normalized/extract.py**: Replaced per-row `decrypt_float()` with `pl.col("value").map_elements(...)` batch decryption
- **tests/test_transform_utils.py**: Added `TestBuildNormalizedTable` with 5 tests: empty records, single encrypted column, multiple encrypted columns, no encryption, schema column ordering

## Consequences

- **Reduced boilerplate**: ~170 lines of repetitive list-append code replaced with dict-collection + single `build_normalized_table()` call
- **Centralized encryption**: All Fernet encryption in transforms goes through one `map_elements` call per column, instead of per-row `encrypt_float()` scattered through loops
- **Same external interface**: `BrokerConnector.transform_snapshot(raw, fernet_key) -> pa.Table` signature unchanged; all 244 tests pass without modification
- **Column name safety**: Dict keys are written once, and `build_normalized_table()` reorders/casts to the target schema, catching mismatches at the cast step
- **`polars` dependency extended**: Transform modules now import `polars` (already a project dependency from ADR 0013b)

## Validation

- All 244 existing tests pass (`pytest tests/ -v`)
- 5 new tests for `build_normalized_table` pass
- `ruff check --fix .` and `ruff format .` clean