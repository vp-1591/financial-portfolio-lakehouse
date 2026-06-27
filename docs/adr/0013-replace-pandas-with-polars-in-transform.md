# 0013: Replace Pandas with Polars in Transform Pipeline

## Context

The bronze (raw) → silver (normalized) transform pipeline used pandas throughout, causing two problems:

1. **Schema bug**: `dt.to_pandas()` → `pa.Table.from_pandas()` loses type information. Binary `payload` columns could become Python objects, timestamps could lose timezone/precision, and strings could become object dtype instead of `pa.string()`. This was the root cause of corrupted silver tables.

2. **Boilerplate & inefficiency**: Every transform function did `raw.to_pandas()` → `for _, row in df.iterrows()` → manual column list appends. This pattern was repeated ~7 times across 3 connectors (~170 lines of boilerplate, 28% of transform code).

## Decision

Replace pandas with a two-layer approach:

1. **Delta → Arrow** (schema-safe read): Use `DeltaTable.to_pyarrow_table()` instead of `dt.to_pandas()` → `pa.Table.from_pandas()`. This preserves exact column types from the Delta table.

2. **Arrow → Polars → Arrow** (transform processing): For downstream operations (allocation, extraction), convert to Polars via `pl.from_arrow()` for efficient column operations.

3. **Shared utilities** (`pipeline/connectors/transform_utils.py`): Extract the repeated decrypt/parse/iterate pattern into `iter_raw_payloads()`, `decode_payload()`, `parse_json()`, and `coerce_fetched_at()` — replacing the manual `for _, row in df.iterrows()` + decrypt + parse boilerplate.

### Data flow (new)

```
Delta → Arrow (to_pyarrow_table) → transform functions → Arrow → Delta
```

Transform functions use `iter_raw_payloads()` which reads columns via `pa.Table.column().to_pylist()` (no pandas dependency). Downstream consumers (allocation, extract) use Polars for grouped/aggregated operations.

### Files changed

- **pyproject.toml**: Added `polars>=1.0`, removed `pandas>=2.0` from pipeline deps
- **pipeline/run.py**: Replaced `dt.to_pandas()` + `pa.Table.from_pandas()` with `dt.to_pyarrow_table()`
- **pipeline/raw/ingest.py**: Replaced pandas dedup with PyArrow-native dedup (set-based key matching + `pc.array(mask)` filter)
- **pipeline/normalized/extract.py**: Replaced `dt.to_pandas()` with `dt.to_pyarrow_table()` → `pl.from_arrow()`
- **pipeline/analytics/allocation.py**: Replaced pandas with Polars for `group_by().agg()` and `map_elements()`
- **pipeline/connectors/ibkr/connector.py**: Replaced pandas flex-source check with `raw.column("source").to_pylist()`
- **pipeline/connectors/xtb/transform.py**: Replaced pandas with `iter_raw_payloads()` from transform_utils
- **pipeline/connectors/trading212/transform.py**: Replaced pandas groupby with `defaultdict(list)` grouping over `iter_raw_payloads()`
- **pipeline/connectors/ibkr/transform.py**: Replaced pandas groupby with `defaultdict(list)` grouping, used `decode_payload()` for flex snapshot
- **pipeline/connectors/transform_utils.py**: New — shared utilities (`DecodedRow`, `decode_payload`, `parse_json`, `coerce_fetched_at`, `iter_raw_payloads`)
- **tests/test_transform_utils.py**: New — 17 unit tests for shared utilities

## Consequences

- **Schema correctness**: All Delta reads now preserve exact types via `to_pyarrow_table()`. The `payload` column stays `pa.binary()`, timestamps stay `pa.timestamp("us", tz="UTC")`.
- **No pandas in pipeline**: The transform layer no longer imports pandas. Polars is used for grouped/aggregated operations in allocation and extract.
- **Reduced boilerplate**: ~170 lines of repeated decrypt/parse/iterate code replaced with `iter_raw_payloads()` calls.
- **`pandas` removed from pipeline dependencies**: Only `polars>=1.0` is needed. Pandas may still be a transitive dependency of `deltalake`.
- **`BrokerConnector` protocol unchanged**: Transform functions still accept `pa.Table` and return `pa.Table`.
- **All 176 tests pass**: 159 original + 17 new for transform_utils.

## Validation

- All 176 tests pass (`pytest tests/ -v`)
- `python -m pipeline.run transform` produces correctly-typed silver tables (binary `value` columns, UTC timestamps)
- DuckDB queries on silver tables confirm `value` column type is `binary`, not `list<binary>`