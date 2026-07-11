# 0061: Handle Missing Struct Fields in T212 CDC Transform

## Context

PR #53 rewrote the T212 CDC transform to use Polars-native `struct.field()` for nested JSON extraction instead of `dict.get()` patterns. The change was correct for the API spec, but caused a production crash:

```
polars.exceptions.StructFieldNotFoundError: filledQuantity
```

When Polars creates a DataFrame from event dicts, it infers struct schemas from the data. The real T212 API can omit optional fields (like `filledQuantity`, `filledValue`) from order objects when they're not applicable. If a field is absent from all events in a batch, it won't appear in the inferred schema, and `struct.field()` raises `StructFieldNotFoundError` — unlike `dict.get()` which silently returns `None`.

## Decision

Add a `_ensure_struct_fields()` helper that backfills optional API fields with `None` before DataFrame construction. This ensures Polars always infers a complete struct schema, so `struct.field()` and `coalesce()` work as expected with fallback chains.

Define explicit field sets for each struct type (`_ORDER_STRUCT_FIELDS`, `_FILL_STRUCT_FIELDS`, `_WALLET_IMPACT_FIELDS`, `_INSTRUMENT_FIELDS`) and call `_ensure_struct_fields()` in `_transform_orders()` and `_transform_dividends()` before creating the Polars DataFrame.

## Constraints

- Must not change the Polars expression logic — only the data pre-processing
- Must handle any optional field being missing, not just `filledQuantity`
- Must handle nested structs (e.g., `order.instrument`, `fill.walletImpact`)
- Must not affect the snapshot transform (which uses `dict.get()` patterns)

## Consequences

- **Positive**: Production T212 CDC pipeline no longer crashes on real API data
- **Positive**: `coalesce()` fallback chains work correctly when fields are absent (null → fallback value)
- **Negative**: Field sets must be maintained when the API spec changes
- **Negative**: Extra Python pre-processing step before DataFrame construction (negligible performance impact)

## Validation

- `test_transform_cdc_order_missing_optional_fields` — order without `filledQuantity`/`filledValue` falls back via coalesce
- All 479 tests pass
- Production deployment verified via Step Functions execution