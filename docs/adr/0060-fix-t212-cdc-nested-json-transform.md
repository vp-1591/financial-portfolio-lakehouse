# ADR 0060: Fix T212 CDC Transform for Nested JSON Structures

## Context

The T212 CDC transform (`pipeline/connectors/trading212/transform.py`) treated all events as flat dicts — e.g., `event.get("ticker")` — but the T212 `/equity/history/orders` endpoint returns nested `HistoricalOrder` objects with `{order: Order, fill: Fill}` structure, and the `/equity/history/dividends` endpoint returns items with a nested `instrument` object. The flat `dict.get()` patterns silently returned empty strings for nested fields, causing 11 of 24 CDC schema columns to be always null and core fields (`event_id`, `ticker`, `isin`, `quantity`) to be empty or zero.

The root cause was complexity: `first_value()` chains, `nested_dict()` + `dict.get()` patterns, and manual dict-building loops made it easy to silently produce wrong data when the API response structure didn't match the assumed flat shape.

## Decision

1. **Replace dict-building loops with Polars-native field extraction.** Each T212 CDC endpoint (orders, dividends, transactions) now has a dedicated transform function (`_transform_orders`, `_transform_dividends`, `_transform_transactions`) that creates a Polars DataFrame from the event list and uses Polars expressions for field mapping.

2. **Use `struct.field()` for nested access** instead of `nested_dict()` + `dict.get()`. Polars struct field access (`pl.col("order").struct.field("ticker")`) fails explicitly when the expected field is missing, rather than silently returning `None`.

3. **Use `coalesce()` for fallback chains** instead of `first_value()`. `pl.coalesce([pl.col("order").struct.field("createdAt"), pl.col("fill").struct.field("filledAt")])` is more readable than `first_value(event, ("createdAt", "filledAt"))` and works correctly with nested structures.

4. **Populate all 24 CDC schema fields** for orders and dividends (previously only 13 were populated). New fields: `settle_date`, `description`, `price`, `side`, `gross_amount`, `fee_amount`, `tax_amount`, `net_amount`, `base_currency`, `fx_rate_to_base`, `amount_base`.

5. **Add `decrypt_cdc_payloads()` and `finalize_table()` to `transform_utils.py`.** `decrypt_cdc_payloads()` replaces `iter_raw_payloads()` for CDC transforms, returning structured `(fetched_at, source, events)` tuples ready for Polars DataFrame construction. `finalize_table()` mirrors `build_normalized_table()` but starts from a Polars DataFrame instead of a list of dicts.

6. **Move `_unwrap_t212_events()` to `transform_utils._unwrap_events()`.** The pagination unwrapping logic is broker-agnostic and now lives in the shared utility module.

7. **Update transaction type map** to match the actual T212 API: `"WITHDRAW"` → `"WITHDRAWAL"` (the API uses WITHDRAW, not WITHDRAWAL), and remove entries that don't appear in `HistoryTransactionItem.type` (BUY, SELL, DIVIDEND, INTEREST, TAX, ADJUSTMENT).

8. **Expand `encrypt_columns`** from 2 columns (`cash_amount`, `quantity`) to 9 columns (adding `price`, `gross_amount`, `fee_amount`, `tax_amount`, `net_amount`, `fx_rate_to_base`, `amount_base`), matching the IBKR convention.

## Constraints

- The T212 snapshot transform (`transform_snapshot`) is unchanged — it still uses `iter_raw_payloads()` and the `client.py` helpers. A future refactor can convert it to Polars expressions.
- The IBKR and XTB CDC transforms are unchanged. They still use `iter_raw_payloads()` and dict-based field mapping.
- `_unwrap_events()` in `transform_utils.py` handles both bare lists and paginated dicts, maintaining backward compatibility with ADR 0059.
- The CDC schema (`cdc_events_normalized_schema`) is unchanged. All 24 columns remain the same.

## Consequences

- **Orders now produce complete CDC rows** with all 24 fields populated from the nested `order`, `fill`, and `fill.walletImpact` structures.
- **Dividends now extract `isin` and `name` from the nested `instrument` object** and compute `gross_amount` from `grossAmountPerShare * quantity`.
- **Transactions remain flat** but now populate `net_amount`, `base_currency`, `fx_rate_to_base`, and `amount_base`.
- **`fee_amount` and `tax_amount` are populated** for orders by extracting from `fill.walletImpact.taxes`, splitting by tax name (CURRENCY_CONVERSION_FEE etc. → fees; FRENCH_TRANSACTION_TAX → taxes).
- **Polars struct access fails explicitly** on missing fields instead of silently returning None, making future schema mismatches easier to detect.
- **Backward compatible**: Previously written rows with null in the new columns coexist fine with new rows that have actual values. Downstream consumers that call `decrypt_float()` on these columns already handle nulls.

## Validation

- All 9 `TestCdcTransform` tests pass with realistic nested JSON fixtures matching the T212 API spec.
- New tests: `test_transform_cdc_order_with_taxes` (fee/tax extraction), `test_transform_cdc_order_sell_side` (side field), `test_transform_cdc_dividend_with_instrument` (nested instrument extraction), `test_transform_cdc_transaction_withdraw_type` (WITHDRAW → WITHDRAWAL mapping).
- All 389 project tests pass.
- `ruff check` and `ruff format` pass without issues.