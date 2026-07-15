# 0078 — Currency Unification Phase 3: Data Quality Fixes

> **Status**: active
> **Created**: 2026-07-14

## Context

Phases 1 and 2 of the Currency Unification roadmap are complete:

- **Phase 1** (ADR 0076, superseded by 0077) reclassified `walletImpact.fxRate` as a wallet→security rate and used it to convert T212 `cash_amount` to `security_ccy`.
- **Phase 2** (ADR 0077) replaced all overloaded column names (`value_currency` → `security_ccy`, `base_currency` → `target_ccy`, `fx_rate_to_base` → `target_fx_rate`, `amount_base` → `target_value`, `value` → `security_value`) and added `normalize_currency()`.

Two data quality bugs remain:

1. **Bug 4**: IBKR `settle_date` fields are stored in compact `YYYYMMDD` format from the Flex XML, but the pipeline expects `YYYY-MM-DD` (or full ISO 8601 datetime).
2. **Bug 5**: `fee_amount`, `tax_amount`, and `gross_amount` are in wallet currency for T212 orders (they should be in `security_ccy`), and IBKR trade fees may be in `ibCommissionCurrency` which can differ from the trade's `security_ccy`.

Additionally, the `fee_amount` and `tax_amount` schema comments in `models.py` contained a Phase 3 TODO marker noting that T212 stores these in wallet currency, and `quality.py` had a stale comment referencing the removed `amount_base` column.

## Decision

### Bug 4: Normalize IBKR `settle_date`

Apply the existing `_normalize_ibkr_datetime()` function to all four `settle_date` fields in the IBKR CDC transform (`_process_ibkr_trade`, `_process_ibkr_cash_transaction`, `_process_ibkr_transfer`, `_process_ibkr_transaction_fee`). This converts `YYYYMMDD` → `YYYY-MM-DDT00:00:00Z` and passes through already-normalized strings unchanged. No new helper function is needed — the existing `_normalize_ibkr_datetime()` handles both compact formats.

### Bug 5 Part A: Convert T212 fee/tax/gross to `security_ccy`

In `_transform_orders()`, multiply `gross_amount`, `fee_amount`, and `tax_amount` by `fx_rate` (the wallet→security rate from `walletImpact.fxRate`). This is the same conversion already applied to `cash_amount` in Phase 1. All three amounts originate in wallet currency (PLN for a Polish account) and need the same wallet→security conversion.

No changes are needed in `_transform_dividends()` or `_transform_transactions()` — their `fee_amount`, `tax_amount`, and `gross_amount` are already in `security_ccy` (0.0 for fees/taxes in dividends/transactions; `price * qty` for gross in dividends).

### Bug 5 Part B: Convert IBKR fees to `security_ccy`

In `_process_ibkr_trade()`, when `ibCommissionCurrency` differs from the trade's `currency` (i.e., `security_ccy`):

1. If `ibCommissionCurrency` matches the account base currency, reverse `fxRateToBase` to convert: `fee_in_security_ccy = raw_fee / fxRateToBase`. This handles the most common cross-currency case (commission in base currency, trade in foreign currency).
2. If `ibCommissionCurrency` differs from both `security_ccy` and `base_currency`, log a warning and leave the fee in its native currency. No reliable rate is available for this triple-currency conversion.
3. If `ibCommissionCurrency` is empty or matches `security_ccy`, no conversion is needed.

No changes are needed in other IBKR event types — `_process_ibkr_cash_transaction`, `_process_ibkr_transfer`, and `_process_ibkr_transaction_fee` don't have `ibCommissionCurrency` and their fees are already in `security_ccy`.

### Schema comments and stale references

- Updated `models.py` comments on `fee_amount` and `tax_amount` to remove the "Phase 3: wallet ccy for T212" TODO, since this phase addresses it.
- Updated `quality.py` comment from `# For CDC, amount_base is encrypted` to `# CDC target_value is encrypted — use security_ccy column for currency coverage`.

## Constraints

- No schema changes — all columns already exist with correct types.
- `fee_amount` and `tax_amount` are not converted to `target_ccy` by `normalize_currency()`. They remain in `security_ccy`. This is an acknowledged limitation — if target-currency fee aggregation is needed in the future, a separate enhancement would add conversion in `normalize_currency()`.
- IBKR cross-currency fees where `ibCommissionCurrency` differs from both `security_ccy` and `base_currency` are left unconverted with a warning. This is the safest approach since no reliable rate is available.
- XTB connector changes are out of scope (XTB CDC stays on its own schema until the CDC Tables roadmap is implemented).

## Consequences

- All monetary amounts in CDC events (`cash_amount`, `gross_amount`, `fee_amount`, `tax_amount`) are now consistently in `security_ccy` for both T212 and IBKR connectors.
- IBKR `settle_date` is normalized to ISO 8601 format, consistent with `event_datetime`.
- The identity `cash_amount ≈ gross_amount - fee_amount - tax_amount` now holds in `security_ccy` for cross-currency T212 trades (previously it only held in wallet currency).
- T212 `gross_amount` values change — they are now in `security_ccy` instead of wallet currency. Any downstream consumer expecting wallet-currency `gross_amount` must be updated.

## Validation

- `test_transform_cdc_order_fee_tax_converted_to_security_ccy`: Cross-currency T212 order with PLN wallet, USD security, fees and taxes in PLN converted to USD.
- `test_transform_cdc_order_cross_currency_fx_rate`: Extended to verify `gross_amount` conversion.
- `test_transform_cdc_order_gbx_security_currency`: Extended to verify `gross_amount` conversion.
- `test_trade_fee_same_currency`: IBKR trade where commission currency matches trade currency — no conversion.
- `test_trade_fee_commission_in_base_currency`: IBKR trade where commission currency matches base currency — fee converted via `1/fxRateToBase`.
- `test_trade_fee_cross_currency_warning`: IBKR trade where commission currency differs from both trade and base currency — warning logged, fee left unconverted.
- `test_transform_cdc_settle_date_normalized`: IBKR CDC output has no compact `YYYYMMDD` dates in `settle_date`.
- Full test suite passes.