# 0080: Store security_value and position_type in consolidated holdings; add instrument_ccy to CDC events

## Context

Two issues undermined the currency unification achieved in Phases 1–3 (ADRs 0076, 0077, 0078):

1. **`security_value` and `position_type` dropped then re-derived.** The `consolidated_holdings` table stored only `target_value` (EUR-converted). `build_portfolio_holdings()` had to re-read and re-decrypt all broker snapshots to recover `security_value` (native-currency value) and `position_type`, causing double-read and double-decryption.

2. **`security_ccy` had inconsistent semantics for dividends.** For T212 dividends, `security_ccy` was set to `coalesce(currency, tickerCurrency)` — the payout currency, not the instrument's trading currency. For trades, `security_ccy` is the instrument's trading currency. This is the same kind of overloading that Phases 1–3 set out to eliminate. A GBX-denominated stock paying a GBP dividend would have `security_ccy=GBP`, not `GBX`.

## Decision

### Store `security_value` and `position_type` in `consolidated_holdings`

Add `security_value` (Fernet-encrypted binary) and `position_type` (string: EQUITY|CASH) to `consolidated_holdings_schema`. Pass both through from snapshots via the `Holding` dataclass (which gains a `position_type` field). Simplify `build_portfolio_holdings()` to read all data directly from `consolidated_holdings` — no more snapshot re-read or left join.

Changes:
- `consolidated_holdings_schema`: added `security_value` and `position_type` columns
- `Holding` dataclass: added `position_type: str = "EQUITY"`
- `consolidate_holdings()`: encrypts and stores `holding.value` as `security_value`, stores `holding.position_type`
- `build_portfolio_holdings()`: reads `security_value` and `position_type` from consolidated, removes snapshot re-read loop and left join
- All three connector `extract_holdings()`: pass `position_type` from snapshot rows

### Add `instrument_ccy` to CDC events and clarify `security_ccy` semantics

Add a nullable `instrument_ccy` column to `cdc_events_normalized_schema`. This captures the instrument's trading currency separately from `security_ccy` (which is the amount currency). The semantics are now:
- `security_ccy` — the currency that `cash_amount`, `gross_amount`, `fee_amount`, `tax_amount` are denominated in (consistent across all event types)
- `instrument_ccy` — the instrument's trading currency, when known; null when unknown or N/A (deposits, fees)

For T212 dividends: `security_ccy = coalesce(currency, tickerCurrency)` (payout currency), `instrument_ccy = tickerCurrency` (instrument's trading currency). For IBKR and XTB: `instrument_ccy = None` (not available from CashTransaction data).

No changes to `normalize_currency()` — it already uses `security_ccy` as the FX source currency, which is correct since `security_ccy` is the amount currency.

Changes:
- `cdc_events_normalized_schema`: added `instrument_ccy` (nullable string) after `security_ccy`
- `dividend_income_schema`: added `instrument_ccy` (nullable string) as display column
- T212 `_transform_dividends()`: added `instrument_ccy = pl.col("tickerCurrency")`
- T212 `_transform_orders()` and `_transform_transactions()`: added `instrument_ccy = pl.lit(None)`
- IBKR all CDC records: added `"instrument_ccy": None`
- XTB all CDC records: added `"instrument_ccy": None`
- `build_dividend_income()`: includes `instrument_ccy` in aggregation (first non-null per group)

## Constraints

- `security_ccy` must remain the amount currency for all event types — changing it would break FX conversion in `normalize_currency()`
- `instrument_ccy` is informational metadata only; `normalize_currency()` does not use it
- `position_type` defaults to "EQUITY" for backward compatibility with callers that don't pass it

## Consequences

- `build_portfolio_holdings()` no longer depends on broker connectors or snapshot Delta tables — the analytics layer is decoupled from the connector layer
- `security_value` is now stored in both `consolidated_holdings` (encrypted) and `portfolio_holdings` (decrypted), eliminating the double-read/double-decryption
- Cross-currency dividends (e.g., GBX stock paying GBP dividends) are now explicitly tracked: `security_ccy=GBP` (amount currency) and `instrument_ccy=GBX` (instrument's trading currency)
- For same-currency events, `instrument_ccy` is null (no additional information beyond `security_ccy`)
- Delta tables are overwritten on each pipeline run, so schema changes are a clean break — no migration needed

## Validation

- All 612 existing tests pass
- `test_transform_cdc_dividend_currency_mismatch_warning`: verifies `security_ccy=EUR` (payout) and `instrument_ccy=USD` (instrument) for cross-currency dividend
- `test_transform_cdc_dividend_same_currency_no_warning`: verifies `security_ccy=USD` and `instrument_ccy=USD` for same-currency dividend
- `build_portfolio_holdings()` reads all data from `consolidated_holdings` without snapshot join
- `build_dividend_income()` includes `instrument_ccy` in output table