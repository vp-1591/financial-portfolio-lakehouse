# 0076 — Fix T212 `walletImpact.fxRate` Usage (Phase 1: Currency Unification)

> **Superseded by [ADR 0077](./0077-currency-unification-phase2-schema-redesign.md)** — Phase 2 replaced all remaining overloaded column names (`value_currency`, `base_currency`, `fx_rate_to_base`, etc.) with unambiguous ones and added `normalize_currency()`.

## Context

The T212 connector transform treated `walletImpact.fxRate` as a "wallet-to-base" rate, but it is actually the **wallet-to-security** rate (e.g., PLN→USD, PLN→GBP, PLN→GBX). This caused Bug 1: `fx_rate_to_base` was mislabeled, and `cash_amount` was stored in wallet currency instead of the security's trading currency.

Additionally, `value_currency` and `base_currency` were both set to `walletImpact.currency` (the wallet currency, e.g., PLN), when they should reflect the security's trading currency (e.g., USD). This meant `amount_base` (= `cash_amount × fx_rate`) was actually in security currency but labeled as base currency.

The roadmap for currency unification (`docs/roadmaps/0010-currency-unification.md`) defines four phases. Phase 1 fixes the T212 transform internals without changing the schema — columns keep their old names (`value_currency`, `base_currency`, `fx_rate_to_base`, `amount_base`) but their values are corrected to reflect security currency semantics.

## Decision

In `_transform_orders()` (T212 CDC order transform):

1. **`value_currency`** is now set to the security's trading currency (`instrument.currency`), not the wallet currency (`walletImpact.currency`). This correctly reflects the currency that `cash_amount` is stored in.

2. **`cash_amount`** is now computed as `net_value × fx_rate` (wallet amount converted to security currency), instead of `net_value` (wallet amount). This uses `walletImpact.fxRate` as the wallet→security conversion rate.

3. **`base_currency`** is now set to the same value as `value_currency` (the security's trading currency), making `amount_base` correctly in `base_currency` units.

4. **`net_amount`** is now computed as `net_value × fx_rate` (matching `cash_amount` in security currency), instead of `net_value` (wallet currency).

5. **`fx_rate_to_base`** and **`amount_base`** values are unchanged (same numeric values), but their semantics are now correctly documented: `fx_rate_to_base` is the wallet→security rate, and `amount_base` is the amount in security currency.

6. **`gross_amount`, `fee_amount`, `tax_amount`** remain in wallet currency — a known inconsistency that Phase 3 will address.

In `_transform_dividends()`:

- No value changes. The existing behavior (`value_currency` = dividend's `currency`, `fx_rate_to_base` = 1.0) is correct for Phase 1 since `walletImpact.fxRate` is unavailable for dividends.
- Added a **warning log** when dividend `currency` differs from `tickerCurrency`, indicating that FX conversion is needed but deferred to Phase 2.

In `_transform_transactions()`: No changes.

## Constraints

- **No schema changes.** Column names (`value_currency`, `base_currency`, `fx_rate_to_base`, `amount_base`) remain until Phase 2.
- **IBKR connector unchanged.** IBKR transforms use different semantics (`fxRateToBase` is account-base→target) and are handled in Phase 2.
- **`gross_amount`, `fee_amount`, `tax_amount` remain in wallet currency.** Converting these requires the same `walletImpact.fxRate` for fees, but fee amounts come from a different source (`walletImpact.taxes`) and may need separate handling. Phase 3 will address this.
- **Demo data has `fx_rate = 1.0`** and same-currency trades, so Phase 1 produces numerically identical results on demo data. New cross-currency tests cover the behavioral change.

## Consequences

- **T212 order `cash_amount` is now in security currency** (e.g., USD for SPYI), not wallet currency (e.g., PLN). Downstream analytics tables that group by `value_currency` will now group T212 trades by security currency instead of wallet currency.
- **The identity `net_amount = gross_amount - fee_amount - tax_amount` breaks for cross-currency trades**, since `net_amount` is in security currency while the others remain in wallet currency. Phase 3 will convert all amounts to `security_ccy`.
- **Dividends where `currency ≠ tickerCurrency`** now produce a warning log but are not converted. Phase 2 will use `CurrencyConverter` for this conversion.
- **The demo pipeline report produces identical results** since demo data has `fx_rate = 1.0` and same-currency trades.

## Validation

- `test_transform_cdc_order_cross_currency_fx_rate` verifies that a PLN wallet buying a USD security produces `cash_amount` in USD (security currency), not PLN (wallet currency).
- `test_transform_cdc_order_gbx_security_currency` verifies GBX (pence) handling with a large `fx_rate`.
- `test_transform_cdc_dividend_currency_mismatch_warning` verifies that dividends with `currency ≠ tickerCurrency` produce a warning log.
- `test_transform_cdc_dividend_same_currency_no_warning` verifies no false warnings.
- All 54 existing T212 connector tests pass.
- `pipeline report --output data/report.html` on demo data renders correctly.