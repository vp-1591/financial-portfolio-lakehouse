# Fix T212 Snapshot Currency Mismatch and GBX Rate Bug

> **Superseded by [ADR 0097](./0097-remove-yahoo-finance-fx-provider.md)** — Yahoo Finance fallback and symbol validation removed. GBX handling via `MINOR_CURRENCY_UNITS` and snapshot `security_ccy` fix remain unchanged.

## Context

Two bugs in the Trading 212 pipeline inflate the reported portfolio value by approximately 3.3× (19,061 EUR instead of 5,776 EUR):

1. **Snapshot `security_ccy` mismatch**: `transform_snapshot()` stores `walletImpact.currentValue` (in wallet currency, e.g. PLN) but labels it with the instrument's trading currency (e.g. EUR, GBX, GBP). This is the same value-currency mismatch that ADR 0076 fixed for CDC events, but the snapshot transform was never updated because the snapshot API doesn't provide `fxRate`.

2. **GBX treated as GBP**: `CurrencyConverter.fetch_rate()` queries `GBXEUR=X` on Yahoo Finance, which returns the GBP→EUR rate (~1.17) instead of the GBX→EUR rate (~0.0117). Since 1 GBP = 100 GBX, this produces a rate 100× too high.

The root cause for Bug 1: `position_security_currency()` returns the instrument's trading currency, but `position_value()` returns `walletImpact.currentValue` which is in the wallet currency. The `position_currency()` helper already exists and returns `walletImpact.currency` first — it just wasn't used for `security_ccy` in snapshots.

For Bug 2: no minor currency unit mapping existed in `CurrencyConverter`. GBX (British pence) is 1/100 of GBP, but the converter had no knowledge of this relationship.

## Decision

### Bug 1: Use `position_currency()` for snapshot `security_ccy`

In `transform_snapshot()`, replace the call to `position_security_currency()` with `position_currency()`. The `position_currency()` function returns `walletImpact.currency` (the wallet currency) first, which correctly matches the denomination of `position_value()` (which returns `walletImpact.currentValue`). This is consistent with how CDC events work — values are labeled in the currency they are actually denominated in.

For snapshot positions without `walletImpact`, `position_currency()` falls back through the same chain as `position_value()`: position-level currency, then instrument currency from metadata, then account currency. This correctly aligns the value's currency with its source.

### Bug 2: Add `MINOR_CURRENCY_UNITS` mapping to `CurrencyConverter`

Add a class-level `MINOR_CURRENCY_UNITS` dictionary mapping minor currency codes to their major unit and conversion factor (e.g., `GBX → (GBP, 100)`). When `fetch_rate()` encounters a minor currency unit, it fetches the major unit's rate and divides by the factor.

Unknown minor currency codes that aren't in `MINOR_CURRENCY_UNITS` and aren't recognized by the API providers fall through to the existing `PortfolioConnectorError`. Frankfurter rejects unknown codes with a 400 error. Yahoo may silently normalise codes (e.g., `GBX→GBP`), but `fetch_yahoo_rate` now validates that the symbol Yahoo echoes back matches the requested symbol — if it differs, a `PortfolioConnectorError` is raised. This ensures a guaranteed loud failure rather than a silent wrong rate.

## Constraints

- No schema changes — `security_ccy` column semantics are corrected, but the column name and type remain the same.
- Must be consistent with ADR 0076/0077 approach for CDC events: values are labeled in their actual denomination.
- GBX handling must also fix the `normalize_currency()` path for CDC events, where `security_ccy = "GBX"` correctly appears for GBX-denominated instruments.
- New minor currencies must be added to `MINOR_CURRENCY_UNITS` explicitly — no silent fallback to wrong rates.

## Consequences

- Snapshot `security_ccy` will show wallet currency (e.g., PLN) instead of instrument currency (e.g., EUR/GBX/GBP) — correctly reflecting that the stored `security_value` is denominated in the wallet currency.
- GBX positions in CDC events will now correctly convert to EUR via GBP (rate / 100).
- Future minor currency codes (e.g., ILA for Israeli agorot) must be added to `MINOR_CURRENCY_UNITS` or they will raise `PortfolioConnectorError` at FX conversion time.
- Downstream consumers that relied on `security_ccy` being the instrument's trading currency (rather than the value's denomination) will need to use `instrument_ccy` (added by ADR 0080) for instrument-level currency information.

## Validation

- New unit tests: `test_gbx_converts_via_gbp_divided_by_100`, `test_gbp_unaffected_by_gbx_mapping`, `test_gbx_rate_cached_after_first_convert`, `test_yahoo_rejects_normalised_symbol`, `test_yahoo_accepts_matching_symbol`, `test_yahoo_accepts_response_without_symbol` in `TestCurrencyConverter`.
- New unit test: `test_snapshot_security_ccy_uses_wallet_currency` in `TestTransformSnapshot` — verifies PLN wallet with EUR/GBX/GBP instruments produces `security_ccy = "PLN"` for all equity positions.
- New unit test: `test_gbx_converted_via_gbp_divided_by_100` in `TestNormalizeCurrency` — verifies GBX→EUR rate is GBP→EUR / 100.
- Updated `security_ccy` assertion in `test_transform_preserves_isin` — verifies wallet currency is used.
- Full test suite passes (646 tests).