# 0077: Currency Unification Phase 2 — Schema Redesign

> **Supersedes ADR 0074** — Phase 1 renamed `value_currency` to `security_currency` and `base_currency` to `currency` in the `Holding` dataclass. Phase 2 replaces all remaining overloaded column names with unambiguous ones.

## Context

Phase 1 (ADR 0076) fixed T212's `walletImpact.fxRate` semantics so that `cash_amount` is now in the security's trading currency. However, the schema still used overloaded column names (`value_currency`, `base_currency`, `fx_rate_to_base`, `amount_base`, `net_amount`, `value`, `security_currency`) whose meaning varied by broker and context. Additionally:

- **Bug 7**: IBKR snapshots pre-converted `value` to account base currency, losing the native-currency amount.
- **Bug 6 remainder**: T212 dividends and transactions lacked FX conversion because no pipeline step computed `target_value`.

The roadmap (`docs/roadmaps/0010-currency-unification.md`) defined Phase 2 to replace all overloaded names and add a `normalize_currency()` step.

## Decision

Replaced all overloaded currency column names with unambiguous ones:

| Old column | New column | Meaning |
|---|---|---|
| `value_currency` | `security_ccy` | Currency the monetary amount is denominated in |
| `base_currency` | `target_ccy` | Pipeline target currency (always EUR) |
| `fx_rate_to_base` | `target_fx_rate` | Rate from `security_ccy` → `target_ccy` |
| `amount_base` | `target_value` | Value converted to `target_ccy` |
| `net_amount` | *(removed)* | Redundant with `cash_amount` |
| `value` (snapshots) | `security_value` | Position value in native currency |
| `security_currency` | `security_ccy` | Shortened for consistency |

Created `pipeline/normalized/normalize.py` with `normalize_currency()` that:
1. Reads `cdc_events` Delta table
2. Decrypts `cash_amount` and `target_fx_rate`
3. For each row: fills `target_fx_rate`, `target_value`, `target_ccy` using `CurrencyConverter`
4. Re-encrypts and overwrites the table

IBKR snapshot transform no longer pre-converts position values — `security_value` now stores the native-currency amount (Bug 7 fix).

IBKR CDC transforms set `target_fx_rate` from `fxRateToBase` only when `account_base == target_ccy`; otherwise `None` (filled later by `normalize_currency()`).

The `Holding` dataclass retains its field names (`currency`, `security_currency`) per ADR 0074 — these are in-memory value objects, not table columns.

## Constraints

- All Delta tables use `write_deltalake(mode="overwrite")`, so schema changes are a clean break — no incremental migration needed.
- The `Holding` dataclass field names are not renamed (ADR 0074).
- XTB CDC transform was kept minimal: only schema conformance (null `target_fx_rate`, `target_value`, `target_ccy`).
- `normalize_currency()` runs after `consolidate_cdc_events()` and before CDC analytics in `pipeline/run.py`.

## Consequences

- **Positive**: Every column name has a single, unambiguous meaning. `target_value` is always in `target_ccy` (EUR). `security_value`/`cash_amount` are always in `security_ccy`.
- **Positive**: IBKR snapshots no longer lose native-currency values. T212 dividends/transactions get FX conversion via `normalize_currency()`.
- **Negative**: All Delta tables must be rebuilt from raw data after this change — old tables are incompatible.
- **Negative**: `normalize_currency()` requires API access (Frankfurter/Yahoo Finance) for currencies not covered by `--fx-rate` overrides, making the pipeline dependent on external services for the normalize step.

## Validation

- All 606 tests pass, including 7 new tests in `tests/test_normalize_currency.py` covering:
  - Same-currency events (`target_fx_rate = 1.0`)
  - IBKR events with broker-provided `target_fx_rate`
  - T212 events falling back to `CurrencyConverter`
  - Mixed broker/currency scenarios
  - Empty table and missing table edge cases
  - Null `cash_amount` graceful handling
- `ruff check` and `ruff format` pass clean.
- `test_consolidate_cdc.py`, `test_connector_protocol.py`, `test_ibkr_connector.py` updated for new column names.