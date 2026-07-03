# 0046: Fix consolidated holdings currency column

## Context

In `consolidate_holdings()`, the `currency` column was populated with `holding.security_currency or holding.currency`. This is wrong because the `value` column has been converted to `converter.target_currency` (e.g., EUR) via FX rates. The `currency` column should reflect the actual denomination of the converted value, not the security's native trading currency.

For example, a GBP-denominated stock in a PLN-based Trading 212 account would have:
- `value` converted from PLN to EUR
- `currency` column showing "GBP" (the security's native currency) instead of "EUR" (what the value is actually in)
- `security_currency` column already correctly showing "GBP"

The `security_currency` column already preserves the security's native currency separately, so no information is lost by fixing `currency` to the target currency.

## Decision

Change line 313 in `consolidate.py` from:
```python
currencies.append(holding.security_currency or holding.currency)
```
to:
```python
currencies.append(converter.target_currency)
```

This ensures `currency` always reflects the denomination of the encrypted `value` in the consolidated holdings table.

## Consequences

- The `currency` column in `consolidated_holdings` will always equal the converter's target currency, making it consistent with the converted `value` column.
- Downstream consumers can rely on `currency` to know what unit the `value` is in, and use `security_currency` for the instrument's native trading currency.
- Existing tests did not cover the `currency` column; a new assertion has been added.

## Validation

- Added assertion in `test_consolidate_multi_broker_holdings` verifying all rows in the `currency` column equal the converter's target currency.
- Existing tests continue to pass.