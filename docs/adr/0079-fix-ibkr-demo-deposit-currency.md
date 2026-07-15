# 0079: Fix IBKR demo deposit currency after Phase 2 column rename

## Context

During Phase 2 of the currency unification project (ADR 0077), the `base_currency` column in CDC events was renamed to `security_ccy`. The `_inject_demo_deposit` function in `pipeline/connectors/ibkr/transform.py` was updated to collect `security_ccy` from existing records instead of `base_currency`.

However, `security_ccy` is the **security's trading currency** (e.g., USD for AAPL), while `base_currency` was the **account's base currency** (e.g., EUR for a Eurozone account). When a EUR-based account's first event trades a USD security, the demo deposit was incorrectly labeled as USD instead of EUR.

The function's own docstring and comment still stated "Demo deposits are in account base currency," contradicting the actual behavior.

## Decision

Pass the authoritative `base_currency_by_account` mapping (already computed from IBKR Flex XML `<Account>` elements) into `_inject_demo_deposit` and use it to determine deposit currency, instead of inferring it from `security_ccy` of existing events.

Changes:
- Added `base_currency_by_account: dict[str, str]` parameter to `_inject_demo_deposit`.
- Replaced the loop that inferred `security_ccy` from the first event per account with `accounts = dict(base_currency_by_account)`.
- Initialized `base_currency_by_account` before the CDC source-iteration loop in `transform_cdc` so it is available even when no CDC rows exist.
- Updated all callers and tests accordingly.
- Added a regression test that explicitly verifies a EUR-based account trading USD stocks gets a EUR deposit.

## Constraints

- Demo deposits must always be in the account's base currency, not the security's trading currency.
- The `base_currency_by_account` mapping must remain consistent with the Flex XML `<Account>` elements.

## Consequences

- Demo deposits now correctly use the account's base currency regardless of what securities are traded.
- The function signature change is internal to the IBKR connector; no external API is affected.
- When `records` is empty but `base_currency_by_account` has entries, deposits are still injected (previously this was a no-op).

## Validation

- `test_deposit_uses_base_currency_not_security_ccy` — regression test verifying a EUR-based account with USD trades gets a EUR deposit.
- `test_multi_account_gets_deposit_per_account` — updated to assert deposits use base currency from `base_currency_by_account`, not from `security_ccy`.
- `test_fallback_date_when_no_records` — updated to cover the case where accounts are known but records are empty.
- All 612 existing tests pass, including the full `test_ibkr_connector.py` suite (65 tests).