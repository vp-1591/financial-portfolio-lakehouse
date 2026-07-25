# Remove Yahoo Finance as FX Rate Provider

## Context

Yahoo Finance v8 API returns non-deterministic `symbol` values in its response metadata. The same `USDEUR=X` request randomly returns either `EUR=X` (~60-80% of the time) or `USDEUR=X` (~20-40%), with the same correct rate. The symbol validation in `fetch_yahoo_rate` correctly rejects mismatches (`EUR=X` ≠ `USDEUR=X`), but this causes random pipeline failures even when nothing changed in the code.

The integration test `test_yahoo_returns_matching_symbol_for_valid_currency` was flaky in CI: it passed in PR #92 by luck but failed in PR #94. The pipeline could succeed 10 times and then crash without any code change — a reliability risk.

Frankfurter API (`api.frankfurter.app`) already covers all currencies needed by the pipeline (USD, EUR, GBP for GBX conversion). Yahoo served only as a fallback, and its non-deterministic behavior meant it could cause failures even when Frankfurter was healthy.

## Decision

Remove Yahoo Finance as an FX rate provider. Frankfurter is now the sole automated provider. The `--fx-rate` CLI override remains as a manual fallback if Frankfurter is unavailable.

Specific changes:

- Remove `YAHOO_FINANCE_BASE_URL` constant, `yahoo_base_url` constructor parameter, and `fetch_yahoo_rate` method from `CurrencyConverter`.
- Remove Yahoo from the provider loop in `fetch_rate`, keeping the single-provider loop pattern for future extensibility.
- Remove the Yahoo-specific unit tests (`test_yahoo_rejects_normalised_symbol`, `test_yahoo_accepts_matching_symbol`, `test_yahoo_accepts_response_without_symbol`) and the integration test (`test_yahoo_returns_matching_symbol_for_valid_currency`).
- Update the `MINOR_CURRENCY_UNITS` comment to reference Frankfurter's 400 error instead of Yahoo symbol validation.
- Update ADR 0077 to remove the Yahoo Finance mention from its consequences.

This supersedes the Yahoo-specific parts of ADR 0095 (the symbol validation safety mechanism). The GBX handling via `MINOR_CURRENCY_UNITS` and the snapshot `security_ccy` fix from ADR 0095 remain unchanged.

## Constraints

- No new providers added in this change. The loop pattern in `fetch_rate` is preserved so a future provider can be added by inserting a tuple.
- GBX→EUR conversion must still work. Frankfurter supports GBP, so `MINOR_CURRENCY_UNITS` ("GBX" → GBP/100) continues to work.
- `request_json` utility method is retained — it is used by `fetch_frankfurter_rate`.

## Consequences

- **Positive**: FX rate fetching is deterministic. If Frankfurter returns a valid rate, it is used; if Frankfurter fails, the error is clear and suggests `--fx-rate`. No more random failures from Yahoo symbol mismatches.
- **Positive**: Simpler code — fewer external dependencies, fewer tests to maintain, no symbol validation logic.
- **Negative**: No automated fallback if Frankfurter is down. The `--fx-rate` manual override is the escape hatch. Frankfurter is backed by ECB reference rates and is reliable, but any single-provider setup has this tradeoff.
- **Negative**: The Yahoo-specific unit tests for symbol validation are removed. These tested behavior that no longer exists in the codebase.

## Validation

- All remaining tests pass (unit + integration).
- `test_gbx_live_conversion_via_gbp` exercises the GBX→GBP→EUR path end-to-end via Frankfurter.
- `test_unknown_currency_raises_error` confirms Frankfurter rejects unknown codes.
- No `yahoo` references remain in `pipeline/` or `tests/`.