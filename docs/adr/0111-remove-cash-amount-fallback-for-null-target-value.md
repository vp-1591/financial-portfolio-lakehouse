# 0111: Remove cash_amount Fallback for Null target_value

## Context

The analytics layer silently substitutes `cash_amount` for null `target_value` at two sites, producing amounts that violate ADR 0077's invariant that `target_value` is always in `target_ccy` (EUR):

1. `pipeline/analytics/cdc_tables.py` `_read_cdc_events` computed `target_value_resolved` with a fallback: when the decrypted `target_value` was null it derived `cash_amount * target_fx_rate`; when target columns were entirely absent it aliased raw `cash_amount` (native currency labeled as target).
2. `pipeline/report/charts.py` `cash_flow_breakdown` switched `value_col` to `cash_amount` when `target_value` was all-null, summing native-currency amounts (PLN, USD, ...) as if they were the base currency.

Root cause: `pipeline/normalized/normalize.py` `normalize_currency()` caught every `CurrencyConverter` exception, logged a warning, and left `target_value`/`target_fx_rate` null. The warning died in logs; the analytics fallback then papered over the null with a wrong value.

The `cash_amount * target_fx_rate` fallback was not dead code: `pipeline/connectors/ibkr/transform.py` sets `target_fx_rate` when the account base equals the target currency but leaves `target_value` None, so the fallback fired for IBKR rows in the pre-normalize state.

## Decision

No fallback. A null or missing target value is surfaced, never replaced:

- `normalize_currency()` collects every converter failure and raises `RuntimeError` ("Could not convert {currencies} to {target_ccy}. Pass --fx-rate CURRENCY=RATE to provide manual rates.") **before** overwriting the table, so nulls are never written.
- `_read_cdc_events` raises `RuntimeError` when `target_value` or `target_ccy` is missing, or when any row has a null in either column — the error names the affected `security_ccy` values and suggests `--fx-rate`.
- The `target_value_resolved` name is kept, but the intermediate column is gone: `_ENCRYPTED_COLUMNS` now maps `target_value -> target_value_resolved`, so decryption produces the reporting column directly and the fallback block is deleted. The three gold builders read `target_value_resolved` unchanged.
- The `default_target_ccy` fallback (first non-null `target_ccy` anywhere, else `"EUR"`) is deleted — it mislabeled native-currency amounts as EUR when `normalize_currency` had not run. It is unreachable now because `_read_cdc_events` guarantees non-null `target_ccy`.
- `cash_flow_breakdown` and `passive_income_timeline` return a flag figure ("target_value is null for some rows ...") when `target_value` has nulls, instead of summing `cash_amount`; `_passive_income_table` renders a warning message instead of a sum.

Alternatives rejected:

- Keep the fallbacks but add a warning: still produces wrong numbers (native amounts labeled as EUR). The corruption is the value itself, not the missing log line.
- Make the chart fail the whole report: the report should still render the unaffected sections (holdings, positions); a per-chart flag preserves that.

## Constraints

- The gold tables keep nullable `target_value` (schema unchanged); the raise enforces non-null at runtime, so no schema migration.
- Charts flag — they do not raise — so a gold table computed by the old analytics code still renders a report with an explicit warning.
- Empty CDC tables stay valid: 0 rows -> no nulls -> no raise.
- The null-`cash_amount` skip in `normalize_currency()` is deleted: it was defensive code (no connector emits null `cash_amount` — IBKR/XTB coerce to 0.0, T212 trades are a product) and it silently wrote exactly the null `target_value` rows this ADR forbids. A row with missing/empty `security_ccy` now raises instead of silently converting at rate 1.0.

## Consequences

- **Positive**: the non-null `target_value`/`target_ccy` invariants are enforced at the boundary where the values are read, so the gold tables can no longer silently contain native-currency amounts labeled EUR.
- **Positive**: a converter outage now fails `normalize-cdc` loudly with an actionable message instead of corrupting the report.
- **Negative**: the normalize step is now fail-fast on currencies without a rate — an unconvertible currency blocks the pipeline until `--fx-rate CURRENCY=RATE` is supplied (consistent with ADR 0077's existing external-API dependency).
- **Negative**: a pre-existing gold table with null `target_value` shows a flag/warning until analytics is re-run against normalized CDC.

## Validation

- `tests/test_cdc_analytics.py`: `test_raises_on_null_target_value_even_with_fx_rate`, `test_raises_on_completely_null_target_value`, and `test_raises_on_missing_target_value_column` assert `RuntimeError`; `test_null_security_ccy_among_null_target_value_rows_raises` asserts `RuntimeError` (not `TypeError`) when a null `security_ccy` sits among null `target_value` rows; `test_xtb_cdc_rows_survive_analytics_end_to_end` runs `normalize_currency(manual_rates={"PLN": 0.25})` before `build_cash_flow_summary`.
- `tests/test_normalize_currency.py`: `test_converter_failure_raises_runtime_error` monkeypatches `CurrencyConverter.fetch_rate` to raise `PortfolioConnectorError` and asserts `normalize_currency` raises `RuntimeError` matching "Could not convert"; `test_missing_security_ccy_raises_runtime_error` asserts `RuntimeError` matching "missing security_ccy" for a null `security_ccy` row (the old `test_null_cash_amount_handled_gracefully` was deleted with the skip).
- `tests/test_charts.py`: `test_flags_on_null_target_value` for both `cash_flow_breakdown` and `passive_income_timeline` asserts a flag figure (title, empty traces, message annotation) instead of a cash_amount sum.
- Full suite: 800 tests pass; `ruff check --fix`, `ruff format`, and `pyright pipeline/ tests/` clean.
