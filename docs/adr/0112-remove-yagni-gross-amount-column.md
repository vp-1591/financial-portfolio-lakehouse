# 0112: Remove YAGNI gross_amount Column from CDC Events Schema

> **Supersedes ADR 0104** — the T212 trade `gross_amount` sign convention is removed together with the column; the `cash_amount` sign convention is retained unchanged (see §Decision). Also supersedes the `gross_amount` clauses of ADR 0078 (the identity `cash_amount ≈ gross_amount − fee_amount − tax_amount` and the Bug 5 Part A `gross_amount` wallet→security conversion).

## Context

`gross_amount` is a first-class column of the broker-neutral `cdc_events_normalized_schema`, originally defined in ADR 0058 (carried forward unchanged into the active schema ADR 0077) and Fernet-encrypted as `pa.binary()` at rest. It is populated by all three brokers:

- **IBKR** — `_process_ibkr_trade` stores gross trade proceeds (`trade["proceeds"]`, signed).
- **Trading 212** — orders store `(filledValue|value) * fx_rate * direction` (sign per ADR 0104), dividends store `price * qty`, transactions store `0.0`.
- **XTB** — `sale_value − purchase_value` from Closed Positions (guard D2/D11 in the XTB overhaul plan).

Every analytics build decrypts it: `_ENCRYPTED_COLUMNS` in `pipeline/analytics/cdc_tables.py` maps `gross_amount` → `gross_amount_decrypted` on each read of `cdc_events`. **No consumer ever selects, sums, or groups it.** Gold aggregations sum only `cash_amount` and `target_value`; `pipeline/report/` has zero references to `gross_amount`; the Cash Flow Breakdown chart reads `target_value` (fallback `cash_amount`). The gross side of a trade (before fees/tax) has no downstream user, and no gross-value/turnover metric exists to justify it. It is a YAGNI violation: schema surface and per-run decrypt cost with zero current consumer.

## Decision

Remove `gross_amount` from the CDC events schema and all producers/consumers:

- Delete the field from `cdc_events_normalized_schema` in `pipeline/normalized/models.py` and its `security_ccy` docstring mention.
- Remove population and `encrypt_columns` entries in all three connectors — `ibkr/transform.py`, `trading212/transform.py` (orders, dividends, transactions), `xtb/transform.py` (inline `encrypt_columns` + `_build_cdc_record`).
- Remove the now-unused XTB `XtbClosedPosition.purchase_value`/`sale_value` parser fields (their only consumer was the `gross_amount` computation).
- Remove the `("gross_amount", "gross_amount_decrypted")` entry from `_ENCRYPTED_COLUMNS` in `pipeline/analytics/cdc_tables.py`.
- Add a schema migration `pipeline/migrations/migrate_cdc_events_drop_gross_amount.py` that rewrites the four Delta tables `quality.py` validates against the schema (`cdc_events`, `ibkr_cdc`, `trading212_cdc`, `xtb_cdc`) to drop the column, so the pre-deploy `check_schema` does not flag an extra field.

**Retained unchanged (originally decided in ADR 0104, §Decision; origin ADR 0058):** the `cash_amount` sign convention — positive = inflow, negative = outflow; T212 trades are signed by `side` (BUY negative, SELL positive) so they match IBKR's signed `netCash`. The T212 `direction` expression stays for `cash_amount`. ADR 0078's remaining decisions (`settle_date` normalization, IBKR cross-currency fee conversion, T212 `fee_amount`/`tax_amount` wallet→security conversion) also remain in force.

**Alternative considered and rejected:** keep the column and add the missing consumer (a gross turnover / trade-volume gold metric). Rejected because it builds new analytics purely to justify a column nothing currently needs. Revisit only if turnover analytics are actually requested.

## Constraints

- The on-disk CDC tables must keep matching `cdc_events_normalized_schema` (order-sensitive `schema.equals` used by `quality.check_schema`).
- `fee_amount`, `tax_amount`, `cash_amount` remain encrypted `pa.binary()` in `security_ccy` — the fee/tax identity holds without the gross term.
- The migration must be run BEFORE deploying the schema-change code, so `pipeline validate` does not report an "extra field: gross_amount" FAIL.
- No new analytics are added; the removal is purely subtractive.

## Consequences

- **Positive**: less schema surface, fewer columns decrypted per analytics build, simpler T212 orders expression, no dead XTB parser fields.
- **Negative**: the gross side of a trade (gross ≈ net + fee + tax) is no longer recorded; a future gross-value/trade-volume metric must reintroduce a column or field.
- **Negative**: existing on-disk `cdc_events` and per-broker `*_cdc` tables carry a stale extra column until the migration runs — deploy ordering is a coordination requirement.
- ADR-0104 is fully superseded (its `gross_amount` half removed, `cash_amount` half carried forward). ADR-0078 remains active except its `gross_amount` clauses.

## Validation

- `tests/test_migrate_cdc_events_drop_gross_amount.py` exercises the migration: migrate / idempotent / absent-table / dry-run / unexpected-column `RuntimeError` / non-`TableNotFoundError` propagation.
- `tests/test_trading212_connector.py` retains the `cash_amount` sign-convention assertions (ADR-0104 §Validation) and the three `result.schema == cdc_events_normalized_schema` checks; `gross_amount` assertions removed.
- `tests/test_xtb_connector.py` retains `fee_amount`/`settle_date` enrichment assertions on sell rows; `gross_amount` and `purchase_value`/`sale_value` assertions removed.
- Full suite: **804 tests pass**; `ruff check` / `ruff format` and `pyright` clean.
- Manual: in staging, run `python -m pipeline.migrations.migrate_cdc_events_drop_gross_amount --mode staging --dry-run`, then without `--dry-run`, before deploying the schema-change code; `pipeline validate` then passes with no extra-field schema.
