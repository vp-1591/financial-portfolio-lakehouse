# Roadmap: Currency Unification — Store in Security Currency, Convert to Target

## Goal

Simplify currency handling so that every silver table stores monetary amounts in
the instrument's trading currency (security currency), and every gold table
adds a target-currency conversion. This eliminates `value_currency`,
`base_currency`, and `fx_rate_to_base` — replacing them with two clear concepts:
`security_ccy` (what unit the amount is in) and `target_ccy` (the reporting
currency, e.g. EUR). It also eliminates all seven currency bugs:

1. **T212 `fx_rate_to_base` misused** — `walletImpact.fxRate` was treated as a wallet→base rate, producing wrong `amount_base` values. It's actually the wallet→security rate (PLN→USD, PLN→GBP, PLN→GBX).
2. **`base_currency` overloaded** — In CDC events it means "account base currency" (USD for IBKR, PLN for T212), but in consolidated holdings it means "reporting target currency" (EUR). Same column, two meanings.
3. **`value_currency` ≠ `security_currency`** — For T212, `value_currency` is the wallet currency (PLN) while `security_currency` is the instrument currency (USD/GBP). For IBKR, they're the same. This inconsistency makes consolidation error-prone.
4. **IBKR `settle_date` format** — IBKR returns `settle_date` as `YYYYMMDD` (compact integer), but the pipeline expects `YYYY-MM-DD`.
5. **Fee/tax currency unspecified** — Fees and taxes are stored without a currency column. For T212 they're in wallet currency (PLN), for IBKR they may be in a different currency than the trade. When `security_ccy` differs from the fee currency, the amounts are in the wrong unit.
6. **T212 dividends/transactions lack conversion** — `walletImpact.fxRate` is only available for orders. Dividends and transactions hardcode `fx_rate_to_base = 1.0`, meaning their `amount_base` equals `cash_amount` (no conversion applied).
7. **IBKR snapshot pre-converts `value`** — IBKR's `fxRateToBase` is applied during the snapshot transform, storing `value` in account base currency (USD) while `value_currency` says the native currency (e.g. GBP). The stored value doesn't match the stated currency.

## Current state

Currency conversion is scattered across three layers with different semantics:

| Layer | Who converts | Rate source | Result |
|-------|-------------|------------|--------|
| T212 CDC transform | Connector | `walletImpact.fxRate` (misused as "to base") | Wrong `amount_base` — the rate is actually wallet→security, not wallet→base (Bug 1) |
| IBKR CDC transform | Connector | `fxRateToBase` (historical, correct) | Correct for trades; null for transfers/fees |
| IBKR snapshot | Connector | `fxRateToBase` (pre-converts value) | `value` is in account base currency but `value_currency` says native currency (Bug 7) |
| Consolidation | `CurrencyConverter` | Live FX rates | Correct for snapshots; not used for CDC |

The column names are overloaded: `base_currency` means account base in CDC but
consolidation target in holdings (Bug 2). `value_currency` means wallet currency
for T212 but trade currency for IBKR (Bug 3). T212 dividends/transactions
hardcode `fx_rate_to_base = 1.0` (Bug 6).

**Key discovery:** `walletImpact.fxRate` is not broken — it's the rate from
wallet currency to the security's trading currency. Verified against demo data:

| Ticker | Security ccy | cash_amount (PLN) | fxRate | cash_amount × fxRate | Expected in security ccy |
|--------|-------------|-------------------|--------|----------------------|--------------------------|
| SPYI   | USD         | 2,000 PLN         | 0.233  | 466 USD               | ≈ 466 USD ✓              |
| VUAG   | GBP         | 5,000 PLN         | 0.199  | 995 GBP               | ≈ 995 GBP ✓              |
| SGLN   | GBX         | 7,500 PLN         | 19.949 | 149,617 GBX           | ≈ 149,617 GBX ✓          |

This rate is only available for T212 orders, not for dividends/transactions.

Relevant ADRs: [ADR 0046](../adr/0046-fix-consolidated-currency-column.md),
[ADR 0058](../adr/0058-cdc-events-schema.md),
[ADR 0065](../adr/0065-cdc-analytics-tables.md),
[ADR 0074](../adr/0074-remove-overloaded-currency-column.md).

Relevant roadmap: [currency-column-clarity](0009-currency-column-clarity.md)
(Phases 1–2 done; removed overloaded `currency` column).

## Success criteria

- [ ] No `value_currency`, `base_currency`, `fx_rate_to_base`, or `net_amount`
      columns exist in any silver or gold schema — replaced by `security_ccy`,
      `target_ccy`, and `target_value` (`net_amount` is simply removed as redundant)
- [ ] Every silver table stores monetary amounts in `security_ccy` (the
      instrument's trading currency for security events, the event's native
      currency for cash events)
- [ ] Every gold table that needs cross-currency aggregation has `target_value`
      (amount converted to `target_ccy`, e.g. EUR) alongside `security_value`
      (amount in `security_ccy`)
- [ ] T212 orders use `walletImpact.fxRate` to convert wallet amounts to
      `security_ccy` amounts — this rate is now correctly understood as
      wallet→security, not wallet→base
- [ ] T212 dividends/transactions use `CurrencyConverter` to convert native
      amounts to `security_ccy` when the dividend currency differs from the
      security's trading currency
- [ ] IBKR snapshots store `value` in native (security) currency, not
      pre-converted to account base currency (Bug 7 fix)
- [ ] IBKR CDC events use `fxRateToBase` as `target_fx_rate` to compute
      `target_value` during normalization
- [ ] All existing tests pass after the refactor
- [ ] `pipeline report --output data/report.html` on demo data produces correct
      totals in EUR across all report sections

## Alternatives considered

| Approach | Why rejected |
|----------|-------------|
| Keep `value_currency` alongside `security_ccy` | Redundant — for security events, they're the same; for cash events, `security_ccy` serves both roles. Two columns for the same concept invites future confusion. |
| Store T212 values in wallet currency (PLN) and convert at analytics time | Conflates security price movement with FX noise. A PLN-denominated SPY position changes when either SPY or PLN/USD moves — the stored value doesn't reflect the true economic exposure. |
| Centralize all FX conversion in one normalization function | Requires every connector to know about `CurrencyConverter` and the target currency. Simpler to let each connector handle the wallet→security conversion (using broker-provided rates where available) and have a single normalization step handle security→target conversion. |
| Drop `target_value` entirely, convert on-the-fly in analytics | Breaks the gold-table contract (pre-computed, ready for reporting). Analytics would need FX rate tables or live API calls at query time. |
| Keep `base_currency` and rename it to always mean "EUR" | Overloaded name carries historical baggage. `target_ccy` clearly communicates "this is the reporting target, not an account base or wallet base." |

## Schema contract

### Silver: CDC events (`cdc_events_normalized`)

| Column | Type | Nullable | Meaning |
|--------|------|----------|---------|
| `security_ccy` | string | no | The currency `cash_amount`, `gross_amount`, `fee_amount`, and `tax_amount` are denominated in. For all event types this is the amount currency — not necessarily the instrument's trading currency (see `instrument_ccy`). |
| `instrument_ccy` | string | yes | The instrument's trading currency, when known (e.g. USD for AAPL, GBX for SGLN.L). Null when unknown or N/A (deposits, fees). For cross-currency dividends, this differs from `security_ccy`: a GBX-denominated stock paying a GBP dividend has `security_ccy=GBP, instrument_ccy=GBX`. |
| `cash_amount` | binary | no | Fernet-encrypted signed cash impact **in `security_ccy`**. For T212 orders: converted from wallet currency using `walletImpact.fxRate`. For T212 dividends/transactions: converted from native currency using `CurrencyConverter` when needed. For IBKR: already in trade currency. |
| `target_fx_rate` | binary | yes | The rate from `security_ccy` to `target_ccy` used to compute `target_value`. Always satisfies `target_value = cash_amount × target_fx_rate`. IBKR: set from `fxRateToBase` at connector stage (historical, trade-date rate). T212: set by `normalize_currency()` via `CurrencyConverter` (the raw `walletImpact.fxRate` is consumed in the connector to compute `cash_amount` in `security_ccy` and then dropped). Set to `1.0` when `security_ccy == target_ccy`. |
| `target_value` | binary | no | Fernet-encrypted `cash_amount` converted to the pipeline target currency (EUR). Computed by the normalization step. |
| `target_ccy` | string | no | Always "EUR" (the pipeline target currency). |
| `fee_amount` | binary | yes | Fernet-encrypted fees. In `security_ccy` (converted if broker reports in a different currency). |
| `tax_amount` | binary | yes | Fernet-encrypted taxes. In `security_ccy` (converted if broker reports in a different currency). |
| (other columns) | — | — | `fetched_at`, `broker`, `account_id`, `event_id`, `source`, `event_type`, `raw_event_type`, `event_datetime`, `settle_date`, `ticker`, `isin`, `description`, `quantity`, `price`, `side`, `gross_amount` — unchanged from current schema |

**Removed:** `value_currency` (replaced by `security_ccy`), `base_currency` (replaced by `target_ccy`), `fx_rate_to_base` (replaced by `target_fx_rate`), `amount_base` (replaced by `target_value`), `net_amount` (always identical to `cash_amount` — redundant column).

### Silver: Broker snapshots

| Column | Type | Meaning |
|--------|------|---------|
| `security_value` | binary | Fernet-encrypted position value **in `security_ccy`**. For T212: converted from wallet currency using `walletImpact.fxRate` (orders) or `CurrencyConverter` (positions without fills). For IBKR: already in trade currency (no pre-conversion). |
| `security_ccy` | string | The currency `security_value` is denominated in. For EQUITY: the instrument's trading currency (USD, GBP, GBX). For CASH: the cash currency (PLN, EUR). Always matches what `security_value` is actually in. |
| (other columns) | — | `fetched_at`, `account_id`, `position_type`, `label`, `isin`, `description`, `name` (T212/XTB), `asset_class` — unchanged |

**Removed:** `value` (renamed to `security_value`), `value_currency` (replaced by `security_ccy`), `security_currency` (merged into `security_ccy` — they were always the same for EQUITY and now CASH uses the same column).

### Gold: Consolidated holdings

| Column | Type | Meaning |
|--------|------|---------|
| `security_value` | binary | Fernet-encrypted. Position value in `security_ccy` (from snapshot, stored directly — no longer re-derived via snapshot join). |
| `security_ccy` | string | Amount currency (from snapshot). |
| `target_value` | binary | Fernet-encrypted. Position value converted to `target_ccy` via `CurrencyConverter`. |
| `target_ccy` | string | Always "EUR". The reporting target currency. |
| `position_type` | string | EQUITY or CASH (from snapshot, stored directly). |
| (other columns) | — | `fetched_at`, `broker`, `ticker`, `identifier`, `description` — unchanged |

**Removed:** `base_currency` (replaced by `target_ccy`), `value` (replaced by `security_value`).

### Gold: Portfolio holdings

| Column | Type | Meaning |
|--------|------|---------|
| `security_value` | float64 | Position value in `security_ccy` (from consolidated holdings, no longer via snapshot join). |
| `security_ccy` | string | Amount currency. |
| `target_value` | float64 | Position value in `target_ccy` (from consolidated holdings). |
| `target_ccy` | string | Always "EUR". |
| `position_type` | string | EQUITY or CASH (from consolidated holdings, no longer via snapshot join). |
| (other columns) | — | `calculated_at`, `broker`, `ticker`, `identifier`, `description` — unchanged |

**Removed:** `value` (renamed to `security_value`), `value_base` (renamed to `target_value`), `value_currency` (replaced by `security_ccy`), `base_currency` (replaced by `target_ccy`).

### Gold: CDC analytics (`dividend_income`, `interest_income`, `cash_flow_summary`)

| Column | Type | Meaning |
|--------|------|---------|
| `security_ccy` | string | The currency `cash_amount` is denominated in. |
| `instrument_ccy` | string | The instrument's trading currency (nullable, dividends only). When it differs from `security_ccy`, the dividend was paid in a different currency than the instrument trades in. |
| `cash_amount` | float64 | Sum of raw amounts **in `security_ccy`**. |
| `target_value` | float64 | Sum converted to `target_ccy`. No longer nullable. |
| `target_ccy` | string | Always "EUR". No longer nullable. |
| (other columns) | — | `calculated_at`, `period_month`, `period_quarter`, `broker`, `event_type` (cash flow), `ticker`, `isin`, `description` (dividends), `event_count` — unchanged |

**Removed:** `value_currency` (replaced by `security_ccy`), `base_currency` (replaced by `target_ccy`), `amount_base` (replaced by `target_value`), `net_amount` (redundant with `cash_amount`).

## Phases

### Phase 1 — Fix T212 `walletImpact.fxRate` usage *[status: done]*

Reclassify `walletImpact.fxRate` from "broken" to "wallet→security rate" and
use it correctly. This immediately fixes Bug 1 and Bug 6 for T212 orders.

**Scope:**
- [x] In `_transform_orders()`: rename `fx_rate_to_base` to `wallet_fx_rate`
      and document it as the rate from wallet currency to security trading
      currency. Use it to compute `cash_amount` in `security_ccy`:
      `cash_amount_security_ccy = net_value_wallet_ccy × wallet_fx_rate`.
- [x] In `_transform_orders()`: set `security_ccy` from the instrument's
      trading currency (e.g., `instrument.currencyCode`). Set `cash_amount`
      to the converted value in `security_ccy`.
- [x] In `_transform_dividends()` and `_transform_transactions()`:
      `walletImpact.fxRate` is not available. For dividends, use the
      dividend's `currency` field as `security_ccy` (dividends are often
      paid in the security's currency). For transactions (deposits, fees),
      use the transaction's native currency as `security_ccy`. Log a warning
      if the dividend currency differs from the security's trading currency.
- [x] Add/update tests verifying that T212 orders produce `cash_amount` in
      `security_ccy` using `walletImpact.fxRate`.
- [x] Verify that `pipeline report` on demo data still renders correctly
      (Phase 1 only changes the T212 transform internals; downstream tables
      still use the old column names until Phase 2).

**Out of scope:**
- Schema changes (Phase 2).
- IBKR changes (Phase 2).
- `target_value` computation (Phase 2).

**Files:** `pipeline/connectors/trading212/transform.py`,
`tests/test_trading212_connector.py`

**Bugs addressed:** Bug 1 (T212 `fx_rate_to_base` misused — now correctly used as wallet→security rate), Bug 6 (T212 dividends/transactions hardcode `fx_rate_to_base = 1.0` — orders fixed; dividends/transactions deferred to Phase 2).

---

### Phase 2 — Schema redesign: `security_ccy` + `target_value` *[status: done]*

Replace `value_currency`, `base_currency`, `fx_rate_to_base`, `amount_base`,
and `value`/`value_base` with the new schema: `security_ccy`, `security_value`,
`target_value`, `target_ccy`. Stop IBKR snapshots from pre-converting `value`.
Wire `target_value` computation into the normalization step.

**Scope:**

#### 2a. Update silver schemas in `pipeline/normalized/models.py`

- [x] **CDC events**: Remove `value_currency`, `base_currency`,
      `fx_rate_to_base`, `amount_base`, `net_amount` (`net_amount` is always
      identical to `cash_amount` — redundant). Add `security_ccy` (string,
      non-null), `target_fx_rate` (binary, nullable), `target_value` (binary,
      non-null), `target_ccy` (string, non-null). Rename `fee_amount` and
      `tax_amount` semantics: they are now in `security_ccy`.
- [x] **Snapshot schemas** (IBKR, T212, XTB): Remove `value`,
      `value_currency`, `security_currency`. Add `security_value` (binary),
      `security_ccy` (string).

#### 2b. Update T212 connector transform

- [x] `_transform_orders()`: Use `walletImpact.fxRate` as wallet→security
      rate. Convert `net_value` from wallet currency to `security_ccy`. Set
      `security_ccy` from instrument trading currency. The raw
      `walletImpact.fxRate` is consumed for this conversion and then
      dropped — it is not stored in the output schema.
- [x] `_transform_dividends()`: Set `security_ccy` from dividend currency.
      **Deviation:** Rather than using `CurrencyConverter` directly in the
      transform, FX conversion is deferred to `normalize_currency()` in the
      normalization step. `security_ccy` is set to
      `pl.coalesce([pl.col('currency'), pl.col('tickerCurrency')])`,
      preferring the dividend's payment currency (the amount currency).
      `instrument_ccy` is set to `tickerCurrency` (the instrument's trading
      currency), capturing the distinction between payout and trading currency
      for cross-currency dividends.
- [x] `_transform_transactions()`: Set `security_ccy` from transaction
      currency (deposits/fees have no security, so this is the native
      currency).
- [x] `_transform_snapshot()`: Convert position values from wallet currency
      to `security_ccy` using `walletImpact.fxRate` for filled positions and
      `CurrencyConverter` for unfilled positions. Set `security_ccy` from
      `position_security_currency()`.
- [x] Remove all `fx_rate_to_base`, `base_currency`, `amount_base`,
      `value_currency`, `security_currency`, `net_amount` logic from T212
      transforms.

#### 2c. Update IBKR connector transform

- [x] `_process_ibkr_trade()`, `_process_ibkr_cash_transaction()`,
      `_process_ibkr_transfer()`, `_process_ibkr_transaction_fee()`: Set
      `security_ccy` from trade/transaction currency (the security's trading
      currency). Pass `fxRateToBase` through to the normalization step as
      `target_fx_rate` (it converts security ccy → account base ccy; when
      account base equals target ccy, this is the correct rate). Remove
      `amount_base`, `base_currency`, `value_currency` computation.
- [x] `transform_snapshot()`: **Stop pre-converting `value` to account base
      currency.** Store `security_value` in the trade currency (the security's
      native currency). Set `security_ccy` from the position's currency.
      Remove `base_value` and `fx_rate` logic.
- [x] Remove all `fx_rate_to_base`, `base_currency`, `amount_base`,
      `value_currency`, `security_currency`, `net_amount` logic from IBKR
      transforms.

#### 2d. Create `normalize_currency()` in `pipeline/normalized/`

- [x] New function that takes a list of CDC event dicts (with `cash_amount` in
      `security_ccy` and optionally a pre-set `target_fx_rate` from IBKR) and
      a `CurrencyConverter`, and computes `target_value`, `target_fx_rate`,
      and `target_ccy` for each event:
      - If `security_ccy == target_ccy`: `target_value = cash_amount`,
        `target_fx_rate = 1.0`.
      - If `target_fx_rate` is already set (IBKR provided `fxRateToBase`):
        verify it converts security ccy → target ccy. If account base ≠
        target ccy, fall back to `CurrencyConverter` and override.
        `target_value = cash_amount × target_fx_rate`.
      - Otherwise (T212): `target_fx_rate = converter.get_rate(security_ccy, target_ccy)`,
        `target_value = cash_amount × target_fx_rate`.
- [x] Same for snapshots: `target_fx_rate = converter.get_rate(security_ccy, target_ccy)`,
      `target_value = security_value × target_fx_rate`.

#### 2e. Update gold schemas and builders

- [x] **`consolidated_holdings`**: Remove `base_currency`, `value`. Add
      `security_ccy`, `target_value` (in `target_ccy`), `target_ccy`.
      `security_value` and `position_type` are now stored in
      `consolidated_holdings` (previously re-derived from snapshots).
- [x] **`portfolio_holdings`**: Remove `value`, `value_base`, `value_currency`,
      `base_currency`. Add `security_value`, `security_ccy`, `target_value`,
      `target_ccy`.
- [x] **`dividend_income`, `interest_income`, `cash_flow_summary`**: Remove
      `value_currency`, `base_currency`, `amount_base`. Add `security_ccy`,
      `target_value`, `target_ccy`. `cash_amount` is in `security_ccy`.
- [x] **`portfolio_allocation`**: Remove `security_currency` (replace with
      `security_ccy` if still needed for the currency exposure chart).

#### 2f. Update downstream consumers

- [x] **`pipeline/analytics/holdings.py`**: Update column references from
      `value`/`value_base`/`value_currency`/`base_currency`/`security_currency`
      to `security_value`/`target_value`/`security_ccy`/`target_ccy`.
- [x] **`pipeline/analytics/cdc_tables.py`**: Update column references from
      `value_currency`/`base_currency`/`amount_base`/`amount_base_resolved`
      to `security_ccy`/`target_ccy`/`target_value`. Remove the
      `amount_base_resolved` fallback logic. Remove `net_amount` from the
      encrypted column list and all decryption/decrypted references.
- [x] **`pipeline/report/charts.py`**: Update column references. Currency
      exposure chart groups by `security_ccy` instead of `security_currency`.
- [x] **`pipeline/report/renderer.py`**: Update `_summary_table()` to use
      `target_value` and `target_ccy` instead of `value_base` and
      `base_currency`.
- [x] **`pipeline/analytics/quality.py`**: Update column references.

#### 2g. Update tests

- [x] Update all connector tests for new column names and semantics.
- [x] Add tests for `normalize_currency()`: T212 events with wallet→security
      conversion, IBKR events with broker-provided rate, same-currency events.
- [x] Verify `pipeline report` on demo data produces correct EUR totals.

**Out of scope:**
- Historical FX rate service for T212 dividends/transactions (acknowledged
  limitation: T212 orders use `walletImpact.fxRate` which is historical,
  but T212 dividends/transactions use `CurrencyConverter` live rates).
- Fee/tax cross-currency conversion (Phase 3).
- IBKR `settle_date` format fix (Phase 3).
- XTB connector changes (XTB CDC stays on its own schema until the CDC
  Tables roadmap is implemented).

**Files:** `pipeline/normalized/models.py`,
`pipeline/normalized/cdc_normalize.py` (new),
`pipeline/normalized/consolidate.py`,
`pipeline/connectors/trading212/transform.py`,
`pipeline/connectors/trading212/client.py`,
`pipeline/connectors/ibkr/transform.py`,
`pipeline/analytics/models.py`, `pipeline/analytics/holdings.py`,
`pipeline/analytics/cdc_tables.py`, `pipeline/report/charts.py`,
`pipeline/report/renderer.py`, `pipeline/analytics/quality.py`, `tests/`

**Bugs addressed:** Bug 2 (`base_currency` overloaded → replaced by `target_ccy`), Bug 3 (`value_currency` ≠ `security_currency` → merged into `security_ccy`), Bug 6 remainder (T212 dividends/transactions conversion → `CurrencyConverter`), Bug 7 (IBKR snapshot pre-converts `value` → stop pre-conversion).

---

### Phase 3 — Fix remaining data quality bugs *[status: done]*

Fix the remaining currency-related bugs that don't require schema changes:
IBKR `settle_date` format, fee/tax currency, and cross-currency fee handling.

**Scope:**
- [x] Normalize IBKR `settle_date` from `YYYYMMDD` to `YYYY-MM-DD` (Bug 4).
- [x] Convert fee/tax amounts to `security_ccy` in both connectors. For T212
      orders: use `walletImpact.fxRate` to convert wallet-currency fees to
      security currency. For IBKR: use `trade.currency` as `security_ccy` and
      convert fees from `ibCommissionCurrency` using `fxRateToBase` when they
      differ. Log a warning for cross-currency fees that can't be converted.
- [x] Write an ADR documenting the `security_ccy`/`target_ccy` schema
      contract, the `walletImpact.fxRate` semantics, and the normalization
      architecture.
- [x] Run full test suite and `pipeline report` on demo data.

**Out of scope:**
- Cross-currency fee conversion when no rate is available (log warning only).
- XTB connector changes.

**Files:** `pipeline/connectors/ibkr/transform.py`,
`pipeline/connectors/trading212/transform.py`,
`pipeline/normalized/models.py`, `docs/adr/`

**Bugs addressed:** Bug 4 (IBKR `settle_date` format — normalize `YYYYMMDD` to `YYYY-MM-DD`), Bug 5 (fee/tax currency unspecified — convert fees/taxes to `security_ccy`).

---

### Phase 4 — Verify end-to-end correctness *[status: planned]*

End-to-end verification that the currency unification is correct across all
pipeline layers and the report renders accurately.

**Scope:**
- [ ] Run the full pipeline on demo data and verify:
  - [ ] T212 `cash_amount` is in `security_ccy` (wallet amounts converted using
        `walletImpact.fxRate` for orders, `CurrencyConverter` for dividends)
  - [ ] IBKR `cash_amount` is in `security_ccy` (already in trade currency)
  - [ ] `target_value` is correct in EUR for all brokers
  - [ ] `target_ccy` is "EUR" for all rows across all tables
  - [ ] `security_ccy` reflects the security's trading currency (not the
        wallet currency) for all security events
  - [ ] Snapshot `security_value` is in native (security) currency for all
        brokers (IBKR no longer pre-converts)
  - [ ] Dividend income, interest income, and cash flow summary aggregate
        `target_value` correctly across brokers
  - [ ] Report shows correct EUR totals in all sections
- [ ] Add integration test verifying `target_ccy` is "EUR" across all output
      tables.
- [ ] Add data quality check: warn if `target_value` differs from
      `security_value` by more than a configurable threshold (catches FX rate
      anomalies without failing the pipeline).

**Out of scope:**
- Adding new report sections or charts.
- Performance optimization.

**Files:** `tests/`, `pipeline/analytics/quality.py`