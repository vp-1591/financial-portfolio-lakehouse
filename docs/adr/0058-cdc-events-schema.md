# ADR 0058: Broker-Neutral CDC Events Schema

> **Superseded by [ADR 0077](./0077-currency-unification-phase2-schema-redesign.md)** — ADR 0077 replaced the column definitions (`currency` → `security_ccy`, `net_amount` → removed, `base_currency` → `target_ccy`, `fx_rate_to_base` → `target_fx_rate`, `amount_base` → `target_value`).

## Context

The pipeline had three broker-specific CDC (Change Data Capture) schemas with incompatible column names and semantics:

- `trading212_cdc_normalized_schema`: `event_type`, `event_id`, `ticker`, `isin`, `currency`, `value`, `quantity`, `event_date`
- `xtb_cdc_normalized_schema`: `operation_id`, `operation_type`, `amount`, `currency`, `comment`, `operation_date`
- `ibkr_cdc_normalized_schema`: placeholder stub with `payload`, `source`

These schemas make cross-broker queries impossible — a single cashflow timeline or dividend income report requires per-broker SQL with different column names and types. Additionally, the current CDC implementation is broken: T212's `fetch_cdc()` silently swallows errors and doesn't paginate, and IBKR's CDC raises `NotImplementedError`.

Since current CDC is non-functional, there is no production data to migrate. This makes it the right time to consolidate into a single schema.

## Decision

Replace all three broker-specific CDC schemas with a single `cdc_events_normalized_schema` that all brokers write to. The schema has:

**10 non-nullable core columns** (every CDC row must have these):

| Column | Type | Description |
|--------|------|-------------|
| `fetched_at` | timestamp(UTC) | Pipeline fetch timestamp |
| `broker` | string | Broker name ("Trading 212", "IBKR", "XTB") |
| `account_id` | string | Broker account identifier |
| `event_id` | string | Stable event identifier (broker-native or deterministic hash) |
| `source` | string | Raw endpoint, Flex section, or report sheet |
| `event_type` | string | Normalized type: TRADE, DIVIDEND, DEPOSIT, WITHDRAWAL, FEE, TAX, INTEREST, TRANSFER, ADJUSTMENT, UNKNOWN |
| `raw_event_type` | string | Broker-native type/status for diagnostics and remapping |
| `event_datetime` | string | Event timestamp from broker |
| `currency` | string | Transaction currency |
| `cash_amount` | binary | Fernet-encrypted signed cash impact in native currency |

**14 nullable trade/security columns** (populated when available):

| Column | Type | Notes |
|--------|------|-------|
| `settle_date` | string | IBKR has it; T212/XTB generally don't |
| `ticker` | string | Security identifier |
| `isin` | string | ISIN code |
| `description` | string | Free-text description |
| `quantity` | binary | Fernet-encrypted share/contract quantity |
| `price` | binary | Fernet-encrypted price per unit |
| `side` | string | BUY or SELL |
| `gross_amount` | binary | Fernet-encrypted gross amount |
| `fee_amount` | binary | Fernet-encrypted fees |
| `tax_amount` | binary | Fernet-encrypted taxes |
| `net_amount` | binary | Fernet-encrypted net amount |
| `base_currency` | string | Account base currency |
| `fx_rate_to_base` | binary | Fernet-encrypted FX rate |
| `amount_base` | binary | Fernet-encrypted amount in base currency |

**Cash amount signing convention**: positive = inflow (deposit, dividend received), negative = outflow (withdrawal, fee, tax paid).

**Event ID stability contract**: Prefer broker-native identifiers (`ibExecutionId`, `tradeId`/`transactionId`, `reference`) when available. Fall back to a deterministic SHA-256 hash of (source, account, date, type, amount, description) when no native ID exists.

## Constraints

- No migration of historical CDC data — current CDC tables are broken and contain no production data worth preserving.
- XTB CDC must continue to work end-to-end (upload XLSX → parse → normalized table) with the new schema.
- All monetary float columns that may contain sensitive data must use `pa.binary()` type with Fernet encryption, consistent with the existing `build_normalized_table()` pattern.
- The schema must support all three brokers (T212, IBKR, XTB) without requiring any column to be non-nullable if it isn't available from all sources.

## Consequences

- **Easier cross-broker queries**: A single `SELECT` on `cdc_events` can filter by `event_type` or `currency` across all brokers.
- **Simpler pipeline**: One schema to maintain instead of three.
- **More nullable columns**: Some columns like `settle_date` will be null for most T212 and XTB events, but this is acceptable for a converged schema.
- **Breaking change**: The old `trading212_cdc`, `xtb_cdc`, and `ibkr_cdc` normalized table schemas are deleted. Any dashboard queries referencing old column names (`value`, `quantity`, `operation_id`, `operation_type`, etc.) must be updated to use new column names (`cash_amount`, `quantity`, `event_id`, `event_type`, etc.).
- **Future work**: A `normalized/cdc_events` Delta table will consolidate per-broker CDC normalized data at the silver layer.

## Validation

- All existing connector tests pass with the new schema (updated for `cdc_events_normalized_schema`).
- XTB CDC transform outputs rows matching the new schema.
- T212 CDC transform outputs rows matching the new schema.
- `build_normalized_table()` produces correctly-typed empty tables when no events are present.
- `ruff check` and `ruff format` pass without issues.