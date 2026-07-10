# Roadmap: CDC Tables

## Goal

Deliver reliable, complete change-data-capture (CDC) tables for Trading 212 and IBKR,
converging both brokers into a shared broker-neutral silver CDC schema so that the
dashboard can report cashflow timelines, dividend income, and trade activity across
all accounts from a single table.

## Current state

### Trading 212 CDC — partially wired but brittle

- `fetch_cdc()` calls the orders, dividends, and transactions endpoints but silently
  skips any endpoint that raises an exception ([`pipeline/connectors/trading212/fetch.py:113`](../../pipeline/connectors/trading212/fetch.py)).
- The client methods expect each history endpoint to return a bare JSON list. The
  vendored API documentation describes paginated responses with `items` and
  `nextPagePath`, so valid paginated responses can be rejected as "unexpected" and
  then swallowed by `fetch_cdc()`.
- Pagination is not implemented — even a compatible first page would not collect full
  history.
- The T212 CDC silver schema (`trading212_cdc_normalized_schema`) uses broker-specific
  column names (`event_type`, `event_id`, `event_date`, `value`, `quantity`) that
  differ from the XTB CDC schema (`operation_id`, `operation_type`, `operation_date`,
  `amount`, `comment`).

### IBKR CDC — not implemented

- The IBKR connector raises `NotImplementedError` for both `fetch_cdc()` and
  `transform_cdc()` ([`pipeline/connectors/ibkr/fetch.py:60`](../../pipeline/connectors/ibkr/fetch.py),
  [`pipeline/connectors/ibkr/transform.py:232`](../../pipeline/connectors/ibkr/transform.py)).
- The IBKR CDC normalized schema (`ibkr_cdc_normalized_schema`) is a placeholder stub
  with only `fetched_at`, `account_id`, `payload`, and `source`.

### XTB CDC — working, out of scope

- XTB CDC works end-to-end (upload XLSX → parse cash operations → normalized table).
  It is context for the silver schema design but no changes are planned.

### Related ADRs

- [ADR 0003](../adr/0003-medallion-architecture.md) — Medallion architecture pipeline
  (defines the bronze/silver/raw layers and `BrokerConnector` protocol with `fetch_cdc`
  and `transform_cdc` methods).
- [ADR 0057](../adr/0057-transform-dedup-and-cash-fixes.md) — Bronze→silver dedup
  and cash extraction fixes (established that CDC transforms are *not* filtered by
  `fetched_at` because CDC rows are chronological events, not snapshots).

## Success criteria

- [ ] Trading 212 `fetch_cdc()` follows `nextPagePath` pagination until no more pages
  exist; a test with a multi-page mock response collects all rows.
- [ ] Trading 212 `fetch_cdc()` logs endpoint-level diagnostics (missing scopes, HTTP
  errors) instead of silently skipping failures; a test confirms a failing endpoint
  produces a visible log/warning, not an empty result.
- [ ] When a CDC fetch returns zero rows, both raw and normalized Delta tables are
  created with the correct schema and zero rows (not skipped).
- [ ] IBKR `fetch_cdc()` retrieves activity data via the Flex Web Service and writes
  raw rows to `raw/ibkr_cdc`; a test with a Flex XML fixture produces non-empty raw
  output.
- [ ] IBKR `transform_cdc()` parses Trade, CashTransaction, Transfer, and
  TransactionFee elements from Flex XML into the broker-neutral silver CDC schema; a
  test with a multi-section XML fixture produces rows for each event type.
- [ ] A single `normalized/cdc_events` Delta table exists with the broker-neutral silver
  schema and contains rows from both T212 and IBKR transforms.
- [ ] The broker-neutral silver schema includes all non-nullable core columns
  (`fetched_at`, `broker`, `account_id`, `event_id`, `source`, `event_type`,
  `raw_event_type`, `event_datetime`, `currency`, `cash_amount`) and nullable
  trade/security columns.
- [ ] All existing connector tests pass without modification (except T212 CDC tests
  updated for the new silver schema).

## Alternatives considered

| Approach | Why rejected |
|----------|-------------|
| Per-broker silver CDC tables (status quo) | Each broker has different column names and semantics, making cross-broker queries require per-broker SQL. A shared schema enables a single `SELECT` for cashflow, dividends, and trades across all accounts. |
| Separate Flex query ID for CDC (`IBKR_FLEX_CDC_QUERY_ID`) | Adds another environment variable and requires the user to maintain two Flex queries. Prefer extending the existing query to include activity sections alongside snapshot sections, or accepting a separate ID only as a future option. |
| One raw Delta table per Flex section | Violates the bronze principle that raw tables are source-level captures. Section-specific parsing belongs in the silver transform, not in raw storage. |
| Derive event IDs from payload hash only | Hash-only IDs are brittle — re-fetching the same event produces a different ID if the payload includes a timestamp or sequence number. Prefer deterministic IDs from broker-native identifiers (`Trade ID`, `Transaction ID`) with a hash fallback only when no native ID exists. |

## Phases

### Phase 1 — Fix Trading 212 CDC pagination and error handling *[status: planned]*

Make `fetch_cdc()` reliable: implement pagination, surface endpoint failures, and
ensure zero-row fetches create empty Delta tables with the expected schema.

**Scope:**

- [ ] Implement pagination in T212 `fetch_cdc()`: follow `nextPagePath` until empty.
  Normalize captured page payloads so `transform_cdc()` consistently sees event
  lists regardless of pagination structure.
- [ ] Replace broad `except Exception: continue` in `fetch_cdc()` with endpoint-level
  diagnostics. Missing scopes should be visible (log/warning), not indistinguishable
  from "no history".
- [ ] Make `ingest_raw()` and `transform_cdc()` write Delta tables with the correct
  schema even when there are zero rows, instead of returning before writing.
- [ ] Add tests for: paginated multi-page fetch, endpoint failure visibility,
  zero-row table creation.

**Out of scope:**

- Changes to the T212 CDC silver schema (that's Phase 4).
- IBKR connector changes.
- XTB connector changes.

**Files:**

- `pipeline/connectors/trading212/fetch.py`
- `pipeline/connectors/trading212/transform.py`
- `pipeline/connectors/trading212/client.py`
- `pipeline/raw/` (ingestion logic)
- `tests/test_trading212_connector.py`

**Links:** [ADR 0003](../adr/0003-medallion-architecture.md), [ADR 0057](../adr/0057-transform-dedup-and-cash-fixes.md)

---

### Phase 2 — Implement IBKR CDC bronze (Flex fetch and raw ingest) *[status: planned]*

Wire the IBKR connector's `fetch_cdc()` to retrieve activity data via the Flex Web
Service and store the full XML response in the `raw/ibkr_cdc` Delta table, encrypted
and source-tagged. Getting real Flex XML into bronze first means the schema design
(Phase 3) can be validated against actual data rather than designed blind.

**Scope:**

- [ ] Implement `fetch_cdc()` in `pipeline/connectors/ibkr/fetch.py` using
  `IbkrFlexClient`. Either require a second `IBKR_FLEX_CDC_QUERY_ID` environment
  variable, or document that the existing Flex query must include activity sections
  (Trades, CashTransactions, Transfers, TransactionFees) alongside snapshot sections.
- [ ] Write the full Flex XML response to the single `raw/ibkr_cdc` table, encrypted
  in the same raw schema used by snapshots. Set `source` to the Flex query/report
  identity; keep section detail inside the raw payload.
- [ ] Update `IbkrConnector.fetch_cdc_kwargs()` to pass the CDC Flex query ID.
- [ ] Add test fixture: Flex XML with one trade, one cash transaction, one transfer,
  and one transaction fee. Verify raw ingestion produces non-empty output.

**Minimal IBKR Flex query sections for CDC:**

Keep the existing snapshot Flex query sections (Account Information, Open Positions,
Cash Report, Currency Conversion Rate). Add CDC-capable activity sections:

- **Trades**: security buy/sell activity.
- **Cash Transactions**: dividends, payment in lieu of dividends, withholding tax,
  deposits and withdrawals, interest, broker fees, other fees/income, price
  adjustments, and commission adjustments.
- **Transfers**: security or cash transfers between accounts/brokers.
- **Transaction Fees**: tax/fee detail tied to trades when the values in Trades and
  Cash Transactions are not enough.
- **Currency Conversion Rate**: From Currency, Rate (already in snapshot query).

Event identity rules for IBKR:

- Prefer `IB Execution ID` for execution-level trade rows when available.
- Otherwise use `Trade ID` or `Transaction ID` plus source section.
- If neither is available for a cash/transfer event, derive the event ID from source
  section, account, date, description, currency, amount/quantity, and payload hash.

**Out of scope:**

- Silver transform (that's Phase 4).
- Changes to the snapshot Flex query or snapshot transform.
- T212 or XTB connector changes.

**Files:**

- `pipeline/connectors/ibkr/fetch.py`
- `pipeline/connectors/ibkr/connector.py`
- `pipeline/run.py` (CLI args for CDC Flex query)
- `tests/test_ibkr_connector.py`

**Links:** [ADR 0013](../adr/0013-add-ibkr-flex-connector-to-pipeline.md)

---

### Phase 3 — Design broker-neutral silver CDC schema *[status: planned]*

Define a single silver CDC schema that can represent events from T212, IBKR, and XTB.
Now that real IBKR Flex XML is flowing through bronze (Phase 2), the schema can be
validated against actual data from all three brokers. The output is a schema decision
(columns, types, nullability) documented in an ADR and implemented as a PyArrow
schema constant.

**Scope:**

- [ ] Define `cdc_events_normalized_schema` in `pipeline/normalized/models.py` with
  the non-nullable core columns and nullable trade/security columns from the column
  table below.
- [ ] Document the schema decision in an ADR, including the rationale for each
  column's nullability and which broker populates it.
- [ ] Validate the schema against real data from each broker (T212 orders/dividends,
  XTB cash operations, IBKR Flex XML sections fetched in Phase 2) to confirm
  coverage.

**Out of scope:**

- Migrating existing broker-specific CDC tables to the new schema (that's Phase 4).
- Writing transform code for any broker.
- Dashboard query changes.

**Recommended silver columns:**

| Column | Required? | Broker availability |
|--------|-----------|---------------------|
| `fetched_at` | yes | All raw rows have it. |
| `broker` | yes | All raw rows have it. |
| `account_id` | yes | All connectors provide or can default it. |
| `event_id` | yes | Broker ID when available; otherwise deterministic hash from source/account/date/type/amount/description/payload. |
| `source` | yes | Raw endpoint, Flex section, or report sheet. |
| `event_type` | yes | Normalized value such as `TRADE`, `DIVIDEND`, `DEPOSIT`, `WITHDRAWAL`, `FEE`, `TAX`, `INTEREST`, `TRANSFER`, `ADJUSTMENT`, `UNKNOWN`. |
| `raw_event_type` | yes | Broker-native type/status/category for diagnostics and future remapping. |
| `event_datetime` | yes | T212 has order/dividend/transaction timestamps, XTB has operation date, IBKR has `Date/Time` or `Date`. |
| `settle_date` | no | Available in IBKR; generally not available in current T212/XTB sources. |
| `currency` | yes | All event sources expose transaction/account currency. |
| `cash_amount` | yes | Signed native-currency cash impact. For trade rows this is net cash if available; for pure security transfers it may be zero/null. |
| `ticker` | no | Available for T212 orders/dividends, IBKR security events, and XTB stock cash operations via `Symbol`; null for non-security cash operations. |
| `isin` | no | Available for T212 instrument/dividend data and IBKR when selected; not generally available for XTB cash operations. |
| `description` | no | T212 may need synthesized text, XTB has comment, IBKR has description. |
| `quantity` | no | Available for T212 orders/dividends and IBKR trades/transfers; derivable for XTB stock operations from comments like `OPEN BUY 0.0196 @ 501.00`; null for generic cash operations. |
| `price` | no | Available for T212 order fills and IBKR trades; derivable for XTB stock operations from comments like `OPEN BUY 0.0196 @ 501.00`; null for non-trade cash operations. |
| `side` | no | Available for T212 orders and IBKR trades; derivable for XTB stock operations from `Stock purchase`/`Stock sale` or `BUY`/`SELL` comment text; null otherwise. |
| `gross_amount` | no | Available or derivable for some trades/dividends; not guaranteed across brokers. |
| `fee_amount` | no | Explicit in IBKR trades/fee sections; T212 public history may not expose detailed fees; XTB may encode fees as separate cash operations. |
| `tax_amount` | no | Explicit in IBKR and T212 order/dividend detail when present; not guaranteed for XTB. |
| `net_amount` | no | Useful alias for cash impact when the broker distinguishes gross/fees/taxes; can equal `cash_amount` when no breakdown exists. |
| `base_currency` | no | Useful for dashboard totals, but not consistently provided by CDC sources. |
| `fx_rate_to_base` | no | IBKR has it; T212 orders expose fill wallet FX for some events; XTB report CDC does not. |
| `amount_base` | no | Derive later from FX service or broker FX fields; do not require it for converged CDC ingestion. |

The non-nullable dashboard core:

`fetched_at`, `broker`, `account_id`, `event_id`, `source`, `event_type`,
`raw_event_type`, `event_datetime`, `currency`, `cash_amount`

Everything security-specific or trade-specific should be nullable in the shared schema.
XTB stock cash operations can populate the trade/security columns, but generic XTB
cash operations such as deposits, withdrawals, fees, or interest should not be forced
into fake trade values.

**Files:**

- `pipeline/normalized/models.py`
- `docs/adr/` (new ADR for the schema decision)

**Links:** [ADR 0003](../adr/0003-medallion-architecture.md)

---

### Phase 4 — Implement CDC silver transform (IBKR and T212 into broker-neutral schema) *[status: planned]*

Parse IBKR Flex XML activity sections and T212 CDC JSON into the broker-neutral
`normalized/cdc_events` table defined in Phase 3. This phase replaces the current
per-broker CDC schemas with the shared schema.

**Scope:**

- [ ] Implement IBKR CDC transform: add parsers for Trade, CashTransaction, Transfer,
  and TransactionFee XML elements in `pipeline/connectors/ibkr/transform.py`.
  Map each section to the broker-neutral silver schema with normalized `event_type`
  and `raw_event_type`, stable `event_id`, signed `cash_amount`, and optional
  trade/security columns. Encrypt numeric value columns consistently with other
  normalized tables.
- [ ] Refactor T212 CDC transform to output the broker-neutral silver schema instead
  of `trading212_cdc_normalized_schema`. Map T212 event types (`ORDER`, `DIVIDEND`,
  `TRANSACTION`) to normalized types (`TRADE`, `DIVIDEND`, etc.).
- [ ] Create `normalized/cdc_events` Delta table using the schema from Phase 3.
- [ ] Add tests for: IBKR CDC transform with Flex XML fixture, T212 CDC transform
  with JSON fixture, deduplication by stable `event_id`, and zero-row table creation.

**Out of scope:**

- Migrating existing historical CDC data in the old per-broker tables (document as
  follow-up).
- XTB CDC transform migration (XTB stays on its own schema until a future roadmap).
- Dashboard query changes.

**Operational notes:**

- Prefer a daily Activity Flex Query window for IBKR CDC.
- Use append mode for raw CDC and merge semantics for normalized CDC based on the
  `event_id` contract.

**Reports enabled by the converged silver CDC table:**

All-broker (core columns only):

- Cashflow timeline by broker/account/currency
- Monthly deposits and withdrawals
- Monthly fees, taxes, interest, dividends, and other cash events
- Net external contributions versus portfolio snapshot value
- Activity audit table with source, raw type, event date, amount, currency
- Broker/account reconciliation: CDC cash movement totals by period vs. snapshot
  cash deltas

When optional columns are populated:

- Trade blotter (needs `ticker`, `quantity`, `price`, `side`)
- Dividend income by instrument (needs `ticker` or `isin`)
- Fees/taxes by instrument or order (needs optional fee/tax and security fields)
- Realized trading activity by security (needs trade-side fields plus cost-basis logic)
- Multi-currency performance (needs `amount_base` or a separate FX normalization step)

**Files:**

- `pipeline/connectors/ibkr/transform.py`
- `pipeline/connectors/trading212/transform.py`
- `pipeline/normalized/models.py`
- `pipeline/normalized/` (new CDC consolidation logic)
- `tests/test_ibkr_connector.py`
- `tests/test_trading212_connector.py`

**Links:** [ADR 0003](../adr/0003-medallion-architecture.md), [ADR 0045](../adr/0045-replace-list-append-with-polars-build-normalized-table.md)