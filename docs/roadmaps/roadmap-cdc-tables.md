# CDC Tables Roadmap

## Current problems

### Trading 212

Trading 212 CDC is partially wired but functionally brittle:

- `fetch_cdc()` calls orders, dividends, and transactions endpoints, but silently
  skips any endpoint that raises.
- The client methods expect each history endpoint to return a bare JSON list.
  The vendored API documentation describes paginated responses with `items` and
  `nextPagePath`, so valid responses can be rejected as "unexpected" and then
  swallowed by `fetch_cdc()`.
- Pagination is not implemented, so even a compatible first page would not
  collect full history.
- Empty raw CDC results do not create empty Delta tables because `ingest_raw()`
  returns before writing when there are zero rows. The transform step also skips
  writing normalized tables when transformed CDC rows are zero.

Implementation outline:

1. Make Trading 212 history methods accept the documented paginated response
   shape and keep following `nextPagePath` until it is empty.
2. Preserve raw page payloads in the raw CDC table, or normalize the captured
   payload shape so `transform_cdc()` consistently sees event lists.
3. Replace broad silent skips with endpoint-level diagnostics. Missing scopes
   should be visible, not indistinguishable from "no history".
4. Decide whether zero-row CDC fetches should create empty Delta tables with the
   expected schema, then make raw ingest and transform behavior consistent.

### IBKR

IBKR CDC is not implemented. The connector raises `NotImplementedError` for
both CDC fetch and CDC transform, so neither raw nor normalized IBKR CDC tables
can be created.

## Minimal IBKR Flex query settings for CDC

Keep the existing snapshot Flex query sections for holdings:

- Account Information
- Open Positions
- Cash Report
- Currency Conversion Rate

Add CDC-capable activity sections for historical events:

- Trades: security buy/sell activity.
- Cash Transactions: dividends, payment in lieu of dividends, withholding tax,
  deposits and withdrawals, interest, broker fees, other fees/income, price
  adjustments, and commission adjustments.
- Transfers: security or cash transfers between accounts/brokers.
- Transaction Fees: tax/fee detail tied to trades when the values in Trades and
  Cash Transactions are not enough.

Minimum useful fields:

| Event source | Minimal fields |
|--------------|----------------|
| Trades | Account ID, Trade ID, Transaction ID, IB Execution ID, Date/Time, Trade Date, Settle Date Target, Asset Class, Symbol, Description, Currency, FX Rate To Base, Buy/Sell, Transaction Type, Quantity, TradePrice, Trade Money, Proceeds, Taxes, IB Commission, IB Commission Currency, Net Cash, ISIN |
| Cash Transactions | Account ID, Transaction ID, Date/Time, Settle Date, Available For Trading Date, Report Date, Currency, FX Rate To Base, Type, Dividend Type, Description, Amount, Symbol, ISIN, Trade ID, Code, Client Reference, Action ID |
| Transfers | Account ID, Transaction ID, Date/Time, Date, Settle Date, Report Date, Currency, FX Rate To Base, Type, Direction, Transfer Company, Transfer Account, Transfer Account Name, Delivering Broker, Symbol, Description, ISIN, Quantity, Transfer Price, Position Amount, Position Amount in Base, P/L Amount, P/L Amount in Base, Cash Transfer, Code, Client Reference |
| Transaction Fees | Account ID, Trade ID, Order ID, Date, Report Date, Settle Date, Currency, FX Rate To Base, Asset Class, Symbol, Description, ISIN, Tax Description, Tax Amount, Quantity, Trade Price, Source, Code |
| Currency Conversion Rate | From Currency, Rate |

For a first implementation, treat `Trade ID` or `Transaction ID` plus source
section as the event identity. Prefer `IB Execution ID` for execution-level trade
rows when available. If neither `Transaction ID` nor `Trade ID` is available for
a cash/transfer event, derive the event ID from source section, account, date,
description, currency, amount/quantity, and payload hash.

Open question for schema design: `Cash Transactions` may already cover
dividends, withholding tax, deposits/withdrawals, broker fees, interest, other
income, and commission adjustments. If that section is complete enough for the
account, separate dividend/deposit/fee-specific sections may be unnecessary.

## IBKR CDC implementation outline

1. Fetch / bronze
   - Reuse `IbkrFlexClient` and Flex Web Service token/query ID flow.
   - Either require a second `IBKR_FLEX_CDC_QUERY_ID` or document that the main
     query must include both snapshot and activity sections.
   - Write the full Flex XML response to the single `raw/ibkr_cdc` table,
     encrypted in the same raw schema used by snapshots. Do not create one raw
     table per Flex section; bronze remains source-level raw API/report capture.
   - Set `source` to the Flex query/report identity and keep section detail
     inside the raw payload. Section-specific parsing belongs in the silver
     transform, not in bronze storage.

2. Transform / silver
   - Add parsers for trade, cash transaction, transfer, and transaction fee XML elements.
   - Normalize to a broker-neutral CDC silver schema shared by IBKR, Trading
     212, and XTB; the current IBKR schema is only a placeholder payload/source
     stub.
   - Produce event rows with stable IDs, broker, account ID, source, raw event
     type, normalized event type, event date/time, currency, signed cash amount,
     optional security identifiers, optional quantity/price, and optional
     fee/tax details.
   - Encrypt numeric value columns consistently with other normalized tables.

3. Tests
   - Add Flex XML fixtures with one trade, one cash transaction, one transfer, and one transaction fee.
   - Cover raw CDC ingestion, CDC transform, deduplication by stable event ID,
     and the zero-row table decision.

4. Operational notes
   - Prefer a daily Activity Flex Query window for CDC.
   - Use append mode for raw CDC and overwrite or merge semantics for normalized
     CDC depending on the chosen event ID contract.

## Broker-neutral silver CDC shape

Bronze tables should stay raw and source-level. The converged silver layer
should be one broker-neutral CDC event table. It must accept richer IBKR and
Trading 212 events while also using XTB cash-operation rows that include stock
trade information (`Type`, `Time`, `Comment`, and `Symbol`).

Recommended silver columns:

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

The truly common, non-nullable dashboard core is therefore:

- `fetched_at`
- `broker`
- `account_id`
- `event_id`
- `source`
- `event_type`
- `raw_event_type`
- `event_datetime`
- `currency`
- `cash_amount`

Everything security-specific or trade-specific should be nullable in the shared
silver schema. XTB stock cash operations can populate the trade/security columns,
but generic XTB cash operations such as deposits, withdrawals, fees, or interest
should not be forced into fake trade values. This still allows IBKR and Trading
212 to power richer trade and dividend views.

## Reporting available from converged silver CDC

Reports that can be supported for all brokers from the common core:

- Cashflow timeline by broker/account/currency.
- Monthly deposits and withdrawals.
- Monthly fees, taxes, interest, dividends, and other cash events where the
  broker event type can be mapped.
- Net external contributions versus portfolio snapshot value.
- Activity audit table with source, raw type, event date, amount, and currency.
- Broker/account reconciliation helpers: CDC cash movement totals by period
  compared with snapshot cash deltas.

Reports that are available only when optional fields are populated:

- Trade blotter: needs `ticker`, `quantity`, `price`, and `side`; this is available for IBKR/T212 and can be derived for XTB stock operations when `Symbol` and parseable trade comments are present.
- Dividend income by instrument: needs `ticker` or `isin`.
- Fees/taxes by instrument or order: needs optional fee/tax and security fields.
- Realized trading activity by security: needs trade-side fields plus cost-basis
  logic outside the CDC table.
- Multi-currency performance in one reporting currency: needs `amount_base` or
  a separate FX normalization step.

The first silver implementation should optimize for complete cash movement
history across all brokers, then promote richer trade/security analytics where
the broker source has enough detail.


