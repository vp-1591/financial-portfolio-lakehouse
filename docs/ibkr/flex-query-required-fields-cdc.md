# IBKR Flex Query — CDC fields (supplement)

This document lists the Flex Query sections and fields needed for **CDC (Change Data
Capture)** that are **not already in** the snapshot query documented in
`docs/ibkr/flex-query-required-fields.md`.

The snapshot query already includes: Account Information, Open Positions, Cash
Report, and Currency Conversion Rate. The CDC sections below must be **added to the
same Activity Flex Query** so that a single `SendRequest`/`GetStatement` call
returns both snapshot and activity data.

The XML elements and attributes consumed by
`pipeline/connectors/ibkr/client.py` and `pipeline/connectors/ibkr/transform.py`
are listed under each section. The Flex Query must be configured so that every
field marked **Required** is selected. Optional fields are read when present and
silently ignored when missing.

The full IBKR field reference is in `docs/_vendor/ibkr/reportingguide.pdf` (July 2019).
The complete list of available fields per section is in
`docs/_vendor/ibkr/flex-query-fields.md`.

---

> ⚠️ **Period setting — must cover historical activity**
>
> The Flex Query **Period** must be wide enough to capture past transactions, not just
> the last business day. If the period is too narrow (e.g. `LastBusinessDay`), the
> CDC sections (Trades, CashTransactions, Transfers, TransactionFees) will be **empty**
> and the `normalized/ibkr_cdc` table will have zero rows.
>
> **Recommended:** `Last365Days` or a specific date range covering your account history.
>
> This does **not** affect the snapshot. The snapshot transform only reads
> OpenPositions, AccountInformation, CashReport, and ConversionRates — these always
> reflect current holdings regardless of the query period. The CDC transform reads
> only the activity sections (Trades, CashTransactions, etc.) which are populated
> based on the period.
>
> If you use a **single Flex Query** for both snapshot and CDC (the default when
> `IBKR_FLEX_CDC_QUERY_ID` is not set), widening the period is safe: snapshot data
> remains unchanged, and CDC data is now populated.

---

## 5. Trades — Required for CDC

Source: Activity Flex Query Reference → Trades. This section produces `<Trade>`
elements. Each trade becomes a `TRADE` event in the broker-neutral CDC schema.

| Field (PDF name)        | XML attribute         | Required | Notes |
|-------------------------|-----------------------|----------|-------|
| Account ID              | `accountId`           | Required | Identifies which account executed the trade. |
| Symbol                  | `symbol`              | Required | Ticker/identifier for the traded security. |
| Description             | `description`         | Required | Human-readable security description. |
| ISIN                    | `isin`                | Required | Cross-broker security identification. |
| Currency                | `currency`            | Required | Trade currency. |
| FX Rate To Base         | `fxRateToBase`        | Required | Exchange rate to account base currency. |
| Date/Time               | `dateTime`            | Required | Trade execution timestamp. |
| Settle Date Target      | `settleDateTarget`    | Required | Target settlement date. |
| Quantity                | `quantity`            | Required | Number of shares/contracts traded. |
| TradePrice              | `tradePrice`          | Required | Price per share/contract. |
| Proceeds                | `proceeds`            | Required | Total trade proceeds (negative for buys). |
| IB Commission           | `ibCommission`        | Required | IBKR commission charged. |
| Net Cash                | `netCash`             | Required | Net cash impact (proceeds − commission). |
| Buy/Sell                | `buySell`             | Required | `BUY` or `SELL`. Maps to `side`. |
| Transaction Type         | `transactionType`     | Required | e.g. `ExTrade`. Maps to `raw_event_type`. |
| IB Execution ID         | `ibExecutionId`       | Required | Preferred stable event ID for trades. |
| Trade ID                | `tradeId`             | Required | Secondary event ID when `ibExecutionId` is missing. |
| Transaction ID          | `transactionId`       | Required | Fallback event ID. |
| Taxes                   | `taxes`               | Required | Taxes on the trade. |
| Asset Class             | `assetClass`          | Optional | e.g. `STK`, `OPT`, `FUT`. Not yet in CDC schema; useful for future asset-type filtering. |
| IB Commission Currency  | `ibCommissionCurrency`| Optional | Currency of the commission. Useful if commission currency differs from trade currency. |
| Multiplier              | `multiplier`          | Optional | Contract multiplier. Needed if options/futures trades are ever supported. |
| Open/Close Indicator    | `openCloseIndicator`  | Optional | `O` for open, `C` for close. Useful for tax-lot tracking and P&L calculations. |
| Related Trade ID        | `relatedTradeId`      | Optional | Links closing trades to opening trades. Useful for tax-lot matching. |

## 6. Cash Transactions — Required for CDC

Source: Activity Flex Query Reference → Cash Transactions. This section produces
`<CashTransaction>` elements. These cover dividends, withholding tax, deposits,
withdrawals, interest, fees, price adjustments, and commission adjustments.

| Field (PDF name)        | XML attribute     | Required | Notes |
|-------------------------|-------------------|----------|-------|
| Account ID              | `accountId`       | Required | Account identifier. |
| Symbol                  | `symbol`          | Required | Related security ticker (may be empty for cash-only events). |
| Description             | `description`     | Required | Human-readable description. |
| ISIN                    | `isin`            | Required | ISIN for the related security. |
| Currency                | `currency`        | Required | Transaction currency. |
| FX Rate To Base         | `fxRateToBase`    | Required | Exchange rate to account base currency. |
| Date/Time               | `dateTime`        | Required | Transaction timestamp. |
| Settle Date             | `settleDate`      | Required | Settlement date. |
| Amount                  | `amount`          | Required | Signed cash amount. Positive = inflow, negative = outflow. |
| Type                    | `type`            | Required | IBKR transaction type. Maps to normalized `event_type`. |
| Transaction ID          | `transactionId`   | Required | Stable event ID. |
| Dividend Type           | `dividendType`    | Optional | Qualifies dividend transactions (e.g. `Qualified`). Useful for tax reporting. |
| Trade ID                | `tradeId`         | Optional | Links to a trade if applicable. Useful for correlating dividends/withholding tax to trades. |
| Asset Class             | `assetClass`      | Optional | Asset class of related security. Not yet in CDC schema; useful for future filtering. |

### Cash Transaction sub-sections to include

In the Flex Query editor, the following Cash Transaction options should be ticked
so that all relevant cash event types are captured. The sub-section names below
match the Flex Query editor checkboxes exactly (see
`docs/_vendor/ibkr/flex-query-fields.md` for the full list).

- [x] Dividends
- [x] Payment in Lieu of Dividends
- [x] Withholding Tax
- [x] 871(m) Withholding
- [x] Deposits & Withdrawals
- [x] Broker Interest Paid
- [x] Broker Interest Received
- [x] Broker Fees
- [x] Other Fees
- [x] Other Income
- [x] Bond Interest Paid
- [x] Bond Interest Received
- [x] Price Adjustments
- [x] Commission Adjustments
- [x] Detail (not Summary — we need individual transactions)

### IBKR Cash Transaction `type` → normalized `event_type` mapping

The `type` attribute values below are the exact strings IBKR puts in the XML.
They differ from the Flex Query editor sub-section names — for example, both
"Broker Interest Paid" and "Broker Interest Received" sub-sections produce
`type="Broker Interest"` elements (distinguished by the sign of `amount`).

| IBKR `type` value        | Normalized `event_type` | Notes |
|--------------------------|--------------------------|-------|
| `Dividends`              | `DIVIDEND`               | |
| `PaymentInLieue`         | `DIVIDEND`               | IBKR spelling (extra 'e'). |
| `Withholding Tax`        | `TAX`                    | |
| `871(m) Withholding`     | `TAX`                    | US withholding on dividend equivalents. |
| `Deposits & Withdrawals` (positive) | `DEPOSIT`      | Sign of `amount` determines direction. |
| `Deposits & Withdrawals` (negative) | `WITHDRAWAL`   | |
| `Broker Interest`        | `INTEREST`               | Covers both "Paid" and "Received" sub-sections. |
| `Bond Interest`          | `INTEREST`               | Covers both "Paid" and "Received" sub-sections. |
| `Broker Fees`            | `FEE`                    | |
| `Other Fees`             | `FEE`                    | |
| `Other Income`           | `ADJUSTMENT`             | |
| `Price Adjustments`      | `ADJUSTMENT`             | |
| `Commission Adjustments` | `FEE`                    | |
| *any other value*        | `UNKNOWN`                | |

## 7. Transfers — Required for CDC

Source: Activity Flex Query Reference → Transfers. This section produces `<Transfer>`
elements covering security and cash transfers between accounts/brokers.

| Field (PDF name)        | XML attribute          | Required | Notes |
|-------------------------|------------------------|----------|-------|
| Account ID              | `accountId`            | Required | Account identifier. |
| Symbol                  | `symbol`              | Required | Security ticker (may be empty for cash-only transfers). |
| Description             | `description`         | Required | Human-readable description. |
| ISIN                    | `isin`                | Required | ISIN for the transferred security. |
| Currency                | `currency`             | Required | Transfer currency. |
| FX Rate To Base         | `fxRateToBase`        | Required | Exchange rate to account base currency. |
| Date/Time               | `dateTime`            | Required | Transfer timestamp. |
| Settle Date             | `settleDate`          | Required | Settlement date. |
| Type                    | `type`                | Required | Transfer type. |
| Direction               | `direction`           | Required | `IN` or `OUT`. |
| Quantity                | `quantity`            | Required | Number of shares/contracts transferred. |
| Transfer Price          | `transferPrice`       | Required | Price per unit at transfer. |
| Cash Transfer           | `cashTransfer`        | Required | Cash amount transferred. |
| Transaction ID          | `transactionId`       | Required | Stable event ID. |
| Asset Class             | `assetClass`          | Optional | Asset class. Not yet in CDC schema; useful for future filtering. |
| Position Amount         | `positionAmount`      | Optional | Position value in local currency. Useful for reconciliation. |
| Position Amount in Base | `positionAmountInBase`| Optional | Position value in base currency. Useful for base-currency valuation. |

## 8. Transaction Fees — Required for CDC

Source: Activity Flex Query Reference → Transaction Fees. This section produces
`<TransactionFee>` elements with tax and fee detail tied to specific trades. Use
this section when the values in Trades and Cash Transactions are not enough to
reconstruct per-trade fee/tax breakdowns.

| Field (PDF name)        | XML attribute     | Required | Notes |
|-------------------------|-------------------|----------|-------|
| Account ID              | `accountId`       | Required | Account identifier. |
| Symbol                  | `symbol`         | Required | Related security ticker. |
| ISIN                    | `isin`           | Required | ISIN for the related security. |
| Currency                | `currency`       | Required | Fee/tax currency. |
| FX Rate To Base         | `fxRateToBase`   | Required | Exchange rate to account base currency. |
| Date                    | `date`           | Required | Fee/tax date. |
| Settle Date             | `settleDate`     | Required | Settlement date. |
| Tax Description         | `taxDescription` | Required | Description of the tax or fee. |
| Tax Amount              | `taxAmount`      | Required | Amount of tax or fee. |
| Trade Price             | `tradePrice`     | Required | Price at which the fee was assessed. |
| Quantity                | `quantity`       | Required | Related quantity. |
| Asset Class             | `assetClass`     | Optional | Asset class. Not yet in CDC schema; useful for future filtering. |
| Order ID                | `orderId`        | Optional | Links to the originating order. Useful for fee-to-order correlation. |
| Trade ID                | `tradeId`        | Optional | Links to the originating trade. Useful for fee-to-trade correlation. |
| Report Date             | `reportDate`     | Optional | Reporting date. Useful for reconciliation with official IBKR reports. |

## Updated section configuration checklist

In the IBKR Flex Query editor, the following sections must be ticked. The first
four are from the snapshot query; the last four are the CDC additions.

- [x] Account Information
- [x] Open Positions
- [x] Cash Report
- [x] Currency Conversion Rate
- [x] Trades
- [x] Cash Transactions (with Detail sub-section — not Summary)
- [x] Transfers
- [x] Transaction Fees

In the **Field** step of the Flex Query editor, make sure the attributes marked
**Required** in the tables above are selected. **Optional** fields are not
currently consumed by the pipeline but are recommended to include — they may be
used in future schema expansions and do not increase response size significantly.
All other fields can be left unticked — the pipeline ignores unrecognised
attributes.