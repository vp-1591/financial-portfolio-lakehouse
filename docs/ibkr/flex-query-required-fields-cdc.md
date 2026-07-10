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
| Asset Class             | `assetClass`          | Required | e.g. `STK`, `OPT`, `FUT`. |
| Date/Time               | `dateTime`            | Required | Trade execution timestamp. |
| Trade Date              | `tradeDate`           | Required | Trade date (may differ from settlement). |
| Settle Date Target      | `settleDateTarget`    | Required | Target settlement date. |
| Quantity                | `quantity`            | Required | Number of shares/contracts traded. |
| TradePrice              | `tradePrice`          | Required | Price per share/contract. |
| Proceeds                | `proceeds`            | Required | Total trade proceeds (negative for buys). |
| IB Commission           | `ibCommission`        | Required | IBKR commission charged. |
| IB Commission Currency  | `ibCommissionCurrency`| Required | Currency of the commission. |
| Net Cash                | `netCash`             | Required | Net cash impact (proceeds − commission). |
| Buy/Sell                | `buySell`             | Required | `BUY` or `SELL`. Maps to `side`. |
| Transaction Type         | `transactionType`     | Required | e.g. `ExTrade`. Maps to `raw_event_type`. |
| IB Execution ID         | `ibExecutionId`       | Required | Preferred stable event ID for trades. |
| Trade ID                | `tradeId`             | Required | Secondary event ID when `ibExecutionId` is missing. |
| Transaction ID          | `transactionId`       | Required | Fallback event ID. |
| Taxes                   | `taxes`               | Required | Taxes on the trade. |
| Conid                   | `conid`               | Required | IBKR contract ID. |
| Security ID              | `securityID`          | Required | External security identifier. |
| Multiplier              | `multiplier`          | Required | Contract multiplier (for options/futures). |
| Open/Close Indicator    | `openCloseIndicator`  | Required | `O` for open, `C` for close. |
| Related Trade ID        | `relatedTradeId`      | Required | Links closing trades to opening trades. |

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
| Dividend Type           | `dividendType`    | Required | Qualifies dividend transactions (e.g. `Qualified`). |
| Trade ID                | `tradeId`         | Required | Links to a trade if applicable. |
| Transaction ID          | `transactionId`   | Required | Stable event ID. |
| Code                    | `code`            | Required | IBKR transaction code flags. |
| Asset Class             | `assetClass`      | Required | Asset class of related security. |
| Conid                   | `conid`           | Required | IBKR contract ID. |
| Security ID              | `securityID`      | Required | External security identifier. |

### Cash Transaction sub-sections to include

In the Flex Query editor, the following Cash Transaction options should be ticked
so that all relevant cash event types are captured:

- [x] Dividends
- [x] Payment in Lieu of Dividends
- [x] Withholding Tax
- [x] Deposits & Withdrawals
- [x] Broker Interest
- [x] Broker Fees
- [x] Other Fees
- [x] Other Income
- [x] Price Adjustments
- [x] Commission Adjustments
- [x] Detail (not Summary — we need individual transactions)

### IBKR Cash Transaction `type` → normalized `event_type` mapping

| IBKR `type` value        | Normalized `event_type` |
|--------------------------|--------------------------|
| `Dividends`              | `DIVIDEND`               |
| `PaymentInLieue`         | `DIVIDEND`               |
| `Withholding Tax`        | `TAX`                    |
| `Deposits & Withdrawals` (positive) | `DEPOSIT`      |
| `Deposits & Withdrawals` (negative) | `WITHDRAWAL`   |
| `Broker Interest`        | `INTEREST`               |
| `Broker Fees`            | `FEE`                    |
| `Other Fees`             | `FEE`                    |
| `Other Income`           | `ADJUSTMENT`             |
| `Price Adjustments`      | `ADJUSTMENT`             |
| `Commission Adjustments` | `FEE`                    |
| *any other value*        | `UNKNOWN`                |

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
| Asset Class             | `assetClass`          | Required | Asset class. |
| Date/Time               | `dateTime`            | Required | Transfer timestamp. |
| Settle Date             | `settleDate`          | Required | Settlement date. |
| Type                    | `type`                | Required | Transfer type. |
| Direction               | `direction`           | Required | `IN` or `OUT`. |
| Quantity                | `quantity`            | Required | Number of shares/contracts transferred. |
| Transfer Price          | `transferPrice`       | Required | Price per unit at transfer. |
| Position Amount         | `positionAmount`      | Required | Position value in local currency. |
| Position Amount in Base | `positionAmountInBase`| Required | Position value in base currency. |
| Cash Transfer           | `cashTransfer`        | Required | Cash amount transferred. |
| Transaction ID          | `transactionId`       | Required | Stable event ID. |
| Conid                   | `conid`               | Required | IBKR contract ID. |
| Security ID              | `securityID`          | Required | External security identifier. |

## 8. Transaction Fees — Required for CDC

Source: Activity Flex Query Reference → Transaction Fees. This section produces
`<TransactionFee>` elements with tax and fee detail tied to specific trades. Use
this section when the values in Trades and Cash Transactions are not enough to
reconstruct per-trade fee/tax breakdowns.

| Field (PDF name)        | XML attribute     | Required | Notes |
|-------------------------|-------------------|----------|-------|
| Account ID              | `accountId`       | Required | Account identifier. |
| Symbol                  | `symbol`         | Required | Related security ticker. |
| Description             | `description`    | Required | Human-readable description. |
| ISIN                    | `isin`           | Required | ISIN for the related security. |
| Currency                | `currency`       | Required | Fee/tax currency. |
| FX Rate To Base         | `fxRateToBase`   | Required | Exchange rate to account base currency. |
| Asset Class             | `assetClass`     | Required | Asset class. |
| Date                    | `date`           | Required | Fee/tax date. |
| Report Date             | `reportDate`     | Required | Reporting date. |
| Settle Date             | `settleDate`     | Required | Settlement date. |
| Tax Description         | `taxDescription` | Required | Description of the tax or fee. |
| Tax Amount              | `taxAmount`      | Required | Amount of tax or fee. |
| Order ID                | `orderId`        | Required | Links to the originating order. |
| Trade ID                | `tradeId`        | Required | Links to the originating trade. |
| Trade Price             | `tradePrice`     | Required | Price at which the fee was assessed. |
| Source                  | `source`         | Required | Source of the fee/tax. |
| Code                    | `code`           | Required | IBKR fee/tax code. |
| Conid                   | `conid`          | Required | IBKR contract ID. |
| Security ID              | `securityID`     | Required | External security identifier. |
| Quantity                | `quantity`       | Required | Related quantity. |

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
**Required** in the tables above are selected. Anything else can be left unticked —
the pipeline ignores unrecognised attributes.