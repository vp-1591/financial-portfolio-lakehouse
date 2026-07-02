# IBKR Flex Query — required fields for the pipeline

The pipeline talks to IBKR via the **Flex Web Service** (`SendRequest` → `GetStatement`).
It does **not** run a local gateway or use the Client Portal API — it only needs a Flex
**token** and a Flex **query ID** pointing at a pre-configured Activity Flex Query
that returns the four sections below.

The XML elements and attributes consumed by
`pipeline/connectors/ibkr/client.py` and `pipeline/connectors/ibkr/transform.py`
are listed under each section. The Flex Query must be configured so that every
field marked **Required** is selected. Optional fields are read when present and
silently ignored when missing.

The full IBKR field reference is in `docs/_vendor/ibkr/reportingguide.pdf` (July 2019).
The complete list of available fields per section is in
`docs/_vendor/ibkr/flex-query-fields.md`.
Page numbers below refer to the PDF.

## 1. Account Information — Required

Source: Activity Flex Query Reference → Account Information (p. 293 in the PDF,
the same field set is documented in the Default Statement at p. 36).

Needed by the pipeline to determine each account's base currency and to build the
`accountId → base_currency` lookup that ties positions, cash and FX rates together
inside one Flex statement. All other fields on this section (e.g.
`Name`, `Account Alias`, `Account Type`, `Customer Type`, `Account Capabilities`,
`Net Liquidation Value`) are **ignored** by the pipeline and can stay unselected.

| Field (PDF name) | XML attribute  | Required | Notes                                                                                                       |
|------------------|----------------|----------|-------------------------------------------------------------------------------------------------------------|
| Account          | `accountId`    | Required | The IBKR account number (e.g. `U123456`). `parse_account_info` drops any row without this attribute.        |
| Base Currency    | `currency`     | Required | Account base currency (e.g. `EUR`). Used to build the `accountId → base_currency` lookup; defaults to `USD` if missing, which is wrong for non-USD accounts. |

> If you trade multiple accounts under one Flex token (Advisor/Master structure),
> make sure all of them are included in the query, otherwise positions from an
> excluded account will be orphaned in the response.

## 2. Open Positions — Required

Source: Activity Flex Query Reference → Open Positions (p. 315 in the PDF, also
documented in the Default Statement at p. 95). This is the section that produces
the `<OpenPosition>` elements.

| Field (PDF name) | XML attribute    | Required | Notes                                                             |
|------------------|------------------|----------|-------------------------------------------------------------------|
| Account ID       | `accountId`     | Required | Identifies which account the position belongs to. Essential for multi-account queries. |
| Symbol           | `symbol`        | Required | Used as the display label in the dashboard.                      |
| Quantity         | `quantity`      | Required | Used as a fallback for `positionValue` when it is missing or 0.  |
| Mark Price       | `markPrice`     | Required | Used as a fallback for `positionValue` when it is missing or 0.   |
| Position Value   | `positionValue` | Required | Market value in the position's currency. Primary value source.    |
| Asset Class      | `assetClass`    | Required | e.g. `STK`, `OPT`, `FUT`, `BOND`.                                |
| Currency         | `currency`      | Required | Local currency of the position.                                   |
| FX Rate To Base  | `fxRateToBase`  | Required | Used to convert the position value into the account base currency. |
| Description      | `description`   | Optional | Fallback display label when `symbol` is empty.                   |
| ISIN             | `isin`          | Optional | Used for cross-broker identification when available.              |

## 3. Cash Report — Required

Source: Activity Flex Query Reference → Cash Report (p. 308 in the PDF, also
documented in the Default Statement at p. 41). This is the section that produces
the `<CashReportCurrency>` elements.

The pipeline relies on this section to capture per-currency cash balances. If it
is missing, no cash rows will be produced for any account.

| Field (PDF name) | XML attribute    | Required | Notes                                                                |
|------------------|------------------|----------|----------------------------------------------------------------------|
| Account ID       | `accountId`     | Required | Identifies which account the cash belongs to.                       |
| Currency         | `currency`      | Required | The cash currency (3-letter ISO 4217 code).                          |
| Ending Cash      | `endingCash`    | Required | End-of-period cash balance. Primary value source.                    |
| Starting Cash    | `startingCash`  | Optional | Only used as a fallback if `endingCash` is 0 or missing.             |

> Cash Report rows whose `currency` is not a 3-letter ISO 4217 code (e.g.
> `BASE SUMMARY`, `Total`) are summary lines and are intentionally skipped by
> the parser to avoid double counting.

## 4. Currency Conversion Rate — Required

Source: Activity Flex Query Reference → Currency Conversion Rate (p. 372 in
the PDF). This is the section that produces the `<ConversionRate>` elements.
In the Flex Query editor, it appears as the **"Include Currency Rates?"** toggle.

The pipeline uses per-position `fxRateToBase` when present; the Currency
Conversion Rate section provides a per-currency fallback for **cash balances**
and for positions that do not carry their own rate. **Without this section,
non-base-currency cash is converted at 1:1**, which produces wrong values for
multi-currency accounts.

| Field (PDF name) | XML attribute    | Required | Notes                                            |
|------------------|------------------|----------|--------------------------------------------------|
| From Currency    | `fromCurrency`   | Required | 3-letter ISO 4217 source currency.               |
| Rate             | `rate`           | Required | Exchange rate from `fromCurrency` to base.        |

## Section configuration checklist

In the IBKR Flex Query editor (Reports → Flex Queries → your Activity Flex
Query → Sections), tick at least the following four sections. Everything else
can stay off.

- [x] Account Information
- [x] Open Positions
- [x] Cash Report
- [x] Currency Conversion Rate

In the **Field** step of the Flex Query editor, make sure the attributes marked
**Required** in the tables above are selected. Anything else can be left
unticked — the pipeline ignores unrecognised attributes.

## How the values reach `.env`

The query ID is referenced from `.env`:

```ini
IBKR_FLEX_TOKEN=your_token_here
IBKR_FLEX_QUERY_ID=your_query_id_here
```

After saving the query in IBKR, copy its **Query ID** into `IBKR_FLEX_QUERY_ID`.
`IBKR_FLEX_TOKEN` is the token generated under
**Settings → Flex Web Service → Token Management** in IBKR Client Portal.