# Trading 212 API Key Permissions

## Available permission toggles

When creating an API key in Trading 212, the following permissions can be toggled:

- Account data
- History
- History - Dividends
- History - Orders
- History - Transactions
- Metadata
- Orders - Execute
- Orders - Read
- Pies - Read
- Pies - Write
- Portfolio

## Permissions required by this pipeline

Enable **only** the following (read-only — no trading capability):

| Permission | Reason |
|---|---|
| **Account data** | Account summary (balance, cash, net worth) — `GET /equity/account/summary` |
| **History** | Parent toggle required to access any History sub-endpoints |
| **History - Dividends** | Dividend history — `GET /equity/history/dividends` |
| **History - Orders** | Order history — `GET /equity/history/orders` |
| **History - Transactions** | Transaction history — `GET /equity/history/transactions` |
| **Metadata** | Instrument metadata (ticker, ISIN, currency) — `GET /equity/metadata/instruments` |
| **Portfolio** | Current open positions — `GET /equity/positions` |

## Permissions NOT needed

Do **not** enable these — the pipeline never writes or trades:

- Orders - Execute
- Orders - Read (only needed if you use the non-history orders endpoint)
- Pies - Read
- Pies - Write