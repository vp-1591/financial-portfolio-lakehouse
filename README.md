# Investment Portfolio Dashboard

Utilities for consolidating broker assets into a single portfolio view.

## IBKR Net Worth Percentages

The first script reads Interactive Brokers assets through the IBKR Client Portal
Web API and prints each position and cash balance as a percentage of net worth.

### IBKR setup

1. Install Java 8 update 192 or newer.
2. Download and unzip the Interactive Brokers Client Portal Gateway.
3. Start the gateway from its directory:

   ```powershell
   bin\run.bat root\conf.yaml
   ```

4. Open `https://localhost:5000` in a browser on the same machine and sign in.
   The local gateway uses a self-signed certificate by default, so the browser
   warning is expected for localhost. After approving the mobile notification or
   QR login, wait until the gateway page reports that the client login succeeded.

The script validates the gateway SSO session with `GET /sso/validate` before it
reads portfolio data. It does not require the `/iserver` brokerage session by
default, because IBKR allows only one active brokerage session per username. If
you log in to TWS, Client Portal, or the IBKR mobile app, IBKR may ask to reset
other sessions and the gateway brokerage session can be invalidated. In that
case the portfolio script can still work as long as the gateway SSO session is
valid.

If the browser remains stuck on the QR-code login page after mobile approval,
restart the Client Portal Gateway, reopen `https://localhost:5000`, and complete
login there before running the script. A raw `HTTP 401` from
`/iserver/auth/status` usually means the brokerage session was not established or
was taken over by another IBKR product, not that the local Python script failed
to scan the QR code.

### Run

```powershell
python .\scripts\ibkr_net_worth.py
```

Optional arguments:

```powershell
python .\scripts\ibkr_net_worth.py --account U1234567
python .\scripts\ibkr_net_worth.py --base-url https://localhost:5001/v1/api
python .\scripts\ibkr_net_worth.py --require-brokerage-session
```

The script calls:

- `GET /sso/validate` to verify the gateway login session.
- `GET /portfolio/accounts` to discover accounts.
- `GET /portfolio2/{accountId}/positions` to fetch near-real-time positions.
- `GET /portfolio/{accountId}/ledger` to fetch cash and net liquidation value.

With `--require-brokerage-session`, the script also calls
`POST /iserver/auth/status`. Use that only when you need to verify the active
brokerage session and are prepared for IBKR to disconnect competing sessions.

Position values and cash balances are converted into the account base currency
using ledger exchange rates before percentages are calculated.

## Trading 212 Net Worth Percentages

The Trading 212 script prints the same net worth percentage table using the
Trading 212 public API. Pass the API key, API secret, and account id on the
command line so credentials are not stored in this repository or in a config
file.

```powershell
python .\scripts\trading212_net_worth.py --api-key "YOUR_API_KEY" --api-secret "YOUR_API_SECRET" --account-id "YOUR_ACCOUNT_ID"
```

For a demo account:

```powershell
python .\scripts\trading212_net_worth.py --api-key "YOUR_DEMO_API_KEY" --api-secret "YOUR_DEMO_API_SECRET" --account-id "YOUR_DEMO_ACCOUNT_ID" --demo
```

Optional arguments:

```powershell
python .\scripts\trading212_net_worth.py --base-url https://live.trading212.com/api/v0
python .\scripts\trading212_net_worth.py --skip-metadata
python .\scripts\trading212_net_worth.py --timeout 30
python .\scripts\trading212_net_worth.py --user-agent "Mozilla/5.0 ..."
```

The script calls:

- `GET /equity/account/summary` to read account currency, cash, and total value.
- `GET /equity/positions` to read open positions.
- `GET /equity/metadata/instruments` to display instrument currencies unless
  `--skip-metadata` is used.

## XTB Net Worth Percentages

The XTB script prints the same net worth percentage table from an exported XTB
Excel report. Pass an absolute path to the `.xlsx` file so the script can be run
from any working directory.

```powershell
python .\scripts\xtb_net_worth.py --file "C:\Users\you\Downloads\account_00000000_en_xlsx_2005-12-31_2026-06-15.xlsx"
```

Optional arguments:

```powershell
python .\scripts\xtb_net_worth.py --file "C:\path\to\report.xlsx" --account-id "XTB-1"
```

The script reads:

- The `OPEN POSITION...` sheet to parse account id, currency, balance, equity,
  and open positions.
- The `CASH OPERATION...` sheet as a fallback source for cash when the open
  position sheet does not contain a balance.

Each XTB open-position row is a lot. The script calculates lot value as
`Purchase value + Gross P/L`, then aggregates lots by symbol so the output has
one row per asset. Net worth uses the report `Equity` when present, otherwise it
falls back to the sum of parsed positions and cash.

## Consolidated Ticker Percentages

The consolidated script reads Trading 212, one or more XTB Excel reports, and
IBKR Client Portal Gateway data, converts all rows to one target currency, and
prints only ticker, percentage, and broker.

```powershell
python .\scripts\portfolio_percentages.py `
  --trading212-api-key "YOUR_API_KEY" `
  --trading212-api-secret "YOUR_API_SECRET" `
  --trading212-account-id "YOUR_ACCOUNT_ID" `
  --xtb-file "C:\path\to\xtb-report-1.xlsx" `
  --xtb-file "C:\path\to\xtb-report-2.xlsx" `
  --ibkr-base-currency EUR `
  --target-currency EUR
```

`--ibkr-base-currency` is only required when IBKR reports the account base
currency as the placeholder `BASE` instead of a real ISO currency code.

Missing FX rates are fetched from Frankfurter first, then Yahoo Finance if the
first provider fails. To avoid network FX lookups or override a rate, pass rates
where one source-currency unit equals the given target-currency amount:

```powershell
python .\scripts\portfolio_percentages.py ... --fx-rate USD=0.92 --fx-rate PLN=0.23
```

To run the live FX integration check:

```powershell
$env:RUN_LIVE_FX_TESTS = "1"
python -m pytest tests\test_live_fx.py
```
