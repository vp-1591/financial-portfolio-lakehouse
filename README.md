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
   warning is expected for localhost.

### Run

```powershell
python .\scripts\ibkr_net_worth.py
```

Optional arguments:

```powershell
python .\scripts\ibkr_net_worth.py --account U1234567
python .\scripts\ibkr_net_worth.py --base-url https://localhost:5001/v1/api
```

The script calls:

- `POST /iserver/auth/status` to verify the gateway session.
- `GET /portfolio/accounts` to discover accounts.
- `GET /portfolio2/{accountId}/positions` to fetch near-real-time positions.
- `GET /portfolio/{accountId}/ledger` to fetch cash and net liquidation value.

Position values and cash balances are converted into the account base currency
using ledger exchange rates before percentages are calculated.
