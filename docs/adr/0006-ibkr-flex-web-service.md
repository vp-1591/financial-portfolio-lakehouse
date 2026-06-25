# 0006: Replace IBKR Client Portal Gateway with Flex Web Service

## Context

The IBKR Client Portal Gateway requires a local Java process, browser SSO authentication,
and conflicts with TWS/IBKR Mobile sessions (only one brokerage session per username). The
gateway frequently produced "not logged in" errors despite being authenticated, making the
IBKR integration unreliable.

The Flex Web Service API uses a token + query ID, with no gateway process, no browser login,
and no session conflicts. Data has a 15–30 minute delay, which is acceptable for portfolio
reporting.

## Decision

Replace the Client Portal Gateway integration in `scripts/ibkr_net_worth.py` with the
IBKR Flex Web Service API. The standalone script now:

- Uses `IbkrFlexClient` (two-step HTTP flow: SendRequest → GetStatement)
- Accepts `--ibkr-flex-token` (required) and `--ibkr-flex-query-id` (default 1554188)
- Parses XML `<OpenPosition>` and `<AccountInformation>` elements for positions, cash,
  net liquidation value, and FX rates
- Removed: `--base-url`, `--account`, `--verify-tls`, `--skip-auth-check`,
  `--require-brokerage-session`

Updated `scripts/portfolio_connectors.py` and `scripts/portfolio_percentages.py` to use
the Flex-based `load_ibkr_holdings()` function. Added `security_currency` field to the
`Asset` dataclass to track position currency separately from base currency.

The pipeline connector (`pipeline/connectors/ibkr/`) still uses the Client Portal Gateway
and will be migrated separately.

## Consequences

- No more Java gateway process, browser login, or session conflicts
- Data has 15–30 minute delay (acceptable for portfolio snapshots)
- Simpler CLI: `python scripts/ibkr_net_worth.py --ibkr-flex-token TOKEN`
- `--ibkr-base-currency` no longer needed (Flex provides `fxRateToBase`)
- Pipeline connector migration is a separate follow-up task

## Validation

All 134 tests pass. Flex XML parsing verified with fixture data covering positions,
cash, FX conversion, net worth calculation, and error handling.