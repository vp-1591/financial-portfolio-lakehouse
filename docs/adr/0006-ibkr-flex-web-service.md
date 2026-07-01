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
- Accepts `--ibkr-flex-token` (required) and `--ibkr-flex-query-id` (required)
- Parses XML `<OpenPosition>`, `<AccountInformation>`, and optionally `<CashReport>` elements
  for positions, cash, net liquidation value, and FX rates
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
- Cash balances resolved through 3-source priority:
  1. **Cash Report** (per-currency `endingCash`) — most precise, requires Cash Report section in Flex Query
     - Summary rows (e.g. `currency="BASE SUMMARY"`) are filtered out to prevent double-counting
  2. **AccountInformation `cashBalance`** — single-currency field from Account Information section
  3. **Derived from NLV minus positions** — fallback when neither source is present; produces single CASH entry in base currency
- FX rates for cash conversion: `fxRateToBase` from OpenPositions → `<ConversionRate>` elements → warning on missing rate
- Pipeline connector migration is a separate follow-up task

## Validation

All 147 tests pass (1 skipped). Flex XML parsing verified with fixture data covering positions,
cash (Cash Report, cashBalance, and NLV-derived), FX conversion, net worth calculation,
and error handling. Cash Report filtering excludes summary rows (BASE SUMMARY) that would
double-count per-currency entries. A warning is emitted to stderr when a non-base-currency
cash entry has no available FX rate (previously defaulted silently to 1.0).