# IBKR Connector Setup

## Flex Web Service

IBKR data is fetched through the Flex Web Service API — no local gateway
process or browser login is required. Data has a 15–30 minute delay compared
to real-time positions.

To set up: log in to [IBKR Client Portal](https://portal.interactivebrokers.com),
navigate to **Performance & Reports → Flex Queries**, create an **Activity Flex
Query** named `get-open-positions` with the Open Positions and Account
Information fields you need, set Format to XML and Period to Last Business Day.
Enable **Flex Web Service Configuration** and generate a token.

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `IBKR_FLEX_TOKEN` | Flex Web Service token |
| `IBKR_FLEX_QUERY_ID` | Flex Query ID |
| `IBKR_FLEX_BASE_URL` | Base URL (default: `https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService`) |
| `IBKR_ENABLED` | Enable/disable connector (default: enabled) |

### Demo mode

When `DEMO=true`, the connector uses `IBKR_FLEX_TOKEN_DEMO` and
`IBKR_FLEX_QUERY_ID_DEMO` instead.

## Detailed field configuration

For the exact fields required in your Flex Query, see:

- [Flex Query Required Fields](../ibkr/flex-query-required-fields.md)
- [Flex Query Required Fields (CDC)](../ibkr/flex-query-required-fields-cdc.md)
