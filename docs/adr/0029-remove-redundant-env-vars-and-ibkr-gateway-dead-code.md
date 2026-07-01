# 0029 — Remove redundant environment variables and IBKR Client Portal Gateway dead code

## Context

The IBKR connector previously supported two access methods: the Client Portal Gateway (a local gateway running at `localhost:5000`) and the Flex Web Service API. The pipeline now exclusively uses Flex. This left five gateway-related environment variables as dead code: `IBKR_BASE_URL`, `IBKR_ACCOUNT`, `IBKR_VERIFY_TLS`, `IBKR_SKIP_AUTH_CHECK`, and `IBKR_REQUIRE_BROKERAGE_SESSION`. The `IbkrClient` class and its gateway-only helpers in `client.py`, as well as `fetch_snapshot()` (the gateway version) in `fetch.py`, were also dead code that was never executed.

Additionally, several environment variables served no useful purpose:

- `T212_ACCOUNT_ID` — always passed as an empty string in the pipeline; Trading 212's API works without it.
- `T212_USER_AGENT` — the default `requests` User-Agent is sufficient; a custom one adds no value.
- `T212_SKIP_METADATA` — the pipeline always benefits from metadata (instrument name, currency, etc.); skipping it degrades data quality for no gain.
- `XTB_ACCOUNT_ID` — always passed as an empty string; the XTB API does not require it.

## Decision

1. **Remove all 9 environment variables**: `IBKR_BASE_URL`, `IBKR_ACCOUNT`, `IBKR_VERIFY_TLS`, `IBKR_SKIP_AUTH_CHECK`, `IBKR_REQUIRE_BROKERAGE_SESSION`, `T212_ACCOUNT_ID`, `T212_USER_AGENT`, `T212_SKIP_METADATA`, `XTB_ACCOUNT_ID`.

2. **Remove `IbkrClient` class** and all gateway-only helpers from `pipeline/connectors/ibkr/client.py`. Remove `fetch_snapshot()` (gateway version) from `pipeline/connectors/ibkr/fetch.py`.

3. **Simplify `IbkrConnector`** to always use the Flex path — no more gateway/Flex dispatch logic.

4. **Hardcode `include_metadata=True`** in the Trading 212 connector. The pipeline always wants metadata for best data quality.

5. **Hardcode `DEFAULT_USER_AGENT`** in the Trading 212 connector (the `requests` library default). Remove the `user_agent` parameter.

6. **Hardcode empty string for `account_id`** in both Trading 212 and XTB fetch calls.

## Consequences

- **Positive**: Simpler configuration — 9 fewer environment variables to document, set, and debug.
- **Positive**: No more gateway/Flex dispatch logic in the IBKR connector. The code path is straightforward.
- **Positive**: Trading 212 always fetches instrument metadata, ensuring best data quality.
- **Positive**: Reduced dead code and maintenance burden.
- **Negative**: Anyone relying on `T212_SKIP_METADATA` to speed up fetches will no longer be able to skip metadata. The trade-off is acceptable because metadata improves data quality and the performance cost is minimal.
- **Negative**: Anyone relying on the IBKR Client Portal Gateway path can no longer use it. The Flex Web Service API is the only supported access method.

## Validation

- All pipeline tests pass after the removal.
- IBKR connector tests exercise the Flex path exclusively.
- Trading 212 connector tests confirm metadata is always fetched.
- Ruff linting and formatting pass cleanly.