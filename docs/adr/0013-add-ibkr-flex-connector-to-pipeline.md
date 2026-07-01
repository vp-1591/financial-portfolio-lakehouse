# 0013 — Add IBKR Flex Web Service connector to pipeline

## Context

The pipeline's IBKR connector (`pipeline/connectors/ibkr/`) uses the Client Portal
Gateway API, which requires a local Java process and browser-based authentication.
The standalone script (`scripts/ibkr_net_worth.py`) already implements the IBKR
Flex Web Service API, which only needs a token and query ID — no local gateway.

Running the pipeline with `--ibkr` currently requires the gateway to be running.
Users who only have Flex Web Service credentials (token + query ID) cannot use
the pipeline fetch command.

## Decision

Add `--ibkr-flex-token`, `--ibkr-flex-query-id`, and `--ibkr-flex-base-url` CLI
arguments to the pipeline runner. When `--ibkr-flex-token` is provided, the IBKR
connector fetches data via the Flex Web Service instead of the Client Portal
Gateway.

Implementation details:

- **`pipeline/connectors/ibkr/fetch.py`**: New `fetch_snapshot_via_flex()` function
  reuses `scripts.ibkr_net_worth.IbkrFlexClient` to request and fetch a Flex
  report. The raw XML is stored as a single encrypted payload row with
  `source="flex"` in the raw Delta table.

- **`pipeline/connectors/ibkr/transform.py`**: New `_transform_flex_snapshot()`
  function parses Flex XML (OpenPosition, AccountInformation, CashReportCurrency,
  ConversionRate elements) into the same normalized schema as the gateway path.
  Reuses parsing helpers from `scripts.ibkr_net_worth`.

- **`pipeline/connectors/ibkr/connector.py`**: `IbkrConnector.fetch_snapshot()`
  dispatches to `fetch_snapshot_via_flex()` when `flex_token` is in kwargs.
  `IbkrConnector.transform_snapshot()` dispatches to `_transform_flex_snapshot()`
  when `source="flex"` is detected in the raw table.

- **`pipeline/run.py`**: Three new arguments under the IBKR group:
  `--ibkr-flex-token`, `--ibkr-flex-query-id`,
  `--ibkr-flex-base-url`. When `--ibkr-flex-token` is provided, `cmd_fetch`
  passes Flex-specific kwargs to the connector.

## Consequences

- Users can now run `python -m pipeline.run fetch --ibkr-flex-token TOKEN` without
  a local Client Portal Gateway.
- The Flex path stores raw XML (not JSON) in the payload column, distinguished by
  `source="flex"`. The transform step handles both formats transparently.
- CDC fetch is not supported via Flex (only snapshot). The `--ibkr-flex-token`
  flag disables CDC fetching for IBKR.
- The `--ibkr` flag (gateway mode) and `--ibkr-flex-token` (Flex mode) are
  mutually exclusive in practice — if both are provided, Flex takes priority for
  snapshot fetching, but gateway args would still be used for CDC.

## Validation

- All 24 tests pass, including 11 new tests for Flex fetch, Flex transform,
  connector dispatch, and CLI argument parsing.
- `python -m pipeline.run fetch --help` shows the new arguments.