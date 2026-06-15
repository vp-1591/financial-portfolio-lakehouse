# Use Broker-Native Identifiers in Portfolio Report

## Context

The consolidated portfolio report needs enough information for downstream LLM
analysis to identify stock and ETF positions. ISIN is useful when a broker
provides it, but IBKR Client Portal position responses do not reliably include
ISIN. IBKR does provide `conid`, its native unique contract identifier, in
position data and contract endpoints.

XTB Excel exports may not include security currency, ISIN, or a full instrument
description. Trading 212 metadata can include ISIN, security currency, and
instrument name. Trading 212 position value can be reported in wallet/account
currency through `walletImpact.currency`, so valuation currency and instrument
currency must be kept separate.

## Decision

The consolidated report will expose a generic `Identifier` column instead of an
ISIN-only column. Trading 212 and XTB rows use `ISIN:<value>` when broker data
or an explicit override provides ISIN. IBKR rows use `IBKR:<conid>`.

The report will include `Ccy` for the security currency when known and
`Description` for broker-provided instrument text. IBKR descriptions are
enriched with `/iserver/contract/{conid}/info` on a best-effort basis and fall
back to position description fields if contract details cannot be fetched.
Trading 212 rows display the instrument currency from position/instrument
metadata while still using the wallet/account currency for value conversion.

## Consequences

IBKR stocks and ETFs can be identified consistently without maintaining an
external ISIN map. The report remains honest about XTB exports that do not
contain instrument currency or description. ISIN override inputs remain
available for rows where users have authoritative mappings.

## Validation

Added unit coverage for IBKR conid identifiers, contract description enrichment,
Trading 212 ISIN identifiers, Trading 212 security currency separate from wallet
currency, and the new report output columns. Ran the focused and full pytest
suites with a command-level watchdog.
