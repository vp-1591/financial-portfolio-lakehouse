"""PyArrow schemas for normalized tables.

Phase 2 (Currency Unification) replaces overloaded column names with
unambiguous ones:

- ``security_ccy`` — the currency that ``cash_amount``, ``gross_amount``,
  ``fee_amount``, and ``tax_amount`` are denominated in.  For all event
  types this is the amount currency, not necessarily the instrument's
  trading currency (see ``instrument_ccy``).
- ``instrument_ccy`` — the instrument's trading currency, when known
  (e.g. USD for AAPL).  Null when unknown or N/A (deposits, fees).
  For cross-currency dividends this differs from ``security_ccy``:
  a GBX-denominated stock paying a GBP dividend has
  ``security_ccy=GBP, instrument_ccy=GBX``.
- ``security_value`` — position value in ``security_ccy`` (snapshots).
- ``target_value`` — value converted to the pipeline target currency
  (EUR) via ``target_fx_rate``.
- ``target_fx_rate`` — the rate from ``security_ccy`` to ``target_ccy``
  used to compute ``target_value``.  Always satisfies
  ``target_value = cash_amount × target_fx_rate``.
- ``target_ccy`` — the pipeline target currency (always EUR).

Removed columns: ``value``, ``value_currency``, ``base_currency``,
``security_currency``, ``fx_rate_to_base``, ``amount_base``, ``net_amount``.
"""

from __future__ import annotations

import pyarrow as pa

# --- Snapshot schemas (3) ---

ibkr_snapshot_normalized_schema = pa.schema(
    [
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("account_id", pa.string()),
        pa.field("position_type", pa.string()),  # EQUITY or CASH
        pa.field("label", pa.string()),
        pa.field("asset_class", pa.string()),
        pa.field("security_value", pa.binary()),  # Fernet-encrypted; in security_ccy
        pa.field("security_ccy", pa.string()),
        pa.field("isin", pa.string()),
        pa.field("description", pa.string()),
    ]
)

trading212_snapshot_normalized_schema = pa.schema(
    [
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("account_id", pa.string()),
        pa.field("position_type", pa.string()),  # EQUITY or CASH
        pa.field("label", pa.string()),
        pa.field("name", pa.string()),
        pa.field("asset_class", pa.string()),
        pa.field("security_value", pa.binary()),  # Fernet-encrypted; in security_ccy
        pa.field("security_ccy", pa.string()),
        pa.field("isin", pa.string()),
    ]
)

xtb_snapshot_normalized_schema = pa.schema(
    [
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("account_id", pa.string()),
        pa.field("position_type", pa.string()),  # EQUITY or CASH
        pa.field("label", pa.string()),
        pa.field("name", pa.string()),
        pa.field("asset_class", pa.string()),
        pa.field("security_value", pa.binary()),  # Fernet-encrypted; in security_ccy
        pa.field("security_ccy", pa.string()),
        pa.field("isin", pa.string()),
    ]
)

# --- CDC schema (broker-neutral) ---

cdc_events_normalized_schema = pa.schema(
    [
        # Non-nullable core columns (every CDC row must have these)
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("broker", pa.string()),
        pa.field("account_id", pa.string()),
        pa.field("event_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field(
            "event_type", pa.string()
        ),  # TRADE, DIVIDEND, DEPOSIT, WITHDRAWAL, FEE, TAX, INTEREST, TRANSFER, ADJUSTMENT, UNKNOWN
        pa.field("raw_event_type", pa.string()),  # Broker-native type/status/category
        pa.field("event_datetime", pa.string()),
        pa.field("security_ccy", pa.string()),  # Currency cash_amount is denominated in
        pa.field(
            "instrument_ccy", pa.string()
        ),  # Instrument's trading currency (nullable; null when unknown/N/A)
        pa.field(
            "cash_amount", pa.binary()
        ),  # Fernet-encrypted; signed cash impact in security_ccy
        # Nullable trade/security columns
        pa.field("settle_date", pa.string()),
        pa.field("ticker", pa.string()),
        pa.field("isin", pa.string()),
        pa.field("description", pa.string()),
        pa.field("quantity", pa.binary()),  # Fernet-encrypted
        pa.field("price", pa.binary()),  # Fernet-encrypted
        pa.field("side", pa.string()),
        pa.field("gross_amount", pa.binary()),  # Fernet-encrypted; in security_ccy
        pa.field("fee_amount", pa.binary()),  # Fernet-encrypted; in security_ccy
        pa.field("tax_amount", pa.binary()),  # Fernet-encrypted; in security_ccy
        # Target currency columns (populated by normalize_currency step)
        pa.field(
            "target_fx_rate", pa.binary()
        ),  # Fernet-encrypted; security_ccy → target_ccy rate; nullable
        pa.field(
            "target_value", pa.binary()
        ),  # Fernet-encrypted; cash_amount converted to target_ccy; nullable
        pa.field(
            "target_ccy", pa.string()
        ),  # Always "EUR" (the pipeline target currency); nullable
    ]
)

# --- Consolidated holdings (1) ---

consolidated_holdings_schema = pa.schema(
    [
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("broker", pa.string()),
        pa.field("ticker", pa.string()),
        pa.field("security_value", pa.binary()),  # Fernet-encrypted; in security_ccy
        pa.field("security_ccy", pa.string()),
        pa.field("target_value", pa.binary()),  # Fernet-encrypted; in target_ccy
        pa.field("target_ccy", pa.string()),  # Always "EUR"
        pa.field("identifier", pa.string()),
        pa.field("description", pa.string()),
        pa.field("position_type", pa.string()),  # EQUITY | CASH
    ]
)
