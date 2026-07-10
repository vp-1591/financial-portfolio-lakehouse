"""PyArrow schemas for normalized tables."""

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
        pa.field("currency", pa.string()),
        pa.field("value", pa.binary()),  # Fernet-encrypted
        pa.field("value_currency", pa.string()),
        pa.field("isin", pa.string()),
        pa.field("description", pa.string()),
        pa.field("security_currency", pa.string()),
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
        pa.field("currency", pa.string()),
        pa.field("value", pa.binary()),  # Fernet-encrypted
        pa.field("value_currency", pa.string()),
        pa.field("isin", pa.string()),
        pa.field("security_currency", pa.string()),
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
        pa.field("currency", pa.string()),
        pa.field("value", pa.binary()),  # Fernet-encrypted
        pa.field("value_currency", pa.string()),
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
        pa.field("currency", pa.string()),
        pa.field(
            "cash_amount", pa.binary()
        ),  # Fernet-encrypted; signed cash impact in native currency
        # Nullable trade/security columns
        pa.field("settle_date", pa.string()),
        pa.field("ticker", pa.string()),
        pa.field("isin", pa.string()),
        pa.field("description", pa.string()),
        pa.field("quantity", pa.binary()),  # Fernet-encrypted
        pa.field("price", pa.binary()),  # Fernet-encrypted
        pa.field("side", pa.string()),
        pa.field("gross_amount", pa.binary()),  # Fernet-encrypted
        pa.field("fee_amount", pa.binary()),  # Fernet-encrypted
        pa.field("tax_amount", pa.binary()),  # Fernet-encrypted
        pa.field("net_amount", pa.binary()),  # Fernet-encrypted
        pa.field("base_currency", pa.string()),
        pa.field("fx_rate_to_base", pa.binary()),  # Fernet-encrypted
        pa.field("amount_base", pa.binary()),  # Fernet-encrypted
    ]
)

# --- Consolidated holdings (1) ---

consolidated_holdings_schema = pa.schema(
    [
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("broker", pa.string()),
        pa.field("ticker", pa.string()),
        pa.field("currency", pa.string()),
        pa.field("value", pa.binary()),  # Fernet-encrypted
        pa.field("identifier", pa.string()),
        pa.field("security_currency", pa.string()),
        pa.field("description", pa.string()),
    ]
)
