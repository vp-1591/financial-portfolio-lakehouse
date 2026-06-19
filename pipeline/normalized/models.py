"""PyArrow schemas for the 7 normalized tables."""

from __future__ import annotations

import pyarrow as pa

# --- Snapshot schemas (3) ---

ibkr_snapshot_normalized_schema = pa.schema([
    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    pa.field("account_id", pa.string()),
    pa.field("position_type", pa.string()),  # EQUITY or CASH
    pa.field("label", pa.string()),
    pa.field("asset_class", pa.string()),
    pa.field("currency", pa.string()),
    pa.field("value", pa.binary()),  # Fernet-encrypted
    pa.field("value_currency", pa.string()),
    pa.field("conid", pa.string()),
    pa.field("isin", pa.string()),
    pa.field("description", pa.string()),
    pa.field("security_currency", pa.string()),
])

trading212_snapshot_normalized_schema = pa.schema([
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
])

xtb_snapshot_normalized_schema = pa.schema([
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
])

# --- CDC schemas (3) ---

trading212_cdc_normalized_schema = pa.schema([
    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    pa.field("account_id", pa.string()),
    pa.field("event_type", pa.string()),  # ORDER, DIVIDEND, TRANSACTION
    pa.field("event_id", pa.string()),
    pa.field("ticker", pa.string()),
    pa.field("isin", pa.string()),
    pa.field("currency", pa.string()),
    pa.field("value", pa.binary()),  # Fernet-encrypted
    pa.field("quantity", pa.binary()),  # Fernet-encrypted
    pa.field("event_date", pa.string()),
])

xtb_cdc_normalized_schema = pa.schema([
    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    pa.field("account_id", pa.string()),
    pa.field("operation_id", pa.string()),
    pa.field("operation_type", pa.string()),
    pa.field("amount", pa.binary()),  # Fernet-encrypted
    pa.field("currency", pa.string()),
    pa.field("comment", pa.string()),
    pa.field("operation_date", pa.string()),
])

ibkr_cdc_normalized_schema = pa.schema([
    # Minimal schema — full columns added when IBKR CDC fetcher is implemented
    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    pa.field("account_id", pa.string()),
    pa.field("payload", pa.binary()),  # Fernet-encrypted
    pa.field("source", pa.string()),
])

# --- Consolidated holdings (1) ---

consolidated_holdings_schema = pa.schema([
    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    pa.field("broker", pa.string()),
    pa.field("ticker", pa.string()),
    pa.field("currency", pa.string()),
    pa.field("value", pa.binary()),  # Fernet-encrypted
    pa.field("identifier", pa.string()),
    pa.field("security_currency", pa.string()),
    pa.field("description", pa.string()),
])