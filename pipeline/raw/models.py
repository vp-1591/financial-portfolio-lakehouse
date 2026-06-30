"""PyArrow schemas for the 6 raw tables.

All raw tables share the same schema shape: fetched_at, broker, source,
payload (Fernet-encrypted), payload_hash, account_id, and source_file.
"""

from __future__ import annotations

import pyarrow as pa

# Uniform schema for all raw tables
RAW_SCHEMA = pa.schema(
    [
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("broker", pa.string()),
        pa.field("source", pa.string()),
        pa.field("payload", pa.binary()),
        pa.field("payload_hash", pa.string()),
        pa.field("account_id", pa.string()),
        pa.field("source_file", pa.string()),
    ]
)

# Individual schema aliases for clarity when writing to specific table paths
ibkr_snapshot_raw_schema = RAW_SCHEMA
ibkr_cdc_raw_schema = RAW_SCHEMA
trading212_snapshot_raw_schema = RAW_SCHEMA
trading212_cdc_raw_schema = RAW_SCHEMA
xtb_snapshot_raw_schema = RAW_SCHEMA
xtb_cdc_raw_schema = RAW_SCHEMA
