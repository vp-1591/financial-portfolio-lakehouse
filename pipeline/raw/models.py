"""PyArrow schema for the raw layer.

One Delta table per broker (``raw/{broker}``) with a single shared schema:
fetched_at, broker, source, payload (Fernet-encrypted), payload_hash, and
source_file. The ``source`` column discriminates snapshot vs events rows
(ADR 0047, AD-1).
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
        pa.field("source_file", pa.string()),
    ]
)
