"""PyArrow schema for the analytics table."""

from __future__ import annotations

import pyarrow as pa

portfolio_allocation_schema = pa.schema(
    [
        pa.field("calculated_at", pa.timestamp("us", tz="UTC")),
        pa.field("ticker", pa.string()),
        pa.field("percentage", pa.float64()),
        pa.field("broker", pa.string()),
        pa.field("identifier", pa.string()),
        pa.field("security_currency", pa.string()),
        pa.field("description", pa.string()),
    ]
)
