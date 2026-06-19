"""BrokerConnector protocol definition."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import pyarrow as pa


@runtime_checkable
class BrokerConnector(Protocol):
    """Protocol that every broker connector must implement."""

    name: str  # e.g. "ibkr", "trading212", "xtb"
    display_name: str  # e.g. "IBKR", "Trading 212", "XTB"

    def fetch_snapshot(self, **kwargs: object) -> pa.Table:
        """Fetch a raw snapshot from the broker and return a raw-layer PyArrow table."""
        ...

    def fetch_cdc(self, **kwargs: object) -> pa.Table:
        """Fetch CDC (change data capture) events from the broker.

        Brokers that do not yet support CDC should raise ``NotImplementedError``.
        """
        ...

    def transform_snapshot(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        """Transform a raw snapshot table into the normalized schema."""
        ...

    def transform_cdc(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        """Transform a raw CDC table into the normalized schema.

        Brokers that do not yet support CDC should raise ``NotImplementedError``.
        """
        ...