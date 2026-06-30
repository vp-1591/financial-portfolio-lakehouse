"""Trading 212 connector: BrokerConnector implementation."""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from pipeline.connectors.trading212 import fetch, transform
from pipeline.connectors.registry import register


@register
class Trading212Connector:
    name = "trading212"
    display_name = "Trading 212"

    def fetch_snapshot(self, **kwargs: Any) -> pa.Table:
        return fetch.fetch_snapshot(**kwargs)

    def fetch_cdc(self, **kwargs: Any) -> pa.Table:
        return fetch.fetch_cdc(**kwargs)

    def transform_snapshot(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        return transform.transform_snapshot(raw, fernet_key)

    def transform_cdc(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        return transform.transform_cdc(raw, fernet_key)
