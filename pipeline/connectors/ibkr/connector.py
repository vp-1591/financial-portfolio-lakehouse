"""IBKR connector: BrokerConnector implementation."""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from pipeline.connectors.base import BrokerConnector
from pipeline.connectors.ibkr import fetch, transform
from pipeline.connectors.registry import register


@register
class IbkrConnector:
    name = "ibkr"
    display_name = "IBKR"

    def fetch_snapshot(self, **kwargs: Any) -> pa.Table:
        flex_token = kwargs.get("flex_token")
        if flex_token:
            return fetch.fetch_snapshot_via_flex(
                token=flex_token,
                query_id=kwargs.get("flex_query_id", "1554188"),
                base_url=kwargs.get("flex_base_url", "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"),
                timeout=kwargs.get("flex_timeout", 30.0),
                retries=kwargs.get("flex_retries", 6),
                delay=kwargs.get("flex_delay", 3.0),
            )
        return fetch.fetch_snapshot(**kwargs)

    def fetch_cdc(self, **kwargs: Any) -> pa.Table:
        return fetch.fetch_cdc(**kwargs)

    def transform_snapshot(self, raw: pa.Table, fernet_key: bytes, **kwargs: Any) -> pa.Table:
        base_currency_override = kwargs.get("base_currency_override")

        # Check if this is a Flex-based snapshot by looking for "flex" source rows
        if "flex" in raw.column("source").to_pylist():
            return transform._transform_flex_snapshot(
                raw, fernet_key, base_currency_override=base_currency_override
            )

        return transform.transform_snapshot(raw, fernet_key, base_currency_override=base_currency_override)

    def transform_cdc(self, raw: pa.Table, fernet_key: bytes) -> pa.Table:
        return transform.transform_cdc(raw, fernet_key)