"""IBKR connector: fetch raw snapshot and CDC data via the Flex Web Service API."""

from __future__ import annotations

import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa

from pipeline.connectors.ibkr.client import IbkrFlexClient
from pipeline.raw.models import RAW_SCHEMA


def fetch_snapshot_via_flex(
    token: str,
    query_id: str,
    base_url: str = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService",
    timeout: float = 30.0,
    retries: int = 6,
    delay: float = 3.0,
) -> pa.Table:
    """Fetch IBKR positions and cash data via the Flex Web Service API.

    No local gateway process or browser login is required — only a Flex
    token and query ID.

    The raw XML response is stored as encrypted payloads in the raw-layer
    table, using ``source="flex"`` to distinguish Flex data during
    transformation.
    """
    client = IbkrFlexClient(
        token=token,
        query_id=query_id,
        base_url=base_url,
        timeout=timeout,
    )

    ref_code = client.request_report()
    root = client.fetch_report(ref_code, retries=retries, delay=delay)

    xml_bytes = ET.tostring(root, encoding="unicode").encode("utf-8")

    now = datetime.now(timezone.utc)
    payload_hash = hashlib.sha256(xml_bytes).hexdigest()

    return pa.table(
        {
            "fetched_at": [now],
            "broker": ["IBKR"],
            "source": ["flex"],
            "payload": [xml_bytes],
            "payload_hash": [payload_hash],
            "source_file": [""],
        },
        schema=RAW_SCHEMA,
    )


def fetch_cdc_via_flex(
    token: str,
    query_id: str,
    base_url: str = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService",
    timeout: float = 30.0,
    retries: int = 6,
    delay: float = 3.0,
) -> pa.Table:
    """Fetch IBKR CDC (activity) data via the Flex Web Service API.

    Uses the same Flex query mechanism as snapshots, but with a source
    of ``"flex_cdc"`` to distinguish CDC activity data during transformation.
    The Flex query should include Trades, CashTransactions, Transfers,
    and TransactionFees sections.
    """
    client = IbkrFlexClient(
        token=token,
        query_id=query_id,
        base_url=base_url,
        timeout=timeout,
    )

    ref_code = client.request_report()
    root = client.fetch_report(ref_code, retries=retries, delay=delay)

    xml_bytes = ET.tostring(root, encoding="unicode").encode("utf-8")

    now = datetime.now(timezone.utc)
    payload_hash = hashlib.sha256(xml_bytes).hexdigest()

    return pa.table(
        {
            "fetched_at": [now],
            "broker": ["IBKR"],
            "source": ["flex_cdc"],
            "payload": [xml_bytes],
            "payload_hash": [payload_hash],
            "source_file": [""],
        },
        schema=RAW_SCHEMA,
    )


def fetch_cdc(**kwargs: Any) -> pa.Table:
    """IBKR CDC fetch via Flex Web Service."""
    return fetch_cdc_via_flex(**kwargs)
