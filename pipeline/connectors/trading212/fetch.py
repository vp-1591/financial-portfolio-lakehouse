"""Trading 212 connector: fetch raw snapshot and CDC data from the API."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone

import pyarrow as pa

from pipeline.connectors.trading212.client import Trading212Client
from pipeline.raw.models import RAW_SCHEMA

logger = logging.getLogger(__name__)


def fetch_snapshot(
    api_key: str,
    api_secret: str,
    base_url: str = "https://live.trading212.com/api/v0",
    timeout: float = 20.0,
) -> pa.Table:
    """Fetch Trading 212 account summary, positions, and instruments metadata."""
    client = Trading212Client(
        base_url,
        api_key=api_key,
        api_secret=api_secret,
        timeout=timeout,
        capture_raw=True,
    )

    now = datetime.now(timezone.utc)
    fetched_ats: list[datetime] = []
    brokers: list[str] = []
    sources: list[str] = []
    payloads: list[bytes] = []
    payload_hashes: list[str] = []
    source_files: list[str] = []

    # Fetch account summary
    client.captured_responses.clear()
    client.account_summary()
    for path, raw_bytes in client.captured_responses:
        fetched_ats.append(now)
        brokers.append("Trading 212")
        sources.append(path)
        payloads.append(raw_bytes)
        payload_hashes.append(hashlib.sha256(raw_bytes).hexdigest())
        source_files.append("")

    # Fetch positions
    client.captured_responses.clear()
    client.positions()
    for path, raw_bytes in client.captured_responses:
        fetched_ats.append(now)
        brokers.append("Trading 212")
        sources.append(path)
        payloads.append(raw_bytes)
        payload_hashes.append(hashlib.sha256(raw_bytes).hexdigest())
        source_files.append("")

    # Fetch instruments metadata (always included for best data quality)
    client.captured_responses.clear()
    client.instruments()
    for path, raw_bytes in client.captured_responses:
        fetched_ats.append(now)
        brokers.append("Trading 212")
        sources.append(path)
        payloads.append(raw_bytes)
        payload_hashes.append(hashlib.sha256(raw_bytes).hexdigest())
        source_files.append("")

    return pa.table(
        {
            "fetched_at": fetched_ats,
            "broker": brokers,
            "source": sources,
            "payload": payloads,
            "payload_hash": payload_hashes,
            "source_file": source_files,
        },
        schema=RAW_SCHEMA,
    )


def fetch_cdc(
    api_key: str,
    api_secret: str,
    base_url: str = "https://live.trading212.com/api/v0",
    timeout: float = 20.0,
) -> pa.Table:
    """Fetch Trading 212 CDC events (orders, dividends, transactions)."""
    client = Trading212Client(
        base_url,
        api_key=api_key,
        api_secret=api_secret,
        timeout=timeout,
        capture_raw=True,
    )

    now = datetime.now(timezone.utc)
    fetched_ats: list[datetime] = []
    brokers: list[str] = []
    sources: list[str] = []
    payloads: list[bytes] = []
    payload_hashes: list[str] = []
    source_files: list[str] = []

    for endpoint_name, fetch_method in [
        ("orders", client.orders),
        ("dividends", client.dividends),
        ("transactions", client.transactions),
    ]:
        client.captured_responses.clear()
        try:
            fetch_method()
        except Exception as exc:
            logger.warning("Trading 212 CDC endpoint %s failed: %s", endpoint_name, exc)
            continue

        for path, raw_bytes in client.captured_responses:
            fetched_ats.append(now)
            brokers.append("Trading 212")
            sources.append(path)
            payloads.append(raw_bytes)
            payload_hashes.append(hashlib.sha256(raw_bytes).hexdigest())
            source_files.append("")

    if not payloads:
        raise RuntimeError(
            "Trading 212 CDC: all endpoints (orders, dividends, transactions) "
            "failed or returned no data"
        )

    return pa.table(
        {
            "fetched_at": fetched_ats,
            "broker": brokers,
            "source": sources,
            "payload": payloads,
            "payload_hash": payload_hashes,
            "source_file": source_files,
        },
        schema=RAW_SCHEMA,
    )
