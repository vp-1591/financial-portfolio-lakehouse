"""Trading 212 connector: fetch raw snapshot and events data from the API."""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime

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
    """Fetch Trading 212 account summary and positions."""
    client = Trading212Client(
        base_url,
        api_key=api_key,
        api_secret=api_secret,
        timeout=timeout,
        capture_raw=True,
    )

    now = datetime.now(UTC)
    fetched_ats: list[datetime] = []
    brokers: list[str] = []
    sources: list[str] = []
    payloads: list[bytes] = []
    payload_hashes: list[str] = []
    account_ids: list[str | None] = []

    # Fetch account summary
    client.captured_responses.clear()
    client.account_summary()
    for path, raw_bytes in client.captured_responses:
        fetched_ats.append(now)
        brokers.append("Trading 212")
        sources.append(path)
        payloads.append(raw_bytes)
        payload_hashes.append(hashlib.sha256(raw_bytes).hexdigest())
        account_ids.append(None)

    # Fetch positions
    client.captured_responses.clear()
    client.positions()
    for path, raw_bytes in client.captured_responses:
        fetched_ats.append(now)
        brokers.append("Trading 212")
        sources.append(path)
        payloads.append(raw_bytes)
        payload_hashes.append(hashlib.sha256(raw_bytes).hexdigest())
        account_ids.append(None)

    return pa.table(
        {
            "fetched_at": fetched_ats,
            "broker": brokers,
            "source": sources,
            "payload": payloads,
            "payload_hash": payload_hashes,
            "account_id": account_ids,
        },
        schema=RAW_SCHEMA,
    )


def fetch_events(
    api_key: str,
    api_secret: str,
    base_url: str = "https://live.trading212.com/api/v0",
    timeout: float = 20.0,
) -> pa.Table:
    """Fetch Trading 212 events (orders, dividends, transactions)."""
    client = Trading212Client(
        base_url,
        api_key=api_key,
        api_secret=api_secret,
        timeout=timeout,
        capture_raw=True,
    )

    now = datetime.now(UTC)
    fetched_ats: list[datetime] = []
    brokers: list[str] = []
    sources: list[str] = []
    payloads: list[bytes] = []
    payload_hashes: list[str] = []
    account_ids: list[str | None] = []

    # Fail loud: ANY endpoint failure aborts the fetch. The transform
    # normalizes the current fetch's events (the single bronze read, AD-6), so
    # a silently skipped endpoint's events would be missing from this run's
    # normalized output — partial data must abort the run (see ADR 0116).
    failed_endpoints: list[str] = []
    for endpoint_name, fetch_method in [
        ("orders", client.orders),
        ("dividends", client.dividends),
        ("transactions", client.transactions),
    ]:
        client.captured_responses.clear()
        try:
            fetch_method()
        except Exception as exc:
            logger.warning(
                "Trading 212 events endpoint %s failed: %s", endpoint_name, exc
            )
            failed_endpoints.append(endpoint_name)
            continue

        for path, raw_bytes in client.captured_responses:
            fetched_ats.append(now)
            brokers.append("Trading 212")
            sources.append(path)
            payloads.append(raw_bytes)
            payload_hashes.append(hashlib.sha256(raw_bytes).hexdigest())
            account_ids.append(None)

    if failed_endpoints:
        raise RuntimeError(
            "Trading 212 events: endpoint(s) failed: " + ", ".join(failed_endpoints)
        )

    if not payloads:
        raise RuntimeError(
            "Trading 212 events: all endpoints (orders, dividends, transactions) "
            "failed or returned no data"
        )

    return pa.table(
        {
            "fetched_at": fetched_ats,
            "broker": brokers,
            "source": sources,
            "payload": payloads,
            "payload_hash": payload_hashes,
            "account_id": account_ids,
        },
        schema=RAW_SCHEMA,
    )
