"""IBKR connector: fetch raw snapshot data from the IBKR Client Portal API."""

from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa

from pipeline.connectors.ibkr.client import (
    IbkrClient,
    IbkrError,
    account_id as get_account_id,
)
from pipeline.raw.models import RAW_SCHEMA


def fetch_snapshot(
    base_url: str = "https://localhost:5000/v1/api",
    account: str | None = None,
    verify_tls: bool = False,
    timeout: float = 20.0,
    skip_auth_check: bool = False,
    require_brokerage_session: bool = False,
) -> pa.Table:
    """Fetch IBKR positions and ledger data and return a raw-layer table.

    Each API response is captured as a separate row with the endpoint
    path stored in the ``source`` column.
    """
    client = IbkrClient(base_url, verify_tls=verify_tls, timeout=timeout, capture_raw=True)

    if not skip_auth_check:
        if require_brokerage_session:
            status = client.auth_status()
            if not status.get("authenticated"):
                message = status.get("message") or "not authenticated"
                raise IbkrError(
                    f"IBKR brokerage session is not authenticated ({message}). "
                    "Open the gateway in your browser and sign in first."
                )
        else:
            status = client.sso_validate()
            if not status.get("RESULT"):
                raise IbkrError(
                    "IBKR gateway SSO session is not valid. Open the gateway in your "
                    "browser and sign in first."
                )

    accounts = client.accounts()
    account_ids = [account] if account else [get_account_id(a) for a in accounts]

    now = datetime.now(timezone.utc)
    fetched_ats: list[datetime] = []
    brokers: list[str] = []
    sources: list[str] = []
    payloads: list[bytes] = []
    payload_hashes: list[str] = []
    account_ids_col: list[str] = []
    source_files: list[str] = []

    import hashlib

    for acct_id in account_ids:
        # Fetch positions
        client.captured_responses.clear()
        client.positions(acct_id)
        for path, raw_bytes in client.captured_responses:
            fetched_ats.append(now)
            brokers.append("IBKR")
            sources.append(path)
            payloads.append(raw_bytes)
            payload_hashes.append(hashlib.sha256(raw_bytes).hexdigest())
            account_ids_col.append(acct_id)
            source_files.append("")

        # Fetch ledger
        client.captured_responses.clear()
        client.ledger(acct_id)
        for path, raw_bytes in client.captured_responses:
            fetched_ats.append(now)
            brokers.append("IBKR")
            sources.append(path)
            payloads.append(raw_bytes)
            payload_hashes.append(hashlib.sha256(raw_bytes).hexdigest())
            account_ids_col.append(acct_id)
            source_files.append("")

    # Also capture contract info for any conids found
    # This is done as a secondary enrichment pass, not stored in raw

    return pa.table(
        {
            "fetched_at": fetched_ats,
            "broker": brokers,
            "source": sources,
            "payload": payloads,
            "payload_hash": payload_hashes,
            "account_id": account_ids_col,
            "source_file": source_files,
        },
        schema=RAW_SCHEMA,
    )


def fetch_cdc(**kwargs: object) -> pa.Table:
    """IBKR CDC is not yet implemented."""
    raise NotImplementedError("IBKR CDC fetching is not yet implemented")