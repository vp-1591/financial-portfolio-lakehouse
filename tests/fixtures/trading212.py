"""Trading 212 fixture builders for raw and normalized Delta tables.

Provides factory functions that return realistic ``pa.Table`` objects
matching the actual schemas used by the Trading 212 connector.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa

from pipeline.crypto import encrypt, encrypt_float, generate_key
from pipeline.raw.models import RAW_SCHEMA
from pipeline.normalized.models import trading212_snapshot_normalized_schema


def t212_raw_snapshot(
    summary: dict[str, Any] | None = None,
    positions: list[dict[str, Any]] | None = None,
    instruments: dict[str, Any] | None = None,
    fernet_key: bytes | None = None,
) -> pa.Table:
    """Build a raw Trading 212 snapshot table with encrypted payloads.

    Default data includes 2 equity positions and a GBP cash balance.
    """
    if fernet_key is None:
        fernet_key = generate_key()
    if summary is None:
        summary = {
            "free": 1500.0,
            "invested": 8000.0,
            "result": 500.0,
            "currency": "GBP",
        }
    if positions is None:
        positions = [
            {
                "ticker": "VWCE",
                "isin": "IE00BK5BQT80",
                "quantity": 25.0,
                "currentPrice": 100.0,
                "ppl": 500.0,
                "fxCurrency": "EUR",
                "value": 2500.0,
            },
            {
                "ticker": "AAPL",
                "isin": "US0378331005",
                "quantity": 10.0,
                "currentPrice": 180.0,
                "ppl": 200.0,
                "fxCurrency": "USD",
                "value": 1800.0,
            },
        ]
    if instruments is None:
        instruments = {
            "IE00BK5BQT80": {"shortName": "VWCE", "currency": "EUR"},
            "US0378331005": {"shortName": "AAPL", "currency": "USD"},
        }

    now = datetime.now(timezone.utc)
    summary_bytes = json.dumps(summary).encode("utf-8")
    positions_bytes = json.dumps(positions).encode("utf-8")
    instruments_bytes = json.dumps(instruments).encode("utf-8")

    return pa.table(
        {
            "fetched_at": [now, now, now],
            "broker": ["trading212", "trading212", "trading212"],
            "source": [
                "/equity/account/summary",
                "/equity/positions",
                "/equity/metadata/instruments",
            ],
            "payload": [
                encrypt(summary_bytes, fernet_key),
                encrypt(positions_bytes, fernet_key),
                encrypt(instruments_bytes, fernet_key),
            ],
            "payload_hash": [
                hashlib.sha256(summary_bytes).hexdigest(),
                hashlib.sha256(positions_bytes).hexdigest(),
                hashlib.sha256(instruments_bytes).hexdigest(),
            ],
            "source_file": ["", "", ""],
        },
        schema=RAW_SCHEMA,
    )


def t212_normalized_snapshot(
    fernet_key: bytes | None = None,
    account_id: str = "T212-DEMO",
) -> pa.Table:
    """Build a normalized Trading 212 snapshot table with encrypted values.

    Default data: 2 equities (VWCE, AAPL) + 1 cash entry (GBP).
    """
    if fernet_key is None:
        fernet_key = generate_key()
    now = datetime.now(timezone.utc)
    return pa.table(
        {
            "fetched_at": [now, now, now],
            "account_id": [account_id, account_id, account_id],
            "position_type": ["EQUITY", "EQUITY", "CASH"],
            "label": ["VWCE_DE_EQ", "AAPL_US_EQ", "CASH:GBP"],
            "name": [
                "Vanguard FTSE All-World UCITS ETF",
                "Apple Inc",
                "Cash GBP",
            ],
            "asset_class": ["STK", "STK", "CASH"],
            "currency": ["EUR", "USD", "GBP"],
            "value": [
                encrypt_float(2500.0, fernet_key),
                encrypt_float(1800.0, fernet_key),
                encrypt_float(1500.0, fernet_key),
            ],
            "value_currency": ["EUR", "USD", "GBP"],
            "isin": ["IE00BK5BQT80", "US0378331005", ""],
            "security_currency": ["EUR", "USD", "GBP"],
        },
        schema=trading212_snapshot_normalized_schema,
    )
