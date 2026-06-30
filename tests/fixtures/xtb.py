"""XTB fixture builders for raw and normalized Delta tables.

Provides factory functions that return realistic ``pa.Table`` objects
matching the actual schemas used by the XTB connector.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa

from pipeline.crypto import encrypt, encrypt_float, generate_key
from pipeline.raw.models import RAW_SCHEMA
from pipeline.normalized.models import xtb_snapshot_normalized_schema


def xtb_raw_snapshot(
    account_id: str = "XTB-12345",
    positions: list[dict[str, Any]] | None = None,
    net_worth: dict[str, Any] | None = None,
    fernet_key: bytes | None = None,
) -> pa.Table:
    """Build a raw XTB snapshot table with encrypted payloads.

    Default data includes 2 equity positions and PLN cash.
    """
    if fernet_key is None:
        fernet_key = generate_key()
    if positions is None:
        positions = [
            {
                "symbol": "VWCE.DE",
                "isin": "IE00BK5BQT80",
                "label": "VWCE.DE",
                "name": "Vanguard FTSE All-World UCITS ETF",
                "volume": 10.0,
                "value": 1000.0,
                "currency": "EUR",
                "asset_class": "EQUITY",
            },
            {
                "symbol": "CDR.PL",
                "isin": "PL9999900006",
                "label": "CDR.PL",
                "name": "CD Projekt",
                "volume": 5.0,
                "value": 2500.0,
                "currency": "PLN",
                "asset_class": "EQUITY",
            },
        ]
    if net_worth is None:
        net_worth = {
            "total_value": 50000.0,
            "currency": "PLN",
        }

    now = datetime.now(timezone.utc)
    payload = {"positions": positions, "net_worth": net_worth}
    payload_bytes = json.dumps(payload).encode("utf-8")
    encrypted_payload = encrypt(payload_bytes, fernet_key)

    return pa.table(
        {
            "fetched_at": [now],
            "broker": ["xtb"],
            "source": ["OPEN POSITION"],
            "payload": [encrypted_payload],
            "payload_hash": [hashlib.sha256(payload_bytes).hexdigest()],
            "account_id": [account_id],
            "source_file": ["report.xlsx"],
        },
        schema=RAW_SCHEMA,
    )


def xtb_normalized_snapshot(
    fernet_key: bytes | None = None,
    account_id: str = "XTB-12345",
) -> pa.Table:
    """Build a normalized XTB snapshot table with encrypted values.

    Default data: 2 equities (VWCE.DE, CDR.PL) + 1 cash entry (PLN).
    """
    if fernet_key is None:
        fernet_key = generate_key()
    now = datetime.now(timezone.utc)
    return pa.table(
        {
            "fetched_at": [now, now, now],
            "account_id": [account_id, account_id, account_id],
            "position_type": ["EQUITY", "EQUITY", "CASH"],
            "label": ["VWCE.DE", "CDR.PL", "CASH:PLN"],
            "name": [
                "Vanguard FTSE All-World UCITS ETF",
                "CD Projekt",
                "Cash PLN",
            ],
            "asset_class": ["STK", "STK", "CASH"],
            "currency": ["EUR", "PLN", "PLN"],
            "value": [
                encrypt_float(1000.0, fernet_key),
                encrypt_float(2500.0, fernet_key),
                encrypt_float(5000.0, fernet_key),
            ],
            "value_currency": ["EUR", "PLN", "PLN"],
            "isin": ["IE00BK5BQT80", "PL9999900006", ""],
        },
        schema=xtb_snapshot_normalized_schema,
    )
