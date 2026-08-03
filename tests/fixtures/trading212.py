"""Trading 212 fixture builders for raw and normalized Delta tables.

Provides factory functions that return realistic ``pa.Table`` objects
matching the actual schemas used by the Trading 212 connector.

The default payloads mirror the real demo API shapes (queried from
``trading212_snapshot_raw`` / ``trading212_snapshot_normalized`` in the
staging environment) so that ``transform_snapshot`` exercised on the raw
fixture reproduces the normalized fixture exactly (round-trip).
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa

from pipeline.crypto import encrypt, encrypt_float, generate_key
from pipeline.normalized.models import trading212_snapshot_normalized_schema
from pipeline.raw.models import RAW_SCHEMA

# Real demo API uses "Trading 212" (mixed case with space) as the broker label.
_T212_BROKER = "Trading 212"


def t212_raw_snapshot(
    summary: dict[str, Any] | None = None,
    positions: list[dict[str, Any]] | None = None,
    instruments: list[dict[str, Any]] | None = None,
    fernet_key: bytes | None = None,
    fetched_at: datetime | None = None,
) -> pa.Table:
    """Build a raw Trading 212 snapshot table with encrypted payloads.

    Default data mirrors the real demo ``trading212_snapshot_raw`` shape:
      * ``summary`` is a nested dict with ``cash`` (dict with
        ``availableToTrade``), ``totalValue``, ``investments`` and ``currency``
        (the demo path staging deploys use, not the scalar-cash live path).
      * ``positions`` is a list of dicts with a nested ``instrument`` object
        (``ticker``/``name``/``isin``/``currency``) and a ``walletImpact``
        object (``currency``/``currentValue``) — matching the real demo keys.
      * ``instruments`` is a LIST of dicts with ``ticker``/``currencyCode``/
        ``name``/``isin``/``shortName`` — the only path production runs. The
        transform guard ``isinstance(instruments_data, list) else []`` silently
        drops a non-list, so a LIST is required to exercise the metadata path.
      * ``broker`` is ``"Trading 212"`` (real demo casing).
    """
    if fernet_key is None:
        fernet_key = generate_key()
    if summary is None:
        summary = {
            "currency": "PLN",
            "totalValue": 5800.0,
            "cash": {
                "availableToTrade": 1500.0,
                "reservedForOrders": 0.0,
                "inPies": 0.0,
            },
            "investments": {"value": 4300.0},
        }
    if positions is None:
        positions = [
            {
                "instrument": {
                    "ticker": "VWCEl_EQ",
                    "name": "Vanguard FTSE All-World UCITS ETF",
                    "isin": "IE00BK5BQT80",
                    "currency": "EUR",
                },
                "quantity": 25.0,
                "currentPrice": 100.0,
                "walletImpact": {
                    "currency": "PLN",
                    "currentValue": 2500.0,
                    "totalCost": 2400.0,
                    "unrealizedProfitLoss": 100.0,
                    "fxImpact": 0.0,
                },
            },
            {
                "instrument": {
                    "ticker": "AAPLu_EQ",
                    "name": "Apple Inc",
                    "isin": "US0378331005",
                    "currency": "USD",
                },
                "quantity": 10.0,
                "currentPrice": 180.0,
                "walletImpact": {
                    "currency": "PLN",
                    "currentValue": 1800.0,
                    "totalCost": 1700.0,
                    "unrealizedProfitLoss": 100.0,
                    "fxImpact": 0.0,
                },
            },
        ]
    if instruments is None:
        instruments = [
            {
                "ticker": "VWCEl_EQ",
                "currencyCode": "EUR",
                "name": "Vanguard FTSE All-World UCITS ETF",
                "isin": "IE00BK5BQT80",
                "shortName": "VWCE",
                "type": "STOCK",
                "workingScheduleId": 56,
            },
            {
                "ticker": "AAPLu_EQ",
                "currencyCode": "USD",
                "name": "Apple Inc",
                "isin": "US0378331005",
                "shortName": "AAPL",
                "type": "STOCK",
                "workingScheduleId": 56,
            },
        ]

    now = fetched_at if fetched_at is not None else datetime.now(UTC)
    summary_bytes = json.dumps(summary).encode("utf-8")
    positions_bytes = json.dumps(positions).encode("utf-8")
    instruments_bytes = json.dumps(instruments).encode("utf-8")

    return pa.table(
        {
            "fetched_at": [now, now, now],
            "broker": [_T212_BROKER, _T212_BROKER, _T212_BROKER],
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
    account_id: str = "",
    fetched_at: datetime | None = None,
) -> pa.Table:
    """Build a normalized Trading 212 snapshot table with encrypted values.

    Default data mirrors the real demo ``trading212_snapshot_normalized`` shape
    and is exactly what ``transform_snapshot(t212_raw_snapshot(...))`` produces
    (round-trip): 2 equities (``VWCEl_EQ``, ``AAPLu_EQ``) + 1 cash entry
    (``CASH PLN``), all ``security_ccy`` = ``PLN`` (the wallet currency the
    demo account uses), ``asset_class`` = ``EQUITY``/``CASH`` (the values the
    T212 transform hardcodes — never ``STK``), ``account_id`` = ``""`` (real
    demo T212 snapshots carry an empty account id) and labels using the real
    single-lowercase-letter suffix format (``VWCEl_EQ``).
    """
    if fernet_key is None:
        fernet_key = generate_key()
    now = fetched_at if fetched_at is not None else datetime.now(UTC)
    return pa.table(
        {
            "fetched_at": [now, now, now],
            "account_id": [account_id, account_id, account_id],
            "position_type": ["EQUITY", "EQUITY", "CASH"],
            "label": ["VWCEl_EQ", "AAPLu_EQ", "CASH PLN"],
            "name": [
                "Vanguard FTSE All-World UCITS ETF",
                "Apple Inc",
                "Cash PLN",
            ],
            "asset_class": ["EQUITY", "EQUITY", "CASH"],
            "security_value": [
                encrypt_float(2500.0, fernet_key),
                encrypt_float(1800.0, fernet_key),
                encrypt_float(1500.0, fernet_key),
            ],
            "security_ccy": ["PLN", "PLN", "PLN"],
            "isin": ["IE00BK5BQT80", "US0378331005", ""],
        },
        schema=trading212_snapshot_normalized_schema,
    )
