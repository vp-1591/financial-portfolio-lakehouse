"""Trading 212 fixture builders for raw and normalized Delta tables.

Provides factory functions that return realistic ``pa.Table`` objects
matching the actual schemas used by the Trading 212 connector.

The default payloads mirror the real demo API shapes (queried from
``trading212_raw`` / ``trading212_snapshot_normalized`` in the staging
environment) so that ``transform_snapshot`` exercised on the raw fixture
reproduces the normalized fixture exactly (round-trip). Since the single-bronze
convention (AD-1), all of a broker's raw rows — snapshot and events alike —
live in one ``raw/{broker}`` table (alias ``{broker}_raw``) discriminated by
``source``; the snapshot fixture is ``RAW_SCHEMA``-shaped and concatenates with
an events fixture into ``t212_raw_merged`` for merged-table tests.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa

from pipeline.crypto import encrypt, encrypt_float, generate_key
from pipeline.normalized.models import snapshot_normalized_schema
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

    Default data mirrors the real demo ``trading212_raw`` shape:
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
    (``CASH PLN``). Equities use their instrument trading currency
    (``EUR``/``USD``) for ``security_ccy`` — the transform pairs
    ``currentPrice * quantity`` with the instrument currency; the CASH row
    uses the wallet currency (``PLN``). ``asset_class`` = ``EQUITY``/``CASH``
    (the values the T212 transform hardcodes — never ``STK``),
    ``account_id`` = ``""`` (real demo T212 snapshots carry an empty account id)
    and labels using the real single-lowercase-letter suffix format
    (``VWCEl_EQ``).
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
            "description": [
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
            "security_ccy": ["EUR", "USD", "PLN"],
            "isin": ["IE00BK5BQT80", "US0378331005", ""],
        },
        schema=snapshot_normalized_schema,
    )


# One realistic historical-order event (nested order/fill per the T212 API
# spec) mirroring the events fixture the transform tests build inline.
_T212_ORDER_EVENT = {
    "order": {
        "id": 12345,
        "ticker": "AAPL_US_EQ",
        "side": "BUY",
        "currency": "USD",
        "createdAt": "2024-01-15T10:30:00Z",
        "instrument": {
            "ticker": "AAPL_US_EQ",
            "isin": "US0378331007",
            "name": "Apple Inc.",
            "currency": "USD",
        },
        "filledQuantity": 10,
        "value": 1500.0,
        "filledValue": 1500.0,
    },
    "fill": {
        "id": 67890,
        "quantity": 10,
        "price": 150.0,
        "filledAt": "2024-01-15T10:30:01Z",
        "walletImpact": {
            "currency": "USD",
            "fxRate": 1.0,
            "netValue": 1500.0,
            "realisedProfitLoss": 0,
            "taxes": [],
        },
    },
}


def t212_raw_events(
    events: list[dict[str, Any]] | None = None,
    source: str = "/equity/history/orders",
    fernet_key: bytes | None = None,
    fetched_at: datetime | None = None,
) -> pa.Table:
    """Build a raw Trading 212 events table (one encrypted payload).

    Default data is one realistic ``/equity/history/orders`` TRADE event
    mirroring the T212 API spec. Under the single-bronze convention (AD-1)
    events rows land in the SAME ``raw/trading212`` table as snapshot rows,
    discriminated by ``source``.
    """
    if fernet_key is None:
        fernet_key = generate_key()
    now = fetched_at if fetched_at is not None else datetime.now(UTC)
    if events is None:
        events = [_T212_ORDER_EVENT]
    payload = json.dumps(events).encode("utf-8")
    return pa.table(
        {
            "fetched_at": [now],
            "broker": [_T212_BROKER],
            "source": [source],
            "payload": [encrypt(payload, fernet_key)],
            "payload_hash": [hashlib.sha256(payload).hexdigest()],
            "source_file": [""],
        },
        schema=RAW_SCHEMA,
    )


def t212_raw_merged(
    summary: dict[str, Any] | None = None,
    positions: list[dict[str, Any]] | None = None,
    instruments: list[dict[str, Any]] | None = None,
    events: list[dict[str, Any]] | None = None,
    fernet_key: bytes | None = None,
    fetched_at: datetime | None = None,
) -> pa.Table:
    """Build a merged raw Trading 212 table holding both fetch kinds.

    Concatenates the snapshot rows (:func:`t212_raw_snapshot`) with the events
    rows (:func:`t212_raw_events`) into the single-bronze ``raw/trading212``
    shape (AD-1/AC-6): one Delta table per broker discriminated by ``source``.
    """
    if fernet_key is None:
        fernet_key = generate_key()
    return pa.concat_tables(
        [
            t212_raw_snapshot(
                summary=summary,
                positions=positions,
                instruments=instruments,
                fernet_key=fernet_key,
                fetched_at=fetched_at,
            ),
            t212_raw_events(
                events=events, fernet_key=fernet_key, fetched_at=fetched_at
            ),
        ],
        schema=RAW_SCHEMA,
    )
