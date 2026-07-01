"""IBKR fixture builders for raw and normalized Delta tables.

Provides factory functions that return realistic ``pa.Table`` objects
matching the actual schemas used by the IBKR connector.

Since the pipeline now exclusively uses the Flex Web Service API, all
fixtures produce Flex-style raw data (source="flex" with XML payloads).
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pyarrow as pa

from pipeline.crypto import encrypt, encrypt_float, generate_key
from pipeline.raw.models import RAW_SCHEMA
from pipeline.normalized.models import ibkr_snapshot_normalized_schema


def ibkr_raw_positions(
    account_id: str = "U123456",
    fernet_key: bytes | None = None,
) -> pa.Table:
    """Build a raw IBKR snapshot table with an encrypted Flex XML payload.

    Default data includes 2 equity positions and a EUR cash balance.
    """
    if fernet_key is None:
        fernet_key = generate_key()

    xml_str = (
        '<FlexQueryResponse queryName="test" type="AF">'
        '<FlexStatements count="1">'
        f'<FlexStatement accountId="{account_id}" fromDate="20260101" toDate="20260625">'
        "<AccountInformation>"
        f'<AccountInformation accountId="{account_id}" currency="EUR"'
        ' netLiquidationValue="10000.00" cashBalance="2000.00"/>'
        "</AccountInformation>"
        "<OpenPositions>"
        f'<OpenPosition accountId="{account_id}" currency="EUR" fxRateToBase="1.0"'
        ' assetClass="STK" symbol="VWCE" description="Vanguard FTSE All-World UCITS ETF"'
        ' conid="12345678" isin="IE00BK5BQT80"'
        ' quantity="100" markPrice="50.0" positionValue="5000.0" side="Long"/>'
        f'<OpenPosition accountId="{account_id}" currency="USD" fxRateToBase="0.9"'
        ' assetClass="STK" symbol="AAPL" description="Apple Inc"'
        ' conid="265598" isin="US0378331005"'
        ' quantity="50" markPrice="60.0" positionValue="3000.0" side="Long"/>'
        "</OpenPositions>"
        "<CashReport>"
        f'<CashReportCurrency accountId="{account_id}" currency="EUR"'
        ' endingCash="2000.00"/>'
        "</CashReport>"
        "</FlexStatement>"
        "</FlexStatements>"
        "</FlexQueryResponse>"
    )

    now = datetime.now(timezone.utc)
    xml_bytes = xml_str.encode("utf-8")
    encrypted_payload = encrypt(xml_bytes, fernet_key)
    payload_hash = hashlib.sha256(xml_bytes).hexdigest()

    return pa.table(
        {
            "fetched_at": [now],
            "broker": ["IBKR"],
            "source": ["flex"],
            "payload": [encrypted_payload],
            "payload_hash": [payload_hash],
            "account_id": [account_id],
            "source_file": [""],
        },
        schema=RAW_SCHEMA,
    )


def ibkr_normalized_snapshot(
    fernet_key: bytes | None = None,
    account_id: str = "U123456",
) -> pa.Table:
    """Build a normalized IBKR snapshot table with encrypted values.

    Default data: 2 equities (VWCE, AAPL) + 1 cash entry (EUR).
    """
    if fernet_key is None:
        fernet_key = generate_key()
    now = datetime.now(timezone.utc)
    return pa.table(
        {
            "fetched_at": [now, now, now],
            "account_id": [account_id, account_id, account_id],
            "position_type": ["EQUITY", "EQUITY", "CASH"],
            "label": ["VWCE", "AAPL", "CASH EUR"],
            "asset_class": ["STK", "STK", "CASH"],
            "currency": ["EUR", "EUR", "EUR"],
            "value": [
                encrypt_float(5000.0, fernet_key),
                encrypt_float(2700.0, fernet_key),  # 3000 USD * 0.9 EUR/USD
                encrypt_float(2000.0, fernet_key),
            ],
            "value_currency": ["EUR", "USD", "EUR"],
            "conid": ["12345678", "265598", ""],
            "isin": ["IE00BK5BQT80", "US0378331005", ""],
            "description": ["Vanguard FTSE All-World", "Apple Inc", "Cash EUR"],
            "security_currency": ["EUR", "USD", "EUR"],
        },
        schema=ibkr_snapshot_normalized_schema,
    )
