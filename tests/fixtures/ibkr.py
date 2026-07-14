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
        ' netLiquidationValue="10000.00"/>'
        "</AccountInformation>"
        "<OpenPositions>"
        f'<OpenPosition accountId="{account_id}" currency="EUR" fxRateToBase="1.0"'
        ' assetClass="STK" symbol="VWCE" description="Vanguard FTSE All-World UCITS ETF"'
        ' isin="IE00BK5BQT80"'
        ' quantity="100" markPrice="50.0" positionValue="5000.0"/>'
        f'<OpenPosition accountId="{account_id}" currency="USD" fxRateToBase="0.9"'
        ' assetClass="STK" symbol="AAPL" description="Apple Inc"'
        ' isin="US0378331005"'
        ' quantity="50" markPrice="60.0" positionValue="3000.0"/>'
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
            "security_value": [
                encrypt_float(5000.0, fernet_key),
                encrypt_float(2700.0, fernet_key),  # 3000 USD * 0.9 EUR/USD
                encrypt_float(2000.0, fernet_key),
            ],
            "security_ccy": ["EUR", "USD", "EUR"],
            "isin": ["IE00BK5BQT80", "US0378331005", ""],
            "description": ["Vanguard FTSE All-World", "Apple Inc", "Cash EUR"],
        },
        schema=ibkr_snapshot_normalized_schema,
    )


def ibkr_raw_cdc(
    account_id: str = "U123456",
    fernet_key: bytes | None = None,
) -> pa.Table:
    """Build a raw IBKR CDC table with an encrypted Flex XML payload.

    Default data includes a Trade, a CashTransaction (dividend),
    a CashTransaction (bond interest), a CashTransaction (deposit),
    a CashTransaction (withdrawal), a Transfer, and a TransactionFee.
    """
    if fernet_key is None:
        fernet_key = generate_key()

    xml_str = (
        '<FlexQueryResponse queryName="test_cdc" type="AF">'
        '<FlexStatements count="1">'
        f'<FlexStatement accountId="{account_id}" fromDate="20260101" toDate="20260625">'
        "<AccountInformation>"
        f'<AccountInformation accountId="{account_id}" currency="EUR"/>'
        "</AccountInformation>"
        "<Trades>"
        f'<Trade accountId="{account_id}" symbol="AAPL" description="Apple Inc"'
        ' isin="US0378331005" currency="USD" fxRateToBase="0.9"'
        ' dateTime="20260115;103000"'
        ' tradeDate="20260115" settleDateTarget="20260117"'
        ' quantity="10" tradePrice="150.0" proceeds="-1500.0"'
        ' ibCommission="-1.0" ibCommissionCurrency="USD" netCash="-1501.0"'
        ' buySell="BUY" transactionType="ExTrade"'
        f' ibExecutionId="e001" tradeId="T001" transactionId="TX001"'
        ' taxes="0.0" conid="265598" securityId="US0378331005"'
        ' multiplier="1" openCloseIndicator="O"/>'
        "</Trades>"
        "<CashTransactions>"
        f'<CashTransaction accountId="{account_id}" symbol="VWCE"'
        ' description="Vanguard FTSE All-World UCITS ETF"'
        ' isin="IE00BK5BQT80" currency="EUR" fxRateToBase="1.0"'
        ' dateTime="20260301" settleDate="20260304"'
        ' amount="42.50" type="Dividends" dividendType="Qualified"'
        ' tradeId="" transactionId="CT001" code=""'
        ' assetClass="STK" conid="23897068" securityId="IE00BK5BQT80"/>'
        f'<CashTransaction accountId="{account_id}" symbol="TLT"'
        ' description="iShares 20+ Year Treasury Bond ETF"'
        ' isin="US4642874848" currency="USD" fxRateToBase="0.9"'
        ' dateTime="20260401" settleDate="20260404"'
        ' amount="35.00" type="Bond Interest Received" dividendType=""'
        ' tradeId="" transactionId="CT002" code=""'
        ' assetClass="STK" conid="7697096" securityId="US4642874848"/>'
        f'<CashTransaction accountId="{account_id}" symbol=""'
        ' description="Deposit EUR"'
        ' isin="" currency="EUR" fxRateToBase="1.0"'
        ' dateTime="20260501" settleDate="20260504"'
        ' amount="5000.00" type="Deposits &amp; Withdrawals" dividendType=""'
        ' tradeId="" transactionId="CT003" code=""'
        ' assetClass="" conid="" securityId=""/>'
        f'<CashTransaction accountId="{account_id}" symbol=""'
        ' description="Withdrawal EUR"'
        ' isin="" currency="EUR" fxRateToBase="1.0"'
        ' dateTime="20260515" settleDate="20260518"'
        ' amount="-2000.00" type="Deposits &amp; Withdrawals" dividendType=""'
        ' tradeId="" transactionId="CT004" code=""'
        ' assetClass="" conid="" securityId=""/>'
        "</CashTransactions>"
        "<Transfers>"
        f'<Transfer accountId="{account_id}" symbol="MSFT"'
        ' description="Microsoft Corp" currency="USD" fxRateToBase="0.9"'
        ' assetClass="STK" dateTime="20260210;120000" settleDate="20260212"'
        ' type="ACATS" direction="IN" quantity="5" transferPrice="400.0"'
        ' positionAmount="2000.0" positionAmountInBase="1800.0"'
        ' cashTransfer="0.0" transactionId="TR001"'
        ' conid="277883" securityId="US5949181045"/>'
        "</Transfers>"
        "<TransactionFees>"
        f'<TransactionFee accountId="{account_id}" symbol="AAPL"'
        ' description="Apple Inc" currency="USD" fxRateToBase="0.9"'
        ' assetClass="STK" date="20260115" reportDate="20260115"'
        ' settleDate="20260117" taxDescription="SEC Fee"'
        ' taxAmount="0.05" orderId="O001" tradeId="T001"'
        ' tradePrice="150.0" source="TRADE" code=""'
        ' conid="265598" securityId="US0378331005" quantity="10"/>'
        "</TransactionFees>"
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
            "source": ["flex_cdc"],
            "payload": [encrypted_payload],
            "payload_hash": [payload_hash],
            "source_file": [""],
        },
        schema=RAW_SCHEMA,
    )
