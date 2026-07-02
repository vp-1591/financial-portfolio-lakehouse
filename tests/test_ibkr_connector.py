"""Tests for the IBKR pipeline connector."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from pipeline.connectors.ibkr.client import (
    IbkrFlexClient,
    as_float,
    parse_account_info,
    parse_cash_report,
    parse_conversion_rates,
    parse_positions,
)
from pipeline.connectors.ibkr import transform
from pipeline.crypto import encrypt, generate_key


class TestClientParsing:
    """Tests for Flex XML parsing helpers."""

    def test_as_float(self) -> None:
        assert as_float(None) == 0.0
        assert as_float("") == 0.0
        assert as_float("42.5") == 42.5
        assert as_float(100) == 100.0
        assert as_float("abc", 5.0) == 5.0

    def test_parse_positions_extracts_open_position_attributes(self) -> None:
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">'
            "<OpenPositions>"
            '<OpenPosition accountId="U123" currency="EUR" fxRateToBase="1.2"'
            ' assetClass="STK" symbol="EUR ETF" description="iShares Core MSCI World"'
            ' isin="IE00BK5BQT80" listingExchange="XETRA"'
            ' reportDate="20260625" quantity="100" markPrice="50.0"'
            ' positionValue="5000.0" costBasisPrice="40.0"'
            ' costBasisMoney="4000.0" percentOfNAV="5.0"'
            ' unrealizedPnl="1000.0"/>'
            "</OpenPositions>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )
        root = ET.fromstring(xml_str)
        positions = parse_positions(root)
        assert len(positions) == 1
        assert positions[0]["symbol"] == "EUR ETF"
        assert positions[0]["isin"] == "IE00BK5BQT80"

    def test_parse_account_info_extracts_attributes(self) -> None:
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">'
            "<AccountInformation>"
            '<AccountInformation accountId="U123" currency="USD"'
            ' netLiquidationValue="78000.00"/>'
            "</AccountInformation>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )
        root = ET.fromstring(xml_str)
        accounts = parse_account_info(root)
        assert len(accounts) == 1
        assert accounts[0]["accountId"] == "U123"
        assert accounts[0]["netLiquidationValue"] == "78000.00"

    def test_parse_cash_report_extracts_ending_cash_per_currency(self) -> None:
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">'
            "<CashReport>"
            '<CashReportCurrency accountId="U123" currency="USD"'
            ' endingCash="5000.00"/>'
            '<CashReportCurrency accountId="U123" currency="EUR"'
            ' endingCash="2000.00"/>'
            "</CashReport>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )
        root = ET.fromstring(xml_str)
        entries = parse_cash_report(root)
        assert len(entries) == 2
        usd = [e for e in entries if e["currency"] == "USD"][0]
        assert usd["accountId"] == "U123"
        assert usd["endingCash"] == "5000.00"

    def test_parse_cash_report_filters_summary_rows(self) -> None:
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">'
            "<CashReport>"
            '<CashReportCurrency accountId="U123" currency="EUR"'
            ' endingCash="3000.00"/>'
            '<CashReportCurrency accountId="U123" currency="PLN"'
            ' endingCash="20000.00"/>'
            '<CashReportCurrency accountId="U123" currency="BASE SUMMARY"'
            ' endingCash="4700.00"/>'
            "</CashReport>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )
        root = ET.fromstring(xml_str)
        entries = parse_cash_report(root)
        currencies = [e["currency"] for e in entries]
        assert "EUR" in currencies
        assert "PLN" in currencies
        assert "BASE SUMMARY" not in currencies
        assert len(entries) == 2

    def test_parse_conversion_rates(self) -> None:
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">'
            "<ConversionRates>"
            '<ConversionRate fromCurrency="EUR" toCurrency="USD" rate="1.1"/>'
            '<ConversionRate fromCurrency="CHF" toCurrency="USD" rate="1.15"/>'
            '<ConversionRate fromCurrency="USD" toCurrency="USD" rate="1.0"/>'
            "</ConversionRates>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )
        root = ET.fromstring(xml_str)
        rates = parse_conversion_rates(root)
        assert rates["EUR"] == 1.1
        assert rates["CHF"] == 1.15
        assert rates["USD"] == 1.0

    def test_ibkr_flex_client_request_report_parses_reference_code(self) -> None:
        response_xml = """\
<FlexStatementResponse>
  <Status>Success</Status>
  <ReferenceCode>98765432</ReferenceCode>
</FlexStatementResponse>
"""
        client = IbkrFlexClient(token="test-token", query_id="999999")
        client._request = lambda path, params: response_xml  # type: ignore[assignment]

        ref_code = client.request_report()
        assert ref_code == "98765432"

    def test_ibkr_flex_client_request_report_raises_on_error(self) -> None:
        response_xml = """\
<FlexStatementResponse>
  <Status>Fail</Status>
  <ErrorCode>1003</ErrorCode>
  <ErrorMessage>Invalid token</ErrorMessage>
</FlexStatementResponse>
"""
        from pipeline.connectors.ibkr.client import IbkrError

        client = IbkrFlexClient(token="bad-token", query_id="999999")
        client._request = lambda path, params: response_xml  # type: ignore[assignment]

        try:
            client.request_report()
            assert False, "Expected IbkrError"
        except IbkrError as exc:
            assert "Invalid token" in str(exc)


class TestFlexTransformSnapshot:
    """Tests for transforming Flex XML data into normalized schema."""

    @pytest.fixture()
    def fernet_key(self) -> bytes:
        key = generate_key()
        self._fernet_key = key
        return key

    def _build_flex_raw_table(
        self,
        xml_str: str,
        account_id: str = "",
        fernet_key: bytes | None = None,
    ) -> pa.Table:
        """Build a raw-layer table with a Flex XML payload."""
        import hashlib

        key = fernet_key or self._fernet_key

        xml_bytes = xml_str.encode("utf-8")
        encrypted_payload = encrypt(xml_bytes, key)
        now = datetime.now(timezone.utc)
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
            schema=pa.schema(
                [
                    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
                    pa.field("broker", pa.string()),
                    pa.field("source", pa.string()),
                    pa.field("payload", pa.binary()),
                    pa.field("payload_hash", pa.string()),
                    pa.field("account_id", pa.string()),
                    pa.field("source_file", pa.string()),
                ]
            ),
        )

    def test_transform_produces_equity_and_cash_rows(self, fernet_key: bytes) -> None:
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U123" fromDate="20240101" toDate="20240102">'
            '<AccountInformation accountId="U123" currency="USD"/>'
            "<OpenPositions>"
            '<OpenPosition symbol="AAPL" currency="USD" positionValue="10000.0" '
            'assetClass="STK" isin="US0378331005" '
            'description="Apple Inc" fxRateToBase="1.0"/>'
            "</OpenPositions>"
            "<CashReport>"
            '<CashReportCurrency accountId="U123" currency="USD" endingCash="500.0"/>'
            "</CashReport>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )

        raw = self._build_flex_raw_table(xml_str, fernet_key=fernet_key)
        result = transform.transform_snapshot(raw, fernet_key)

        assert result.num_rows >= 2
        types = result.column("position_type").to_pylist()
        assert "EQUITY" in types
        assert "CASH" in types

    def test_transform_preserves_isin(self, fernet_key: bytes) -> None:
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U456" fromDate="20240101" toDate="20240102">'
            '<AccountInformation accountId="U456" currency="EUR"/>'
            "<OpenPositions>"
            '<OpenPosition symbol="SAP" currency="EUR" positionValue="5000.0" '
            'assetClass="STK" isin="DE0007164600" '
            'description="SAP SE" fxRateToBase="1.0"/>'
            "</OpenPositions>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )

        raw = self._build_flex_raw_table(xml_str, fernet_key=fernet_key)
        result = transform.transform_snapshot(raw, fernet_key)

        isins = result.column("isin").to_pylist()
        assert "DE0007164600" in isins

    def test_transform_skips_zero_value_positions(self, fernet_key: bytes) -> None:
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U789" fromDate="20240101" toDate="20240102">'
            '<AccountInformation accountId="U789" currency="USD"/>'
            "<OpenPositions>"
            '<OpenPosition symbol="ZERO" currency="USD" positionValue="0.0" '
            'assetClass="STK" fxRateToBase="1.0"/>'
            "</OpenPositions>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )

        raw = self._build_flex_raw_table(xml_str, fernet_key=fernet_key)
        result = transform.transform_snapshot(raw, fernet_key)

        types = result.column("position_type").to_pylist()
        assert "EQUITY" not in types

    def test_transform_currency_override(self, fernet_key: bytes) -> None:
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U999" fromDate="20240101" toDate="20240102">'
            '<AccountInformation accountId="U999" currency="BASE"/>'
            "<OpenPositions>"
            '<OpenPosition symbol="AAPL" currency="USD" positionValue="5000.0" '
            'assetClass="STK" fxRateToBase="0.9"/>'
            "</OpenPositions>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )

        raw = self._build_flex_raw_table(xml_str, fernet_key=fernet_key)
        result = transform.transform_snapshot(
            raw, fernet_key, base_currency_override="CHF"
        )

        currencies = result.column("currency").to_pylist()
        assert all(c == "CHF" for c in currencies)


class TestConnectorFlexDispatch:
    """Tests for IbkrConnector dispatching to the Flex path."""

    def test_fetch_snapshot_uses_flex_when_token_provided(self) -> None:
        """When flex_token is provided, IbkrConnector should use fetch_snapshot_via_flex."""
        from unittest.mock import MagicMock, patch

        from pipeline.connectors.registry import get

        with patch(
            "pipeline.connectors.ibkr.fetch.fetch_snapshot_via_flex"
        ) as mock_flex:
            mock_flex.return_value = MagicMock()
            connector = get("ibkr")

            connector.fetch_snapshot(
                flex_token="my-token",
                flex_query_id="42",
                flex_base_url="https://example.com",
            )

            mock_flex.assert_called_once_with(
                token="my-token",
                query_id="42",
                base_url="https://example.com",
                timeout=30.0,
                retries=6,
                delay=3.0,
            )

    def test_transform_snapshot_uses_flex_for_flex_source(
        self, fernet_key: bytes
    ) -> None:
        """IbkrConnector.transform_snapshot should use Flex transform for flex source data."""
        from pipeline.connectors.registry import get

        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U123" fromDate="20240101" toDate="20240102">'
            '<AccountInformation accountId="U123" currency="USD"/>'
            "<OpenPositions>"
            '<OpenPosition symbol="AAPL" currency="USD" positionValue="10000" '
            'assetClass="STK" fxRateToBase="1.0"/>'
            "</OpenPositions>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )
        import hashlib

        xml_bytes = xml_str.encode("utf-8")
        encrypted_payload = encrypt(xml_bytes, fernet_key)
        now = datetime.now(timezone.utc)

        raw = pa.table(
            {
                "fetched_at": [now],
                "broker": ["IBKR"],
                "source": ["flex"],
                "payload": [encrypted_payload],
                "payload_hash": [hashlib.sha256(xml_bytes).hexdigest()],
                "account_id": [""],
                "source_file": [""],
            },
            schema=pa.schema(
                [
                    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
                    pa.field("broker", pa.string()),
                    pa.field("source", pa.string()),
                    pa.field("payload", pa.binary()),
                    pa.field("payload_hash", pa.string()),
                    pa.field("account_id", pa.string()),
                    pa.field("source_file", pa.string()),
                ]
            ),
        )

        connector = get("ibkr")
        result = connector.transform_snapshot(raw, fernet_key)

        assert result.num_rows >= 1
        assert "EQUITY" in result.column("position_type").to_pylist()
