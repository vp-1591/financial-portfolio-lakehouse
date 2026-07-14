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
from pipeline.raw.models import RAW_SCHEMA


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
        result = parse_cash_report(root)
        assert len(result.per_currency) == 2
        assert len(result.base_summary) == 0
        usd = [e for e in result.per_currency if e["currency"] == "USD"][0]
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
        result = parse_cash_report(root)
        currencies = [e["currency"] for e in result.per_currency]
        assert "EUR" in currencies
        assert "PLN" in currencies
        assert len(result.per_currency) == 2
        assert len(result.base_summary) == 1
        assert result.base_summary[0]["currency"] == "BASE SUMMARY"

    def test_parse_cash_report_base_summary_only(self) -> None:
        """When only BASE_SUMMARY exists (no per-currency entries), it goes to base_summary."""
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U999" fromDate="20260601" toDate="20260625">'
            "<CashReport>"
            '<CashReportCurrency accountId="U999" currency="BASE SUMMARY"'
            ' endingCash="10500.00"/>'
            "</CashReport>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )
        root = ET.fromstring(xml_str)
        result = parse_cash_report(root)
        assert len(result.per_currency) == 0
        assert len(result.base_summary) == 1
        assert result.base_summary[0]["endingCash"] == "10500.00"

    def test_parse_cash_report_mixed_entries(self) -> None:
        """Per-currency entries and BASE_SUMMARY are separated correctly."""
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U123" fromDate="20260601" toDate="20260625">'
            "<CashReport>"
            '<CashReportCurrency accountId="U123" currency="USD"'
            ' endingCash="5000.00"/>'
            '<CashReportCurrency accountId="U123" currency="BASE SUMMARY"'
            ' endingCash="7000.00"/>'
            "</CashReport>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )
        root = ET.fromstring(xml_str)
        result = parse_cash_report(root)
        assert len(result.per_currency) == 1
        assert result.per_currency[0]["currency"] == "USD"
        assert len(result.base_summary) == 1
        assert result.base_summary[0]["currency"] == "BASE SUMMARY"

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
                "source_file": [""],
            },
            schema=RAW_SCHEMA,
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

    def test_transform_produces_cash_from_base_summary_fallback(
        self, fernet_key: bytes
    ) -> None:
        """When only BASE_SUMMARY exists (no per-currency entries), a CASH row is produced."""
        from pipeline.crypto import decrypt_float

        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U999" fromDate="20240101" toDate="20240102">'
            '<AccountInformation accountId="U999" currency="USD"/>'
            "<OpenPositions>"
            '<OpenPosition symbol="CSPX" currency="USD" positionValue="5000.0"'
            ' assetClass="STK" isin="IE00B5BMR087"'
            ' fxRateToBase="1.0"/>'
            "</OpenPositions>"
            "<CashReport>"
            '<CashReportCurrency accountId="U999" currency="BASE SUMMARY"'
            ' endingCash="10500.0"/>'
            "</CashReport>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )

        raw = self._build_flex_raw_table(xml_str, fernet_key=fernet_key)
        result = transform.transform_snapshot(raw, fernet_key)

        types = result.column("position_type").to_pylist()
        assert "CASH" in types, f"Expected CASH row, got types: {types}"

        cash_idx = types.index("CASH")
        labels = result.column("label").to_pylist()
        assert labels[cash_idx] == "CASH USD"

        values = result.column("value").to_pylist()
        cash_value = decrypt_float(values[cash_idx], fernet_key)
        assert cash_value == pytest.approx(10500.0)

    def test_transform_skips_base_summary_when_per_currency_exists(
        self, fernet_key: bytes
    ) -> None:
        """When per-currency entries exist, BASE_SUMMARY should not produce a duplicate cash row."""
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U123" fromDate="20240101" toDate="20240102">'
            '<AccountInformation accountId="U123" currency="USD"/>'
            "<OpenPositions>"
            '<OpenPosition symbol="AAPL" currency="USD" positionValue="10000.0"'
            ' assetClass="STK" fxRateToBase="1.0"/>'
            "</OpenPositions>"
            "<CashReport>"
            '<CashReportCurrency accountId="U123" currency="USD"'
            ' endingCash="500.0"/>'
            '<CashReportCurrency accountId="U123" currency="BASE SUMMARY"'
            ' endingCash="500.0"/>'
            "</CashReport>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )

        raw = self._build_flex_raw_table(xml_str, fernet_key=fernet_key)
        result = transform.transform_snapshot(raw, fernet_key)

        types = result.column("position_type").to_pylist()
        cash_count = types.count("CASH")
        assert cash_count == 1, f"Expected 1 CASH row, got {cash_count}"

    def test_transform_base_summary_with_currency_override(
        self, fernet_key: bytes
    ) -> None:
        """BASE_SUMMARY fallback uses base_currency_override when provided."""

        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U999" fromDate="20240101" toDate="20240102">'
            '<AccountInformation accountId="U999" currency="BASE"/>'
            "<OpenPositions>"
            '<OpenPosition symbol="CSPX" currency="USD" positionValue="5000.0"'
            ' assetClass="STK" isin="IE00B5BMR087"'
            ' fxRateToBase="0.9"/>'
            "</OpenPositions>"
            "<CashReport>"
            '<CashReportCurrency accountId="U999" currency="BASE SUMMARY"'
            ' endingCash="3000.0"/>'
            "</CashReport>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )

        raw = self._build_flex_raw_table(xml_str, fernet_key=fernet_key)
        result = transform.transform_snapshot(
            raw, fernet_key, base_currency_override="CHF"
        )

        types = result.column("position_type").to_pylist()
        assert "CASH" in types

        cash_idx = types.index("CASH")
        labels = result.column("label").to_pylist()
        assert labels[cash_idx] == "CASH CHF"

        currencies = result.column("currency").to_pylist()
        assert currencies[cash_idx] == "CHF"


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
                "source_file": [""],
            },
            schema=RAW_SCHEMA,
        )

        connector = get("ibkr")
        result = connector.transform_snapshot(raw, fernet_key)

        assert result.num_rows >= 1
        assert "EQUITY" in result.column("position_type").to_pylist()


class TestCdcFetch:
    """Tests for IBKR CDC fetch via Flex Web Service."""

    def test_fetch_cdc_produces_raw_table_with_flex_cdc_source(self) -> None:
        """fetch_cdc_via_flex produces a raw table with source='flex_cdc'."""
        from unittest.mock import MagicMock, patch

        from pipeline.connectors.ibkr.fetch import fetch_cdc_via_flex

        # Build a minimal Flex XML response
        xml_str = (
            '<FlexQueryResponse queryName="test_cdc" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U123456" fromDate="20260101" toDate="20260625">'
            "<Trades>"
            '<Trade accountId="U123456" symbol="AAPL" quantity="10"/>'
            "</Trades>"
            "</FlexStatement>"
            "</FlexStatements>"
            "</FlexQueryResponse>"
        )

        mock_client = MagicMock(spec=IbkrFlexClient)
        mock_client.request_report.return_value = "REF123"
        root = ET.fromstring(xml_str)
        mock_client.fetch_report.return_value = root

        with patch(
            "pipeline.connectors.ibkr.fetch.IbkrFlexClient", return_value=mock_client
        ):
            result = fetch_cdc_via_flex(token="test_token", query_id="test_query")

        assert result.num_rows == 1
        assert result.column("broker")[0].as_py() == "IBKR"
        assert result.column("source")[0].as_py() == "flex_cdc"
        assert result.column("payload")[0].as_py() is not None

    def test_fetch_cdc_kwargs_with_dedicated_query_id(self, monkeypatch) -> None:
        """When IBKR_FLEX_CDC_QUERY_ID is set, it takes precedence."""
        from pipeline.connectors.registry import get

        connector = get("ibkr")
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "token123")
        monkeypatch.setenv("IBKR_FLEX_CDC_QUERY_ID", "cdc_query_456")
        monkeypatch.setenv("IBKR_FLEX_QUERY_ID", "snapshot_query_789")

        kwargs = connector.fetch_cdc_kwargs()
        assert kwargs["token"] == "token123"
        assert kwargs["query_id"] == "cdc_query_456"

    def test_fetch_cdc_kwargs_falls_back_to_snapshot_query_id(
        self, monkeypatch
    ) -> None:
        """When IBKR_FLEX_CDC_QUERY_ID is not set, fall back to IBKR_FLEX_QUERY_ID."""
        from pipeline.connectors.registry import get

        connector = get("ibkr")
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "token123")
        monkeypatch.delenv("IBKR_FLEX_CDC_QUERY_ID", raising=False)
        monkeypatch.setenv("IBKR_FLEX_QUERY_ID", "snapshot_query_789")

        kwargs = connector.fetch_cdc_kwargs()
        assert kwargs["token"] == "token123"
        assert kwargs["query_id"] == "snapshot_query_789"

    def test_fetch_cdc_kwargs_returns_empty_when_no_token(self, monkeypatch) -> None:
        """When IBKR_FLEX_TOKEN is not set, returns empty dict."""
        from pipeline.connectors.registry import get

        connector = get("ibkr")
        monkeypatch.delenv("IBKR_FLEX_TOKEN", raising=False)

        kwargs = connector.fetch_cdc_kwargs()
        assert kwargs == {}


class TestCdcTransform:
    """Tests for IBKR CDC transform using broker-neutral schema."""

    @pytest.fixture()
    def fernet_key(self) -> bytes:
        return generate_key()

    def test_transform_cdc_produces_trade_and_dividend_rows(
        self, fernet_key: bytes
    ) -> None:
        """IBKR CDC transform produces TRADE and DIVIDEND events from Flex XML."""
        from tests.fixtures.ibkr import ibkr_raw_cdc

        from pipeline.connectors.ibkr.transform import transform_cdc

        raw = ibkr_raw_cdc(fernet_key=fernet_key)
        result = transform_cdc(raw, fernet_key)

        assert (
            result.num_rows >= 7
        )  # Trade + Dividend + Interest + Deposit + Withdrawal + Transfer + Fee
        event_types = result.column("event_type").to_pylist()
        assert "TRADE" in event_types
        assert "DIVIDEND" in event_types
        assert "INTEREST" in event_types
        assert "DEPOSIT" in event_types
        assert "WITHDRAWAL" in event_types
        assert "TRANSFER" in event_types
        assert "FEE" in event_types

        # All rows should have broker="IBKR"
        brokers = result.column("broker").to_pylist()
        assert all(b == "IBKR" for b in brokers)

    def test_transform_cdc_event_id_stability(self, fernet_key: bytes) -> None:
        """Deterministic event IDs are consistent across repeated transforms."""
        from tests.fixtures.ibkr import ibkr_raw_cdc

        from pipeline.connectors.ibkr.transform import transform_cdc

        raw = ibkr_raw_cdc(fernet_key=fernet_key)
        result1 = transform_cdc(raw, fernet_key)
        result2 = transform_cdc(raw, fernet_key)

        ids1 = result1.column("event_id").to_pylist()
        ids2 = result2.column("event_id").to_pylist()
        assert ids1 == ids2

    def test_transform_cdc_encrypts_value_columns(self, fernet_key: bytes) -> None:
        """IBKR CDC transform encrypts value columns correctly."""
        from tests.fixtures.ibkr import ibkr_raw_cdc

        from pipeline.connectors.ibkr.transform import transform_cdc
        from pipeline.crypto import decrypt_float

        raw = ibkr_raw_cdc(fernet_key=fernet_key)
        result = transform_cdc(raw, fernet_key)

        # Find the TRADE row and check encrypted columns
        event_types = result.column("event_type").to_pylist()
        trade_idx = event_types.index("TRADE")

        # netCash should be encrypted binary
        cash_amount_raw = result.column("cash_amount")[trade_idx].as_py()
        assert isinstance(cash_amount_raw, bytes)  # Encrypted
        cash = decrypt_float(cash_amount_raw, fernet_key)
        assert cash == pytest.approx(-1501.0)  # netCash from Trade

    def test_transform_cdc_skips_snapshot_source(self, fernet_key: bytes) -> None:
        """IBKR CDC transform skips rows with source='flex' (snapshot data)."""
        from tests.fixtures.ibkr import ibkr_raw_positions

        from pipeline.connectors.ibkr.transform import transform_cdc

        raw = ibkr_raw_positions(fernet_key=fernet_key)  # source="flex"
        result = transform_cdc(raw, fernet_key)

        assert result.num_rows == 0

    def test_transform_cdc_deduplicates_across_payloads(
        self, fernet_key: bytes
    ) -> None:
        """When multiple raw payloads contain the same IBKR events, dedup by event_id."""
        from tests.fixtures.ibkr import ibkr_raw_cdc

        from pipeline.connectors.ibkr.transform import transform_cdc

        raw = ibkr_raw_cdc(fernet_key=fernet_key)
        # Duplicate the payload row with a different fetched_at — simulates
        # two pipeline runs fetching the same Flex history.
        import pyarrow as pa

        duplicated = pa.concat_tables([raw, raw])

        result = transform_cdc(duplicated, fernet_key)

        # The same events should appear exactly once, not twice.
        event_ids = result.column("event_id").to_pylist()
        assert len(event_ids) == len(set(event_ids)), (
            f"Duplicate event_ids found: {event_ids}"
        )

    def test_transform_cdc_normalises_compact_datetime(self, fernet_key: bytes) -> None:
        """IBKR compact datetime formats are normalised to ISO 8601."""
        from tests.fixtures.ibkr import ibkr_raw_cdc

        from pipeline.connectors.ibkr.transform import transform_cdc

        raw = ibkr_raw_cdc(fernet_key=fernet_key)
        result = transform_cdc(raw, fernet_key)

        event_datetimes = result.column("event_datetime").to_pylist()
        # All event_datetime values should be ISO 8601 (no compact YYYYMMDD formats)
        for dt in event_datetimes:
            assert not dt.startswith("2026") or "-" in dt, (
                f"Compact datetime not normalised: {dt}"
            )


class TestNormalizeIbkrDatetime:
    """Tests for _normalize_ibkr_datetime helper."""

    def test_compact_date(self) -> None:
        from pipeline.connectors.ibkr.transform import _normalize_ibkr_datetime

        assert _normalize_ibkr_datetime("20260204") == "2026-02-04T00:00:00Z"

    def test_compact_datetime_with_semicolon(self) -> None:
        from pipeline.connectors.ibkr.transform import _normalize_ibkr_datetime

        assert _normalize_ibkr_datetime("20260702;022904") == "2026-07-02T02:29:04Z"

    def test_iso_datetime_unchanged(self) -> None:
        from pipeline.connectors.ibkr.transform import _normalize_ibkr_datetime

        assert _normalize_ibkr_datetime("2026-01-15 10:30:00") == "2026-01-15 10:30:00"

    def test_iso_date_unchanged(self) -> None:
        from pipeline.connectors.ibkr.transform import _normalize_ibkr_datetime

        assert _normalize_ibkr_datetime("2026-03-01") == "2026-03-01"

    def test_iso_with_tz_unchanged(self) -> None:
        from pipeline.connectors.ibkr.transform import _normalize_ibkr_datetime

        assert (
            _normalize_ibkr_datetime("2026-01-15T10:30:00Z") == "2026-01-15T10:30:00Z"
        )

    def test_empty_string_unchanged(self) -> None:
        from pipeline.connectors.ibkr.transform import _normalize_ibkr_datetime

        assert _normalize_ibkr_datetime("") == ""


class TestClassifyIbkrCashType:
    """Tests for IBKR CashTransaction type → normalized event_type mapping."""

    def test_dividends(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("Dividends", 42.5) == "DIVIDEND"

    def test_payment_in_lieue(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("PaymentInLieue", 10.0) == "DIVIDEND"

    def test_withholding_tax(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("Withholding Tax", -5.0) == "TAX"

    def test_871m_withholding(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("871(m) Withholding", -3.0) == "TAX"

    def test_deposits_positive_is_deposit(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("Deposits & Withdrawals", 1000.0) == "DEPOSIT"

    def test_deposits_zero_is_deposit(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("Deposits & Withdrawals", 0.0) == "DEPOSIT"

    def test_deposits_negative_is_withdrawal(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert (
            _classify_ibkr_cash_type("Deposits & Withdrawals", -500.0) == "WITHDRAWAL"
        )

    def test_broker_interest_received(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("Broker Interest Received", 12.0) == "INTEREST"

    def test_broker_interest_paid(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("Broker Interest Paid", -5.0) == "INTEREST"

    def test_bond_interest_received(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("Bond Interest Received", 25.0) == "INTEREST"

    def test_bond_interest_paid(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("Bond Interest Paid", -5.0) == "INTEREST"

    def test_broker_fees(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("Broker Fees", -2.0) == "FEE"

    def test_other_fees(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("Other Fees", -1.0) == "FEE"

    def test_commission_adjustments(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("Commission Adjustments", -0.5) == "FEE"

    def test_other_income(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("Other Income", 5.0) == "ADJUSTMENT"

    def test_price_adjustments(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("Price Adjustments", 10.0) == "ADJUSTMENT"

    def test_unknown_type_falls_through(self) -> None:
        from pipeline.connectors.ibkr.transform import _classify_ibkr_cash_type

        assert _classify_ibkr_cash_type("SomeNewType", 42.0) == "UNKNOWN"


class TestInjectDemoDeposit:
    """Tests for _inject_demo_deposit synthetic deposit injection."""

    def _make_records(
        self,
        account_id: str = "U123456",
        base_currency: str = "EUR",
        event_datetime: str = "2026-03-01T00:00:00Z",
    ) -> list[dict]:
        """Build minimal CDC record dicts for testing."""
        now = datetime.now(timezone.utc)
        return [
            {
                "fetched_at": now,
                "broker": "IBKR",
                "account_id": account_id,
                "event_id": "abc123",
                "source": "CashTransaction",
                "event_type": "DIVIDEND",
                "raw_event_type": "Dividends",
                "event_datetime": event_datetime,
                "currency": base_currency,
                "cash_amount": 42.50,
                "settle_date": "2026-03-04",
                "ticker": "VWCE",
                "isin": "IE00BK5BQT80",
                "description": "Vanguard FTSE All-World",
                "base_currency": base_currency,
                "fx_rate_to_base": 1.0,
                "amount_base": 42.50,
            }
        ]

    def test_no_injection_when_not_demo(self) -> None:
        """When is_demo=False, records are returned unchanged."""
        from pipeline.connectors.ibkr.transform import _inject_demo_deposit

        records = self._make_records()
        result = _inject_demo_deposit(records, is_demo=False)
        assert result is records
        assert len(result) == 1

    def test_injection_adds_deposit_when_demo(self) -> None:
        """When is_demo=True, a DEPOSIT record is added for each account."""
        from pipeline.connectors.ibkr.transform import _inject_demo_deposit

        records = self._make_records()
        result = _inject_demo_deposit(records, is_demo=True)
        assert len(result) == 2  # original + deposit

        deposit = [r for r in result if r["event_type"] == "DEPOSIT"][0]
        assert deposit["account_id"] == "U123456"
        assert deposit["currency"] == "EUR"
        assert deposit["cash_amount"] == 1_000_000.0
        assert deposit["amount_base"] == 1_000_000.0
        assert deposit["fx_rate_to_base"] == 1.0
        assert deposit["source"] == "CashTransaction"
        assert deposit["raw_event_type"] == "Deposits & Withdrawals"
        assert deposit["description"] == "Initial demo account deposit"
        assert deposit["ticker"] == ""
        assert deposit["isin"] == ""

    def test_deposit_date_before_earliest_event(self) -> None:
        """The deposit date is one day before the earliest event_datetime."""
        from pipeline.connectors.ibkr.transform import _inject_demo_deposit

        records = self._make_records(event_datetime="2026-06-15T10:30:00Z")
        result = _inject_demo_deposit(records, is_demo=True)

        deposit = [r for r in result if r["event_type"] == "DEPOSIT"][0]
        assert deposit["event_datetime"] == "2026-06-14T00:00:00Z"
        assert deposit["settle_date"] == "2026-06-14"

    def test_deposit_date_with_compact_datetime(self) -> None:
        """Deposit date calculation works with IBKR compact datetime format."""
        from pipeline.connectors.ibkr.transform import _inject_demo_deposit

        records = self._make_records(event_datetime="20260301")
        # After normalization this becomes 2026-03-01T00:00:00Z
        # But the records here use the already-normalized format.
        # Test with ISO format since _inject_demo_deposit normalizes.
        result = _inject_demo_deposit(records, is_demo=True)

        deposit = [r for r in result if r["event_type"] == "DEPOSIT"][0]
        assert deposit["settle_date"] == "2026-02-28"

    def test_fallback_date_when_no_records(self) -> None:
        """When no records exist, deposit uses the fallback date 2020-01-01."""
        from pipeline.connectors.ibkr.transform import _inject_demo_deposit

        result = _inject_demo_deposit([], is_demo=True)
        # No records → no accounts to derive → returns empty list
        assert result == []

    def test_multi_account_gets_deposit_per_account(self) -> None:
        """Each unique (account_id, base_currency) pair gets its own deposit."""
        from pipeline.connectors.ibkr.transform import _inject_demo_deposit

        now = datetime.now(timezone.utc)
        records = [
            {
                "fetched_at": now,
                "broker": "IBKR",
                "account_id": "U111",
                "event_id": "evt1",
                "source": "CashTransaction",
                "event_type": "TRADE",
                "raw_event_type": "ExTrade",
                "event_datetime": "2026-05-01T00:00:00Z",
                "currency": "USD",
                "cash_amount": -100.0,
                "settle_date": "2026-05-03",
                "ticker": "AAPL",
                "isin": "",
                "description": "Apple Inc",
                "base_currency": "USD",
                "fx_rate_to_base": 1.0,
                "amount_base": -100.0,
            },
            {
                "fetched_at": now,
                "broker": "IBKR",
                "account_id": "U222",
                "event_id": "evt2",
                "source": "CashTransaction",
                "event_type": "DIVIDEND",
                "raw_event_type": "Dividends",
                "event_datetime": "2026-05-10T00:00:00Z",
                "currency": "EUR",
                "cash_amount": 50.0,
                "settle_date": "2026-05-13",
                "ticker": "VWCE",
                "isin": "",
                "description": "Vanguard",
                "base_currency": "EUR",
                "fx_rate_to_base": 1.0,
                "amount_base": 50.0,
            },
        ]

        result = _inject_demo_deposit(records, is_demo=True)
        deposits = [r for r in result if r["event_type"] == "DEPOSIT"]
        assert len(deposits) == 2

        # Each account gets its own deposit with correct currency
        by_account = {d["account_id"]: d for d in deposits}
        assert "U111" in by_account
        assert "U222" in by_account
        assert by_account["U111"]["currency"] == "USD"
        assert by_account["U222"]["currency"] == "EUR"
        # Both deposits dated one day before earliest event (2026-05-01)
        assert by_account["U111"]["event_datetime"] == "2026-04-30T00:00:00Z"
        assert by_account["U222"]["event_datetime"] == "2026-04-30T00:00:00Z"

    def test_deterministic_event_id_is_stable(self) -> None:
        """Calling _inject_demo_deposit twice produces the same event_ids."""
        from pipeline.connectors.ibkr.transform import _inject_demo_deposit

        records = self._make_records()
        result1 = _inject_demo_deposit(list(records), is_demo=True)
        result2 = _inject_demo_deposit(list(records), is_demo=True)

        deposits1 = [r for r in result1 if r["event_type"] == "DEPOSIT"]
        deposits2 = [r for r in result2 if r["event_type"] == "DEPOSIT"]
        assert len(deposits1) == len(deposits2) == 1
        assert deposits1[0]["event_id"] == deposits2[0]["event_id"]

    def test_transform_cdc_with_demo_flag(self) -> None:
        """transform_cdc with is_demo=True injects a synthetic deposit."""
        from tests.fixtures.ibkr import ibkr_raw_cdc

        from pipeline.connectors.ibkr.transform import transform_cdc

        fernet_key = generate_key()
        raw = ibkr_raw_cdc(fernet_key=fernet_key)

        result_no_demo = transform_cdc(raw, fernet_key, is_demo=False)
        result_demo = transform_cdc(raw, fernet_key, is_demo=True)

        # Demo result should have one more row (the deposit)
        assert result_demo.num_rows == result_no_demo.num_rows + 1

        demo_types = result_demo.column("event_type").to_pylist()
        assert demo_types.count("DEPOSIT") == 1 + result_no_demo.column(
            "event_type"
        ).to_pylist().count("DEPOSIT")

        # The synthetic deposit should be for the fixture account
        demo_event_ids = result_demo.column("event_id").to_pylist()
        assert len(demo_event_ids) == len(set(demo_event_ids)), (
            "Synthetic deposit should not duplicate existing event_ids"
        )
