"""Tests for the IBKR pipeline connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest

from pipeline.connectors.ibkr.client import (
    IbkrClient,
    IbkrError,
    account_id,
    as_float,
    cash_assets,
    exchange_rates,
    net_liquidation_value,
    position_conid,
    position_description,
    position_isin,
    position_label,
    position_value,
    to_base_currency,
)
from pipeline.connectors.ibkr import transform
from pipeline.crypto import decrypt, decrypt_float, encrypt, generate_key


class TestClientParsing:
    """Tests preserved from tests/test_ibkr_net_worth.py."""

    def test_to_base_currency_uses_exchange_rate_as_base_value_per_unit(self) -> None:
        assert to_base_currency(100.0, "EUR", {"EUR": 1.2}) == 120.0

    def test_net_liquidation_value_without_base_converts_each_currency(self) -> None:
        ledger = {
            "USD": {"currency": "USD", "netliquidationvalue": 50.0, "exchangerate": 1.0},
            "EUR": {"currency": "EUR", "netliquidationvalue": 100.0, "exchangerate": 1.2},
        }
        assert net_liquidation_value(ledger) == 170.0

    def test_position_isin_reads_common_ibkr_fields(self) -> None:
        assert position_isin({"isin": "US0378331005"}) == "US0378331005"
        assert (
            position_isin({"secIdType": "ISIN", "secId": "IE00BK5BQT80"})
            == "IE00BK5BQT80"
        )
        assert position_isin({"secIdType": "CUSIP", "secId": "037833100"}) == ""

    def test_position_conid_and_description_helpers(self) -> None:
        position = {
            "conid": 208813719,
            "contractDesc": "GOOGL",
            "currency": "USD",
        }
        assert position_conid(position) == "208813719"
        assert (
            position_description(position, {"companyName": "Alphabet Inc Class A"})
            == "Alphabet Inc Class A"
        )
        assert position_description(position) == "GOOGL"

    def test_as_float(self) -> None:
        assert as_float(None) == 0.0
        assert as_float("") == 0.0
        assert as_float("42.5") == 42.5
        assert as_float(100) == 100.0
        assert as_float("abc", 5.0) == 5.0

    def test_account_id(self) -> None:
        assert account_id({"accountId": "U123"}) == "U123"
        assert account_id({"id": "U456"}) == "U456"

    def test_account_id_raises_on_empty(self) -> None:
        with pytest.raises(IbkrError):
            account_id({})

    def test_position_label(self) -> None:
        assert position_label({"contractDesc": "AAPL"}) == "AAPL"
        assert position_label({"description": "Apple"}) == "Apple"
        assert position_label({}) == "UNKNOWN"

    def test_cash_assets(self) -> None:
        ledger = {
            "BASE": {"currency": "USD", "cashbalance": 50.0, "exchangerate": 1.0},
            "EUR": {"currency": "EUR", "cashbalance": 100.0, "exchangerate": 1.2},
        }
        assets = cash_assets("U123", ledger)
        assert len(assets) == 1
        assert assets[0]["label"] == "CASH EUR"
        assert assets[0]["value"] == pytest.approx(120.0)


class TestTransformSnapshot:
    """Tests for the raw → normalized transform."""

    @pytest.fixture()
    def fernet_key(self) -> bytes:
        key = generate_key()
        self._fernet_key = key
        return key

    def _build_raw_table(
        self,
        positions: list[dict],
        ledger: dict,
        account_id: str = "U123",
    ) -> pa.Table:
        """Build a raw-layer table from fake API responses.

        Payloads are encrypted to match the real pipeline flow where
        raw Delta tables store encrypted payloads.
        """
        key = self._fernet_key
        positions_bytes = encrypt(json.dumps(positions).encode("utf-8"), key)
        ledger_bytes = encrypt(json.dumps(ledger).encode("utf-8"), key)
        now = datetime.now(timezone.utc)

        import hashlib

        # Hash the original (unencrypted) payloads for dedup
        positions_hash = hashlib.sha256(json.dumps(positions).encode("utf-8")).hexdigest()
        ledger_hash = hashlib.sha256(json.dumps(ledger).encode("utf-8")).hexdigest()

        return pa.table(
            {
                "fetched_at": [now, now],
                "broker": ["IBKR", "IBKR"],
                "source": [
                    f"/portfolio2/{account_id}/positions?sort=position&direction=d",
                    f"/portfolio/{account_id}/ledger",
                ],
                "payload": [positions_bytes, ledger_bytes],
                "payload_hash": [positions_hash, ledger_hash],
                "account_id": [account_id, account_id],
                "source_file": ["", ""],
            },
            schema=pa.schema([
                pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
                pa.field("broker", pa.string()),
                pa.field("source", pa.string()),
                pa.field("payload", pa.binary()),
                pa.field("payload_hash", pa.string()),
                pa.field("account_id", pa.string()),
                pa.field("source_file", pa.string()),
            ]),
        )

    def test_transform_produces_equity_and_cash_rows(self, fernet_key: bytes) -> None:
        positions = [
            {
                "contractDesc": "EUR ETF",
                "assetClass": "STK",
                "currency": "EUR",
                "mktValue": 100.0,
                "secIdType": "ISIN",
                "secId": "IE00BK5BQT80",
            }
        ]
        ledger = {
            "BASE": {"currency": "USD", "netliquidationvalue": 240.0, "exchangerate": 1.0},
            "EUR": {"currency": "EUR", "cashbalance": 100.0, "exchangerate": 1.2},
        }

        raw = self._build_raw_table(positions, ledger)
        result = transform.transform_snapshot(raw, fernet_key)

        assert result.num_rows == 2  # 1 equity + 1 cash
        # Check position types
        types = result.column("position_type").to_pylist()
        assert "EQUITY" in types
        assert "CASH" in types

        # Verify encrypted value can be decrypted
        values = result.column("value").to_pylist()
        decrypted_values = [decrypt_float(v, fernet_key) for v in values]
        assert any(v == pytest.approx(120.0) for v in decrypted_values)  # EUR ETF * 1.2
        assert any(v == pytest.approx(120.0) for v in decrypted_values)  # CASH EUR * 1.2

    def test_transform_preserves_isin(self, fernet_key: bytes) -> None:
        positions = [
            {
                "contractDesc": "TEST",
                "assetClass": "STK",
                "currency": "USD",
                "mktValue": 50.0,
                "isin": "US0378331005",
            }
        ]
        ledger = {
            "BASE": {"currency": "USD", "netliquidationvalue": 50.0, "exchangerate": 1.0},
            "USD": {"currency": "USD", "cashbalance": 0.0, "exchangerate": 1.0},
        }

        raw = self._build_raw_table(positions, ledger)
        result = transform.transform_snapshot(raw, fernet_key)

        isins = result.column("isin").to_pylist()
        assert "US0378331005" in isins

    def test_transform_skips_zero_value_positions(self, fernet_key: bytes) -> None:
        positions = [
            {
                "contractDesc": "ZERO",
                "assetClass": "STK",
                "currency": "USD",
                "mktValue": 0.0,
            }
        ]
        ledger = {
            "BASE": {"currency": "USD", "netliquidationvalue": 100.0, "exchangerate": 1.0},
            "USD": {"currency": "USD", "cashbalance": 0.0, "exchangerate": 1.0},
        }

        raw = self._build_raw_table(positions, ledger)
        result = transform.transform_snapshot(raw, fernet_key)

        # Only cash if non-zero, or empty if all zero
        position_types = result.column("position_type").to_pylist()
        assert "EQUITY" not in position_types


class TestFlexFetchSnapshot:
    """Tests for the Flex Web Service fetch path."""

    def test_fetch_snapshot_via_flex_stores_xml_payload(self) -> None:
        """fetch_snapshot_via_flex should return a raw table with the XML payload."""
        from unittest.mock import patch, MagicMock
        import xml.etree.ElementTree as ET

        from pipeline.connectors.ibkr.fetch import fetch_snapshot_via_flex

        # Build a minimal Flex XML response
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U123" fromDate="20240101" toDate="20240102">'
            '<AccountInformation accountId="U123" currency="USD"/>'
            '<OpenPositions>'
            '<OpenPosition symbol="AAPL" currency="USD" positionValue="10000" '
            'assetClass="STK" conid="265598" isin="US0378331005" '
            'description="Apple Inc" fxRateToBase="1.0"/>'
            '</OpenPositions>'
            '</FlexStatement>'
            '</FlexStatements>'
            '</FlexQueryResponse>'
        )
        root = ET.fromstring(xml_str)

        with patch("scripts.ibkr_net_worth.IbkrFlexClient") as MockClient:
            mock_instance = MagicMock()
            MockClient.return_value = mock_instance
            mock_instance.request_report.return_value = "12345"
            mock_instance.fetch_report.return_value = root

            result = fetch_snapshot_via_flex(token="test-token")

            assert result.num_rows == 1
            assert result.column("source")[0].as_py() == "flex"
            assert result.column("broker")[0].as_py() == "IBKR"
            MockClient.assert_called_once_with(
                token="test-token",
                query_id="1554188",
                base_url="https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService",
                timeout=30.0,
            )
            mock_instance.request_report.assert_called_once()
            mock_instance.fetch_report.assert_called_once_with("12345", retries=6, delay=3.0)

    def test_fetch_snapshot_via_flex_passes_query_id_and_base_url(self) -> None:
        """Custom query_id and base_url should be forwarded to IbkrFlexClient."""
        from unittest.mock import patch, MagicMock
        import xml.etree.ElementTree as ET

        from pipeline.connectors.ibkr.fetch import fetch_snapshot_via_flex

        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="0"/>'
            '</FlexQueryResponse>'
        )
        root = ET.fromstring(xml_str)

        with patch("scripts.ibkr_net_worth.IbkrFlexClient") as MockClient:
            mock_instance = MagicMock()
            MockClient.return_value = mock_instance
            mock_instance.request_report.return_value = "99999"
            mock_instance.fetch_report.return_value = root

            fetch_snapshot_via_flex(
                token="my-token",
                query_id="42",
                base_url="https://custom.example.com/api",
                timeout=60.0,
                retries=3,
                delay=5.0,
            )

            MockClient.assert_called_once_with(
                token="my-token",
                query_id="42",
                base_url="https://custom.example.com/api",
                timeout=60.0,
            )
            mock_instance.fetch_report.assert_called_once_with("99999", retries=3, delay=5.0)


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
        from pipeline.crypto import encrypt

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
            schema=pa.schema([
                pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
                pa.field("broker", pa.string()),
                pa.field("source", pa.string()),
                pa.field("payload", pa.binary()),
                pa.field("payload_hash", pa.string()),
                pa.field("account_id", pa.string()),
                pa.field("source_file", pa.string()),
            ]),
        )

    def test_transform_flex_produces_equity_and_cash(self, fernet_key: bytes) -> None:
        """Flex XML with positions and cash report should produce equity + cash rows."""
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U123" fromDate="20240101" toDate="20240102">'
            '<AccountInformation accountId="U123" currency="USD" cashBalance="500.0"/>'
            '<OpenPositions>'
            '<OpenPosition symbol="AAPL" currency="USD" positionValue="10000.0" '
            'assetClass="STK" conid="265598" isin="US0378331005" '
            'description="Apple Inc" fxRateToBase="1.0"/>'
            '</OpenPositions>'
            '<CashReport>'
            '<CashReportCurrency accountId="U123" currency="USD" endingCash="500.0"/>'
            '</CashReport>'
            '</FlexStatement>'
            '</FlexStatements>'
            '</FlexQueryResponse>'
        )

        raw = self._build_flex_raw_table(xml_str, fernet_key=fernet_key)
        result = transform._transform_flex_snapshot(raw, fernet_key)

        assert result.num_rows >= 2
        types = result.column("position_type").to_pylist()
        assert "EQUITY" in types
        assert "CASH" in types

    def test_transform_flex_preserves_isin(self, fernet_key: bytes) -> None:
        """ISIN from Flex OpenPosition should appear in normalized output."""
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U456" fromDate="20240101" toDate="20240102">'
            '<AccountInformation accountId="U456" currency="EUR"/>'
            '<OpenPositions>'
            '<OpenPosition symbol="SAP" currency="EUR" positionValue="5000.0" '
            'assetClass="STK" conid="12345" isin="DE0007164600" '
            'description="SAP SE" fxRateToBase="1.0"/>'
            '</OpenPositions>'
            '</FlexStatement>'
            '</FlexStatements>'
            '</FlexQueryResponse>'
        )

        raw = self._build_flex_raw_table(xml_str, fernet_key=fernet_key)
        result = transform._transform_flex_snapshot(raw, fernet_key)

        isins = result.column("isin").to_pylist()
        assert "DE0007164600" in isins

    def test_transform_flex_skips_zero_value_positions(self, fernet_key: bytes) -> None:
        """Positions with zero value should be skipped."""
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U789" fromDate="20240101" toDate="20240102">'
            '<AccountInformation accountId="U789" currency="USD"/>'
            '<OpenPositions>'
            '<OpenPosition symbol="ZERO" currency="USD" positionValue="0.0" '
            'assetClass="STK" fxRateToBase="1.0"/>'
            '</OpenPositions>'
            '</FlexStatement>'
            '</FlexStatements>'
            '</FlexQueryResponse>'
        )

        raw = self._build_flex_raw_table(xml_str, fernet_key=fernet_key)
        result = transform._transform_flex_snapshot(raw, fernet_key)

        types = result.column("position_type").to_pylist()
        assert "EQUITY" not in types

    def test_transform_flex_currency_override(self, fernet_key: bytes) -> None:
        """base_currency_override should override the detected base currency."""
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U999" fromDate="20240101" toDate="20240102">'
            '<AccountInformation accountId="U999" currency="BASE"/>'
            '<OpenPositions>'
            '<OpenPosition symbol="AAPL" currency="USD" positionValue="5000.0" '
            'assetClass="STK" fxRateToBase="0.9"/>'
            '</OpenPositions>'
            '</FlexStatement>'
            '</FlexStatements>'
            '</FlexQueryResponse>'
        )

        raw = self._build_flex_raw_table(xml_str, fernet_key=fernet_key)
        result = transform._transform_flex_snapshot(raw, fernet_key, base_currency_override="CHF")

        currencies = result.column("currency").to_pylist()
        assert all(c == "CHF" for c in currencies)


class TestConnectorFlexDispatch:
    """Tests for IbkrConnector dispatching to Flex vs Gateway path."""

    def test_fetch_snapshot_dispatches_to_flex_when_token_provided(self) -> None:
        """When flex_token is provided, IbkrConnector should use fetch_snapshot_via_flex."""
        from unittest.mock import patch, MagicMock

        from pipeline.connectors.registry import get

        with patch("pipeline.connectors.ibkr.fetch.fetch_snapshot_via_flex") as mock_flex:
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

    def test_fetch_snapshot_dispatches_to_gateway_when_no_token(self) -> None:
        """When no flex_token is provided, IbkrConnector should use gateway fetch."""
        from unittest.mock import patch, MagicMock

        from pipeline.connectors.registry import get

        with patch("pipeline.connectors.ibkr.fetch.fetch_snapshot") as mock_gateway:
            mock_gateway.return_value = MagicMock()
            connector = get("ibkr")

            connector.fetch_snapshot(
                base_url="https://localhost:5000/v1/api",
                account="U123",
            )

            mock_gateway.assert_called_once_with(
                base_url="https://localhost:5000/v1/api",
                account="U123",
            )

    def test_transform_snapshot_dispatches_to_flex_for_flex_source(self, fernet_key: bytes) -> None:
        """IbkrConnector.transform_snapshot should use Flex transform for flex source data."""
        from pipeline.connectors.registry import get

        # Build a minimal flex raw table
        xml_str = (
            '<FlexQueryResponse queryName="test" type="AF">'
            '<FlexStatements count="1">'
            '<FlexStatement accountId="U123" fromDate="20240101" toDate="20240102">'
            '<AccountInformation accountId="U123" currency="USD"/>'
            '<OpenPositions>'
            '<OpenPosition symbol="AAPL" currency="USD" positionValue="10000" '
            'assetClass="STK" fxRateToBase="1.0"/>'
            '</OpenPositions>'
            '</FlexStatement>'
            '</FlexStatements>'
            '</FlexQueryResponse>'
        )
        from pipeline.crypto import encrypt
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
            schema=pa.schema([
                pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
                pa.field("broker", pa.string()),
                pa.field("source", pa.string()),
                pa.field("payload", pa.binary()),
                pa.field("payload_hash", pa.string()),
                pa.field("account_id", pa.string()),
                pa.field("source_file", pa.string()),
            ]),
        )

        connector = get("ibkr")
        result = connector.transform_snapshot(raw, fernet_key)

        assert result.num_rows >= 1
        assert "EQUITY" in result.column("position_type").to_pylist()


