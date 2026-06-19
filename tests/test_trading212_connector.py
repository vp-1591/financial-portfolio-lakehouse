"""Tests for the Trading 212 pipeline connector."""

from __future__ import annotations

import json
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest

from pipeline.connectors.trading212.client import (
    Trading212Client,
    Trading212Error,
    Trading212HttpError,
    account_currency,
    as_float,
    basic_auth_header,
    cash_value,
    concise_details,
    first_value,
    instrument_currency_by_ticker,
    instrument_isin_by_ticker,
    instrument_name_by_ticker,
    is_access_denied_html,
    nested_dict,
    net_worth_value,
    position_currency,
    position_isin,
    position_label,
    position_name,
    position_security_currency,
    position_value,
)
from pipeline.connectors.trading212.transform import transform_snapshot
from pipeline.crypto import decrypt_float, generate_key


class TestClientParsing:
    """Tests preserved from tests/test_trading212_net_worth.py."""

    def test_as_float(self) -> None:
        assert as_float(None) == 0.0
        assert as_float("") == 0.0
        assert as_float(42) == 42.0
        assert as_float("3.14") == 3.14
        assert as_float("abc", -1.0) == -1.0

    def test_is_access_denied_html(self) -> None:
        assert is_access_denied_html("<html><h1>Access denied</h1></html>")
        assert not is_access_denied_html('{"error":"not found"}')

    def test_concise_details_returns_plain_text_body(self) -> None:
        assert concise_details("unauthorized") == "unauthorized"

    def test_basic_auth_header(self) -> None:
        assert (
            basic_auth_header(" api-key ", " api-secret ")
            == "Basic YXBpLWtleTphcGktc2VjcmV0"
        )

    def test_account_currency(self) -> None:
        assert account_currency({"currencyCode": "EUR"}) == "EUR"
        assert account_currency({"baseCurrency": "USD"}) == "USD"
        assert account_currency({}) == ""

    def test_cash_value(self) -> None:
        assert cash_value({"free": 25.0}) == 25.0
        assert cash_value({"availableFunds": 100.0}) == 100.0
        assert cash_value({}) == 0.0

    def test_net_worth_value(self) -> None:
        assert net_worth_value({"total": 225.0}, 0.0) == 225.0
        assert net_worth_value({}, 100.0) == 100.0

    def test_position_label(self) -> None:
        position = {
            "instrument": {
                "ticker": "VWCE_DE_EQ",
                "currencyCode": "EUR",
                "name": "VWCE ETF",
                "isin": "IE00BK5BQT80",
            },
        }
        assert position_label(position) == "VWCE_DE_EQ"

    def test_position_name(self) -> None:
        position = {
            "instrument": {
                "ticker": "VWCE_DE_EQ",
                "name": "VWCE ETF",
            },
        }
        assert position_name(position, {}) == "VWCE ETF"

    def test_position_isin(self) -> None:
        position = {
            "instrument": {
                "ticker": "VWCE_DE_EQ",
                "isin": "IE00BK5BQT80",
            },
        }
        assert position_isin(position, {}) == "IE00BK5BQT80"

    def test_position_isin_uses_instrument_metadata_lookup(self) -> None:
        position = {"ticker": "VWCE_DE_EQ"}
        assert (
            position_isin(position, {"VWCE_DE_EQ": "IE00BK5BQT80"})
            == "IE00BK5BQT80"
        )

    def test_position_value_prefers_wallet_impact(self) -> None:
        position = {
            "walletImpact": {"currency": "PLN", "currentValue": 1290.0},
            "quantity": 3,
            "currentPrice": 100.0,
        }
        assert position_value(position) == 1290.0

    def test_position_value_falls_back_to_quantity_times_price(self) -> None:
        position = {"quantity": 2, "currentPrice": 100.0}
        assert position_value(position) == 200.0

    def test_position_currency_uses_wallet_currency(self) -> None:
        position = {"walletImpact": {"currency": "PLN", "currentValue": 100.0}}
        assert position_currency(position, {}, "EUR") == "PLN"

    def test_position_security_currency_uses_instrument_currency(self) -> None:
        position = {
            "instrument": {"ticker": "IS3N", "currencyCode": "EUR"},
        }
        assert position_security_currency(position, {}, "PLN") == "EUR"

    def test_instrument_currencies(self) -> None:
        instruments = [{"ticker": "VUAA", "currencyCode": "USD", "name": "Vanguard ETF"}]
        assert instrument_currency_by_ticker(instruments) == {"VUAA": "USD"}

    def test_instrument_names(self) -> None:
        instruments = [{"ticker": "VUAA", "name": "Vanguard ETF"}]
        assert instrument_name_by_ticker(instruments) == {"VUAA": "Vanguard ETF"}

    def test_instrument_isins(self) -> None:
        instruments = [{"ticker": "VUAA", "isin": "IE00BK5BQT80"}]
        assert instrument_isin_by_ticker(instruments) == {"VUAA": "IE00BK5BQT80"}

    def test_access_denied_html_gets_actionable_error(self) -> None:
        error = Trading212HttpError(
            "GET",
            "https://live.trading212.com/api/v0/equity/account/info",
            403,
            "<html><h1>Access denied</h1></html>",
        )
        assert "access denied by Trading 212" in str(error)
        assert "--user-agent" in str(error) or "user-agent" in str(error).lower()

    def test_unauthorized_error_is_not_padded_with_guesses(self) -> None:
        error = Trading212HttpError(
            "GET",
            "https://live.trading212.com/api/v0/equity/account/summary",
            401,
            '{"error":"API key is invalid"}',
        )
        assert str(error) == (
            'GET https://live.trading212.com/api/v0/equity/account/summary '
            'failed: HTTP 401 {"error": "API key is invalid"}'
        )


class TestTransformSnapshot:
    """Tests for the raw → normalized transform."""

    @pytest.fixture()
    def fernet_key(self) -> bytes:
        return generate_key()

    def _build_raw_table(
        self,
        summary: dict,
        positions: list[dict],
        instruments: list[dict] | None = None,
        account_id: str = "T212-1",
    ) -> pa.Table:
        """Build a raw-layer table from fake API responses."""
        import hashlib

        now = datetime.now(timezone.utc)
        sources = ["/equity/account/summary", "/equity/positions"]
        payloads_data = [
            json.dumps(summary).encode("utf-8"),
            json.dumps(positions).encode("utf-8"),
        ]
        if instruments is not None:
            sources.append("/equity/metadata/instruments")
            payloads_data.append(json.dumps(instruments).encode("utf-8"))

        return pa.table(
            {
                "fetched_at": [now] * len(sources),
                "broker": ["Trading 212"] * len(sources),
                "source": sources,
                "payload": payloads_data,
                "payload_hash": [hashlib.sha256(p).hexdigest() for p in payloads_data],
                "account_id": [account_id] * len(sources),
                "source_file": [""] * len(sources),
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
        summary = {"currencyCode": "EUR", "free": 25.0, "total": 225.0}
        positions = [
            {"ticker": "VUAA", "quantity": 2, "currentPrice": 100.0},
            {"ticker": "ZERO", "quantity": 0, "currentPrice": 100.0},
        ]
        instruments = [{"ticker": "VUAA", "currencyCode": "USD", "name": "Vanguard ETF"}]

        raw = self._build_raw_table(summary, positions, instruments)
        result = transform_snapshot(raw, fernet_key)

        # 1 equity (ZERO is zero-value) + 1 cash
        assert result.num_rows == 2
        types = result.column("position_type").to_pylist()
        assert "EQUITY" in types
        assert "CASH" in types

        # Verify encrypted values decrypt correctly
        values = result.column("value").to_pylist()
        decrypted = [decrypt_float(v, fernet_key) for v in values]
        assert any(v == pytest.approx(200.0) for v in decrypted)  # VUAA
        assert any(v == pytest.approx(25.0) for v in decrypted)  # CASH EUR

    def test_transform_preserves_isin(self, fernet_key: bytes) -> None:
        summary = {"currencyCode": "EUR", "total": 100.0}
        positions = [
            {
                "instrument": {
                    "ticker": "IS3Nd_EQ",
                    "currencyCode": "EUR",
                    "name": "iShares Core MSCI World",
                    "isin": "IE00B4L5Y983",
                },
                "walletImpact": {"currency": "PLN", "currentValue": 100.0},
            }
        ]
        instruments = [
            {
                "ticker": "IS3Nd_EQ",
                "currencyCode": "EUR",
                "name": "iShares Core MSCI World UCITS ETF",
                "isin": "IE00B4L5Y983",
            }
        ]

        raw = self._build_raw_table(summary, positions, instruments)
        result = transform_snapshot(raw, fernet_key)

        isins = result.column("isin").to_pylist()
        assert "IE00B4L5Y983" in isins