"""Tests for the Trading 212 pipeline connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from pipeline.connectors.trading212.client import (
    Trading212HttpError,
    account_currency,
    as_float,
    basic_auth_header,
    cash_value,
    concise_details,
    instrument_currency_by_ticker,
    instrument_isin_by_ticker,
    instrument_name_by_ticker,
    is_access_denied_html,
    net_worth_value,
    position_currency,
    position_isin,
    position_label,
    position_name,
    position_security_currency,
    position_value,
)
from pipeline.connectors.trading212.transform import transform_snapshot
from pipeline.crypto import decrypt_float, encrypt, generate_key


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
        import base64 as b64

        expected = b64.b64encode(b"api-key:api-secret").decode("ascii")
        assert basic_auth_header(" api-key ", " api-secret ") == f"Basic {expected}"

    def test_auth_method_is_basic_with_key_and_secret(self) -> None:
        """Regression test: T212 API requires HTTP Basic auth (key:secret), not Bearer.

        Commit f7c3674 changed Basic → Bearer based on a misdiagnosed 401
        (the real cause was an IP-restricted API key). The local API spec
        at docs/docs.trading212.com/api/section/general-information/api.json
        defines authWithSecretKey as { scheme: basic }. This test prevents
        a silent downgrade to Bearer or any other auth method.
        """
        import base64 as b64

        header = basic_auth_header("mykey", "mysecret")
        # Must start with "Basic " — never "Bearer " or a raw key
        assert header.startswith("Basic "), f"Expected Basic auth, got: {header}"
        decoded = b64.b64decode(header[len("Basic ") :]).decode("utf-8")
        assert decoded == "mykey:mysecret", f"Expected key:secret, got: {decoded}"

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
        assert position_isin(position, {"VWCE_DE_EQ": "IE00BK5BQT80"}) == "IE00BK5BQT80"

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
        instruments = [
            {"ticker": "VUAA", "currencyCode": "USD", "name": "Vanguard ETF"}
        ]
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
            "GET https://live.trading212.com/api/v0/equity/account/summary "
            'failed: HTTP 401 {"error": "API key is invalid"}'
        )


class TestTransformSnapshot:
    """Tests for the raw → normalized transform."""

    @pytest.fixture()
    def fernet_key(self) -> bytes:
        key = generate_key()
        self._fernet_key = key
        return key

    def _build_raw_table(
        self,
        summary: dict,
        positions: list[dict],
        instruments: list[dict] | None = None,
        account_id: str = "T212-1",
    ) -> pa.Table:
        """Build a raw-layer table from fake API responses.

        Payloads are encrypted to match the real pipeline flow where
        raw Delta tables store encrypted payloads.
        """
        import hashlib

        key = self._fernet_key
        now = datetime.now(timezone.utc)
        sources = ["/equity/account/summary", "/equity/positions"]
        raw_payloads = [
            json.dumps(summary).encode("utf-8"),
            json.dumps(positions).encode("utf-8"),
        ]
        if instruments is not None:
            sources.append("/equity/metadata/instruments")
            raw_payloads.append(json.dumps(instruments).encode("utf-8"))

        # Encrypt payloads like the real pipeline does in ingest_raw
        encrypted_payloads = [encrypt(p, key) for p in raw_payloads]

        return pa.table(
            {
                "fetched_at": [now] * len(sources),
                "broker": ["Trading 212"] * len(sources),
                "source": sources,
                "payload": encrypted_payloads,
                "payload_hash": [hashlib.sha256(p).hexdigest() for p in raw_payloads],
                "account_id": [account_id] * len(sources),
                "source_file": [""] * len(sources),
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
        summary = {"currencyCode": "EUR", "free": 25.0, "total": 225.0}
        positions = [
            {"ticker": "VUAA", "quantity": 2, "currentPrice": 100.0},
            {"ticker": "ZERO", "quantity": 0, "currentPrice": 100.0},
        ]
        instruments = [
            {"ticker": "VUAA", "currencyCode": "USD", "name": "Vanguard ETF"}
        ]

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
