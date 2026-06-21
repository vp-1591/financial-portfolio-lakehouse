"""Tests for the IBKR pipeline connector."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import sys

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
from pipeline.connectors.ibkr.transform import transform_snapshot
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
        result = transform_snapshot(raw, fernet_key)

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
        result = transform_snapshot(raw, fernet_key)

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
        result = transform_snapshot(raw, fernet_key)

        # Only cash if non-zero, or empty if all zero
        position_types = result.column("position_type").to_pylist()
        assert "EQUITY" not in position_types