"""Tests for the Trading 212 pipeline connector."""

from __future__ import annotations

import base64 as b64
import hashlib
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import ClassVar
from unittest import mock
from unittest.mock import MagicMock

import polars as pl
import pyarrow as pa
import pytest

from pipeline.connectors.registry import get
from pipeline.connectors.trading212.client import (
    Trading212Client,
    Trading212Error,
    Trading212HttpError,
    account_currency,
    as_float,
    basic_auth_header,
    cash_value,
    concise_details,
    is_access_denied_html,
)
from pipeline.connectors.trading212.fetch import fetch_events
from pipeline.connectors.trading212.transform import (
    transform_events,
    transform_snapshot,
)
from pipeline.connectors.transform_utils import _unwrap_events
from pipeline.crypto import decrypt_float, encrypt, generate_key
from pipeline.normalized.models import events_normalized_schema
from pipeline.raw.models import RAW_SCHEMA
from tests.fixtures.trading212 import t212_normalized_snapshot, t212_raw_snapshot


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

        expected = b64.b64encode(b"api-key:api-secret").decode("ascii")
        assert basic_auth_header(" api-key ", " api-secret ") == f"Basic {expected}"

    def test_auth_method_is_basic_with_key_and_secret(self) -> None:
        """Regression test: T212 API requires HTTP Basic auth (key:secret), not Bearer.

        Commit f7c3674 changed Basic → Bearer based on a misdiagnosed 401
        (the real cause was an IP-restricted API key). The local API spec
        at docs/_vendor/trading212/api/section/general-information/api.json
        defines authWithSecretKey as { scheme: basic }. This test prevents
        a silent downgrade to Bearer or any other auth method.
        """

        header = basic_auth_header("mykey", "mysecret")
        # Must start with "Basic " — never "Bearer " or a raw key
        assert header.startswith("Basic "), f"Expected Basic auth, got: {header}"
        decoded = b64.b64decode(header[len("Basic ") :]).decode("utf-8")
        assert decoded == "mykey:mysecret", f"Expected key:secret, got: {decoded}"

    def test_account_currency(self) -> None:
        assert account_currency({"currency": "EUR"}) == "EUR"
        assert account_currency({}) == ""

    def test_cash_value(self) -> None:
        assert cash_value({}) == 0.0

    def test_cash_value_nested_dict_available_to_trade(self) -> None:
        """Demo API returns cash as a nested dict with availableToTrade."""
        summary = {
            "cash": {"availableToTrade": 10500.0, "reservedForOrders": 0, "inPies": 0}
        }
        assert cash_value(summary) == 10500.0

    def test_cash_value_nested_dict_no_available_to_trade(self) -> None:
        """Nested dict without availableToTrade returns 0.0."""
        summary = {"cash": {"reservedForOrders": 0}}
        assert cash_value(summary) == 0.0

    def test_cash_value_none_value(self) -> None:
        """Explicit None value for cash returns 0.0."""
        assert cash_value({"cash": None}) == 0.0

    def test_access_denied_html_gets_actionable_error(self) -> None:
        error = Trading212HttpError(
            "GET",
            "https://live.trading212.com/api/v0/equity/account/info",
            403,
            "<html><h1>Access denied</h1></html>",
        )
        assert "access denied by Trading 212" in str(error)
        assert "Verify your API credentials" in str(error)

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
    ) -> pa.Table:
        """Build a raw-layer table from fake API responses.

        Payloads are encrypted to match the real pipeline flow where
        raw Delta tables store encrypted payloads.
        """

        key = self._fernet_key
        now = datetime.now(UTC)
        sources = ["/equity/account/summary", "/equity/positions"]
        raw_payloads = [
            json.dumps(summary).encode("utf-8"),
            json.dumps(positions).encode("utf-8"),
        ]

        # Encrypt payloads like the real pipeline does in ingest_raw
        encrypted_payloads = [encrypt(p, key) for p in raw_payloads]

        return pa.table(
            {
                "fetched_at": [now] * len(sources),
                "broker": ["Trading 212"] * len(sources),
                "source": sources,
                "payload": encrypted_payloads,
                "payload_hash": [hashlib.sha256(p).hexdigest() for p in raw_payloads],
                "account_id": [None] * len(sources),
            },
            schema=RAW_SCHEMA,
        )

    def test_transform_produces_equity_and_cash_rows(self, fernet_key: bytes) -> None:
        summary = {
            "currency": "EUR",
            "cash": {"availableToTrade": 25.0},
            "totalValue": 225.0,
        }
        positions = [
            {
                "instrument": {
                    "ticker": "VUAA",
                    "name": "Vanguard ETF",
                    "isin": "IE00BK5BQT80",
                    "currency": "USD",
                },
                "quantity": 2,
                "currentPrice": 100.0,
            },
            {
                "instrument": {
                    "ticker": "ZERO",
                    "name": "Zero Inc",
                    "isin": "US0000000000",
                    "currency": "USD",
                },
                "quantity": 0,
                "currentPrice": 100.0,
            },
        ]

        raw = self._build_raw_table(summary, positions)
        result = transform_snapshot(raw, fernet_key)

        # 1 equity (ZERO is zero-value) + 1 cash
        assert result.num_rows == 2
        types = result.column("position_type").to_pylist()
        assert "EQUITY" in types
        assert "CASH" in types

        # Verify encrypted values decrypt correctly
        values = result.column("security_value").to_pylist()
        decrypted = [decrypt_float(v, fernet_key) for v in values]
        assert any(v == pytest.approx(200.0) for v in decrypted)  # VUAA
        assert any(v == pytest.approx(25.0) for v in decrypted)  # CASH EUR

    def test_transform_preserves_isin(self, fernet_key: bytes) -> None:
        summary = {
            "currency": "EUR",
            "cash": {"availableToTrade": 0.0},
            "totalValue": 100.0,
        }
        positions = [
            {
                "instrument": {
                    "ticker": "IS3Nd_EQ",
                    "currency": "EUR",
                    "name": "iShares Core MSCI World",
                    "isin": "IE00B4L5Y983",
                },
                "quantity": 2,
                "currentPrice": 50.0,
                "walletImpact": {"currency": "PLN", "currentValue": 100.0},
            }
        ]

        raw = self._build_raw_table(summary, positions)
        result = transform_snapshot(raw, fernet_key)

        isins = result.column("isin").to_pylist()
        assert "IE00B4L5Y983" in isins

        # security_ccy should be instrument currency (EUR from
        # instrument.currency), not wallet currency (PLN from
        # walletImpact) — the transform pairs currentPrice*quantity with
        # the instrument currency when both factors are available.
        ccys = result.column("security_ccy").to_pylist()
        assert ccys[0] == "EUR"

    def test_transform_produces_cash_from_nested_cash_dict(
        self, fernet_key: bytes
    ) -> None:
        """Demo API returns cash as a nested dict — transform should extract availableToTrade."""
        summary = {
            "currency": "PLN",
            "cash": {"availableToTrade": 10500.0, "reservedForOrders": 0, "inPies": 0},
            "totalValue": 15000.0,
        }
        positions = [
            {
                "instrument": {
                    "ticker": "VUAA",
                    "name": "Vanguard ETF",
                    "isin": "IE00BK5BQT80",
                    "currency": "USD",
                },
                "quantity": 2,
                "currentPrice": 100.0,
            },
        ]

        raw = self._build_raw_table(summary, positions)
        result = transform_snapshot(raw, fernet_key)

        types = result.column("position_type").to_pylist()
        assert "CASH" in types

        cash_idx = types.index("CASH")
        values = result.column("security_value").to_pylist()
        cash_amount = decrypt_float(values[cash_idx], fernet_key)
        assert cash_amount == pytest.approx(10500.0)

    def test_snapshot_security_ccy_uses_instrument_currency(
        self, fernet_key: bytes
    ) -> None:
        """Snapshot security_ccy should reflect the currency of security_value.

        When currentPrice and quantity are available, security_ccy must be
        the instrument's trading currency (EUR/GBX/GBP), not the wallet
        currency (PLN). The transform pairs ``currentPrice * quantity`` with
        the instrument currency; only the CASH row uses the wallet currency.
        """
        summary = {
            "currency": "PLN",
            "cash": {"availableToTrade": 10500.0},
            "totalValue": 24989.22,
        }
        positions = [
            {
                "ticker": "SPYId_EQ",
                "quantity": 40.724,
                "currentPrice": 11.325,
                "walletImpact": {"currency": "PLN", "currentValue": 1991.29},
                "instrument": {
                    "ticker": "SPYId_EQ",
                    "name": "SPY",
                    "isin": "IE00B44Z5B85",
                    "currency": "EUR",
                },
            },
            {
                "ticker": "SGLNl_EQ",
                "quantity": 25.167,
                "currentPrice": 5901.0,
                "walletImpact": {"currency": "PLN", "currentValue": 7506.67},
                "instrument": {
                    "ticker": "SGLNl_EQ",
                    "name": "SGLN",
                    "isin": "GB00B579F147",
                    "currency": "GBX",
                },
            },
            {
                "ticker": "VUAGl_EQ",
                "quantity": 9.176,
                "currentPrice": 107.619,
                "walletImpact": {"currency": "PLN", "currentValue": 4991.41},
                "instrument": {
                    "ticker": "VUAGl_EQ",
                    "name": "VUAG",
                    "isin": "IE00B76SQL35",
                    "currency": "GBP",
                },
            },
        ]

        raw = self._build_raw_table(summary, positions)
        result = transform_snapshot(raw, fernet_key)

        ccys = result.column("security_ccy").to_pylist()
        types = result.column("position_type").to_pylist()

        # Equities use their instrument trading currency (EUR/GBX/GBP) as
        # security_ccy, not the wallet currency (PLN); CASH stays PLN.
        equity_ccys = [ccys[i] for i, t in enumerate(types) if t == "EQUITY"]
        cash_ccys = [ccys[i] for i, t in enumerate(types) if t == "CASH"]
        assert equity_ccys == ["EUR", "GBX", "GBP"], (
            f"Expected equity security_ccy [EUR, GBX, GBP], got {equity_ccys}"
        )
        assert cash_ccys == ["PLN"], (
            f"Expected cash security_ccy [PLN], got {cash_ccys}"
        )

    def test_transform_snapshot_pairs_instrument_value_with_instrument_ccy(
        self, fernet_key: bytes
    ) -> None:
        """Equity with currentPrice*quantity uses instrument currency and value.

        The transform pairs ``currentPrice * quantity`` (instrument currency)
        with the instrument's trading currency from ``instrument.currency``,
        NOT the wallet currency from ``walletImpact.currency``.
        """
        summary = {
            "currency": "PLN",
            "cash": {"availableToTrade": 0.0},
            "totalValue": 2500.0,
        }
        positions = [
            {
                "ticker": "VWCEl_EQ",
                "quantity": 25.0,
                "currentPrice": 100.0,
                "walletImpact": {"currency": "PLN", "currentValue": 2500.0},
                "instrument": {
                    "ticker": "VWCEl_EQ",
                    "name": "VWCE",
                    "isin": "IE00BK5BQT80",
                    "currency": "EUR",
                },
            }
        ]

        raw = self._build_raw_table(summary, positions)
        result = transform_snapshot(raw, fernet_key)

        types = result.column("position_type").to_pylist()
        assert "EQUITY" in types
        equity_idx = types.index("EQUITY")

        ccys = result.column("security_ccy").to_pylist()
        assert ccys[equity_idx] == "EUR", (
            f"Expected instrument ccy EUR, got {ccys[equity_idx]}"
        )

        values = result.column("security_value").to_pylist()
        val = decrypt_float(values[equity_idx], fernet_key)
        assert val == pytest.approx(2500.0), (
            f"Expected quantity*currentPrice = 2500.0, got {val}"
        )

    def test_transform_snapshot_with_mixed_endpoint_timestamps(
        self, fernet_key: bytes
    ) -> None:
        """Snapshot transform populates rows even if endpoints have different fetched_at timestamps.

        Regression test: when dedup_raw skips unchanged endpoints (e.g. account summary),
        the raw table stores rows with different fetched_at timestamps. filter_latest_snapshot
        must keep the latest payload per source so that summary_data is not lost.

        This test uses >=2 rows per source so the per-source dedup in
        filter_latest_snapshot is actually exercised: each source has a stale
        (older) row and a fresh (newer) row. If the dedup is disabled
        (e.g. body replaced with ``return raw``), transform_snapshot sees
        duplicate summary/positions payloads and the stale summary (with a
        *different* cash balance) leaks into the output, changing the cash
        row's security_value and failing the assertion below.
        """

        # Two summary payloads: stale has cash=50.0, fresh has cash=250.0.
        # filter_latest_snapshot must keep only the fresh row per source.
        # The stale row is placed AFTER the fresh row in table order so that
        # if dedup is disabled, the transform's last-row-wins loop overwrites
        # the fresh summary with the stale one (cash=50.0), failing the
        # assertion below. With dedup enabled, only the fresh row survives.
        fresh_summary = {
            "currency": "EUR",
            "cash": {"availableToTrade": 250.0},
            "totalValue": 250.0,
        }
        stale_summary = {
            "currency": "EUR",
            "cash": {"availableToTrade": 50.0},
            "totalValue": 250.0,
        }
        fresh_positions = [
            {
                "instrument": {
                    "ticker": "VUAA",
                    "name": "Vanguard ETF",
                    "isin": "IE00BK5BQT80",
                    "currency": "USD",
                },
                "quantity": 2,
                "currentPrice": 100.0,
            }
        ]
        stale_positions = [
            {
                "instrument": {
                    "ticker": "VUAA",
                    "name": "Vanguard ETF",
                    "isin": "IE00BK5BQT80",
                    "currency": "USD",
                },
                "quantity": 2,
                "currentPrice": 100.0,
            }
        ]

        now = datetime.now(UTC)
        t_older = now - timedelta(hours=1)

        sources = [
            "/equity/account/summary",
            "/equity/account/summary",
            "/equity/positions",
            "/equity/positions",
        ]
        # fresh (now) first, stale (t_older) second — so last-row-wins without
        # dedup picks the stale summary.
        fetched_ats = [now, t_older, now, t_older]
        raw_payloads = [
            json.dumps(fresh_summary).encode("utf-8"),
            json.dumps(stale_summary).encode("utf-8"),
            json.dumps(fresh_positions).encode("utf-8"),
            json.dumps(stale_positions).encode("utf-8"),
        ]
        encrypted_payloads = [encrypt(p, fernet_key) for p in raw_payloads]

        raw = pa.table(
            {
                "fetched_at": fetched_ats,
                "broker": ["Trading 212"] * 4,
                "source": sources,
                "payload": encrypted_payloads,
                "payload_hash": [hashlib.sha256(p).hexdigest() for p in raw_payloads],
                "account_id": [None] * 4,
            },
            schema=RAW_SCHEMA,
        )

        result = transform_snapshot(raw, fernet_key)
        assert result.num_rows == 2
        types = result.column("position_type").to_pylist()
        assert "EQUITY" in types
        assert "CASH" in types

        # The cash row must come from the FRESH summary (cash=250.0), not the
        # stale one (cash=50.0). Because the stale row is placed after the fresh
        # row in table order, disabling filter_latest_snapshot lets the
        # transform's last-row-wins loop pick the stale summary, producing a
        # 50.0 cash row instead of 250.0. Asserting the exact decrypted cash
        # value pins this: a disabled dedup fails here.
        cash_idx = types.index("CASH")
        cash_value = decrypt_float(
            result.column("security_value")[cash_idx].as_py(), fernet_key
        )
        assert cash_value == pytest.approx(250.0), (
            f"Expected fresh cash=250.0 after per-source dedup, got {cash_value}"
        )

    def test_transform_snapshot_works_without_instruments_source(
        self, fernet_key: bytes
    ) -> None:
        """The transform no longer reads /metadata/instruments; summary + positions suffice."""
        summary = {
            "currency": "PLN",
            "cash": {"availableToTrade": 1500.0},
            "totalValue": 4000.0,
        }
        positions = [
            {
                "instrument": {
                    "ticker": "VWCEl_EQ",
                    "name": "Vanguard FTSE All-World",
                    "isin": "IE00BK5BQT80",
                    "currency": "EUR",
                },
                "quantity": 25.0,
                "currentPrice": 100.0,
            },
        ]
        raw = self._build_raw_table(
            summary, positions
        )  # no instruments= → 2 sources only
        result = transform_snapshot(raw, fernet_key)
        assert result.num_rows == 2  # 1 equity + 1 cash
        labels = result.column("label").to_pylist()
        assert "VWCEl_EQ" in labels
        assert any(l.startswith("CASH") for l in labels)

    def test_transform_snapshot_raises_on_null_price(self, fernet_key: bytes) -> None:
        """A position with null currentPrice/quantity fast-fails (data corruption).

        Neither field is ever null on real data (72/72 staging positions carry
        both), so a null means a corrupted/truncated payload, not a normal API
        state. The transform raises rather than silently dropping the position.
        """
        summary = {
            "currency": "PLN",
            "cash": {"availableToTrade": 1500.0},
            "totalValue": 4000.0,
        }
        positions = [
            {
                "instrument": {
                    "ticker": "SUSP",
                    "name": "Suspended Co",
                    "isin": "US0000000000",
                    "currency": "USD",
                },
                "quantity": 10.0,
                "currentPrice": None,  # null price → corruption, fast-fail
            },
        ]
        raw = self._build_raw_table(summary, positions)

        with pytest.raises(ValueError, match="SUSP"):
            transform_snapshot(raw, fernet_key)

    def test_transform_snapshot_raises_on_null_quantity(
        self, fernet_key: bytes
    ) -> None:
        """A position with null quantity fast-fails (same guard as null price).

        The guard is ``if price is None or quantity is None``; the sibling
        null-price test only exercises the ``price`` half. This covers the
        ``quantity`` half so a regression that dropped the quantity check
        (e.g. ``and`` instead of ``or``, or a price-only guard) is caught.
        """
        summary = {
            "currency": "PLN",
            "cash": {"availableToTrade": 1500.0},
            "totalValue": 4000.0,
        }
        positions = [
            {
                "instrument": {
                    "ticker": "SUSP",
                    "name": "Suspended Co",
                    "isin": "US0000000000",
                    "currency": "USD",
                },
                "quantity": None,  # null quantity → corruption, fast-fail
                "currentPrice": 100.0,
            },
        ]
        raw = self._build_raw_table(summary, positions)

        with pytest.raises(ValueError, match="SUSP"):
            transform_snapshot(raw, fernet_key)

    def test_transform_snapshot_raises_value_error_when_instrument_none(
        self, fernet_key: bytes
    ) -> None:
        """A null ``instrument`` plus a null price surfaces a ValueError.

        Without the guard in the error-message builder, ``instrument.get(...)``
        on a None instrument raises AttributeError and masks the intended
        ValueError. This locks in that the guard works: the corruption surfaces
        as a ValueError, not an AttributeError.
        """
        summary = {
            "currency": "PLN",
            "cash": {"availableToTrade": 1500.0},
            "totalValue": 4000.0,
        }
        positions = [
            {
                "instrument": None,  # corrupted payload: instrument itself is null
                "quantity": 10.0,
                "currentPrice": None,
            },
        ]
        raw = self._build_raw_table(summary, positions)

        with pytest.raises(ValueError, match="null currentPrice/quantity"):
            transform_snapshot(raw, fernet_key)

    def test_transform_snapshot_skips_zero_value_quietly(
        self, fernet_key: bytes
    ) -> None:
        """A genuinely zero-value position (both fields present, value 0) is skipped.

        This is a legitimate closed/empty position, not corruption: currentPrice
        and quantity are both present and non-null, only their product is 0. The
        transform drops it quietly (no raise, no row emitted).
        """
        summary = {
            "currency": "PLN",
            "cash": {"availableToTrade": 1500.0},
            "totalValue": 4000.0,
        }
        positions = [
            {
                "instrument": {
                    "ticker": "CLOSED",
                    "name": "Closed Position",
                    "isin": "US0000000001",
                    "currency": "USD",
                },
                "quantity": 0.0,  # legitimate zero → skipped quietly
                "currentPrice": 100.0,
            },
        ]
        raw = self._build_raw_table(summary, positions)

        result = transform_snapshot(raw, fernet_key)

        # Only the CASH row survives; the zero-value equity is skipped, no raise.
        labels = result.column("label").to_pylist()
        assert "CLOSED" not in labels
        assert any("CASH" in lbl for lbl in labels)


class TestClientPagination:
    """Tests for Trading212Client._fetch_paginated()."""

    def test_fetch_paginated_returns_bare_list(self) -> None:
        """When API returns a bare list, _fetch_paginated returns it directly."""

        client = Trading212Client(
            "https://demo.trading212.com/api/v0",
            api_key="test",
            api_secret="test",
        )
        items = [{"id": 1}, {"id": 2}]
        client.request = MagicMock(return_value=items)  # type: ignore[method-assign]

        result = client._fetch_paginated("/equity/history/orders")
        assert result == items
        client.request.assert_called_once_with("GET", "/equity/history/orders")

    def test_fetch_paginated_collects_all_pages(self) -> None:
        """When API returns paginated dict responses, all items are collected."""

        client = Trading212Client(
            "https://demo.trading212.com/api/v0",
            api_key="test",
            api_secret="test",
        )
        page1 = {
            "items": [{"id": 1}, {"id": 2}],
            "nextPagePath": "/equity/history/orders?cursor=abc",
        }
        page2 = {
            "items": [{"id": 3}],
            "nextPagePath": None,
        }
        client.request = MagicMock(side_effect=[page1, page2])  # type: ignore[method-assign]

        result = client._fetch_paginated("/equity/history/orders")
        assert len(result) == 3
        assert result[0]["id"] == 1
        assert result[2]["id"] == 3
        assert client.request.call_count == 2

    def test_fetch_paginated_single_page(self) -> None:
        """Paginated response with nextPagePath=None returns items from one call."""

        client = Trading212Client(
            "https://demo.trading212.com/api/v0",
            api_key="test",
            api_secret="test",
        )
        single_page = {
            "items": [{"id": 10}],
            "nextPagePath": None,
        }
        client.request = MagicMock(return_value=single_page)  # type: ignore[method-assign]

        result = client._fetch_paginated("/equity/history/dividends")
        assert len(result) == 1
        assert result[0]["id"] == 10

    def test_fetch_paginated_raises_on_unexpected_type(self) -> None:
        """Non-list, non-dict responses raise Trading212Error."""

        client = Trading212Client(
            "https://demo.trading212.com/api/v0",
            api_key="test",
            api_secret="test",
        )
        client.request = MagicMock(return_value="unexpected string")  # type: ignore[method-assign]

        with pytest.raises(Trading212Error, match="Unexpected response type"):
            client._fetch_paginated("/equity/history/orders")

    def test_fetch_paginated_raises_on_missing_items(self) -> None:
        """Dict response without 'items' key raises Trading212Error."""

        client = Trading212Client(
            "https://demo.trading212.com/api/v0",
            api_key="test",
            api_secret="test",
        )
        client.request = MagicMock(return_value={"data": "no items"})  # type: ignore[method-assign]

        with pytest.raises(Trading212Error, match="missing 'items' list"):
            client._fetch_paginated("/equity/history/orders")

    def test_orders_uses_pagination(self) -> None:
        """orders() delegates to _fetch_paginated."""

        client = Trading212Client(
            "https://demo.trading212.com/api/v0",
            api_key="test",
            api_secret="test",
        )
        expected = [{"id": 1}]
        client._fetch_paginated = MagicMock(return_value=expected)  # type: ignore[method-assign]

        result = client.orders()
        assert result == expected
        client._fetch_paginated.assert_called_once_with("/equity/history/orders")


class TestSnapshotFetch:
    """Tests for the Trading 212 snapshot fetch."""

    def test_fetch_snapshot_does_not_request_instruments(self) -> None:
        """fetch_snapshot calls account_summary + positions but NOT instruments."""
        from pipeline.connectors.trading212.fetch import fetch_snapshot

        with mock.patch(
            "pipeline.connectors.trading212.fetch.Trading212Client"
        ) as MockCls:
            instance = MockCls.return_value
            instance.captured_responses = []

            fetch_snapshot(
                api_key="test",
                api_secret="test",
                base_url="https://demo.trading212.com/api/v0",
            )

            instance.account_summary.assert_called_once()
            instance.positions.assert_called_once()
            instance.instruments.assert_not_called()


class TestEventsFetch:
    """Tests for Trading 212 events fetch error handling."""

    def test_fetch_events_raises_on_single_endpoint_failure(self, caplog) -> None:
        """A single failing events endpoint aborts the fetch (fail loud).

        The transform normalizes the current fetch's events (the single bronze
        read, AD-6), so partial events data must never reach it — a RuntimeError
        is raised even though the other endpoints succeeded. A WARNING naming the
        failing endpoint is still logged first.
        """

        with (
            caplog.at_level(
                logging.WARNING, logger="pipeline.connectors.trading212.fetch"
            ),
            mock.patch(
                "pipeline.connectors.trading212.fetch.Trading212Client"
            ) as MockCls,
        ):
            instance = MockCls.return_value
            instance.orders.side_effect = Trading212Error("orders failed")
            instance.dividends.return_value = []
            instance.transactions.return_value = []
            instance.captured_responses = []

            # Only orders fails; dividends/transactions succeed — fetch_events
            # must still raise, naming the failing endpoint.
            with pytest.raises(RuntimeError, match="orders"):
                fetch_events(
                    api_key="test",
                    api_secret="test",
                    base_url="https://demo.trading212.com/api/v0",
                )

        # A warning must be logged naming the failing endpoint before the raise.
        warning_messages = [
            record.message
            for record in caplog.records
            if record.levelno == logging.WARNING
        ]
        assert any("events endpoint orders failed" in m for m in warning_messages), (
            f"Warning did not name the failing endpoint: {warning_messages}"
        )

    def test_fetch_events_fails_loud_even_with_partial_data(self) -> None:
        """One endpoint failing aborts the run even when others returned data.

        Matches the spec's PARTIAL_ENDPOINT matrix row: partial data must not
        reach the transform — the fetch raises RuntimeError and the run aborts
        before transform.
        """

        with mock.patch(
            "pipeline.connectors.trading212.fetch.Trading212Client"
        ) as MockCls:
            instance = MockCls.return_value
            instance.captured_responses = []

            def _capture(path: str, raw: bytes):
                def _fetch():
                    instance.captured_responses.append((path, raw))

                return _fetch

            instance.orders.side_effect = Trading212Error("orders failed")
            instance.dividends.side_effect = _capture(
                "/equity/history/dividends", b'[{"id": 2}]'
            )
            instance.transactions.side_effect = _capture(
                "/equity/history/transactions", b'[{"id": 3}]'
            )

            with pytest.raises(RuntimeError, match="orders"):
                fetch_events(
                    api_key="test",
                    api_secret="test",
                    base_url="https://demo.trading212.com/api/v0",
                )

    def test_fetch_events_raises_when_all_endpoints_empty(self) -> None:
        """When all events endpoints return empty lists, fetch_events raises RuntimeError."""

        with mock.patch(
            "pipeline.connectors.trading212.fetch.Trading212Client"
        ) as MockCls:
            instance = MockCls.return_value
            instance.orders.return_value = []
            instance.dividends.return_value = []
            instance.transactions.return_value = []
            instance.captured_responses = []

            with pytest.raises(
                RuntimeError, match="all endpoints.*failed or returned no data"
            ):
                fetch_events(
                    api_key="test",
                    api_secret="test",
                    base_url="https://demo.trading212.com/api/v0",
                )

    def test_fetch_events_strips_pagination_cursor_from_source(self) -> None:
        """Stored source drops the ?cursor= token so distinct sources are stable.

        The stored ``source`` column is the pagination-stripped endpoint base
        (AC-7: ``SELECT DISTINCT source`` unchanged across runs) — the per-run
        cursor token must not leak into the stored value.
        """

        with mock.patch(
            "pipeline.connectors.trading212.fetch.Trading212Client"
        ) as MockCls:
            instance = MockCls.return_value
            instance.captured_responses = []

            def _capture(path: str, raw: bytes):
                def _fetch():
                    instance.captured_responses.append((path, raw))

                return _fetch

            instance.orders.side_effect = _capture(
                "/equity/history/orders",
                b'{"items": [{"id": 1}], "nextPagePath": null}',
            )
            instance.dividends.side_effect = _capture(
                "/equity/history/dividends?cursor=abc",
                b'{"items": [{"id": 2}], "nextPagePath": null}',
            )
            instance.transactions.side_effect = _capture(
                "/equity/history/transactions?cursor=def",
                b'{"items": [{"id": 3}], "nextPagePath": null}',
            )

            result = fetch_events(
                api_key="test",
                api_secret="test",
                base_url="https://demo.trading212.com/api/v0",
            )

        assert result.column("source").to_pylist() == [
            "/equity/history/orders",
            "/equity/history/dividends",
            "/equity/history/transactions",
        ]


class TestUnwrapEvents:
    """Tests for _unwrap_events helper (moved from transform_utils)."""

    def test_bare_list_returns_as_is(self) -> None:

        events = [{"id": 1}, {"id": 2}]
        assert _unwrap_events(events) is events

    def test_paginated_dict_extracts_items(self) -> None:

        payload = {"items": [{"id": 1}], "nextPagePath": None}
        assert _unwrap_events(payload) == [{"id": 1}]

    def test_paginated_dict_empty_items(self) -> None:

        payload = {"items": [], "nextPagePath": None}
        assert _unwrap_events(payload) == []

    def test_dict_without_items_returns_empty(self) -> None:

        assert _unwrap_events({"error": "not found"}) == []

    def test_non_dict_non_list_returns_empty(self) -> None:

        assert _unwrap_events("string") == []
        assert _unwrap_events(42) == []
        assert _unwrap_events(None) == []


class TestEventsTransform:
    """Tests for the T212 events transform using Polars-native field extraction."""

    @pytest.fixture()
    def fernet_key(self) -> bytes:
        return generate_key()

    def _build_raw_events_table(
        self,
        events: list[dict] | dict,
        source: str,
        fernet_key: bytes,
        fetched_at: datetime | None = None,
    ) -> pa.Table:
        """Build a raw-layer table with encrypted events payloads."""

        now = fetched_at if fetched_at is not None else datetime.now(UTC)
        raw_payloads = [json.dumps(events).encode("utf-8")]
        encrypted_payloads = [encrypt(p, fernet_key) for p in raw_payloads]

        return pa.table(
            {
                "fetched_at": [now],
                "broker": ["Trading 212"],
                "source": [source],
                "payload": encrypted_payloads,
                "payload_hash": [hashlib.sha256(p).hexdigest() for p in raw_payloads],
                "account_id": [None],
            },
            schema=RAW_SCHEMA,
        )

    # -- Realistic nested order fixture matching the T212 API spec --

    @staticmethod
    def _make_order_event(**overrides) -> dict:
        """Build a realistic HistoricalOrder event with nested order/fill."""
        order = {
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
        }
        fill = {
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
        }
        event = {"order": order, "fill": fill}
        # Apply overrides at the event level
        event.update(overrides)
        return event

    def test_transform_events_orders_produces_trade_events(
        self, fernet_key: bytes
    ) -> None:
        """T212 orders are transformed into TRADE events with all fields populated."""

        events = [self._make_order_event()]
        raw = self._build_raw_events_table(events, "/equity/history/orders", fernet_key)
        result = transform_events(raw, fernet_key)

        assert result.num_rows == 1
        assert result.schema == events_normalized_schema

        # Core non-nullable fields
        assert result.column("event_type")[0].as_py() == "TRADE"
        assert result.column("raw_event_type")[0].as_py() == "ORDER"
        assert result.column("broker")[0].as_py() == "Trading 212"
        assert result.column("event_id")[0].as_py() == "12345"
        assert result.column("event_datetime")[0].as_py() == "2024-01-15T10:30:00Z"
        # Phase 1: security_ccy is now the security's trading currency (USD),
        # not the wallet currency.  Same value here since wallet ccy == security ccy.
        assert result.column("security_ccy")[0].as_py() == "USD"

        # Nullable trade fields — now populated via nested struct access
        assert result.column("ticker")[0].as_py() == "AAPL_US_EQ"
        assert result.column("isin")[0].as_py() == "US0378331007"
        assert result.column("description")[0].as_py() == "Apple Inc."
        assert result.column("side")[0].as_py() == "BUY"

        # Encrypted monetary fields — in security currency (USD) after Phase 1.
        # Signed by direction (ADR 0058): BUY = outflow -> negative.
        cash = decrypt_float(result.column("cash_amount")[0].as_py(), fernet_key)
        assert cash == pytest.approx(-1500.0)  # netValue * fx_rate * direction
        qty = decrypt_float(result.column("quantity")[0].as_py(), fernet_key)
        assert qty == pytest.approx(10.0)
        price = decrypt_float(result.column("price")[0].as_py(), fernet_key)
        assert price == pytest.approx(150.0)
        # target_fx_rate, target_value, target_ccy are null for T212 orders;
        # they are computed later by normalize_currency.
        assert result.column("target_fx_rate")[0].as_py() is None
        assert result.column("target_value")[0].as_py() is None
        assert result.column("target_ccy")[0].as_py() is None

    def test_transform_events_deduplicates_across_payloads(
        self, fernet_key: bytes
    ) -> None:
        """Re-fetched T212 events payloads are deduped by (event_type, event_id).

        T212 fetches full history on every run, so repeated pipeline runs
        re-append the same orders to the raw layer.  Mirrors the IBKR dedup
        test at test_ibkr_connector.py::test_transform_events_deduplicates_across_payloads.
        """
        events = [self._make_order_event()]
        raw = self._build_raw_events_table(events, "/equity/history/orders", fernet_key)
        duplicated = pa.concat_tables([raw, raw])

        result = transform_events(duplicated, fernet_key)

        # The same order should appear exactly once, not twice.
        assert result.num_rows == 1
        keys = list(
            zip(
                result.column("event_type").to_pylist(),
                result.column("event_id").to_pylist(),
            )
        )
        assert len(keys) == len(set(keys)), f"Duplicate keys found: {keys}"

    def test_transform_events_dedup_scopes_by_event_type(
        self, fernet_key: bytes
    ) -> None:
        """An order and a dividend sharing event_id "12345" are kept distinct.

        order.id is an integer cast to string; dividend.reference is a separate
        string ID space.  Dedup must be scoped by event_type, not event_id alone,
        or an order and dividend with the same numeric string id would collide.
        """
        order_event = self._make_order_event()  # order.id 12345 -> event_id "12345"
        dividend_event = {
            "reference": "12345",  # same string as the order id
            "ticker": "AAPL_US_EQ",
            "instrument": {
                "ticker": "AAPL_US_EQ",
                "isin": "US0378331007",
                "name": "Apple Inc.",
                "currency": "USD",
            },
            "amount": 10.0,
            "currency": "USD",
            "grossAmountPerShare": 0.10,
            "paidOn": "2024-02-15",
            "quantity": 100,
            "tickerCurrency": "USD",
            "type": "ORDINARY",
        }
        raw_orders = self._build_raw_events_table(
            [order_event], "/equity/history/orders", fernet_key
        )
        raw_dividends = self._build_raw_events_table(
            [dividend_event], "/equity/history/dividends", fernet_key
        )
        raw = pa.concat_tables([raw_orders, raw_dividends])

        result = transform_events(raw, fernet_key)

        # Both events survive — event_type scopes the uniqueness.
        assert result.num_rows == 2
        keys = list(
            zip(
                result.column("event_type").to_pylist(),
                result.column("event_id").to_pylist(),
            )
        )
        assert len(keys) == len(set(keys)), f"Duplicate keys found: {keys}"
        assert ("TRADE", "12345") in keys
        assert ("DIVIDEND", "12345") in keys

    def test_transform_events_dedup_keeps_latest_fetched_version(
        self, fernet_key: bytes
    ) -> None:
        """When an event is re-fetched with a corrected value, the latest wins.

        unique()'s default keep="any" is non-deterministic and does not
        guarantee the row kept after a descending fetched_at sort is the
        newest.  keep="first" honors that sort so a field correction in a
        later fetch survives.  This is the load-bearing guard for ADR 0105's
        "latest fetched_at wins" decision.
        """
        earlier = datetime(2024, 1, 1, 10, 0, tzinfo=UTC)
        later = datetime(2024, 1, 2, 10, 0, tzinfo=UTC)

        stale_event = self._make_order_event()
        stale_event["fill"]["walletImpact"]["netValue"] = 1500.0
        corrected_event = self._make_order_event()
        corrected_event["fill"]["walletImpact"]["netValue"] = 2000.0
        raw_stale = self._build_raw_events_table(
            [stale_event], "/equity/history/orders", fernet_key, fetched_at=earlier
        )
        raw_corrected = self._build_raw_events_table(
            [corrected_event],
            "/equity/history/orders",
            fernet_key,
            fetched_at=later,
        )
        raw = pa.concat_tables([raw_stale, raw_corrected])

        result = transform_events(raw, fernet_key)

        assert result.num_rows == 1
        assert result.column("event_id")[0].as_py() == "12345"
        cash = decrypt_float(result.column("cash_amount")[0].as_py(), fernet_key)
        # BUY = outflow -> negative; latest correction (netValue 2000.0) must win.
        assert cash == pytest.approx(-2000.0)

    def test_transform_events_order_with_taxes(self, fernet_key: bytes) -> None:
        """T212 orders with walletImpact.taxes correctly split fees and taxes."""

        event = self._make_order_event()
        event["fill"]["walletImpact"]["taxes"] = [
            {"name": "CURRENCY_CONVERSION_FEE", "quantity": 3.0, "currency": "EUR"},
            {"name": "FRENCH_TRANSACTION_TAX", "quantity": 1.5, "currency": "EUR"},
        ]
        raw = self._build_raw_events_table(
            [event], "/equity/history/orders", fernet_key
        )
        result = transform_events(raw, fernet_key)

        assert result.num_rows == 1
        fee = decrypt_float(result.column("fee_amount")[0].as_py(), fernet_key)
        assert fee == pytest.approx(3.0)  # CURRENCY_CONVERSION_FEE
        tax = decrypt_float(result.column("tax_amount")[0].as_py(), fernet_key)
        assert tax == pytest.approx(1.5)  # FRENCH_TRANSACTION_TAX

    def test_transform_events_order_sell_side(self, fernet_key: bytes) -> None:
        """T212 SELL orders correctly map the side field and stay positive.

        Per the sign convention (ADR 0058), SELL = inflow -> positive cash.
        The direction sign applies only to BUY; SELL keeps the magnitude.
        """

        event = self._make_order_event()
        event["order"]["side"] = "SELL"
        raw = self._build_raw_events_table(
            [event], "/equity/history/orders", fernet_key
        )
        result = transform_events(raw, fernet_key)

        assert result.column("side")[0].as_py() == "SELL"
        cash = decrypt_float(result.column("cash_amount")[0].as_py(), fernet_key)
        assert cash == pytest.approx(1500.0)  # SELL inflow stays positive

    def test_transform_events_order_cross_currency_fx_rate(
        self, fernet_key: bytes
    ) -> None:
        """T212 orders with wallet→security FX rate produce cash_amount in security ccy.

        Bug 1 fix: walletImpact.fxRate is the wallet→security rate.
        For a PLN wallet buying a USD security:
          - net_value (wallet ccy) = 2000 PLN
          - fx_rate = 0.25 (PLN→USD)
          - cash_amount (security ccy) = 2000 * 0.25 = 500 USD
          - security_ccy = "USD" (security, not "PLN")
        """

        event = self._make_order_event()
        event["order"]["ticker"] = "SPYI_US_EQ"
        event["order"]["currency"] = "USD"
        event["order"]["instrument"] = {
            "ticker": "SPYI_US_EQ",
            "isin": "US46434G7510",
            "name": "SPDR SSGA Global Infrastructure ETF",
            "currency": "USD",
        }
        event["fill"]["walletImpact"] = {
            "currency": "PLN",
            "fxRate": 0.25,
            "netValue": 2000.0,
            "realisedProfitLoss": 0,
            "taxes": [],
        }
        event["fill"]["price"] = 50.0  # USD per share
        event["fill"]["quantity"] = 10

        raw = self._build_raw_events_table(
            [event], "/equity/history/orders", fernet_key
        )
        result = transform_events(raw, fernet_key)

        assert result.num_rows == 1

        # security_ccy should be USD (security ccy), not PLN (wallet ccy)
        assert result.column("security_ccy")[0].as_py() == "USD"

        # cash_amount should be in security ccy: 2000 PLN * 0.25 PLN->USD = 500
        # USD, negated because this is a BUY (outflow) per ADR 0058.
        cash = decrypt_float(result.column("cash_amount")[0].as_py(), fernet_key)
        assert cash == pytest.approx(-(2000.0 * 0.25))

        # target_fx_rate, target_value, target_ccy are null for T212 orders;
        # they are computed later by normalize_currency.
        assert result.column("target_fx_rate")[0].as_py() is None
        assert result.column("target_value")[0].as_py() is None

    def test_transform_events_order_gbx_security_currency(
        self, fernet_key: bytes
    ) -> None:
        """T212 order for a GBX-denominated security correctly converts wallet amount.

        GBX is British pence (1/100 GBP).  The fx_rate from PLN→GBX is a large
        number because GBX is a sub-unit.
          - net_value (wallet ccy) = 7500 PLN
          - fx_rate = 19.949 (PLN→GBX)
          - cash_amount (security ccy) = 7500 * 19.949 ≈ 149617.5 GBX
        """

        event = self._make_order_event()
        event["order"]["ticker"] = "SGLN_UK_EQ"
        event["order"]["currency"] = "GBX"
        event["order"]["instrument"] = {
            "ticker": "SGLN_UK_EQ",
            "isin": "GB00B579F147",
            "name": "iShares Core UK Gilts",
            "currency": "GBX",
        }
        event["fill"]["walletImpact"] = {
            "currency": "PLN",
            "fxRate": 19.949,
            "netValue": 7500.0,
            "realisedProfitLoss": 0,
            "taxes": [],
        }
        event["fill"]["price"] = 14961.75  # GBX per share
        event["fill"]["quantity"] = 10

        raw = self._build_raw_events_table(
            [event], "/equity/history/orders", fernet_key
        )
        result = transform_events(raw, fernet_key)

        assert result.num_rows == 1
        assert result.column("security_ccy")[0].as_py() == "GBX"

        cash = decrypt_float(result.column("cash_amount")[0].as_py(), fernet_key)
        # Negated for BUY (outflow) per ADR 0058.
        assert cash == pytest.approx(-(7500.0 * 19.949), rel=1e-6)

        # target_fx_rate, target_value, target_ccy are null for T212 orders
        assert result.column("target_value")[0].as_py() is None

    def test_transform_events_order_fee_tax_converted_to_security_ccy(
        self, fernet_key: bytes
    ) -> None:
        """T212 cross-currency orders convert fee/tax from wallet ccy to security ccy.

        Bug 5 fix: fee_amount and tax_amount are multiplied by
        walletImpact.fxRate (wallet→security) to convert from wallet currency to
        security currency.
          - wallet ccy = PLN, security ccy = USD, fx_rate = 0.25 (PLN→USD)
          - fee_amount (wallet) = 4.0 PLN → 1.0 USD
          - tax_amount (wallet) = 2.0 PLN → 0.5 USD
        """

        event = self._make_order_event()
        event["order"]["ticker"] = "SPYI_US_EQ"
        event["order"]["currency"] = "USD"
        event["order"]["filledValue"] = 2000.0
        event["order"]["instrument"] = {
            "ticker": "SPYI_US_EQ",
            "isin": "US46434G7510",
            "name": "SPDR SSGA Global Infrastructure ETF",
            "currency": "USD",
        }
        event["fill"]["walletImpact"] = {
            "currency": "PLN",
            "fxRate": 0.25,
            "netValue": 2000.0,
            "realisedProfitLoss": 0,
            "taxes": [
                {"name": "TRANSACTION_FEE", "quantity": 4.0, "currency": "PLN"},
                {"name": "FRENCH_TRANSACTION_TAX", "quantity": 2.0, "currency": "PLN"},
            ],
        }
        event["fill"]["price"] = 50.0  # USD per share
        event["fill"]["quantity"] = 10

        raw = self._build_raw_events_table(
            [event], "/equity/history/orders", fernet_key
        )
        result = transform_events(raw, fernet_key)

        assert result.num_rows == 1

        # cash_amount: 2000 PLN * 0.25 = 500 USD, negated for BUY (outflow).
        cash = decrypt_float(result.column("cash_amount")[0].as_py(), fernet_key)
        assert cash == pytest.approx(-500.0)

        # fee_amount: 4.0 PLN * 0.25 = 1.0 USD (positive magnitude, unchanged)
        fee = decrypt_float(result.column("fee_amount")[0].as_py(), fernet_key)
        assert fee == pytest.approx(1.0)

        # tax_amount: 2.0 PLN * 0.25 = 0.5 USD (positive magnitude, unchanged)
        tax = decrypt_float(result.column("tax_amount")[0].as_py(), fernet_key)
        assert tax == pytest.approx(0.5)

    def test_transform_events_dividend_currency_mismatch_warning(
        self, fernet_key: bytes, caplog: pytest.LogCaptureFixture
    ) -> None:
        """T212 dividends with currency != tickerCurrency produce a warning log."""

        events = [
            {
                "reference": "DIV-XCCY",
                "ticker": "VWCE",
                "instrument": {
                    "ticker": "VWCE",
                    "isin": "IE00BK5BQT80",
                    "name": "Vanguard FTSE All-World",
                    "currency": "USD",
                },
                "amount": 42.50,
                "currency": "EUR",  # differs from tickerCurrency
                "grossAmountPerShare": 0.425,
                "paidOn": "2024-03-01",
                "quantity": 100,
                "tickerCurrency": "USD",  # instrument currency
                "type": "ORDINARY",
            }
        ]
        raw = self._build_raw_events_table(
            events, "/equity/history/dividends", fernet_key
        )

        with caplog.at_level(logging.WARNING):
            result = transform_events(raw, fernet_key)

        assert result.num_rows == 1
        assert result.column("security_ccy")[0].as_py() == "EUR"
        assert result.column("instrument_ccy")[0].as_py() == "USD"

        # Verify the currency mismatch warning was logged
        assert any(
            "differs from tickerCurrency" in record.message for record in caplog.records
        ), (
            f"Expected currency mismatch warning, got: {[r.message for r in caplog.records]}"
        )

    def test_transform_events_dividend_same_currency_no_warning(
        self, fernet_key: bytes, caplog: pytest.LogCaptureFixture
    ) -> None:
        """T212 dividends with currency == tickerCurrency produce no mismatch warning."""

        events = [
            {
                "reference": "DIV-SAME",
                "ticker": "AAPL",
                "instrument": {
                    "ticker": "AAPL",
                    "isin": "US0378331007",
                    "name": "Apple Inc.",
                    "currency": "USD",
                },
                "amount": 50.0,
                "currency": "USD",  # same as tickerCurrency
                "grossAmountPerShare": 0.50,
                "paidOn": "2024-03-15",
                "quantity": 100,
                "tickerCurrency": "USD",
                "type": "ORDINARY",
            }
        ]
        raw = self._build_raw_events_table(
            events, "/equity/history/dividends", fernet_key
        )

        with caplog.at_level(logging.WARNING):
            result = transform_events(raw, fernet_key)

        assert result.num_rows == 1
        # Same currency: instrument_ccy equals security_ccy
        assert result.column("security_ccy")[0].as_py() == "USD"
        assert result.column("instrument_ccy")[0].as_py() == "USD"
        # No currency mismatch warning should be logged
        assert not any(
            "differs from tickerCurrency" in record.message for record in caplog.records
        ), f"Unexpected warning logged: {[r.message for r in caplog.records]}"

    def test_transform_events_dividends_produces_dividend_events(
        self, fernet_key: bytes
    ) -> None:
        """T212 dividends are transformed into DIVIDEND events with nested instrument."""

        events = [
            {
                "reference": "DIV-001",
                "ticker": "VWCE",
                "instrument": {
                    "ticker": "VWCE",
                    "isin": "IE00BK5BQT80",
                    "name": "Vanguard FTSE All-World",
                    "currency": "USD",
                },
                "amount": 42.50,
                "currency": "EUR",
                "grossAmountPerShare": 0.425,
                "paidOn": "2024-03-01",
                "quantity": 100,
                "tickerCurrency": "USD",
                "type": "ORDINARY",
            }
        ]
        raw = self._build_raw_events_table(
            events, "/equity/history/dividends", fernet_key
        )
        result = transform_events(raw, fernet_key)

        assert result.num_rows == 1
        assert result.column("event_type")[0].as_py() == "DIVIDEND"
        assert result.column("raw_event_type")[0].as_py() == "ORDINARY"
        assert result.column("isin")[0].as_py() == "IE00BK5BQT80"
        assert result.column("ticker")[0].as_py() == "VWCE"
        assert result.column("description")[0].as_py() == "Vanguard FTSE All-World"

        cash = decrypt_float(result.column("cash_amount")[0].as_py(), fernet_key)
        assert cash == pytest.approx(42.50)
        qty = decrypt_float(result.column("quantity")[0].as_py(), fernet_key)
        assert qty == pytest.approx(100.0)
        price = decrypt_float(result.column("price")[0].as_py(), fernet_key)
        assert price == pytest.approx(0.425)

    def test_transform_events_transactions_classifies_event_types(
        self, fernet_key: bytes
    ) -> None:
        """T212 transactions are classified into normalized event types."""

        events = [
            {
                "reference": "TX-001",
                "type": "DEPOSIT",
                "currency": "EUR",
                "amount": 1000.0,
                "dateTime": "2024-01-01T09:00:00Z",
            }
        ]
        raw = self._build_raw_events_table(
            events, "/equity/history/transactions", fernet_key
        )
        result = transform_events(raw, fernet_key)

        assert result.num_rows == 1
        assert result.column("event_type")[0].as_py() == "DEPOSIT"
        assert result.column("raw_event_type")[0].as_py() == "DEPOSIT"
        cash = decrypt_float(result.column("cash_amount")[0].as_py(), fernet_key)
        assert cash == pytest.approx(1000.0)

    def test_transform_events_transaction_withdraw_type(
        self, fernet_key: bytes
    ) -> None:
        """T212 WITHDRAW transactions are mapped to WITHDRAWAL event type."""

        events = [
            {
                "reference": "TX-002",
                "type": "WITHDRAW",
                "currency": "PLN",
                "amount": -500.0,
                "dateTime": "2024-02-01T12:00:00Z",
            }
        ]
        raw = self._build_raw_events_table(
            events, "/equity/history/transactions", fernet_key
        )
        result = transform_events(raw, fernet_key)

        assert result.num_rows == 1
        assert result.column("event_type")[0].as_py() == "WITHDRAWAL"
        assert result.column("raw_event_type")[0].as_py() == "WITHDRAW"

    def test_transform_events_empty_events_produces_empty_table(
        self, fernet_key: bytes
    ) -> None:
        """When no events are parsed, transform returns an empty schema-correct table."""

        events: list[dict] = []
        raw = self._build_raw_events_table(events, "/equity/history/orders", fernet_key)
        result = transform_events(raw, fernet_key)

        assert result.num_rows == 0
        assert result.schema == events_normalized_schema

    def test_transform_events_unwraps_paginated_dict(self, fernet_key: bytes) -> None:
        """Paginated T212 responses (dict with 'items') are unwrapped correctly."""

        events = [self._make_order_event()]
        paginated_payload = {"items": events, "nextPagePath": None}
        raw = self._build_raw_events_table(
            paginated_payload, "/equity/history/orders", fernet_key
        )
        result = transform_events(raw, fernet_key)

        assert result.num_rows == 1
        assert result.column("event_type")[0].as_py() == "TRADE"
        assert result.column("ticker")[0].as_py() == "AAPL_US_EQ"

    def test_transform_events_paginated_dict_with_empty_items(
        self, fernet_key: bytes
    ) -> None:
        """Paginated response with empty items list produces zero rows."""

        paginated_payload = {"items": [], "nextPagePath": None}
        raw = self._build_raw_events_table(
            paginated_payload, "/equity/history/dividends", fernet_key
        )
        result = transform_events(raw, fernet_key)

        assert result.num_rows == 0
        assert result.schema == events_normalized_schema

    def test_transform_events_order_missing_optional_fields(
        self, fernet_key: bytes
    ) -> None:
        """Orders missing optional struct fields (e.g. filledQuantity) don't crash.

        The real T212 API may omit fields like filledQuantity/filledValue
        from order objects.  Polars struct.field() raises
        StructFieldNotFoundError on absent fields, so the transform must
        pre-fill missing keys with None.
        """

        # Build an order event without filledQuantity or filledValue on
        # the order object — this is exactly what the real API returns
        # when those fields are not populated.
        event = self._make_order_event()
        del event["order"]["filledQuantity"]
        del event["order"]["filledValue"]

        raw = self._build_raw_events_table(
            [event], "/equity/history/orders", fernet_key
        )
        result = transform_events(raw, fernet_key)

        assert result.num_rows == 1
        # quantity falls back to fill.quantity (10.0)
        qty = decrypt_float(result.column("quantity")[0].as_py(), fernet_key)
        assert qty == pytest.approx(10.0)


class TestT212FixtureRoundTrip:
    """Round-trip: transform_snapshot(t212_raw_snapshot(...)) reproduces the
    normalized fixture.

    This is the reference template for F4's golden test (per PLAN.md
    "Golden-test safety net"). The expected normalized table is sourced from
    the fixture (built from real demo bronze shapes — not from running the
    SUT), so a transform bug does not get baked into the expected values.
    """

    # Columns compared as decrypted plaintext floats (Fernet tokens are
    # non-deterministic; never compare encrypted bytes).
    _FLOAT_COLS: ClassVar[list[str]] = ["security_value"]
    # Columns compared as exact strings.
    _STR_COLS: ClassVar[list[str]] = [
        "account_id",
        "position_type",
        "label",
        "description",
        "asset_class",
        "security_ccy",
        "isin",
    ]

    def test_transform_snapshot_reproduces_normalized_fixture(self) -> None:
        fernet_key = generate_key()
        fetched_at = datetime.now(UTC)

        raw = t212_raw_snapshot(fernet_key=fernet_key, fetched_at=fetched_at)
        result = transform_snapshot(raw, fernet_key)
        expected = t212_normalized_snapshot(
            fernet_key=fernet_key, fetched_at=fetched_at
        )

        # Schema must match exactly.
        assert result.schema.equals(expected.schema), (
            f"Schema mismatch:\nresult={result.schema}\nexpected={expected.schema}"
        )
        assert result.num_rows == expected.num_rows
        assert result.num_rows == 3

        # Sort both by label for stable row-by-row comparison (transform row
        # order follows positions iteration, which is stable here, but sorting
        # makes the comparison robust to future source-iteration changes).
        result_sorted = result.sort_by("label")
        expected_sorted = expected.sort_by("label")

        for col in self._STR_COLS:
            actual_vals = result_sorted.column(col).to_pylist()
            expected_vals = expected_sorted.column(col).to_pylist()
            assert actual_vals == expected_vals, (
                f"Column {col} mismatch: {actual_vals} != {expected_vals}"
            )

        for col in self._FLOAT_COLS:
            actual_enc = result_sorted.column(col).to_pylist()
            expected_enc = expected_sorted.column(col).to_pylist()
            actual_vals = [decrypt_float(v, fernet_key) for v in actual_enc]
            expected_vals = [decrypt_float(v, fernet_key) for v in expected_enc]
            for a, e in zip(actual_vals, expected_vals):
                assert a == pytest.approx(e), f"Column {col} value mismatch: {a} != {e}"

        # fetched_at: transform propagates the raw table's fetched_at; the
        # fixture uses the same fixed timestamp, so they must match.
        assert result_sorted.column("fetched_at").to_pylist() == (
            expected_sorted.column("fetched_at").to_pylist()
        )

    def test_round_trip_known_values(self) -> None:
        """Spot-check the known decrypted values the round-trip must produce.

        Pins the exact amounts (2500.0 / 1800.0 / 1500.0) so a
        ``security_value = 1.0`` or ``decrypt_float → 1.0`` mutation fails.
        """
        fernet_key = generate_key()
        fetched_at = datetime.now(UTC)

        raw = t212_raw_snapshot(fernet_key=fernet_key, fetched_at=fetched_at)
        result = transform_snapshot(raw, fernet_key)

        labels = result.column("label").to_pylist()
        values = [
            decrypt_float(v, fernet_key)
            for v in result.column("security_value").to_pylist()
        ]
        by_label = dict(zip(labels, values))

        assert by_label["VWCEl_EQ"] == pytest.approx(2500.0)
        assert by_label["AAPLu_EQ"] == pytest.approx(1800.0)
        assert by_label["CASH PLN"] == pytest.approx(1500.0)


class TestTrading212ExtractHoldingsValues:
    """H3: extract_holdings must surface decrypted security_value as
    ``holdings[i].value``. A ``value=0.0`` mutation in the connector must
    fail these assertions.
    """

    @staticmethod
    def _normalized_df(fernet_key: bytes) -> pl.DataFrame:
        """Build a decrypted normalized DataFrame for extract_holdings."""
        table = t212_normalized_snapshot(fernet_key=fernet_key)
        df = pl.from_arrow(table)
        # extract_holdings reads security_value_decrypted (added by
        # pipeline.normalized.extract._decrypt_df in production).
        return df.with_columns(
            pl.col("security_value")
            .map_elements(
                lambda v: decrypt_float(v, fernet_key),
                return_dtype=pl.Float64,
            )
            .alias("security_value_decrypted")
        )

    def test_extract_holdings_values_match_known_amounts(self) -> None:
        fernet_key = generate_key()
        df = self._normalized_df(fernet_key)
        connector = get("trading212")
        holdings = connector.extract_holdings(df, fernet_key)

        assert len(holdings) == 3
        by_ticker = {h.ticker: h for h in holdings}

        # Decrypted value must equal the known fixture amount (not just > 0).
        assert by_ticker["VWCEl_EQ"].value == pytest.approx(2500.0)
        assert by_ticker["AAPLu_EQ"].value == pytest.approx(1800.0)
        assert by_ticker["CASH PLN"].value == pytest.approx(1500.0)

        # broker/security_currency/ccy reflect the fixture: equities use
        # instrument currency (EUR/USD), CASH uses wallet currency (PLN).
        assert all(h.broker == "Trading 212" for h in holdings)
        assert by_ticker["VWCEl_EQ"].currency == "EUR"
        assert by_ticker["AAPLu_EQ"].currency == "USD"
        assert by_ticker["CASH PLN"].currency == "PLN"
        assert by_ticker["VWCEl_EQ"].security_currency == "EUR"
        assert by_ticker["AAPLu_EQ"].security_currency == "USD"
        assert by_ticker["CASH PLN"].security_currency == "PLN"

    def test_extract_holdings_value_not_zeroed(self) -> None:
        """A value=0.0 mutation in extract_holdings must fail here."""
        fernet_key = generate_key()
        df = self._normalized_df(fernet_key)
        connector = get("trading212")
        holdings = connector.extract_holdings(df, fernet_key)

        # Every holding must carry a non-zero decrypted value — pinning the
        # known amounts above is the primary guard, but this explicit check
        # catches a blanket zeroing mutation.
        assert all(
            h.value == pytest.approx(v)
            for h, v in zip(holdings, (2500.0, 1800.0, 1500.0))
        )
