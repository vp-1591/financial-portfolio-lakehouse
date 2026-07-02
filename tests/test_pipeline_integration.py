"""Integration tests for pipeline run.py, ingest.py, and path creation.

Covers bugs found during end-to-end runs:
- XTB set_column() PyArrow 3-arg API
- Missing parent directory creation for Delta table paths
- Transform functions must decrypt payloads before JSON parsing
- T212 auth uses Basic Auth, not Bearer token
- allocate_percentages handles missing Delta table gracefully
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from pipeline.crypto import encrypt, generate_key
from pipeline.raw.ingest import encrypt_raw_payloads
from pipeline.raw.models import RAW_SCHEMA


class TestEncryptRawPayloadsSetColumn:
    """Test that encrypt_raw_payloads uses the correct PyArrow set_column API.

    PyArrow >= 24 changed set_column() to accept 3 positional arguments
    (i, field_, column) instead of 4. This test ensures the call works.
    """

    def test_encrypt_payloads_roundtrip(self) -> None:
        key = generate_key()
        payloads = [b"payload1", b"payload2", b"payload3"]
        table = pa.table(
            {
                "fetched_at": [None] * 3,
                "broker": ["TEST"] * 3,
                "source": ["test_source"] * 3,
                "payload": payloads,
                "payload_hash": ["hash1", "hash2", "hash3"],
                "account_id": ["ACCT1"] * 3,
                "source_file": [""] * 3,
            },
            schema=RAW_SCHEMA,
        )

        result = encrypt_raw_payloads(table, key)

        # Encrypted payloads should differ from originals
        original_payloads = table.column("payload").to_pylist()
        encrypted_payloads = result.column("payload").to_pylist()
        assert encrypted_payloads != original_payloads

        # Should have same number of rows
        assert result.num_rows == 3

    def test_encrypt_empty_table(self) -> None:
        key = generate_key()
        table = pa.table(
            {
                "fetched_at": [],
                "broker": [],
                "source": [],
                "payload": [],
                "payload_hash": [],
                "account_id": [],
                "source_file": [],
            },
            schema=RAW_SCHEMA,
        )

        result = encrypt_raw_payloads(table, key)
        assert result.num_rows == 0


class TestT212CdcKwargsSeparation:
    """Test that CDC fetch calls work with the same kwargs as snapshot."""

    @patch("pipeline.connectors.trading212.fetch.fetch_cdc")
    @patch("pipeline.connectors.trading212.fetch.fetch_snapshot")
    def test_cdc_and_snapshot_use_same_kwargs(
        self, mock_snapshot: MagicMock, mock_cdc: MagicMock
    ) -> None:
        from pipeline.connectors.registry import get

        connector = get("trading212")

        common_kwargs = {
            "api_key": "test_key",
            "api_secret": "test_secret",
            "base_url": "https://live.trading212.com/api/v0",
        }

        mock_snapshot.return_value = pa.table(
            {
                "fetched_at": [None],
                "broker": ["Trading 212"],
                "source": ["test"],
                "payload": [b"{}"],
                "payload_hash": ["hash"],
                "account_id": [""],
                "source_file": [""],
            },
            schema=RAW_SCHEMA,
        )

        mock_cdc.return_value = pa.table(
            {
                "fetched_at": [None],
                "broker": ["Trading 212"],
                "source": ["test_cdc"],
                "payload": [b"[]"],
                "payload_hash": ["hash_cdc"],
                "account_id": [""],
                "source_file": [""],
            },
            schema=RAW_SCHEMA,
        )

        # Both snapshot and CDC should work with the same kwargs
        connector.fetch_snapshot(**common_kwargs)
        connector.fetch_cdc(**common_kwargs)

        # Verify both were called
        mock_snapshot.assert_called_once()
        mock_cdc.assert_called_once()


class TestT212BasicAuth:
    """Test that Trading 212 client uses HTTP Basic authentication.

    The T212 API v0 requires Authorization: Basic <base64(API_KEY:API_SECRET)>.
    This was changed back from Bearer token after discovering that the Bearer
    change (commit f7c3674) was based on a misdiagnosed 401 — the real cause
    was an IP-restricted API key. The local API spec at
    docs/_vendor/trading212/api/section/general-information/api.json defines
    authWithSecretKey as { scheme: basic }.
    """

    def test_basic_auth_header_format(self) -> None:
        import base64

        from pipeline.connectors.trading212.client import basic_auth_header

        result = basic_auth_header("my-api-key", "my-api-secret")
        expected = base64.b64encode(b"my-api-key:my-api-secret").decode("ascii")
        assert result == f"Basic {expected}"

    def test_basic_auth_header_strips_whitespace(self) -> None:
        import base64

        from pipeline.connectors.trading212.client import basic_auth_header

        result = basic_auth_header("  test-key-123  ", "  test-secret-456  ")
        expected = base64.b64encode(b"test-key-123:test-secret-456").decode("ascii")
        assert result == f"Basic {expected}"

    def test_auth_method_is_basic_with_key_and_secret(self) -> None:
        """Regression test: prevents silent downgrade to Bearer or raw-key auth.

        Commit f7c3674 changed Basic → Bearer based on a misdiagnosed 401.
        The real cause was an IP-restricted API key. This test ensures
        the auth method stays as HTTP Basic with key:secret encoding.
        """
        import base64

        from pipeline.connectors.trading212.client import basic_auth_header

        header = basic_auth_header("mykey", "mysecret")
        # Must start with "Basic " — never "Bearer " or a raw key
        assert header.startswith("Basic "), f"Expected Basic auth, got: {header}"
        decoded = base64.b64decode(header[len("Basic ") :]).decode("utf-8")
        assert decoded == "mykey:mysecret", f"Expected key:secret, got: {decoded}"

    @patch("urllib.request.urlopen")
    def test_client_sends_basic_auth(self, mock_urlopen: MagicMock) -> None:
        import base64

        from pipeline.connectors.trading212.client import Trading212Client

        # Mock the response
        mock_response = MagicMock()
        mock_response.read.return_value = b'{"currencyCode": "EUR"}'
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        client = Trading212Client(
            base_url="https://live.trading212.com/api/v0",
            api_key="test-key",
            api_secret="test-secret",
        )
        client.account_summary()
        client.account_summary()

        # Verify the request was made with Basic auth
        call_args = mock_urlopen.call_args
        request = call_args[0][0]
        expected = base64.b64encode(b"test-key:test-secret").decode("ascii")
        assert request.get_header("Authorization") == f"Basic {expected}"


class TestTransformDecryptsPayloads:
    """Test that transform functions decrypt payloads before JSON parsing.

    Raw Delta tables store encrypted payloads. Transforms must call
    decrypt() before json.loads() or all rows are silently skipped.
    """

    def test_xtb_transform_decrypts_encrypted_payload(self) -> None:
        """XTB transform_snapshot must decrypt payloads from raw Delta tables."""
        from pipeline.connectors.xtb.transform import transform_snapshot

        key = generate_key()
        import json
        from datetime import datetime, timezone

        positions_data = {
            "positions": [
                {
                    "label": "TEST",
                    "name": "Test Asset",
                    "asset_class": "EQUITY",
                    "currency": "USD",
                    "value": 100.0,
                    "isin": "",
                },
            ],
            "net_worth": 100.0,
        }
        encrypted_payload = encrypt(json.dumps(positions_data).encode("utf-8"), key)

        raw = pa.table(
            {
                "fetched_at": [datetime.now(timezone.utc)],
                "broker": ["XTB"],
                "source": ["OPEN POSITION"],
                "payload": [encrypted_payload],
                "payload_hash": ["abc"],
                "account_id": ["XTB"],
                "source_file": ["test.xlsx"],
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

        result = transform_snapshot(raw, key)
        assert result.num_rows >= 1, (
            "XTB transform should produce rows from encrypted payload"
        )

    def test_t212_transform_decrypts_encrypted_payload(self) -> None:
        """T212 transform_snapshot must decrypt payloads from raw Delta tables."""
        from pipeline.connectors.trading212.transform import transform_snapshot

        key = generate_key()
        import json
        from datetime import datetime, timezone

        summary = {"currencyCode": "EUR", "total": 100.0}
        positions = [{"ticker": "VUAA", "quantity": 1, "currentPrice": 100.0}]

        encrypted_summary = encrypt(json.dumps(summary).encode("utf-8"), key)
        encrypted_positions = encrypt(json.dumps(positions).encode("utf-8"), key)

        raw = pa.table(
            {
                "fetched_at": [datetime.now(timezone.utc)] * 2,
                "broker": ["Trading 212"] * 2,
                "source": ["/equity/account/summary", "/equity/positions"],
                "payload": [encrypted_summary, encrypted_positions],
                "payload_hash": ["hash1", "hash2"],
                "account_id": ["T212"] * 2,
                "source_file": ["", ""],
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

        result = transform_snapshot(raw, key)
        assert result.num_rows >= 1, (
            "T212 transform should produce rows from encrypted payload"
        )


class TestDirectoryCreation:
    """Test that Delta table writes create parent directories if missing.

    On first run, data/ subdirectories don't exist yet.
    ingest_raw, consolidate_holdings, and allocate_percentages must
    create them before writing.
    """

    def test_ingest_raw_creates_parent_dirs(self, tmp_path: Path) -> None:

        from pipeline.raw.ingest import ingest_raw

        key = generate_key()
        table_path = str(tmp_path / "raw" / "test_broker" / "snapshot")

        # Build a minimal raw table
        raw = pa.table(
            {
                "fetched_at": [None],
                "broker": ["TEST"],
                "source": ["test"],
                "payload": [b"test_data"],
                "payload_hash": ["abc123"],
                "account_id": ["ACCT1"],
                "source_file": [""],
            },
            schema=RAW_SCHEMA,
        )

        count = ingest_raw(raw, table_path, key)
        assert count == 1
        assert Path(table_path).exists()

    def test_consolidate_creates_parent_dirs(self, tmp_path: Path) -> None:
        from pipeline.normalized.consolidate import (
            CurrencyConverter,
            Holding,
            consolidate_holdings,
        )

        key = generate_key()
        table_path = str(tmp_path / "normalized" / "consolidated_holdings")

        converter = CurrencyConverter("EUR", manual_rates={"USD": 0.9})
        holdings = [
            Holding("TestBroker", "AAPL", "USD", 100.0),
        ]

        result = consolidate_holdings(holdings, key, converter, table_path=table_path)
        assert result.num_rows == 1
        assert Path(table_path).exists()

    def test_allocate_creates_parent_dirs(self, tmp_path: Path) -> None:

        from pipeline.analytics.allocation import allocate_percentages

        key = generate_key()

        # First create the consolidated holdings table
        holdings_path = str(tmp_path / "normalized" / "consolidated_holdings")
        from pipeline.normalized.consolidate import (
            CurrencyConverter,
            Holding,
            consolidate_holdings,
        )

        converter = CurrencyConverter("EUR", manual_rates={"USD": 0.9})
        holdings = [Holding("TestBroker", "AAPL", "USD", 100.0)]
        consolidate_holdings(holdings, key, converter, table_path=holdings_path)

        # Now allocate should create analytics dir
        analytics_path = str(tmp_path / "analytics" / "portfolio_allocation")
        result = allocate_percentages(
            table_path=holdings_path,
            fernet_key=key,
            analytics_path=analytics_path,
        )
        assert result.num_rows == 1
        assert Path(analytics_path).exists()


class TestAllocateMissingTable:
    """Test that allocate_percentages raises FileNotFoundError when
    the consolidated_holdings Delta table doesn't exist.

    Previously this raised a low-level Delta error with no context.
    """

    def test_allocate_raises_filenotfound_for_missing_table(
        self, tmp_path: Path
    ) -> None:
        from pipeline.analytics.allocation import allocate_percentages

        key = generate_key()
        missing_path = str(tmp_path / "nonexistent" / "consolidated_holdings")

        with pytest.raises(
            FileNotFoundError, match="Consolidated holdings table not found"
        ):
            allocate_percentages(
                table_path=missing_path,
                fernet_key=key,
                analytics_path=str(tmp_path / "analytics" / "allocation"),
            )
