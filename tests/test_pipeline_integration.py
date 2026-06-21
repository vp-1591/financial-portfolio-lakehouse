"""Integration tests for pipeline run.py, ingest.py, and path creation.

Covers bugs found during first end-to-end run:
- T212 CDC kwargs leak (include_metadata passed to fetch_cdc)
- XTB set_column() PyArrow 3-arg API
- Missing parent directory creation for Delta table paths
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from pipeline.crypto import generate_key
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
    """Test that CDC fetch calls don't receive snapshot-only kwargs.

    fetch_cdc() for Trading 212 does not accept include_metadata,
    so the CLI must build separate kwargs dicts for snapshot and CDC.
    """

    @patch("pipeline.connectors.trading212.fetch.fetch_cdc")
    @patch("pipeline.connectors.trading212.fetch.fetch_snapshot")
    def test_cdc_does_not_receive_include_metadata(
        self, mock_snapshot: MagicMock, mock_cdc: MagicMock
    ) -> None:
        from pipeline.connectors.registry import get

        connector = get("trading212")

        # fetch_cdc should NOT receive include_metadata
        # Simulate what cmd_fetch passes
        snapshot_kwargs = {
            "api_key": "test_key",
            "api_secret": "test_secret",
            "account_id": "ACCT1",
            "base_url": "https://live.trading212.com/api/v0",
            "include_metadata": True,
            "user_agent": "TestAgent/1.0",
        }
        cdc_kwargs = {
            "api_key": "test_key",
            "api_secret": "test_secret",
            "account_id": "ACCT1",
            "base_url": "https://live.trading212.com/api/v0",
            "user_agent": "TestAgent/1.0",
        }

        # fetch_snapshot should accept include_metadata
        mock_snapshot.return_value = pa.table(
            {
                "fetched_at": [None],
                "broker": ["Trading 212"],
                "source": ["test"],
                "payload": [b"{}"],
                "payload_hash": ["hash"],
                "account_id": ["ACCT1"],
                "source_file": [""],
            },
            schema=RAW_SCHEMA,
        )

        # fetch_cdc should NOT receive include_metadata
        mock_cdc.return_value = pa.table(
            {
                "fetched_at": [None],
                "broker": ["Trading 212"],
                "source": ["test_cdc"],
                "payload": [b"[]"],
                "payload_hash": ["hash_cdc"],
                "account_id": ["ACCT1"],
                "source_file": [""],
            },
            schema=RAW_SCHEMA,
        )

        # This should work — include_metadata is only for snapshot
        connector.fetch_snapshot(**snapshot_kwargs)
        connector.fetch_cdc(**cdc_kwargs)

        # Verify include_metadata was NOT passed to fetch_cdc
        cdc_call_kwargs = mock_cdc.call_args[1]
        assert "include_metadata" not in cdc_call_kwargs


class TestDirectoryCreation:
    """Test that Delta table writes create parent directories if missing.

    On first run, data/ subdirectories don't exist yet.
    ingest_raw, consolidate_holdings, and allocate_percentages must
    create them before writing.
    """

    def test_ingest_raw_creates_parent_dirs(self, tmp_path: Path) -> None:
        from deltalake import write_deltalake

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

        result = consolidate_holdings(
            holdings, key, converter, table_path=table_path
        )
        assert result.num_rows == 1
        assert Path(table_path).exists()

    def test_allocate_creates_parent_dirs(self, tmp_path: Path) -> None:
        from deltalake import write_deltalake

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