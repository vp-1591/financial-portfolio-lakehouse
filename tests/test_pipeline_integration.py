"""End-to-end tests for pipeline run.py, ingest.py, and path creation.

Covers bugs found during end-to-end runs:
- XTB set_column() PyArrow 3-arg API
- Missing parent directory creation for Delta table paths
- Transform functions must decrypt payloads before JSON parsing
- T212 auth uses Basic Auth, not Bearer token
- build_portfolio_holdings handles missing Delta table gracefully
"""

from __future__ import annotations

import base64
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from pipeline import run as run_module
from pipeline.connectors.registry import get
from pipeline.connectors.trading212.client import Trading212Client, basic_auth_header
from pipeline.connectors.trading212.transform import (
    transform_snapshot as t212_transform_snapshot,
)
from pipeline.connectors.xtb.transform import (
    transform_snapshot as xtb_transform_snapshot,
)
from pipeline.crypto import encrypt, encrypt_float, generate_key
from pipeline.normalized.consolidate import (
    CurrencyConverter,
    Holding,
    consolidate_holdings,
)
from pipeline.normalized.models import snapshot_normalized_schema
from pipeline.query import clear_table_cache, refresh
from pipeline.raw.ingest import encrypt_raw_payloads, ingest_raw
from pipeline.raw.models import RAW_SCHEMA
from pipeline.secrets import reset_mode
from pipeline.storage import StorageConfig, use_storage
from tests.local_backend import LocalBackend
from tests.test_xtb_connector import _build_xlsx_bytes


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

        result = basic_auth_header("my-api-key", "my-api-secret")
        expected = base64.b64encode(b"my-api-key:my-api-secret").decode("ascii")
        assert result == f"Basic {expected}"

    def test_basic_auth_header_strips_whitespace(self) -> None:

        result = basic_auth_header("  test-key-123  ", "  test-secret-456  ")
        expected = base64.b64encode(b"test-key-123:test-secret-456").decode("ascii")
        assert result == f"Basic {expected}"

    def test_auth_method_is_basic_with_key_and_secret(self) -> None:
        """Regression test: prevents silent downgrade to Bearer or raw-key auth.

        Commit f7c3674 changed Basic → Bearer based on a misdiagnosed 401.
        The real cause was an IP-restricted API key. This test ensures
        the auth method stays as HTTP Basic with key:secret encoding.
        """

        header = basic_auth_header("mykey", "mysecret")
        # Must start with "Basic " — never "Bearer " or a raw key
        assert header.startswith("Basic "), f"Expected Basic auth, got: {header}"
        decoded = base64.b64decode(header[len("Basic ") :]).decode("utf-8")
        assert decoded == "mykey:mysecret", f"Expected key:secret, got: {decoded}"

    @patch("urllib.request.urlopen")
    def test_client_sends_basic_auth(self, mock_urlopen: MagicMock) -> None:

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
    """Test that transform functions decrypt payloads before parsing.

    Raw Delta tables store encrypted payloads. Transforms must call
    decrypt() before parsing (JSON or XLSX) or all rows are silently skipped.
    """

    def test_xtb_transform_decrypts_encrypted_payload(self) -> None:
        """XTB transform_snapshot must decrypt .xlsx payloads from raw Delta tables."""
        key = generate_key()

        xlsx_bytes = _build_xlsx_bytes(include_isin=True)
        encrypted_payload = encrypt(xlsx_bytes, key)

        raw = pa.table(
            {
                "fetched_at": [None],
                "broker": ["XTB"],
                "source": ["OPEN POSITION"],
                "payload": [encrypted_payload],
                "payload_hash": ["abc"],
                "source_file": ["test.xlsx"],
            },
            schema=RAW_SCHEMA,
        )

        result = xtb_transform_snapshot(raw, key)
        assert result.num_rows >= 1, (
            "XTB transform should produce rows from encrypted .xlsx payload"
        )

    def test_t212_transform_decrypts_encrypted_payload(self) -> None:
        """T212 transform_snapshot must decrypt payloads from raw Delta tables."""
        key = generate_key()

        summary = {"currencyCode": "EUR", "total": 100.0}
        positions = [{"ticker": "VUAA", "quantity": 1, "currentPrice": 100.0}]

        encrypted_summary = encrypt(json.dumps(summary).encode("utf-8"), key)
        encrypted_positions = encrypt(json.dumps(positions).encode("utf-8"), key)

        raw = pa.table(
            {
                "fetched_at": [datetime.now(UTC)] * 2,
                "broker": ["Trading 212"] * 2,
                "source": ["/equity/account/summary", "/equity/positions"],
                "payload": [encrypted_summary, encrypted_positions],
                "payload_hash": ["hash1", "hash2"],
                "source_file": ["", ""],
            },
            schema=RAW_SCHEMA,
        )

        result = t212_transform_snapshot(raw, key)
        assert result.num_rows >= 1, (
            "T212 transform should produce rows from encrypted payload"
        )


class TestDirectoryCreation:
    """Test that Delta table writes create parent directories if missing.

    On first run, data/ subdirectories don't exist yet.
    ingest_raw, consolidate_holdings, and build_portfolio_holdings must
    create them before writing.
    """

    def test_ingest_raw_creates_parent_dirs(
        self, tmp_path: Path, tmp_data_dir, docker_mode
    ) -> None:

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
                "source_file": [""],
            },
            schema=RAW_SCHEMA,
        )

        count = ingest_raw(raw, table_path, key)
        assert count == 1
        assert Path(table_path).exists()

    def test_consolidate_creates_parent_dirs(
        self, tmp_path: Path, tmp_data_dir, docker_mode
    ) -> None:

        key = generate_key()
        table_path = str(tmp_path / "normalized" / "consolidated_holdings")

        converter = CurrencyConverter("EUR", manual_rates={"USD": 0.9})
        holdings = [
            Holding("TestBroker", "AAPL", "USD", 100.0),
        ]

        result = consolidate_holdings(holdings, key, converter, table_path=table_path)
        assert result.num_rows == 1
        assert Path(table_path).exists()


class TestCliDispatchIntegration:
    """End-to-end ``main()`` dispatch integration tests (F9 C1-C3).

    Exercises the full argparse parse -> ``commands[args.command](args)``
    dispatch path with a REAL subcommand (``query``) against a populated
    local backend.  Unlike ``test_run_subcommands.py`` (which monkeypatches
    the ``cmd_*`` handler to test dispatch *routing* only), these tests let
    ``cmd_query``, ``refresh()``, ``get_connection()``, and DuckDB run for
    real — only ``resolve_storage()`` is stubbed (to install a
    ``LocalBackend``, since real S3 is unavailable in tests).

    A dispatch-break mutation (removing ``"query"`` from the commands dict
    in ``pipeline/run.py``) raises ``KeyError`` here, failing this test —
    the existing ``test_run_subcommands.py`` monkeypatched-dispatch tests
    cover the same mutation for ``run-connector``; this test extends
    coverage to the ``query`` subcommand running end-to-end.
    """

    def _write_ibkr_normalized(self, data: Path, key: bytes) -> None:
        """Write a one-row IBKR normalized Delta table for query tests."""

        now = datetime.now(UTC)
        table = pa.table(
            {
                "fetched_at": [now],
                "account_id": ["U123456"],
                "position_type": ["EQUITY"],
                "label": ["VWCE"],
                "asset_class": ["STK"],
                "security_value": [encrypt_float(5000.0, key)],
                "security_ccy": ["EUR"],
                "isin": ["IE00BK5BQT80"],
                "description": ["Vanguard FTSE All-World"],
            },
            schema=snapshot_normalized_schema,
        )
        write_deltalake(
            str(data / "normalized" / "ibkr_snapshot"), table, mode="overwrite"
        )

    def test_main_query_runs_end_to_end(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """main() dispatches 'query' and cmd_query runs a real DuckDB query.

        Verifies the dispatch integration: argparse parses the subcommand,
        ``main()`` dispatches to the real ``cmd_query`` (not a stub), which
        queries a real Delta table via DuckDB and prints results.
        """

        key = generate_key()
        data = tmp_path / "data"
        for subdir in ("normalized/ibkr_snapshot", "raw/ibkr_snapshot"):
            (data / subdir).mkdir(parents=True, exist_ok=True)
        self._write_ibkr_normalized(data, key)

        secrets = tmp_path / ".secrets"
        secrets.mkdir(parents=True, exist_ok=True)
        (secrets / "encryption.key").write_bytes(key)

        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(secrets),
            encryption_key_file=str(secrets / "encryption.key"),
            backend=LocalBackend(data),
        )

        # Stub resolve_storage to install the LocalBackend config — real S3
        # is not available in tests.  Everything else (argparse, dispatch,
        # cmd_query, DuckDB) runs for real.
        monkeypatch.setattr(
            "pipeline.storage.resolve_storage", lambda: use_storage(config)
        )
        monkeypatch.setenv("ENCRYPTION_KEY", key.decode("utf-8"))
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "pipeline.run",
                "query",
                "SELECT label FROM ibkr_snapshot_normalized",
                "--mode",
                "docker",
            ],
        )

        clear_table_cache()
        try:
            rc = run_module.main()
            assert rc == 0
            assert "VWCE" in capsys.readouterr().out
        finally:
            refresh()
            reset_mode()

    def test_main_query_decrypt_flag_runs_end_to_end(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """main() with --decrypt dispatches and decrypts the real query output.

        Extends the dispatch integration to the ``--decrypt`` flag: the real
        ``cmd_query`` invokes ``decrypt_df`` and the decrypted float value
        appears in output.  The ``_decrypt_value * 10`` mutation (5000 ->
        50000) fails this test because ``"5000.0"`` is not a substring of
        ``"50000.0"``.
        """

        key = generate_key()
        data = tmp_path / "data"
        for subdir in ("normalized/ibkr_snapshot", "raw/ibkr_snapshot"):
            (data / subdir).mkdir(parents=True, exist_ok=True)
        self._write_ibkr_normalized(data, key)

        secrets = tmp_path / ".secrets"
        secrets.mkdir(parents=True, exist_ok=True)
        (secrets / "encryption.key").write_bytes(key)

        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(secrets),
            encryption_key_file=str(secrets / "encryption.key"),
            backend=LocalBackend(data),
        )

        monkeypatch.setattr(
            "pipeline.storage.resolve_storage", lambda: use_storage(config)
        )
        monkeypatch.setenv("ENCRYPTION_KEY", key.decode("utf-8"))
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "pipeline.run",
                "query",
                "SELECT label, security_value FROM ibkr_snapshot_normalized",
                "--decrypt",
                "--mode",
                "docker",
            ],
        )

        clear_table_cache()
        try:
            rc = run_module.main()
            assert rc == 0
            out = capsys.readouterr().out
            assert "VWCE" in out
            # Bounded equality — catches the _decrypt_value * 10 mutation.
            assert "5000.0" in out
        finally:
            refresh()
            reset_mode()
