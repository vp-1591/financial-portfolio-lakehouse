"""Tests for the ``query`` CLI subcommand."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from pipeline.crypto import encrypt_float, generate_key
from pipeline.normalized.models import ibkr_snapshot_normalized_schema
from pipeline.run import cmd_query
from pipeline.storage import LocalBackend, StorageConfig, use_storage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_ibkr_snapshot(data_dir: Path, fernet_key: bytes) -> None:
    """Write a small normalized IBKR Delta table for query tests."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    table = pa.table(
        {
            "fetched_at": [now, now, now],
            "account_id": ["U123456", "U123456", "U123456"],
            "position_type": ["EQUITY", "EQUITY", "CASH"],
            "label": ["VWCE", "AAPL", "CASH EUR"],
            "asset_class": ["STK", "STK", "CASH"],
            "value": [
                encrypt_float(5000.0, fernet_key),
                encrypt_float(2700.0, fernet_key),
                encrypt_float(2000.0, fernet_key),
            ],
            "value_currency": ["EUR", "USD", "EUR"],
            "isin": ["IE00BK5BQT80", "US0378331005", ""],
            "description": ["Vanguard FTSE All-World", "Apple Inc", "Cash EUR"],
            "security_currency": ["EUR", "USD", "EUR"],
        },
        schema=ibkr_snapshot_normalized_schema,
    )
    path = str(data_dir / "normalized" / "ibkr_snapshot")
    write_deltalake(path, table, mode="overwrite")


def _setup_env(tmp_path: Path) -> tuple[Path, bytes]:
    """Create data dir structure, write IBKR data, set up storage and env."""
    key = generate_key()
    data = tmp_path / "data"
    for subdir in [
        "raw/ibkr_snapshot",
        "raw/ibkr_cdc",
        "raw/trading212_snapshot",
        "raw/trading212_cdc",
        "raw/xtb_snapshot",
        "raw/xtb_cdc",
        "normalized/ibkr_snapshot",
        "normalized/ibkr_cdc",
        "normalized/trading212_snapshot",
        "normalized/trading212_cdc",
        "normalized/xtb_snapshot",
        "normalized/xtb_cdc",
        "normalized/consolidated_holdings",
        "analytics/portfolio_allocation",
    ]:
        (data / subdir).mkdir(parents=True, exist_ok=True)

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
    use_storage(config)

    _write_ibkr_snapshot(data, key)

    os.environ["ENCRYPTION_KEY"] = key.decode("utf-8")

    return data, key


def _make_args(
    sql: str, decrypt: bool = False, fmt: str = "table"
) -> argparse.Namespace:
    """Create an argparse Namespace mimicking the query subcommand args."""
    return argparse.Namespace(sql=sql, decrypt=decrypt, format=fmt)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestQueryCLI:
    """Tests for cmd_query function."""

    def setup_method(self) -> None:
        """Reset the query module cache between tests."""
        from pipeline.query import clear_table_cache

        clear_table_cache()

    def teardown_method(self) -> None:
        """Clean up env var."""
        os.environ.pop("ENCRYPTION_KEY", None)

    def test_basic_query(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A valid SQL query returns exit code 0 and prints results."""
        _setup_env(tmp_path)

        args = _make_args("SELECT * FROM ibkr_snapshot_normalized")
        result = cmd_query(args)

        assert result == 0
        captured = capsys.readouterr()
        assert "VWCE" in captured.out
        assert "AAPL" in captured.out

    def test_decrypt_flag(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """--decrypt auto-detects binary columns and decrypts float values."""
        _setup_env(tmp_path)

        args = _make_args(
            "SELECT label, value FROM ibkr_snapshot_normalized", decrypt=True
        )
        result = cmd_query(args)

        assert result == 0
        captured = capsys.readouterr()
        # Decrypted float values should appear (5000.0, 2700.0, 2000.0)
        assert "5000.0" in captured.out or "5000" in captured.out
        assert "2700.0" in captured.out or "2700" in captured.out
        assert "2000.0" in captured.out or "2000" in captured.out

    def test_no_decrypt(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Without --decrypt, binary columns remain as binary in output."""
        _setup_env(tmp_path)

        args = _make_args("SELECT label, value FROM ibkr_snapshot_normalized")
        result = cmd_query(args)

        assert result == 0
        captured = capsys.readouterr()
        # Without decrypt, the 'value' column should still contain binary data
        # (Polars will show it as bytes or a hex-like representation)
        # Just verify the command succeeds and labels are present
        assert "VWCE" in captured.out

    def test_invalid_sql(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Invalid SQL returns exit code 1 and prints error to stderr."""
        # Don't need storage for an invalid query test, but we do need
        # the query module importable
        args = _make_args("SELECT FROM INVALID!!!")
        result = cmd_query(args)

        assert result == 1
        captured = capsys.readouterr()
        assert "Query error" in captured.err

    def test_csv_format(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """--format csv produces CSV output with headers."""
        _setup_env(tmp_path)

        args = _make_args(
            "SELECT label, value_currency FROM ibkr_snapshot_normalized", fmt="csv"
        )
        result = cmd_query(args)

        assert result == 0
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        # First line should be the header
        assert "label" in lines[0]
        assert len(lines) >= 2  # header + at least one data row

    def test_json_format(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """--format json produces valid JSON output."""
        _setup_env(tmp_path)

        args = _make_args(
            "SELECT label, value_currency FROM ibkr_snapshot_normalized", fmt="json"
        )
        result = cmd_query(args)

        assert result == 0
        captured = capsys.readouterr()
        # Should be valid JSON
        parsed = json.loads(captured.out)
        assert isinstance(parsed, list)
        assert len(parsed) >= 1

    def test_list_tables(self, tmp_path: Path) -> None:
        """list_tables() discovers the IBKR snapshot table after setup."""
        _setup_env(tmp_path)

        from pipeline.query import list_tables

        tables = list_tables()
        assert "ibkr_snapshot_normalized" in tables
