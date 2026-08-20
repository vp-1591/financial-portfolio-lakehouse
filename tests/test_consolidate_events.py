"""Tests for event consolidation.

Decision: docs/adr/0108-xtb-new-format-connector-overhaul.md
Only enabled connectors are read; missing or empty event tables are skipped
and recorded as quality warnings.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from pipeline.connectors.transform_utils import build_normalized_table
from pipeline.crypto import generate_key
from pipeline.normalized.consolidate_events import consolidate_events
from pipeline.normalized.models import events_normalized_schema
from pipeline.storage import StorageConfig, get_storage, use_storage
from tests.local_backend import LocalBackend


class TestConsolidateEvents:
    """Tests for consolidating broker events into a unified table."""

    @pytest.fixture()
    def fernet_key(self) -> bytes:
        return generate_key()

    def _make_events_table(
        self,
        broker: str,
        events: list[dict],
        fernet_key: bytes,
    ) -> pa.Table:
        """Build an events table for a single broker."""
        now = datetime.now(UTC)
        records = []
        for event in events:
            record = {
                "fetched_at": now,
                "broker": broker,
                "account_id": event.get("account_id", ""),
                "event_id": event.get("event_id", ""),
                "source": event.get("source", ""),
                "event_type": event.get("event_type", ""),
                "raw_event_type": event.get("raw_event_type", ""),
                "event_datetime": event.get("event_datetime", ""),
                "security_ccy": event.get("security_ccy", ""),
                "cash_amount": event.get("cash_amount", 0.0),
            }
            records.append(record)

        return build_normalized_table(
            records,
            events_normalized_schema,
            fernet_key,
            encrypt_columns=["cash_amount"],
        )

    def _make_empty_events_table(self, fernet_key: bytes) -> pa.Table:
        """Build an empty events table with the correct schema."""
        return build_normalized_table(
            [],
            events_normalized_schema,
            fernet_key,
            encrypt_columns=["cash_amount"],
        )

    @staticmethod
    def _real_storage(tmp_path: Path) -> StorageConfig:
        """Build a tmp_path-backed StorageConfig with a LocalBackend.

        Used by the real-write re-read tests so ``consolidate_events``
        reads/writes real local Delta tables instead of mocked stubs.
        """
        data = tmp_path / "data"
        for subdir in [
            "normalized/ibkr_events",
            "normalized/trading212_events",
            "normalized/xtb_events",
            "normalized/events",
        ]:
            (data / subdir).mkdir(parents=True, exist_ok=True)
        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(data),
        )
        use_storage(config)
        return config

    def _write_events_delta(
        self, broker: str, events: list[dict], fernet_key: bytes, table_name: str
    ) -> None:
        """Build an events table for *broker* and write it to a real Delta table."""
        table = self._make_events_table(broker, events, fernet_key)
        storage = get_storage()
        path = storage.normalized_path(table_name)
        storage.backend.ensure_parent(path)
        write_deltalake(
            path, table, mode="overwrite", storage_options=storage.storage_options
        )

    def test_consolidate_merges_all_brokers(self, fernet_key: bytes) -> None:
        """Consolidation merges rows from required + optional broker events tables."""

        t212_table = self._make_events_table(
            "Trading 212",
            [
                {
                    "event_id": "t212-1",
                    "event_type": "TRADE",
                    "raw_event_type": "ORDER",
                    "source": "/equity/history/orders",
                    "event_datetime": "2024-01-15",
                    "security_ccy": "USD",
                    "cash_amount": 1500.0,
                }
            ],
            fernet_key,
        )
        ibkr_table = self._make_events_table(
            "IBKR",
            [
                {
                    "event_id": "ibkr-1",
                    "event_type": "DIVIDEND",
                    "raw_event_type": "Dividends",
                    "source": "CashTransaction",
                    "event_datetime": "2024-03-01",
                    "security_ccy": "EUR",
                    "cash_amount": 42.5,
                }
            ],
            fernet_key,
        )
        xtb_table = self._make_events_table(
            "XTB",
            [
                {
                    "event_id": "xtb-1",
                    "event_type": "DEPOSIT",
                    "raw_event_type": "Deposit",
                    "source": "XTB_REPORT",
                    "event_datetime": "2024-02-01",
                    "security_ccy": "PLN",
                    "cash_amount": 500.0,
                }
            ],
            fernet_key,
        )

        # Mock DeltaTable and write_deltalake
        call_count = [0]

        def mock_delta_table(path, **kwargs):
            call_count[0] += 1
            if "xtb" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: xtb_table})()
            if "trading212" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: t212_table})()
            if "ibkr" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: ibkr_table})()
            raise Exception("unknown path")

        with (
            patch(
                "pipeline.normalized.consolidate_events.DeltaTable",
                side_effect=mock_delta_table,
            ),
            patch("pipeline.normalized.consolidate_events.write_deltalake"),
            patch("pipeline.normalized.consolidate_events.get_storage") as mock_storage,
        ):
            mock_storage.return_value.storage_options = {}
            mock_storage.return_value.normalized_path = lambda x: f"data/normalized/{x}"
            mock_storage.return_value.backend.ensure_parent = lambda x: None

            result = consolidate_events(["ibkr", "trading212", "xtb"])

        assert result is not None
        assert result.num_rows == 3
        brokers = result.column("broker").to_pylist()
        assert "Trading 212" in brokers
        assert "IBKR" in brokers
        assert "XTB" in brokers

    def test_consolidate_skips_when_enabled_broker_missing(
        self, fernet_key: bytes
    ) -> None:
        """A missing enabled event table does not prevent partial consolidation."""

        t212_table = self._make_events_table(
            "Trading 212",
            [
                {
                    "event_id": "t212-1",
                    "event_type": "TRADE",
                    "raw_event_type": "ORDER",
                    "source": "/equity/history/orders",
                    "event_datetime": "2024-01-15",
                    "security_ccy": "USD",
                    "cash_amount": 1500.0,
                }
            ],
            fernet_key,
        )

        def mock_delta_table(path, **kwargs):
            # ibkr_events is missing (raises)
            if "ibkr" in str(path):
                raise FileNotFoundError("ibkr_events not found")
            # trading212_events is present
            if "trading212" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: t212_table})()
            raise Exception("unknown path")

        with (
            patch(
                "pipeline.normalized.consolidate_events.DeltaTable",
                side_effect=mock_delta_table,
            ),
            patch("pipeline.normalized.consolidate_events.write_deltalake"),
            patch("pipeline.normalized.consolidate_events.get_storage") as mock_storage,
        ):
            mock_storage.return_value.storage_options = {}
            mock_storage.return_value.normalized_path = lambda x: f"data/normalized/{x}"

            result = consolidate_events(["ibkr", "trading212"])
            assert result.num_rows == 1

    def test_consolidate_skips_when_enabled_broker_empty(
        self, fernet_key: bytes
    ) -> None:
        """An empty enabled event table does not prevent partial consolidation."""

        t212_table = self._make_events_table(
            "Trading 212",
            [
                {
                    "event_id": "t212-1",
                    "event_type": "TRADE",
                    "raw_event_type": "ORDER",
                    "source": "/equity/history/orders",
                    "event_datetime": "2024-01-15",
                    "security_ccy": "USD",
                    "cash_amount": 1500.0,
                }
            ],
            fernet_key,
        )
        empty_table = self._make_empty_events_table(fernet_key)

        def mock_delta_table(path, **kwargs):
            if "ibkr" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: empty_table})()
            if "trading212" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: t212_table})()
            raise Exception("unknown path")

        with (
            patch(
                "pipeline.normalized.consolidate_events.DeltaTable",
                side_effect=mock_delta_table,
            ),
            patch("pipeline.normalized.consolidate_events.write_deltalake"),
            patch("pipeline.normalized.consolidate_events.get_storage") as mock_storage,
        ):
            mock_storage.return_value.storage_options = {}
            mock_storage.return_value.normalized_path = lambda x: f"data/normalized/{x}"

            result = consolidate_events(["ibkr", "trading212"])
            assert result.num_rows == 1

    def test_consolidate_skips_when_xtb_missing(self, fernet_key: bytes) -> None:
        """XTB is not in the required gate: a missing xtb_events is skipped."""

        t212_table = self._make_events_table(
            "Trading 212",
            [
                {
                    "event_id": "t212-1",
                    "event_type": "TRADE",
                    "raw_event_type": "ORDER",
                    "source": "/orders",
                    "event_datetime": "2024-01-15",
                    "security_ccy": "USD",
                    "cash_amount": 100.0,
                }
            ],
            fernet_key,
        )
        ibkr_table = self._make_events_table(
            "IBKR",
            [
                {
                    "event_id": "ibkr-1",
                    "event_type": "DIVIDEND",
                    "raw_event_type": "Div",
                    "source": "CashTransaction",
                    "event_datetime": "2024-03-01",
                    "security_ccy": "EUR",
                    "cash_amount": 42.5,
                }
            ],
            fernet_key,
        )

        def mock_delta_table(path, **kwargs):
            if "xtb" in str(path):
                raise Exception("no data")
            if "trading212" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: t212_table})()
            if "ibkr" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: ibkr_table})()
            raise Exception("unknown path")

        with (
            patch(
                "pipeline.normalized.consolidate_events.DeltaTable",
                side_effect=mock_delta_table,
            ),
            patch("pipeline.normalized.consolidate_events.write_deltalake"),
            patch("pipeline.normalized.consolidate_events.get_storage") as mock_storage,
        ):
            mock_storage.return_value.storage_options = {}
            mock_storage.return_value.normalized_path = lambda x: f"data/normalized/{x}"
            mock_storage.return_value.backend.ensure_parent = lambda x: None

            result = consolidate_events(["ibkr", "trading212", "xtb"])
            # XTB is skipped; the remaining enabled connectors consolidate.
            assert result.num_rows == 2
            brokers = result.column("broker").to_pylist()
            assert "IBKR" in brokers
            assert "Trading 212" in brokers

    def test_consolidate_skips_when_xtb_empty(self, fernet_key: bytes) -> None:
        """XTB is not in the required gate: an empty xtb_events is skipped."""

        t212_table = self._make_events_table(
            "Trading 212",
            [
                {
                    "event_id": "t212-1",
                    "event_type": "TRADE",
                    "raw_event_type": "ORDER",
                    "source": "/orders",
                    "event_datetime": "2024-01-15",
                    "security_ccy": "USD",
                    "cash_amount": 100.0,
                }
            ],
            fernet_key,
        )
        ibkr_table = self._make_events_table(
            "IBKR",
            [
                {
                    "event_id": "ibkr-1",
                    "event_type": "DIVIDEND",
                    "raw_event_type": "Div",
                    "source": "CashTransaction",
                    "event_datetime": "2024-03-01",
                    "security_ccy": "EUR",
                    "cash_amount": 42.5,
                }
            ],
            fernet_key,
        )
        empty_table = self._make_empty_events_table(fernet_key)

        def mock_delta_table(path, **kwargs):
            if "xtb" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: empty_table})()
            if "trading212" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: t212_table})()
            if "ibkr" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: ibkr_table})()
            raise Exception("unknown path")

        with (
            patch(
                "pipeline.normalized.consolidate_events.DeltaTable",
                side_effect=mock_delta_table,
            ),
            patch("pipeline.normalized.consolidate_events.write_deltalake"),
            patch("pipeline.normalized.consolidate_events.get_storage") as mock_storage,
        ):
            mock_storage.return_value.storage_options = {}
            mock_storage.return_value.normalized_path = lambda x: f"data/normalized/{x}"
            mock_storage.return_value.backend.ensure_parent = lambda x: None

            result = consolidate_events(["ibkr", "trading212", "xtb"])
            # XTB is skipped; the remaining enabled connectors consolidate.
            assert result.num_rows == 2
            brokers = result.column("broker").to_pylist()
            assert "IBKR" in brokers
            assert "Trading 212" in brokers

    def test_consolidate_overwrites_events_re_read(
        self, fernet_key: bytes, tmp_path: Path
    ) -> None:
        """consolidate_events OVERWRITES (not appends) the events table.

        Pre-populates ``events`` with a sentinel row, runs the writer
        against real local Delta tables, re-opens the persisted table and
        asserts the sentinel is GONE (overwrite) rather than retained
        alongside the new rows (append).  Catches the
        ``mode="overwrite" -> "append"`` mutation (A2 D2).
        """

        storage = self._real_storage(tmp_path)

        # Write enabled broker event tables (real Delta).
        self._write_events_delta(
            "IBKR",
            [
                {
                    "event_id": "ibkr-1",
                    "event_type": "DIVIDEND",
                    "raw_event_type": "Dividends",
                    "source": "CashTransaction",
                    "event_datetime": "2024-03-01",
                    "security_ccy": "EUR",
                    "cash_amount": 42.5,
                }
            ],
            fernet_key,
            "ibkr_events",
        )
        self._write_events_delta(
            "Trading 212",
            [
                {
                    "event_id": "t212-1",
                    "event_type": "TRADE",
                    "raw_event_type": "ORDER",
                    "source": "/equity/history/orders",
                    "event_datetime": "2024-01-15",
                    "security_ccy": "USD",
                    "cash_amount": 1500.0,
                }
            ],
            fernet_key,
            "trading212_events",
        )
        self._write_events_delta(
            "XTB",
            [
                {
                    "event_id": "xtb-1",
                    "event_type": "DEPOSIT",
                    "raw_event_type": "Deposit",
                    "source": "XTB_REPORT",
                    "event_datetime": "2024-02-01",
                    "security_ccy": "PLN",
                    "cash_amount": 500.0,
                }
            ],
            fernet_key,
            "xtb_events",
        )

        # Pre-populate the OUTPUT table with a sentinel/stale row.
        sentinel = self._make_events_table(
            "STALE_SENTINEL",
            [
                {
                    "event_id": "stale-1",
                    "event_type": "TRADE",
                    "raw_event_type": "STALE",
                    "source": "stale",
                    "event_datetime": "2020-01-01",
                    "security_ccy": "USD",
                    "cash_amount": 1.0,
                }
            ],
            fernet_key,
        )
        out_path = storage.normalized_path("events")
        storage.backend.ensure_parent(out_path)
        write_deltalake(
            out_path,
            sentinel,
            mode="overwrite",
            storage_options=storage.storage_options,
        )

        # Run the writer.
        result = consolidate_events(["ibkr", "trading212", "xtb"])
        assert result.num_rows == 3

        # Re-open the persisted Delta table and verify overwrite semantics.
        readback = DeltaTable(out_path, storage_options=storage.storage_options)
        persisted = readback.to_pyarrow_table()
        brokers = persisted.column("broker").to_pylist()
        # overwrite: sentinel GONE -> 3 rows; append would retain sentinel -> 4 rows.
        assert persisted.num_rows == 3
        assert "STALE_SENTINEL" not in brokers
        assert "IBKR" in brokers
        assert "Trading 212" in brokers
        assert "XTB" in brokers

    def test_consolidate_includes_xtb_events(
        self, fernet_key: bytes, tmp_path: Path
    ) -> None:
        """A non-empty XTB events table is included in the consolidation.

        Writes a non-empty ``xtb_events`` Delta table alongside the other required
        broker tables, runs the writer, and asserts the XTB rows appear in the
        result.  Catches the ``if table.num_rows > 0 -> == 0`` inverted-condition
        mutation (A2 D4).  XTB is not in the required gate, but a non-empty
        ``xtb_events`` is still consolidated like any other broker.

        Note: this exercises the events *consolidation* path with an inline XTB
        events table (broker-neutral schema), NOT the XTB *connector* fixture
        (xlsx positions).  The XTB events schema is identical to every other
        broker's, so no connector fixture is required.
        """

        self._real_storage(tmp_path)

        self._write_events_delta(
            "IBKR",
            [
                {
                    "event_id": "ibkr-1",
                    "event_type": "DIVIDEND",
                    "raw_event_type": "Dividends",
                    "source": "CashTransaction",
                    "event_datetime": "2024-03-01",
                    "security_ccy": "EUR",
                    "cash_amount": 42.5,
                }
            ],
            fernet_key,
            "ibkr_events",
        )
        self._write_events_delta(
            "Trading 212",
            [
                {
                    "event_id": "t212-1",
                    "event_type": "TRADE",
                    "raw_event_type": "ORDER",
                    "source": "/orders",
                    "event_datetime": "2024-01-15",
                    "security_ccy": "USD",
                    "cash_amount": 100.0,
                }
            ],
            fernet_key,
            "trading212_events",
        )
        self._write_events_delta(
            "XTB",
            [
                {
                    "event_id": "xtb-1",
                    "event_type": "DEPOSIT",
                    "raw_event_type": "DEPOSIT",
                    "source": "xtb",
                    "event_datetime": "2024-02-01",
                    "security_ccy": "PLN",
                    "cash_amount": 500.0,
                }
            ],
            fernet_key,
            "xtb_events",
        )

        result = consolidate_events(["ibkr", "trading212", "xtb"])
        brokers = result.column("broker").to_pylist()
        # The non-empty XTB table must be included (3 rows total).
        # Under the D4 mutation (num_rows > 0 -> == 0), the non-empty XTB
        # table is skipped -> 2 rows, no XTB.
        assert result.num_rows == 3
        assert "XTB" in brokers
        assert "IBKR" in brokers
        assert "Trading 212" in brokers
