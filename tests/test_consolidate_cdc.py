"""Tests for CDC event consolidation.

Decision: docs/adr/0087-make-cdc-mandatory-and-fail-on-empty-silver-cdc.md
CDC is mandatory for ibkr and trading212; consolidation raises RuntimeError
when a required broker CDC table is missing or empty.  XTB is optional.
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
from pipeline.normalized.models import cdc_events_normalized_schema
from pipeline.storage import StorageConfig, get_storage, use_storage
from tests.local_backend import LocalBackend


class TestConsolidateCdc:
    """Tests for consolidating broker CDC events into a unified table."""

    @pytest.fixture()
    def fernet_key(self) -> bytes:
        return generate_key()

    def _make_cdc_table(
        self,
        broker: str,
        events: list[dict],
        fernet_key: bytes,
    ) -> pa.Table:
        """Build a CDC events table for a single broker."""
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
            cdc_events_normalized_schema,
            fernet_key,
            encrypt_columns=["cash_amount"],
        )

    def _make_empty_cdc_table(self, fernet_key: bytes) -> pa.Table:
        """Build an empty CDC events table with the correct schema."""
        return build_normalized_table(
            [],
            cdc_events_normalized_schema,
            fernet_key,
            encrypt_columns=["cash_amount"],
        )

    @staticmethod
    def _real_storage(tmp_path: Path) -> StorageConfig:
        """Build a tmp_path-backed StorageConfig with a LocalBackend.

        Used by the real-write re-read tests so ``consolidate_cdc_events``
        reads/writes real local Delta tables instead of mocked stubs.
        """
        data = tmp_path / "data"
        for subdir in [
            "normalized/ibkr_cdc",
            "normalized/trading212_cdc",
            "normalized/xtb_cdc",
            "normalized/cdc_events",
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

    def _write_cdc_delta(
        self, broker: str, events: list[dict], fernet_key: bytes, table_name: str
    ) -> None:
        """Build a CDC table for *broker* and write it to a real Delta table."""
        table = self._make_cdc_table(broker, events, fernet_key)
        storage = get_storage()
        path = storage.normalized_path(table_name)
        storage.backend.ensure_parent(path)
        write_deltalake(
            path, table, mode="overwrite", storage_options=storage.storage_options
        )

    def test_consolidate_merges_all_brokers(self, fernet_key: bytes) -> None:
        """Consolidation merges rows from required + optional broker CDC tables."""
        from pipeline.normalized.consolidate_cdc import consolidate_cdc_events

        t212_table = self._make_cdc_table(
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
        ibkr_table = self._make_cdc_table(
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

        # Mock DeltaTable and write_deltalake
        call_count = [0]

        def mock_delta_table(path, **kwargs):
            call_count[0] += 1
            if "xtb" in str(path):
                raise Exception("no data")
            if "trading212" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: t212_table})()
            if "ibkr" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: ibkr_table})()
            raise Exception("unknown path")

        with (
            patch(
                "pipeline.normalized.consolidate_cdc.DeltaTable",
                side_effect=mock_delta_table,
            ),
            patch("pipeline.normalized.consolidate_cdc.write_deltalake"),
            patch("pipeline.normalized.consolidate_cdc.get_storage") as mock_storage,
        ):
            mock_storage.return_value.storage_options = {}
            mock_storage.return_value.normalized_path = lambda x: f"data/normalized/{x}"
            mock_storage.return_value.backend.ensure_parent = lambda x: None

            result = consolidate_cdc_events()

        assert result is not None
        assert result.num_rows == 2
        brokers = result.column("broker").to_pylist()
        assert "Trading 212" in brokers
        assert "IBKR" in brokers

    def test_consolidate_raises_when_required_broker_missing(
        self, fernet_key: bytes
    ) -> None:
        """Consolidation raises RuntimeError when a required broker CDC table is missing."""
        from pipeline.normalized.consolidate_cdc import consolidate_cdc_events

        t212_table = self._make_cdc_table(
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
            # ibkr_cdc is missing (raises)
            if "ibkr" in str(path):
                raise FileNotFoundError("ibkr_cdc not found")
            # trading212_cdc is present
            if "trading212" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: t212_table})()
            raise Exception("unknown path")

        with (
            patch(
                "pipeline.normalized.consolidate_cdc.DeltaTable",
                side_effect=mock_delta_table,
            ),
            patch("pipeline.normalized.consolidate_cdc.write_deltalake"),
            patch("pipeline.normalized.consolidate_cdc.get_storage") as mock_storage,
        ):
            mock_storage.return_value.storage_options = {}
            mock_storage.return_value.normalized_path = lambda x: f"data/normalized/{x}"

            with pytest.raises(
                RuntimeError, match="Required CDC table ibkr_cdc not found"
            ):
                consolidate_cdc_events()

    def test_consolidate_raises_when_required_broker_empty(
        self, fernet_key: bytes
    ) -> None:
        """Consolidation raises RuntimeError when a required broker CDC table is empty."""
        from pipeline.normalized.consolidate_cdc import consolidate_cdc_events

        t212_table = self._make_cdc_table(
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
        empty_table = self._make_empty_cdc_table(fernet_key)

        def mock_delta_table(path, **kwargs):
            if "ibkr" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: empty_table})()
            if "trading212" in str(path):
                return type("DT", (), {"to_pyarrow_table": lambda self: t212_table})()
            raise Exception("unknown path")

        with (
            patch(
                "pipeline.normalized.consolidate_cdc.DeltaTable",
                side_effect=mock_delta_table,
            ),
            patch("pipeline.normalized.consolidate_cdc.write_deltalake"),
            patch("pipeline.normalized.consolidate_cdc.get_storage") as mock_storage,
        ):
            mock_storage.return_value.storage_options = {}
            mock_storage.return_value.normalized_path = lambda x: f"data/normalized/{x}"

            with pytest.raises(
                RuntimeError, match="Required CDC table ibkr_cdc is empty"
            ):
                consolidate_cdc_events()

    def test_consolidate_skips_xtb_when_missing(self, fernet_key: bytes) -> None:
        """XTB CDC table is optional: consolidation succeeds even if it's missing."""
        from pipeline.normalized.consolidate_cdc import consolidate_cdc_events

        t212_table = self._make_cdc_table(
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
        ibkr_table = self._make_cdc_table(
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
                "pipeline.normalized.consolidate_cdc.DeltaTable",
                side_effect=mock_delta_table,
            ),
            patch("pipeline.normalized.consolidate_cdc.write_deltalake"),
            patch("pipeline.normalized.consolidate_cdc.get_storage") as mock_storage,
        ):
            mock_storage.return_value.storage_options = {}
            mock_storage.return_value.normalized_path = lambda x: f"data/normalized/{x}"
            mock_storage.return_value.backend.ensure_parent = lambda x: None

            result = consolidate_cdc_events()

        assert result is not None
        assert result.num_rows == 2

    def test_consolidate_skips_xtb_when_empty(self, fernet_key: bytes) -> None:
        """XTB CDC table is optional: consolidation succeeds even if it's empty."""
        from pipeline.normalized.consolidate_cdc import consolidate_cdc_events

        t212_table = self._make_cdc_table(
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
        ibkr_table = self._make_cdc_table(
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
        empty_table = self._make_empty_cdc_table(fernet_key)

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
                "pipeline.normalized.consolidate_cdc.DeltaTable",
                side_effect=mock_delta_table,
            ),
            patch("pipeline.normalized.consolidate_cdc.write_deltalake"),
            patch("pipeline.normalized.consolidate_cdc.get_storage") as mock_storage,
        ):
            mock_storage.return_value.storage_options = {}
            mock_storage.return_value.normalized_path = lambda x: f"data/normalized/{x}"
            mock_storage.return_value.backend.ensure_parent = lambda x: None

            result = consolidate_cdc_events()

        assert result is not None
        assert result.num_rows == 2

    def test_consolidate_overwrites_cdc_events_re_read(
        self, fernet_key: bytes, tmp_path: Path
    ) -> None:
        """consolidate_cdc_events OVERWRITES (not appends) the cdc_events table.

        Pre-populates ``cdc_events`` with a sentinel row, runs the writer
        against real local Delta tables, re-opens the persisted table and
        asserts the sentinel is GONE (overwrite) rather than retained
        alongside the new rows (append).  Catches the
        ``mode="overwrite" -> "append"`` mutation (A2 D2).
        """
        from pipeline.normalized.consolidate_cdc import consolidate_cdc_events

        storage = self._real_storage(tmp_path)

        # Write required broker CDC tables (real Delta).
        self._write_cdc_delta(
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
            "ibkr_cdc",
        )
        self._write_cdc_delta(
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
            "trading212_cdc",
        )

        # Pre-populate the OUTPUT table with a sentinel/stale row.
        sentinel = self._make_cdc_table(
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
        out_path = storage.normalized_path("cdc_events")
        storage.backend.ensure_parent(out_path)
        write_deltalake(
            out_path,
            sentinel,
            mode="overwrite",
            storage_options=storage.storage_options,
        )

        # Run the writer.
        result = consolidate_cdc_events()
        assert result.num_rows == 2

        # Re-open the persisted Delta table and verify overwrite semantics.
        readback = DeltaTable(out_path, storage_options=storage.storage_options)
        persisted = readback.to_pyarrow_table()
        brokers = persisted.column("broker").to_pylist()
        # overwrite: sentinel GONE -> 2 rows; append would retain sentinel -> 3 rows.
        assert persisted.num_rows == 2
        assert "STALE_SENTINEL" not in brokers
        assert "IBKR" in brokers
        assert "Trading 212" in brokers

    def test_consolidate_includes_nonempty_optional_broker(
        self, fernet_key: bytes, tmp_path: Path
    ) -> None:
        """A NON-empty optional-broker CDC table is included in the consolidation.

        Writes a non-empty ``xtb_cdc`` Delta table (the only optional broker in
        the CDC path) alongside the required broker tables, runs the writer,
        and asserts the XTB rows appear in the result.  Catches the
        ``if table.num_rows > 0 -> == 0`` inverted-condition mutation (A2 D4).

        Note: this exercises the CDC *consolidation* optional-broker success
        path with an inline XTB CDC table (broker-neutral schema), NOT the
        deferred F3 XTB *connector* fixture (xlsx positions).  The XTB CDC
        event schema is identical to every other broker's, so no F3 fixture
        is required.
        """
        from pipeline.normalized.consolidate_cdc import consolidate_cdc_events

        self._real_storage(tmp_path)

        self._write_cdc_delta(
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
            "ibkr_cdc",
        )
        self._write_cdc_delta(
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
            "trading212_cdc",
        )
        self._write_cdc_delta(
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
            "xtb_cdc",
        )

        result = consolidate_cdc_events()
        brokers = result.column("broker").to_pylist()
        # The non-empty optional XTB table must be included (3 rows total).
        # Under the D4 mutation (num_rows > 0 -> == 0), the non-empty XTB
        # table is skipped -> 2 rows, no XTB.
        assert result.num_rows == 3
        assert "XTB" in brokers
        assert "IBKR" in brokers
        assert "Trading 212" in brokers
