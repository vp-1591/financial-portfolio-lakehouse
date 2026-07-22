"""Tests for CDC event consolidation.

Decision: docs/adr/0087-make-cdc-mandatory-and-fail-on-empty-silver-cdc.md
CDC is mandatory for ibkr and trading212; consolidation raises RuntimeError
when a required broker CDC table is missing or empty.  XTB is optional.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pyarrow as pa
import pytest

from pipeline.connectors.transform_utils import build_normalized_table
from pipeline.crypto import generate_key
from pipeline.normalized.models import cdc_events_normalized_schema


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
        now = datetime.now(timezone.utc)
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
