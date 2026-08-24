"""Tests for the post-handoff transform contract (AD-8).

The in-memory encrypted-fetch handoff (issue #154) is removed: ``fetch_connector``
returns only a ``FetchResult``, ``transform_connector`` reads the merged bronze
table once (AD-6), and ``ingest_raw`` returns nothing. Covers: the golden
regression (table-read output identical to the pre-removal handoff output),
empty-table skip, the real ingest_raw -> transform boundary, the events-fetch
branch of ``fetch_connector``, and the ``ingest_raw`` merge-on-key contract.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from pipeline import run as run_module
from pipeline.connectors.registry import get
from pipeline.connectors.transform_utils import empty_arrow_table
from pipeline.crypto import decrypt, encrypt
from pipeline.raw.ingest import ingest_raw
from pipeline.raw.models import RAW_SCHEMA
from pipeline.run import FetchResult, fetch_connector, transform_connector
from pipeline.storage import get_storage
from tests.fixtures.trading212 import (
    t212_normalized_snapshot,
    t212_raw_events,
    t212_raw_snapshot,
)


def _raw_one_row(fernet_key: bytes) -> pa.Table:
    """One RAW_SCHEMA row for mocked ingest_raw returns."""
    now = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
    payload = b"{}"
    return pa.table(
        {
            "fetched_at": [now],
            "broker": ["Trading 212"],
            "source": ["/equity/account/summary"],
            "payload": [encrypt(payload, fernet_key)],
            "payload_hash": [hashlib.sha256(payload).hexdigest()],
            "account_id": [None],
        },
        schema=RAW_SCHEMA,
    )


def _plaintext_raw(table: pa.Table, fernet_key: bytes) -> pa.Table:
    """Return the raw table with payloads decrypted to plaintext (as fetched).

    The fixtures ship Fernet-encrypted payloads; ``ingest_raw`` encrypts its
    input, so feeding it the fixture would double-encrypt. Decrypting first
    simulates the real fetch, which returns plaintext API bytes.
    """
    payloads = table.column("payload").to_pylist()
    plain = [decrypt(p, fernet_key) for p in payloads]
    idx = table.schema.get_field_index("payload")
    return table.set_column(idx, "payload", pa.array(plain, type=pa.binary()))


def _decrypted_columns(table: pa.Table, fernet_key: bytes) -> dict[str, list]:
    """Column values with Fernet binary columns decrypted to plaintext."""
    columns: dict[str, list] = {}
    for name in table.column_names:
        column = table.column(name)
        if pa.types.is_binary(column.type) or pa.types.is_large_binary(column.type):
            columns[name] = [
                None if value is None else decrypt(value, fernet_key)
                for value in column.to_pylist()
            ]
        else:
            columns[name] = column.to_pylist()
    return columns


class TestTransformConnectorTableRead:
    """transform_connector reads the merged bronze table once (AD-6)."""

    @staticmethod
    def _write_merged_raw(merged: pa.Table) -> None:
        raw_path = run_module.get_raw_path("trading212")
        get_storage().backend.ensure_parent(raw_path)
        write_deltalake(
            raw_path,
            merged,
            mode="append",
            storage_options=get_storage().storage_options,
        )

    def _read_normalized(self, layer: str) -> pa.Table:
        norm_path = get_storage().normalized_path(f"trading212_{layer}")
        return DeltaTable(
            norm_path, storage_options=get_storage().storage_options
        ).to_pyarrow_table()

    def test_table_read_output_matches_golden_fixture(
        self, tmp_data_dir: Path, fernet_key: bytes
    ) -> None:
        """Golden (T5.3): the table-read path reproduces the pre-removal
        handoff output — the round-trip-verified normalized fixture.

        The handoff path (ADR 0116) produced exactly this fixture; removing
        the handoff must not change the normalized output.
        """
        fetched_at = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
        snap = t212_raw_snapshot(fernet_key=fernet_key, fetched_at=fetched_at)
        evt = t212_raw_events(fernet_key=fernet_key, fetched_at=fetched_at)
        self._write_merged_raw(pa.concat_tables([snap, evt], schema=RAW_SCHEMA))

        connector = get("trading212")
        rc = transform_connector(connector, fernet_key)
        assert rc == 0
        read_snap = self._read_normalized("snapshot")
        read_events = self._read_normalized("events")

        # Golden: the table-read output matches the round-trip-verified fixture
        # (decrypted contents — Fernet ciphertext is randomized per encrypt).
        expected_snap = t212_normalized_snapshot(
            fernet_key=fernet_key, fetched_at=fetched_at
        )
        assert read_snap.num_rows == 3
        assert read_events.num_rows == 1
        assert _decrypted_columns(read_snap, fernet_key) == _decrypted_columns(
            expected_snap, fernet_key
        )
        assert read_events.column("event_type").to_pylist() == ["TRADE"]

    def test_empty_raw_table_skips_without_rewriting(
        self, tmp_data_dir: Path, fernet_key: bytes, caplog: pytest.LogCaptureFixture
    ) -> None:
        norm_path = get_storage().normalized_path("trading212_snapshot")
        get_storage().backend.ensure_parent(norm_path)
        write_deltalake(
            norm_path,
            t212_normalized_snapshot(fernet_key=fernet_key),
            mode="overwrite",
            storage_options=get_storage().storage_options,
        )
        self._write_merged_raw(empty_arrow_table(RAW_SCHEMA))
        with caplog.at_level(logging.WARNING, logger="pipeline.run"):
            rc = transform_connector(get("trading212"), fernet_key)
        assert rc == 0
        assert any("raw table is empty" in r.message for r in caplog.records)
        # 0-row table skips: the pre-existing normalized table is not
        # rewritten.
        read_back = DeltaTable(
            norm_path, storage_options=get_storage().storage_options
        ).to_pyarrow_table()
        assert read_back.num_rows == 3

    def test_real_ingest_raw_then_transform(
        self, tmp_path: Path, tmp_data_dir: Path, fernet_key: bytes
    ) -> None:
        """A REAL ingest_raw write, followed by the table-read transform,
        yields the golden normalized output (end-to-end fetch->transform
        boundary, previously threaded via the handoff).
        """
        fetched_at = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
        snap = _plaintext_raw(
            t212_raw_snapshot(fernet_key=fernet_key, fetched_at=fetched_at),
            fernet_key,
        )
        evt = _plaintext_raw(
            t212_raw_events(fernet_key=fernet_key, fetched_at=fetched_at),
            fernet_key,
        )

        raw_path = run_module.get_raw_path("trading212")
        assert ingest_raw(snap, raw_path, fernet_key, "trading212") is None
        assert ingest_raw(evt, raw_path, fernet_key, "trading212") is None

        connector = get("trading212")
        rc = transform_connector(connector, fernet_key)
        assert rc == 0
        read_snap = self._read_normalized("snapshot")
        read_events = self._read_normalized("events")

        expected_snap = t212_normalized_snapshot(
            fernet_key=fernet_key, fetched_at=fetched_at
        )
        assert _decrypted_columns(read_snap, fernet_key) == _decrypted_columns(
            expected_snap, fernet_key
        )
        assert read_events.num_rows == 1


class TestFetchConnectorEventsBranch:
    """fetch_connector returns a FetchResult; the events branch still writes."""

    def test_events_fetch_branch_writes_events(
        self, tmp_data_dir: Path, fernet_key: bytes
    ) -> None:
        """A successful events fetch reaches ingest_raw (the events branch).

        Guards the events branch of fetch_connector: deleting the events
        fetch call must fail this test (verification-gap finding — every
        older fetch test patched ``fetch_events_kwargs`` to ``{}``, so the
        branch was never exercised).
        """
        connector = get("trading212")
        with (
            patch(
                "pipeline.raw.ingest.ingest_raw",
                return_value=_raw_one_row(fernet_key),
            ) as mock_ingest,
            patch.object(
                connector,
                "fetch_kwargs",
                return_value=[{"api_key": "k", "api_secret": "s", "base_url": "u"}],
            ),
            patch.object(
                connector, "fetch_snapshot", return_value=MagicMock(num_rows=1)
            ),
            patch.object(
                connector,
                "fetch_events_kwargs",
                return_value={"api_key": "k", "api_secret": "s", "base_url": "u"},
            ),
            patch.object(connector, "fetch_events", return_value=MagicMock(num_rows=1)),
        ):
            rc = fetch_connector(connector, MagicMock(), fernet_key)
        assert rc == FetchResult.SUCCESS
        # ingest_raw called once for the snapshot batch and once for events.
        assert mock_ingest.call_count == 2


class TestIngestRawReturnsNone:
    """ingest_raw returns None; the merge-on-key write is the only contract."""

    @staticmethod
    def _raw_table(fernet_key: bytes) -> pa.Table:
        now = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
        payloads = [b'{"summary": 1}', b'{"positions": []}']
        return pa.table(
            {
                "fetched_at": [now, now],
                "broker": ["Trading 212", "Trading 212"],
                "source": ["/equity/account/summary", "/equity/positions"],
                "payload": [encrypt(p, fernet_key) for p in payloads],
                "payload_hash": [hashlib.sha256(p).hexdigest() for p in payloads],
                "account_id": [None, None],
            },
            schema=RAW_SCHEMA,
        )

    def test_second_ingest_does_not_grow_table(
        self, tmp_path: Path, tmp_data_dir: Path, fernet_key: bytes
    ) -> None:
        table_path = str(tmp_path / "raw" / "trading212")
        raw = self._raw_table(fernet_key)
        assert ingest_raw(raw, table_path, fernet_key, "trading212") is None

        # Identical re-fetch: the merge matches both rows in place (no net
        # growth) — the current fetch is written, nothing is returned.
        assert ingest_raw(raw, table_path, fernet_key, "trading212") is None
        dt = DeltaTable(table_path, storage_options=get_storage().storage_options)
        assert dt.to_pyarrow_table().num_rows == 2  # merged in place, no growth
