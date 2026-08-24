"""Tests for the encrypted in-memory fetch handoff (issue #154).

``fetch_connector`` builds an encrypted pre-dedup handoff
(``{"snapshot": ..., "events": ...}``) for connectors that declare
``handoff_supported``; ``transform_connector`` uses it when present so the
transform never re-reads the accumulated raw table, and falls back to the
Delta read otherwise. Covers: the declared capability, output-identical
handoff vs table-read (golden), unchanged-endpoint pre-dedup semantics,
empty-fetch skip, missing-layer fallback, ``cmd_run_connector`` threading,
and the ``ingest_raw``/``dedup_raw`` contract changes.
"""

from __future__ import annotations

import argparse
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
from pipeline.crypto import decrypt, encrypt, generate_key
from pipeline.raw.ingest import dedup_raw, ingest_raw
from pipeline.raw.models import RAW_SCHEMA
from pipeline.run import (
    FetchResult,
    cmd_run_connector,
    fetch_connector,
    transform_connector,
)
from pipeline.storage import get_storage
from tests.fixtures.trading212 import (
    t212_normalized_snapshot,
    t212_raw_events,
    t212_raw_snapshot,
)


class TestHandoffCapability:
    """handoff_supported is a declared per-connector capability (issue #154)."""

    def test_trading212_declares_handoff_supported(self) -> None:
        assert getattr(get("trading212"), "handoff_supported", False) is True

    def test_ibkr_and_xtb_keep_the_default(self) -> None:
        assert getattr(get("ibkr"), "handoff_supported", False) is False
        assert getattr(get("xtb"), "handoff_supported", False) is False


class TestFetchConnectorHandoff:
    """fetch_connector builds the handoff only for declared connectors."""

    @patch("pipeline.raw.ingest.ingest_raw", return_value=MagicMock(num_rows=1))
    def test_builds_handoff_for_supported_connector(
        self, mock_ingest: MagicMock, tmp_data_dir: Path
    ) -> None:
        connector = get("trading212")
        with (
            patch.object(
                connector,
                "fetch_kwargs",
                return_value=[{"api_key": "k", "api_secret": "s", "base_url": "u"}],
            ),
            patch.object(
                connector, "fetch_snapshot", return_value=MagicMock(num_rows=1)
            ),
            patch.object(connector, "fetch_events_kwargs", return_value={}),
        ):
            fernet_key = generate_key()
            rc, handoff = fetch_connector(connector, MagicMock(), fernet_key)
            assert rc == FetchResult.SUCCESS
            assert handoff is not None
            assert set(handoff) == {"snapshot"}
            assert handoff["snapshot"].num_rows == 1

    @patch("pipeline.raw.ingest.ingest_raw", return_value=MagicMock(num_rows=1))
    def test_no_handoff_for_non_supported_connector(
        self, mock_ingest: MagicMock, tmp_data_dir: Path
    ) -> None:
        connector = get("ibkr")
        with (
            patch.object(
                connector,
                "fetch_kwargs",
                return_value=[
                    {"flex_token": "t", "flex_query_id": "q", "flex_base_url": "u"}
                ],
            ),
            patch.object(
                connector, "fetch_snapshot", return_value=MagicMock(num_rows=1)
            ),
            patch.object(connector, "fetch_events_kwargs", return_value={}),
        ):
            fernet_key = generate_key()
            rc, handoff = fetch_connector(connector, argparse.Namespace(), fernet_key)
            assert rc == FetchResult.SUCCESS
            assert handoff is None

    def test_events_fetch_populates_handoff_events(
        self, tmp_data_dir: Path, fernet_key: bytes
    ) -> None:
        """A successful events fetch lands in handoff['events'].

        Guards the events branch of fetch_connector: deleting
        ``handoff['events'] = ...`` must fail this test (verification-gap
        finding — every older fetch test patched ``fetch_events_kwargs`` to
        ``{}``, so the branch was never exercised).
        """
        connector = get("trading212")
        with (
            patch(
                "pipeline.raw.ingest.ingest_raw",
                return_value=_raw_one_row(fernet_key),
            ),
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
            rc, handoff = fetch_connector(connector, MagicMock(), fernet_key)
        assert rc == FetchResult.SUCCESS
        assert handoff is not None
        assert set(handoff) == {"snapshot", "events"}
        assert handoff["events"].num_rows == 1

    def test_multibatch_handoff_concatenates_snapshot(
        self, tmp_data_dir: Path, fernet_key: bytes
    ) -> None:
        """Multiple fetch_kwargs batches concatenate, not last-wins.

        The Protocol documents ``fetch_kwargs`` returns one or more batches;
        the handoff must carry ALL of them or the transform would drop data.
        """
        connector = get("trading212")
        with (
            patch(
                "pipeline.raw.ingest.ingest_raw",
                return_value=_raw_one_row(fernet_key),
            ),
            patch.object(
                connector,
                "fetch_kwargs",
                return_value=[
                    {"api_key": "k", "api_secret": "s", "base_url": "u"},
                    {"api_key": "k", "api_secret": "s", "base_url": "u"},
                ],
            ),
            patch.object(
                connector, "fetch_snapshot", return_value=MagicMock(num_rows=1)
            ),
            patch.object(connector, "fetch_events_kwargs", return_value={}),
        ):
            rc, handoff = fetch_connector(connector, MagicMock(), fernet_key)
        assert rc == FetchResult.SUCCESS
        assert handoff is not None
        assert set(handoff) == {"snapshot"}
        assert handoff["snapshot"].num_rows == 2  # concatenated, not last batch


def _raw_one_row(fernet_key: bytes) -> pa.Table:
    """One RAW_SCHEMA row so ``pa.concat_tables`` works on handoff layers."""
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


class TestTransformConnectorHandoff:
    """transform_connector uses the in-memory handoff when present (issue #154)."""

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

    def test_handoff_output_identical_to_table_read(
        self, tmp_data_dir: Path, fernet_key: bytes
    ) -> None:
        fetched_at = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
        snap = t212_raw_snapshot(fernet_key=fernet_key, fetched_at=fetched_at)
        evt = t212_raw_events(fernet_key=fernet_key, fetched_at=fetched_at)
        self._write_merged_raw(pa.concat_tables([snap, evt], schema=RAW_SCHEMA))

        connector = get("trading212")
        rc = transform_connector(
            connector, fernet_key, raw_tables={"snapshot": snap, "events": evt}
        )
        assert rc == 0
        handoff_snap = self._read_normalized("snapshot")
        handoff_events = self._read_normalized("events")

        rc = transform_connector(connector, fernet_key)  # table-read fallback
        assert rc == 0
        read_snap = self._read_normalized("snapshot")
        read_events = self._read_normalized("events")

        # Golden: handoff output matches the round-trip-verified fixture AND
        # the accumulated-table-read output (decrypted contents — Fernet
        # ciphertext is randomized per encrypt).
        expected_snap = t212_normalized_snapshot(
            fernet_key=fernet_key, fetched_at=fetched_at
        )
        assert handoff_snap.num_rows == 3
        assert handoff_events.num_rows == 1
        assert _decrypted_columns(handoff_snap, fernet_key) == _decrypted_columns(
            expected_snap, fernet_key
        )
        assert _decrypted_columns(handoff_snap, fernet_key) == _decrypted_columns(
            read_snap, fernet_key
        )
        assert _decrypted_columns(handoff_events, fernet_key) == _decrypted_columns(
            read_events, fernet_key
        )

    def test_empty_handoff_skips_without_rewriting(
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
        empty = empty_arrow_table(RAW_SCHEMA)
        with caplog.at_level(logging.WARNING, logger="pipeline.run"):
            rc = transform_connector(
                get("trading212"),
                fernet_key,
                raw_tables={"snapshot": empty, "events": empty},
            )
        assert rc == 0
        assert any("current fetch is empty" in r.message for r in caplog.records)
        # 0-row handoff skips: the pre-existing normalized table is not
        # rewritten (today's accumulated-table behavior).
        read_back = DeltaTable(
            norm_path, storage_options=get_storage().storage_options
        ).to_pyarrow_table()
        assert read_back.num_rows == 3

    def test_missing_handoff_layer_falls_back_to_table_read(
        self, tmp_data_dir: Path, fernet_key: bytes
    ) -> None:
        fetched_at = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
        snap = t212_raw_snapshot(fernet_key=fernet_key, fetched_at=fetched_at)
        evt = t212_raw_events(fernet_key=fernet_key, fetched_at=fetched_at)
        self._write_merged_raw(pa.concat_tables([snap, evt], schema=RAW_SCHEMA))

        # Handoff covers only the snapshot layer; events fall back to the table.
        rc = transform_connector(
            get("trading212"), fernet_key, raw_tables={"snapshot": snap}
        )
        assert rc == 0
        events = self._read_normalized("events")
        assert events.num_rows == 1
        assert events.column("event_type").to_pylist() == ["TRADE"]

    def test_handoff_path_does_not_open_delta_table(
        self, tmp_data_dir: Path, fernet_key: bytes
    ) -> None:
        """Regression guard: the handoff path never reads the accumulated raw table.

        transform_connector imports ``DeltaTable`` from ``deltalake`` at call
        time, so patching ``deltalake.DeltaTable`` intercepts the table-read
        fallback. When both handoff layers are present, the accumulated
        ``raw/trading212`` table must never be opened — that is the whole
        point of the memory fix (issue #154). The events write now opens the
        normalized events target for its MERGE (AD-4); that is a silver
        write, not a bronze re-read, and is allowed.
        """
        fetched_at = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
        snap = t212_raw_snapshot(fernet_key=fernet_key, fetched_at=fetched_at)
        evt = t212_raw_events(fernet_key=fernet_key, fetched_at=fetched_at)
        self._write_merged_raw(pa.concat_tables([snap, evt], schema=RAW_SCHEMA))
        connector = get("trading212")
        with patch("deltalake.DeltaTable") as mock_dt:
            rc = transform_connector(
                connector, fernet_key, raw_tables={"snapshot": snap, "events": evt}
            )
        assert rc == 0
        raw_path = run_module.get_raw_path("trading212")
        opened_paths = [call.args[0] for call in mock_dt.call_args_list]
        assert opened_paths, "the events MERGE opens its normalized target"
        assert all(path != raw_path for path in opened_paths)

    def test_real_ingest_raw_result_threaded_to_transform(
        self, tmp_path: Path, tmp_data_dir: Path, fernet_key: bytes
    ) -> None:
        """A REAL ingest_raw pre-dedup result, threaded as the handoff, yields the
        same normalized output as the table read (end-to-end threading test).
        """
        fetched_at = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
        snap = t212_raw_snapshot(fernet_key=fernet_key, fetched_at=fetched_at)
        evt = t212_raw_events(fernet_key=fernet_key, fetched_at=fetched_at)

        # Thread real ingest_raw results (encrypted pre-dedup current fetch)
        # through the fetch→transform boundary, as cmd_run_connector does.
        raw_path = run_module.get_raw_path("trading212")
        handoff = {
            "snapshot": ingest_raw(snap, raw_path, fernet_key),
            "events": ingest_raw(evt, raw_path, fernet_key),
        }
        connector = get("trading212")
        rc = transform_connector(connector, fernet_key, raw_tables=handoff)
        assert rc == 0
        handoff_snap = self._read_normalized("snapshot")
        handoff_events = self._read_normalized("events")

        rc = transform_connector(connector, fernet_key)  # table-read fallback
        assert rc == 0
        read_snap = self._read_normalized("snapshot")
        read_events = self._read_normalized("events")

        assert _decrypted_columns(handoff_snap, fernet_key) == _decrypted_columns(
            read_snap, fernet_key
        )
        assert _decrypted_columns(handoff_events, fernet_key) == _decrypted_columns(
            read_events, fernet_key
        )


class TestCmdRunConnectorThreadsHandoff:
    """cmd_run_connector passes fetch_connector's handoff to transform_connector."""

    @pytest.mark.usefixtures("docker_mode")
    @patch("pipeline.run.run_validation", return_value=0)
    @patch("pipeline.run.transform_connector", return_value=0)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_handoff_threaded_to_transform(
        self,
        mock_key: MagicMock,
        mock_transform: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        handoff = {"snapshot": MagicMock(), "events": MagicMock()}
        with patch(
            "pipeline.run.fetch_connector",
            return_value=(FetchResult.SUCCESS, handoff),
        ) as mock_fetch:
            rc = cmd_run_connector(argparse.Namespace(connector="trading212"))
        assert rc == 0
        mock_fetch.assert_called_once()
        mock_transform.assert_called_once_with(
            get("trading212"), b"test-key", raw_tables=handoff
        )


class TestIngestRawReturnsPreDedupHandoff:
    """ingest_raw returns the current PRE-DEDUP encrypted fetch (issue #154)."""

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

    def test_second_ingest_returns_pre_dedup_fetch(
        self, tmp_path: Path, tmp_data_dir: Path, fernet_key: bytes
    ) -> None:
        table_path = str(tmp_path / "raw" / "trading212")
        raw = self._raw_table(fernet_key)
        first = ingest_raw(raw, table_path, fernet_key)
        assert first.num_rows == 2

        # Identical re-fetch: all rows are deduped out of the write, but the
        # returned pre-dedup handoff still carries the current fetch — an
        # unchanged endpoint (deduped away) must still reach the transform.
        second = ingest_raw(raw, table_path, fernet_key)
        assert second.num_rows == 2
        dt = DeltaTable(table_path, storage_options=get_storage().storage_options)
        assert dt.to_pyarrow_table().num_rows == 2  # only the first run's rows


class TestDedupRawProjected:
    """dedup_raw's projected key read ignores the table's broker column."""

    @staticmethod
    def _raw_table(rows: list[tuple[str, str]], fernet_key: bytes) -> pa.Table:
        now = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
        payloads = [b"{}" for _ in rows]
        return pa.table(
            {
                "fetched_at": [now] * len(rows),
                "broker": [broker for broker, _ in rows],
                "source": [source for _, source in rows],
                "payload": [encrypt(p, fernet_key) for p in payloads],
                "payload_hash": [hashlib.sha256(p).hexdigest() for p in payloads],
                "account_id": [None] * len(rows),
            },
            schema=RAW_SCHEMA,
        )

    def test_projected_read_dedups_identically(
        self, tmp_path: Path, tmp_data_dir: Path, fernet_key: bytes
    ) -> None:
        table_path = str(tmp_path / "raw" / "dedup_broker")
        existing = self._raw_table(
            [("B", "/equity/account/summary"), ("B", "/equity/positions")],
            fernet_key,
        )
        get_storage().backend.ensure_parent(table_path)
        write_deltalake(
            table_path,
            existing,
            mode="append",
            storage_options=get_storage().storage_options,
        )
        new = self._raw_table(
            [
                ("C", "/equity/account/summary"),  # source/hash duplicate → dropped
                ("B", "/equity/positions"),  # duplicate → dropped
                ("B", "/equity/history/orders"),  # new → kept
            ],
            fernet_key,
        )
        deduped = dedup_raw(new, table_path)
        assert deduped.num_rows == 1
        assert deduped.column("source").to_pylist() == ["/equity/history/orders"]
