"""CAP-3 regression guards for the single-bronze-per-broker raw (PR #150).

Each broker's raw payloads — snapshot and events alike — land in ONE Delta
table ``raw/{broker}`` (alias ``{broker}_raw``) discriminated by ``source``.
These tests prove the routing contract (AC-3, AD-2/AD-3/AD-4): the snapshot
transform sees only snapshot rows, the events transform sees only events rows,
``filter_latest_snapshot`` stays keyed per distinct ``source`` (never a global
max), and both silver tables per broker are unchanged in schema and contents
when fed from a merged raw table.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pyarrow as pa

from pipeline.connectors.ibkr.transform import (
    transform_events as ibkr_transform_events,
)
from pipeline.connectors.ibkr.transform import (
    transform_snapshot as ibkr_transform_snapshot,
)
from pipeline.connectors.trading212.transform import (
    transform_events as t212_transform_events,
)
from pipeline.connectors.trading212.transform import (
    transform_snapshot as t212_transform_snapshot,
)
from pipeline.connectors.transform_utils import filter_latest_snapshot
from pipeline.connectors.xtb.transform import (
    transform_events as xtb_transform_events,
)
from pipeline.connectors.xtb.transform import (
    transform_snapshot as xtb_transform_snapshot,
)
from pipeline.crypto import decrypt, encrypt, generate_key
from pipeline.normalized.models import (
    events_normalized_schema,
    snapshot_normalized_schema,
)
from pipeline.raw.models import RAW_SCHEMA
from tests.fixtures.ibkr import ibkr_raw_events, ibkr_raw_merged, ibkr_raw_positions
from tests.fixtures.trading212 import (
    t212_raw_events,
    t212_raw_merged,
    t212_raw_snapshot,
)
from tests.fixtures.xtb import xtb_raw_snapshot


def _decrypted_columns(table: pa.Table, fernet_key: bytes) -> dict[str, list]:
    """Column values with Fernet columns decrypted to their plaintext.

    Fernet ciphertext is randomized per ``encrypt`` call, so two transforms of
    the same payloads produce different binary bytes; the equivalence checks
    below therefore compare decrypted contents, never raw ciphertext.
    """
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


def _assert_equivalent(actual: pa.Table, expected: pa.Table, fernet_key: bytes) -> None:
    assert actual.schema.equals(expected.schema)
    assert actual.num_rows == expected.num_rows
    assert _decrypted_columns(actual, fernet_key) == _decrypted_columns(
        expected, fernet_key
    )


# ---------------------------------------------------------------------------
# T5.4 mirror: shared-bronze end state — no per-role ``*_events`` raw table
# ---------------------------------------------------------------------------


class TestSharedBronzeNoEventsRaw:
    """Mirror of ``test_xtb_connector.py::test_shared_bronze_no_xtb_events_raw``
    for ibkr/trading212: both fetch kinds live in one ``raw/{broker}`` table
    (alias ``{broker}_raw``); no ``*_events`` raw table is needed.
    """

    def test_ibkr_merged_raw_carries_both_fetch_kinds(self) -> None:
        merged = ibkr_raw_merged(fernet_key=generate_key())
        assert merged.schema.equals(RAW_SCHEMA)
        assert set(merged.column("source").to_pylist()) == {"flex", "flex_events"}

    def test_trading212_merged_raw_carries_both_fetch_kinds(self) -> None:
        fetched_at = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
        merged = t212_raw_merged(fernet_key=generate_key(), fetched_at=fetched_at)
        assert merged.schema.equals(RAW_SCHEMA)
        assert set(merged.column("source").to_pylist()) == {
            "/equity/account/summary",
            "/equity/positions",
            "/equity/metadata/instruments",
            "/equity/history/orders",
        }


# ---------------------------------------------------------------------------
# T6.1 — Trading 212 positions payloads can never reach the events silver
# ---------------------------------------------------------------------------


class TestTrading212PositionsNeverReachEventsSilver:
    """T6.1 (AC-3): a merged ``raw/trading212`` holding a ``/equity/positions``
    row produces NO rows in ``transform_events`` — positions list payloads
    provably cannot reach the events silver (prefix-anchored gates, AD-2).
    """

    def test_snapshot_only_raw_produces_no_events(self, fernet_key: bytes) -> None:
        fetched_at = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
        snapshot = t212_raw_snapshot(fernet_key=fernet_key, fetched_at=fetched_at)
        # The fixture carries /equity/account/summary, /equity/positions and
        # the legacy /equity/metadata/instruments row — none match an events
        # request path, so transform_events must emit an empty table.
        result = t212_transform_events(snapshot, fernet_key)
        assert result.num_rows == 0

    def test_merged_table_emits_only_the_events_rows(self, fernet_key: bytes) -> None:
        fetched_at = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
        snapshot = t212_raw_snapshot(fernet_key=fernet_key, fetched_at=fetched_at)
        events = t212_raw_events(fernet_key=fernet_key, fetched_at=fetched_at)
        merged = pa.concat_tables([snapshot, events], schema=RAW_SCHEMA)
        result = t212_transform_events(merged, fernet_key)
        # The positions/summary rows contribute nothing: only the order row.
        assert result.num_rows == 1
        assert result.column("event_type").to_pylist() == ["TRADE"]
        assert result.column("raw_event_type").to_pylist() == ["ORDER"]


# ---------------------------------------------------------------------------
# T6.2 — IBKR flex / flex_events in the same merged raw route correctly
# ---------------------------------------------------------------------------


class TestIbkrMergedRoutesToRightSilver:
    """T6.2 (AC-3): ``flex`` and ``flex_events`` in the SAME merged ``raw/ibkr``
    table route to the right silver — snapshot emits no event, events emits no
    snapshot."""

    def test_merged_snapshot_and_events_match_split_inputs(
        self, fernet_key: bytes
    ) -> None:
        snapshot_only = ibkr_raw_positions(fernet_key=fernet_key)
        events_only = ibkr_raw_events(fernet_key=fernet_key)
        merged = pa.concat_tables([snapshot_only, events_only], schema=RAW_SCHEMA)

        # snapshot transform: the flex_events row is gated out (source != "flex").
        _assert_equivalent(
            ibkr_transform_snapshot(merged, fernet_key),
            ibkr_transform_snapshot(snapshot_only, fernet_key),
            fernet_key,
        )
        # events transform: the flex row is gated out (source != "flex_events").
        _assert_equivalent(
            ibkr_transform_events(merged, fernet_key),
            ibkr_transform_events(events_only, fernet_key),
            fernet_key,
        )


# ---------------------------------------------------------------------------
# T6.3 — filter_latest_snapshot stays keyed per distinct source
# ---------------------------------------------------------------------------


class TestFilterLatestSnapshotPerSourceKeyedness:
    """T6.3 (AD-4 / AC-3): dedup is per distinct ``source``, never a global
    max — two rows with DIFFERENT sources but different ``fetched_at`` both
    survive."""

    @staticmethod
    def _raw_row(source: str, fetched_at: datetime, fernet_key: bytes) -> pa.Table:
        payload = b'{"k": 1}'
        return pa.table(
            {
                "fetched_at": [fetched_at],
                "broker": ["Trading 212"],
                "source": [source],
                "payload": [encrypt(payload, fernet_key)],
                "payload_hash": [hashlib.sha256(payload).hexdigest()],
                "source_file": [""],
            },
            schema=RAW_SCHEMA,
        )

    def test_different_sources_with_different_fetched_at_both_survive(
        self, fernet_key: bytes
    ) -> None:
        t1 = datetime(2026, 8, 1, 6, 0, tzinfo=UTC)
        t2 = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
        raw = pa.concat_tables(
            [
                self._raw_row("/equity/account/summary", t1, fernet_key),
                self._raw_row("/equity/positions", t2, fernet_key),
            ],
            schema=RAW_SCHEMA,
        )
        result = filter_latest_snapshot(raw)
        assert result.num_rows == 2
        assert sorted(result.column("source").to_pylist()) == [
            "/equity/account/summary",
            "/equity/positions",
        ]

    def test_same_source_keeps_only_latest_fetched_at(self, fernet_key: bytes) -> None:
        t1 = datetime(2026, 8, 1, 6, 0, tzinfo=UTC)
        t2 = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
        raw = pa.concat_tables(
            [
                self._raw_row("/equity/account/summary", t1, fernet_key),
                self._raw_row("/equity/account/summary", t2, fernet_key),
            ],
            schema=RAW_SCHEMA,
        )
        result = filter_latest_snapshot(raw)
        assert result.num_rows == 1
        assert result.column("fetched_at").to_pylist() == [t2]


# ---------------------------------------------------------------------------
# T6.4 — both silver tables per broker unchanged after the merge
# ---------------------------------------------------------------------------


class TestSilverTablesUnchangedAfterMerge:
    """T6.4 (AC-3): feeding a merged raw table (holding both fetch kinds) to
    the existing snapshot/events transforms yields silver tables identical in
    schema and contents to the split-table baselines."""

    def test_ibkr_silver_identical_from_merged_raw(self, fernet_key: bytes) -> None:
        snapshot_only = ibkr_raw_positions(fernet_key=fernet_key)
        events_only = ibkr_raw_events(fernet_key=fernet_key)
        merged = pa.concat_tables([snapshot_only, events_only], schema=RAW_SCHEMA)
        merged_snap = ibkr_transform_snapshot(merged, fernet_key)
        merged_events = ibkr_transform_events(merged, fernet_key)
        assert merged_snap.schema.equals(snapshot_normalized_schema)
        # IBKR events keep their existing transform output shape
        # (large_string/large_binary); the merge must not alter it.
        _assert_equivalent(
            merged_snap,
            ibkr_transform_snapshot(snapshot_only, fernet_key),
            fernet_key,
        )
        _assert_equivalent(
            merged_events,
            ibkr_transform_events(events_only, fernet_key),
            fernet_key,
        )

    def test_trading212_silver_identical_from_merged_raw(
        self, fernet_key: bytes
    ) -> None:
        fetched_at = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)
        snapshot_only = t212_raw_snapshot(fernet_key=fernet_key, fetched_at=fetched_at)
        events_only = t212_raw_events(fernet_key=fernet_key, fetched_at=fetched_at)
        merged = t212_raw_merged(fernet_key=fernet_key, fetched_at=fetched_at)
        merged_snap = t212_transform_snapshot(merged, fernet_key)
        merged_events = t212_transform_events(merged, fernet_key)
        assert merged_snap.schema.equals(snapshot_normalized_schema)
        assert merged_events.schema == events_normalized_schema
        _assert_equivalent(
            merged_snap,
            t212_transform_snapshot(snapshot_only, fernet_key),
            fernet_key,
        )
        _assert_equivalent(
            merged_events,
            t212_transform_events(events_only, fernet_key),
            fernet_key,
        )

    def test_xtb_shared_bronze_feeds_both_silvers_unchanged(
        self, fernet_key: bytes
    ) -> None:
        raw = xtb_raw_snapshot(fernet_key=fernet_key)
        snap = xtb_transform_snapshot(raw, fernet_key)
        ev = xtb_transform_events(raw, fernet_key)
        assert snap.schema.equals(snapshot_normalized_schema)
        assert ev.schema == events_normalized_schema
        # Fixture defaults: 2 EQUITY aggregates + 1 CASH; 6 cash-ledger events.
        assert snap.num_rows == 3
        assert ev.num_rows == 6
