"""Story 5-2: write-time merge-on-key retention + per-run VACUUM (AD-1, AD-3).

Exercises the raw merge write (AC-1..AC-4) and the per-run vacuum (AC-5)
against REAL local Delta tables in ``tmp_path`` — deltalake and pyarrow are
never mocked. Groups: T4.1 merge semantics, T4.2 NULL-key append bound (AC-3),
T4.3 Trading 212 pagination endpoint-base keying (AC-4), T4.4 vacuum
``dry_run=False`` physical removal, T4.5 ``SELECT DISTINCT source`` per-broker
regression guard.
"""

from __future__ import annotations

import hashlib
import json
import pathlib
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable

from pipeline.raw.ingest import ingest_raw
from pipeline.raw.models import RAW_SCHEMA
from pipeline.raw.retention import (
    merge_predicate,
    retention_key,
    retention_value,
    vacuum_raw,
)
from pipeline.storage import get_storage

_BASE_TS = datetime(2026, 8, 3, 6, 0, tzinfo=UTC)


def _row(
    source: str,
    payload: bytes,
    *,
    account_id: str | None = None,
    fetched_at: datetime = _BASE_TS,
    broker: str = "Trading 212",
) -> dict[str, Any]:
    """One RAW_SCHEMA row spec; ``payload_hash`` is the plaintext digest."""
    return {
        "fetched_at": fetched_at,
        "broker": broker,
        "source": source,
        "payload": payload,
        "payload_hash": hashlib.sha256(payload).hexdigest(),
        "account_id": account_id,
    }


def _raw_table(*rows: dict[str, Any]) -> pa.Table:
    return pa.table(
        {
            "fetched_at": [r["fetched_at"] for r in rows],
            "broker": [r["broker"] for r in rows],
            "source": [r["source"] for r in rows],
            "payload": [r["payload"] for r in rows],
            "payload_hash": [r["payload_hash"] for r in rows],
            "account_id": [r["account_id"] for r in rows],
        },
        schema=RAW_SCHEMA,
    )


def _read(table_path: str) -> pa.Table:
    return DeltaTable(
        table_path, storage_options=get_storage().storage_options
    ).to_pyarrow_table()


def _by_source(table: pa.Table) -> dict[str, str]:
    """Map stored ``source`` -> stored ``payload_hash`` (identifies the row)."""
    return dict(
        zip(
            table.column("source").to_pylist(),
            table.column("payload_hash").to_pylist(),
        )
    )


def _age_tombstones(table_dir: pathlib.Path, days: int = 8) -> None:
    """Age every tombstone in the local Delta log past the 7-day default.

    Rewrites each ``_delta_log`` commit's ``remove`` actions in place so the
    VACUUM retention check sees them as older than the default threshold —
    the real deltalake vacuum code path, no clock mocking.
    """
    cutoff_ms = int((datetime.now(UTC) - timedelta(days=days)).timestamp() * 1000)
    for commit in sorted((table_dir / "_delta_log").glob("*.json")):
        lines = []
        for raw in commit.read_text().splitlines():
            raw = raw.strip()
            if '"remove"' in raw:
                obj = json.loads(raw)
                obj["remove"]["deletionTimestamp"] = cutoff_ms
                raw = json.dumps(obj)
            lines.append(raw)
        commit.write_text("\n".join(lines) + "\n")


class TestRetentionPolicy:
    """The retention key mapping is the single source of truth (AD-1)."""

    def test_retention_keys_per_broker(self) -> None:
        assert retention_key("xtb") == "account_id"
        assert retention_key("trading212") == "source"
        assert retention_key("ibkr") == "source"

    def test_trading212_key_is_pagination_stripped(self) -> None:
        assert (
            retention_value("trading212", "/equity/history/orders?cursor=abc")
            == "/equity/history/orders"
        )
        assert retention_value("trading212", "/equity/positions") == "/equity/positions"
        assert retention_value("ibkr", "/flex/query") == "/flex/query"

    def test_merge_predicates(self) -> None:
        assert (
            merge_predicate("trading212")
            == "split_part(s.source, '?', 1) = split_part(t.source, '?', 1)"
        )
        assert merge_predicate("xtb") == "s.account_id = t.account_id"
        assert merge_predicate("ibkr") == "s.source = t.source"


class TestMergeSemantics:
    """AC-1/AC-7: replace on key, insert new, leave absent keys untouched."""

    def test_merge_replaces_inserts_and_leaves_absent_keys(
        self, tmp_path, tmp_data_dir, fernet_key
    ) -> None:
        table_path = str(tmp_path / "raw" / "trading212")
        first = _raw_table(
            _row("/equity/account/summary", b"s1"),
            _row("/equity/positions", b"p1"),
        )
        ingest_raw(first, table_path, fernet_key, "trading212")

        second = _raw_table(
            _row("/equity/account/summary", b"s2"),
            _row("/equity/history/orders", b"o1"),
        )
        ingest_raw(second, table_path, fernet_key, "trading212")

        rows = _by_source(_read(table_path))
        # The re-fetched endpoint was replaced, the absent key untouched, the
        # new key inserted — no row growth, no orphan rows.
        assert set(rows) == {
            "/equity/account/summary",
            "/equity/positions",
            "/equity/history/orders",
        }
        assert rows["/equity/account/summary"] == hashlib.sha256(b"s2").hexdigest()
        assert rows["/equity/positions"] == hashlib.sha256(b"p1").hexdigest()
        assert rows["/equity/history/orders"] == hashlib.sha256(b"o1").hexdigest()

    def test_same_batch_twice_is_noop(self, tmp_path, tmp_data_dir, fernet_key) -> None:
        table_path = str(tmp_path / "raw" / "ibkr")
        batch = _raw_table(_row("/flex/query", b"x"), _row("/flex/query2", b"y"))
        ingest_raw(batch, table_path, fernet_key, "ibkr")
        ingest_raw(batch, table_path, fernet_key, "ibkr")
        rows = _by_source(_read(table_path))
        assert len(rows) == 2
        assert rows["/flex/query"] == hashlib.sha256(b"x").hexdigest()
        assert rows["/flex/query2"] == hashlib.sha256(b"y").hexdigest()

    def test_batch_with_duplicate_key_keeps_newest_fetch(
        self, tmp_path, tmp_data_dir, fernet_key
    ) -> None:
        """F1.2: a batch carrying the same key twice is resolved in-batch.

        The row with the latest ``fetched_at`` wins; an older snapshot of the
        same key cannot be the merge's insert payload.
        """
        table_path = str(tmp_path / "raw" / "trading212")
        batch = _raw_table(
            _row("/equity/history/orders", b"stale", fetched_at=_BASE_TS),
            _row(
                "/equity/history/orders",
                b"fresh",
                fetched_at=_BASE_TS.replace(hour=7),
            ),
        )
        ingest_raw(batch, table_path, fernet_key, "trading212")
        rows = _by_source(_read(table_path))
        assert rows == {"/equity/history/orders": hashlib.sha256(b"fresh").hexdigest()}


class TestNullKeyAppendBound:
    """AC-3: NULL retention keys are appended, never merged."""

    def test_null_key_row_is_appended_every_run(
        self, tmp_path, tmp_data_dir, fernet_key
    ) -> None:
        table_path = str(tmp_path / "raw" / "xtb")
        row = _row("XTB_REPORT", b"rpt", account_id=None, broker="XTB")
        ingest_raw(_raw_table(row), table_path, fernet_key, "xtb")
        ingest_raw(_raw_table(row), table_path, fernet_key, "xtb")
        # Two runs, two rows: the second was appended, never merged away.
        assert _read(table_path).num_rows == 2

    def test_in_batch_dedup_limits_null_key_growth(
        self, tmp_path, tmp_data_dir, fernet_key
    ) -> None:
        table_path = str(tmp_path / "raw" / "xtb")
        batch = _raw_table(
            *(
                _row("XTB_REPORT", b"rpt", account_id=None, broker="XTB")
                for _ in range(3)
            )
        )
        ingest_raw(batch, table_path, fernet_key, "xtb")
        # One distinct (source, payload_hash) among three identical null-key
        # rows -> one row lands, not three.
        assert _read(table_path).num_rows == 1

    def test_null_key_and_keyed_rows_split_in_one_batch(
        self, tmp_path, tmp_data_dir, fernet_key
    ) -> None:
        table_path = str(tmp_path / "raw" / "xtb")
        batch = _raw_table(
            _row("XTB_REPORT", b"acct", account_id="123", broker="XTB"),
            _row("XTB_REPORT", b"noauth", account_id=None, broker="XTB"),
        )
        ingest_raw(batch, table_path, fernet_key, "xtb")
        assert _read(table_path).num_rows == 2
        ingest_raw(batch, table_path, fernet_key, "xtb")
        # The keyed row merged in place; the null-key row appended again.
        assert _read(table_path).num_rows == 3


class TestPaginationEndpointKeying:
    """AC-4: paginated T212 pages merge onto the endpoint base, not the cursor."""

    def test_pages_collapse_to_one_endpoint_row(
        self, tmp_path, tmp_data_dir, fernet_key
    ) -> None:
        table_path = str(tmp_path / "raw" / "trading212")
        run1 = _raw_table(
            _row("/equity/history/orders", b'{"items":[1]}'),
            _row("/equity/history/orders?cursor=abc", b'{"items":[2]}'),
        )
        ingest_raw(run1, table_path, fernet_key, "trading212")
        rows = _by_source(_read(table_path))
        assert len(rows) == 1  # pages merged onto the endpoint base
        # The final page's row is kept; its suffixed source is the stored one.
        assert (
            rows["/equity/history/orders?cursor=abc"]
            == hashlib.sha256(b'{"items":[2]}').hexdigest()
        )

        # A different cursor token next run still lands on the SAME row.
        run2 = _raw_table(
            _row("/equity/history/orders", b'{"items":[1]}'),
            _row("/equity/history/orders?cursor=def", b'{"items":[3]}'),
        )
        ingest_raw(run2, table_path, fernet_key, "trading212")
        rows = _by_source(_read(table_path))
        assert len(rows) == 1
        assert (
            rows["/equity/history/orders?cursor=def"]
            == hashlib.sha256(b'{"items":[3]}').hexdigest()
        )

        # The endpoint absent from the batch: its stored row stays untouched.
        ingest_raw(
            _raw_table(_row("/equity/positions", b"p1")),
            table_path,
            fernet_key,
            "trading212",
        )
        rows = _by_source(_read(table_path))
        assert (
            rows["/equity/history/orders?cursor=def"]
            == hashlib.sha256(b'{"items":[3]}').hexdigest()
        )
        assert rows["/equity/positions"] == hashlib.sha256(b"p1").hexdigest()


class TestVacuum:
    """AC-5/AC-7: the per-run vacuum physically removes aged tombstones."""

    def test_vacuum_noop_when_table_absent(self, tmp_path) -> None:
        vacuum_raw(str(tmp_path / "raw" / "does-not-exist"))  # no raise

    def test_dry_run_lists_but_only_dry_run_false_removes(
        self, tmp_path, tmp_data_dir, fernet_key
    ) -> None:
        table_dir = tmp_path / "raw" / "xtb"
        table_path = str(table_dir)
        ingest_raw(
            _raw_table(_row("XTB_REPORT", b"v1", account_id="123", broker="XTB")),
            table_path,
            fernet_key,
            "xtb",
        )
        ingest_raw(
            _raw_table(_row("XTB_REPORT", b"v2", account_id="123", broker="XTB")),
            table_path,
            fernet_key,
            "xtb",
        )
        files_before = sorted(p.name for p in table_dir.glob("*.parquet"))
        assert len(files_before) == 2  # v1's file was tombstoned by the merge

        _age_tombstones(table_dir)

        # deltalake 1.6.0 vacuums dry_run=True by default: it lists the aged
        # file but leaves it on disk.
        listed = DeltaTable(table_path).vacuum(dry_run=True)
        assert listed, "the aged tombstone must be listed for removal"
        assert sorted(p.name for p in table_dir.glob("*.parquet")) == files_before

        vacuum_raw(table_path)  # dry_run=False — physically removes
        files_after = sorted(p.name for p in table_dir.glob("*.parquet"))
        assert set(files_after) < set(files_before)
        assert len(files_after) == 1
        rows = _by_source(_read(table_path))
        assert rows["XTB_REPORT"] == hashlib.sha256(b"v2").hexdigest()


class TestDistinctSourceGuard:
    """AC-7: SELECT DISTINCT source per broker is unchanged by re-fetches."""

    def test_ibkr_sources_stable_across_refetch(
        self, tmp_path, tmp_data_dir, fernet_key
    ) -> None:
        table_path = str(tmp_path / "raw" / "ibkr")
        run1 = _raw_table(_row("/flex/query", b"r1"), _row("/flex/query2", b"r2"))
        ingest_raw(run1, table_path, fernet_key, "ibkr")
        assert set(_by_source(_read(table_path))) == {"/flex/query", "/flex/query2"}

        run2 = _raw_table(_row("/flex/query", b"r1b"), _row("/flex/query2", b"r2b"))
        ingest_raw(run2, table_path, fernet_key, "ibkr")
        rows = _by_source(_read(table_path))
        # Same distinct vocabulary; each source replaced in place, no orphans.
        assert set(rows) == {"/flex/query", "/flex/query2"}
        assert rows["/flex/query"] == hashlib.sha256(b"r1b").hexdigest()
        assert rows["/flex/query2"] == hashlib.sha256(b"r2b").hexdigest()
