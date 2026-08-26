"""Tests for the single-bronze migration (merge raw per-layer tables).

Covers the three unit-testable entry points:

- :func:`merge_broker` against local temp Delta tables (``storage_opts={}``;
  deltalake accepts local paths with empty storage options, mirroring the
  ``tests/test_migrate_cdc_events_drop_gross_amount.py`` pattern).  Real
  deltalake/polars are used -- nothing is mocked.
- :func:`delete_broker_sources` against an in-memory :class:`FakeS3` client
  double (no AWS calls), mirroring the ``tests/test_migrate_cdc_to_events.py``
  pattern for S3-level operations.
- :func:`run_migration` wiring: ``_FakeBackend``/``_fake_config`` with
  ``use_storage(...)`` and fake credential storage options so the boto3
  default-credentials chain is skipped.

The per-layer raw names (``{broker}_snapshot``, ``{broker}_events``, including
the orphaned ``xtb_events``) are the migration's historical inputs and are
exempt from the story's grep bars.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

import pipeline.migrations.migrate_single_bronze as mod
from pipeline.migrations.migrate_raw_account_id import _OLD_RAW_SCHEMA
from pipeline.migrations.migrate_single_bronze import (
    delete_broker_sources,
    merge_broker,
)
from pipeline.raw.models import RAW_SCHEMA
from pipeline.storage import StorageConfig, use_storage

_TS = pa.timestamp("us", tz="UTC")
_BROKERS = ("ibkr", "trading212", "xtb")


def _t(hour: int) -> datetime:
    """A deterministic UTC timestamp for a given hour on 2024-01-01."""
    return datetime(2024, 1, 1, hour, tzinfo=UTC)


def _raw_table(rows: list[dict[str, Any]]) -> pa.Table:
    """Build a RAW_SCHEMA raw table from a list of row dicts."""
    table = pa.table(
        {
            "fetched_at": pa.array([r["fetched_at"] for r in rows], type=_TS),
            "broker": pa.array([r["broker"] for r in rows], type=pa.string()),
            "source": pa.array([r["source"] for r in rows], type=pa.string()),
            "payload": pa.array(
                [r.get("payload", b"\x01") for r in rows], type=pa.binary()
            ),
            "payload_hash": pa.array(
                [r["payload_hash"] for r in rows], type=pa.string()
            ),
            "account_id": pa.array(
                [r.get("account_id") for r in rows], type=pa.string()
            ),
        }
    )
    return table.cast(RAW_SCHEMA)


def _legacy_raw_table(rows: list[dict[str, Any]]) -> pa.Table:
    """Build a pre-5-1 raw table (``source_file`` instead of ``account_id``)."""
    table = pa.table(
        {
            "fetched_at": pa.array([r["fetched_at"] for r in rows], type=_TS),
            "broker": pa.array([r["broker"] for r in rows], type=pa.string()),
            "source": pa.array([r["source"] for r in rows], type=pa.string()),
            "payload": pa.array(
                [r.get("payload", b"\x01") for r in rows], type=pa.binary()
            ),
            "payload_hash": pa.array(
                [r["payload_hash"] for r in rows], type=pa.string()
            ),
            "source_file": pa.array(
                [r.get("source_file") for r in rows], type=pa.string()
            ),
        }
    )
    return table.cast(_OLD_RAW_SCHEMA)


def _write(path: Path, table: pa.Table) -> None:
    write_deltalake(str(path), table, mode="overwrite", schema_mode="overwrite")


def _read(path: str | Path) -> pa.Table:
    return DeltaTable(str(path)).to_pyarrow_table()


def _table_exists(path: str | Path) -> bool:
    try:
        DeltaTable(str(path))
        return True
    except Exception:
        return False


def _broker_paths(tmp_path: Path, broker: str) -> tuple[str, str, str]:
    return (
        str(tmp_path / f"{broker}_snapshot"),
        str(tmp_path / f"{broker}_events"),
        str(tmp_path / broker),
    )


# ---------------------------------------------------------------------------
# merge_broker (local temp Delta tables)
# ---------------------------------------------------------------------------


def test_merge_combines_snapshot_and_events_into_one_table(tmp_path: Path) -> None:
    snap, events, dest = _broker_paths(tmp_path, "ibkr")
    _write(
        Path(snap),
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h1",
                    "account_id": "snap",
                }
            ]
        ),
    )
    _write(
        Path(events),
        _raw_table(
            [
                {
                    "fetched_at": _t(2),
                    "broker": "IBKR",
                    "source": "flex_events",
                    "payload_hash": "h2",
                    "account_id": "events",
                }
            ]
        ),
    )

    report = merge_broker("ibkr", (snap, events), dest, {})

    assert report.verified is True
    assert report.written is True
    result = _read(dest)
    assert result.num_rows == 2
    # Order-sensitive schema equality (mirrors quality.check_schema).
    assert result.schema.equals(RAW_SCHEMA)
    assert sorted(result.column("source").to_pylist()) == ["flex", "flex_events"]


def test_merge_dedups_on_dedup_key(tmp_path: Path) -> None:
    snap, events, dest = _broker_paths(tmp_path, "trading212")
    # Same (broker, source, payload_hash) appears in both sources; the events
    # source also carries a distinct row.
    _write(
        Path(snap),
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "Trading 212",
                    "source": "/equity/account/summary",
                    "payload_hash": "same",
                    "account_id": "a",
                }
            ]
        ),
    )
    _write(
        Path(events),
        _raw_table(
            [
                {
                    "fetched_at": _t(2),
                    "broker": "Trading 212",
                    "source": "/equity/account/summary",
                    "payload_hash": "same",
                    "account_id": "b",
                },
                {
                    "fetched_at": _t(3),
                    "broker": "Trading 212",
                    "source": "/equity/history/orders",
                    "payload_hash": "h2",
                    "account_id": "c",
                },
            ]
        ),
    )

    report = merge_broker("trading212", (snap, events), dest, {})

    assert report.merged_rows == 2
    result = _read(dest)
    assert result.num_rows == 2
    assert sorted(result.column("payload_hash").to_pylist()) == ["h2", "same"]


def test_merge_latest_fetched_at_tie_break_preserves_account_id(
    tmp_path: Path,
) -> None:
    snap, events, dest = _broker_paths(tmp_path, "xtb")
    _write(
        Path(snap),
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "XTB",
                    "source": "XTB_REPORT",
                    "payload_hash": "same",
                    "account_id": "acc-old",
                }
            ]
        ),
    )
    _write(
        Path(events),
        _raw_table(
            [
                {
                    "fetched_at": _t(4),
                    "broker": "XTB",
                    "source": "XTB_REPORT",
                    "payload_hash": "same",
                    "account_id": "acc-new",
                }
            ]
        ),
    )

    report = merge_broker("xtb", (snap, events), dest, {})

    assert report.merged_rows == 1
    row = _read(dest).to_pylist()[0]
    # Latest fetched_at wins and carries its account_id (ADR 0108 D18).
    assert row["fetched_at"] == _t(4)
    assert row["account_id"] == "acc-new"
    assert row["source"] == "XTB_REPORT"


def test_merge_absent_sources_exit_zero(tmp_path: Path) -> None:
    _, _, dest = _broker_paths(tmp_path, "xtb")
    missing = (str(tmp_path / "xtb_snapshot"), str(tmp_path / "xtb_events"))

    report = merge_broker("xtb", missing, dest, {})

    assert report.verified is False
    assert report.written is False
    assert _table_exists(dest) is False


def test_merge_absent_sources_with_existing_destination_is_noop(tmp_path: Path) -> None:
    snap, events, dest = _broker_paths(tmp_path, "ibkr")
    _write(
        Path(dest),
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h1",
                    "account_id": "snap",
                }
            ]
        ),
    )

    report = merge_broker("ibkr", (snap, events), dest, {})

    # Already migrated: sources gone, merged table present with exact schema.
    assert report.verified is True
    assert report.written is False
    assert _read(dest).num_rows == 1


def test_merge_idempotent_rerun_reproduces_destination(tmp_path: Path) -> None:
    snap, events, dest = _broker_paths(tmp_path, "trading212")
    _write(
        Path(snap),
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "Trading 212",
                    "source": "/equity/account/summary",
                    "payload_hash": "h1",
                    "account_id": "a",
                }
            ]
        ),
    )
    _write(
        Path(events),
        _raw_table(
            [
                {
                    "fetched_at": _t(2),
                    "broker": "Trading 212",
                    "source": "/equity/history/orders",
                    "payload_hash": "h2",
                    "account_id": "b",
                }
            ]
        ),
    )

    merge_broker("trading212", (snap, events), dest, {})
    first = _read(dest)
    # Interrupted-run recovery: sources still present, destination overwritten
    # deterministically from the same dedup key.
    report = merge_broker("trading212", (snap, events), dest, {})

    assert report.verified is True
    second = _read(dest)
    assert second.num_rows == first.num_rows == 2
    assert second.to_pydict() == first.to_pydict()


def test_merge_refuses_to_overwrite_destination_with_extra_rows(
    tmp_path: Path,
) -> None:
    snap, events, dest = _broker_paths(tmp_path, "ibkr")
    _write(
        Path(snap),
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h1",
                    "account_id": "snap",
                }
            ]
        ),
    )
    _write(
        Path(events),
        _raw_table(
            [
                {
                    "fetched_at": _t(2),
                    "broker": "IBKR",
                    "source": "flex_events",
                    "payload_hash": "h2",
                    "account_id": "events",
                }
            ]
        ),
    )
    # The destination already holds a row the sources no longer have -- e.g. a
    # fetch appended it after the renamed-path code deployed while a partial
    # source deletion left the per-layer tables behind.
    _write(
        Path(dest),
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h1",
                    "account_id": "snap",
                },
                {
                    "fetched_at": _t(5),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h3",
                    "account_id": "new-fetch",
                },
            ]
        ),
    )

    with pytest.raises(RuntimeError, match="Conflict"):
        merge_broker("ibkr", (snap, events), dest, {})

    # The destination was not clobbered: the appended row survived.
    assert sorted(_read(dest).column("payload_hash").to_pylist()) == ["h1", "h3"]


def test_merge_destination_subset_of_sources_reruns(tmp_path: Path) -> None:
    snap, events, dest = _broker_paths(tmp_path, "trading212")
    _write(
        Path(snap),
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "Trading 212",
                    "source": "/equity/account/summary",
                    "payload_hash": "h1",
                    "account_id": "a",
                }
            ]
        ),
    )
    _write(
        Path(events),
        _raw_table(
            [
                {
                    "fetched_at": _t(2),
                    "broker": "Trading 212",
                    "source": "/equity/history/orders",
                    "payload_hash": "h2",
                    "account_id": "b",
                }
            ]
        ),
    )
    # An interrupted run left only part of the merged rows behind; the
    # destination is a subset of the sources, so the re-run proceeds.
    _write(
        Path(dest),
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "Trading 212",
                    "source": "/equity/account/summary",
                    "payload_hash": "h1",
                    "account_id": "a",
                }
            ]
        ),
    )

    report = merge_broker("trading212", (snap, events), dest, {})

    assert report.verified is True
    assert sorted(_read(dest).column("payload_hash").to_pylist()) == ["h1", "h2"]


def test_merge_dry_run_does_not_write(tmp_path: Path) -> None:
    snap, events, dest = _broker_paths(tmp_path, "ibkr")
    _write(
        Path(snap),
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h1",
                    "account_id": "snap",
                }
            ]
        ),
    )
    _write(
        Path(events),
        _raw_table(
            [
                {
                    "fetched_at": _t(2),
                    "broker": "IBKR",
                    "source": "flex_events",
                    "payload_hash": "h2",
                    "account_id": "events",
                }
            ]
        ),
    )

    report = merge_broker("ibkr", (snap, events), dest, {}, dry_run=True)

    assert report.dry_run is True
    assert report.verified is False
    assert _table_exists(dest) is False


def test_merge_destination_schema_conflict_raises(tmp_path: Path) -> None:
    snap, events, dest = _broker_paths(tmp_path, "ibkr")
    _write(
        Path(snap),
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h1",
                    "account_id": "snap",
                }
            ]
        ),
    )
    bad = _raw_table(
        [
            {
                "fetched_at": _t(1),
                "broker": "IBKR",
                "source": "flex",
                "payload_hash": "h1",
                "account_id": "snap",
            }
        ]
    ).append_column("unexpected", pa.array(["x"], type=pa.string()))
    _write(Path(dest), bad)

    with pytest.raises(RuntimeError, match="Conflict"):
        merge_broker("ibkr", (snap, events), dest, {})

    # The conflicting destination was not clobbered.
    assert "unexpected" in _read(dest).column_names


def test_merge_legacy_schema_sources_are_verified(tmp_path: Path) -> None:
    """Prod's per-layer tables predate 5-1: merging them as-is is valid.

    The merged table keeps the legacy ``source_file`` layout; the follow-up
    ``migrate_raw_account_id`` run rewrites it to ``RAW_SCHEMA``.
    """
    snap, events, dest = _broker_paths(tmp_path, "xtb")
    _write(
        Path(snap),
        _legacy_raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "XTB",
                    "source": "XTB_REPORT",
                    "payload_hash": "h1",
                    "source_file": "PLN_123_20240101_20240201.xlsx",
                }
            ]
        ),
    )

    report = merge_broker("xtb", (snap, events), dest, {})

    assert report.verified is True
    assert "source_file" in _read(dest).column_names
    assert "account_id" not in _read(dest).column_names


def test_merge_destination_with_legacy_schema_is_not_a_conflict(
    tmp_path: Path,
) -> None:
    snap, events, dest = _broker_paths(tmp_path, "ibkr")
    row = {
        "fetched_at": _t(1),
        "broker": "IBKR",
        "source": "flex",
        "payload_hash": "h1",
        "source_file": "q.xml",
    }
    _write(Path(snap), _legacy_raw_table([row]))
    _write(Path(dest), _legacy_raw_table([row]))

    report = merge_broker("ibkr", (snap, events), dest, {})

    assert report.verified is True


def test_verify_merged_table_rejects_unknown_schema(tmp_path: Path) -> None:
    dest = str(tmp_path / "ibkr")
    bad = _raw_table(
        [
            {
                "fetched_at": _t(1),
                "broker": "IBKR",
                "source": "flex",
                "payload_hash": "h1",
            }
        ]
    ).append_column("unexpected", pa.array(["x"], type=pa.string()))
    _write(Path(dest), bad)

    assert mod.verify_merged_table(dest, {}, expected_rows=1) is False


def test_merge_verification_failure_refuses_delete(monkeypatch, tmp_path: Path) -> None:
    snap, events, dest = _broker_paths(tmp_path, "ibkr")
    _write(
        Path(snap),
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h1",
                    "account_id": "snap",
                }
            ]
        ),
    )
    _write(
        Path(events),
        _raw_table(
            [
                {
                    "fetched_at": _t(2),
                    "broker": "IBKR",
                    "source": "flex_events",
                    "payload_hash": "h2",
                    "account_id": "events",
                }
            ]
        ),
    )
    monkeypatch.setattr(mod, "verify_merged_table", lambda *a, **k: False)

    with pytest.raises(RuntimeError, match="(?i)refusing to delete"):
        merge_broker("ibkr", (snap, events), dest, {})

    # Source tables untouched: no partial deletion.
    assert _read(snap).num_rows == 1
    assert _read(events).num_rows == 1


def test_merge_s3_destination_requires_client(monkeypatch, tmp_path: Path) -> None:
    snap, events, _ = _broker_paths(tmp_path, "ibkr")
    s3_dest = "s3://test-bucket/raw/ibkr"
    monkeypatch.setattr(mod, "_path_exists", lambda path, opts: False)
    _write(
        Path(snap),
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h1",
                }
            ]
        ),
    )

    with pytest.raises(RuntimeError, match="(?i)s3 client is required"):
        merge_broker("ibkr", (snap, events), s3_dest, {})


def test_merge_s3_destination_uses_staged_upload(monkeypatch, tmp_path: Path) -> None:
    """S3 destinations go through the staged boto3 rewrite (delta-rs' S3
    uploader aborts sustained uploads after a 180s retry budget), carrying
    the destination's live files as stale paths and continuing its version
    history; a fresh destination starts at version 0 with nothing stale."""
    import polars as pl

    snap, events, _ = _broker_paths(tmp_path, "ibkr")
    for hour, source, payload_hash in ((1, "flex", "h1"), (2, "flex_events", "h2")):
        _write(
            Path(snap if source == "flex" else events),
            _raw_table(
                [
                    {
                        "fetched_at": _t(hour),
                        "broker": "IBKR",
                        "source": source,
                        "payload_hash": payload_hash,
                    }
                ]
            ),
        )

    recorded: dict[str, object] = {}
    sentinel_client = object()
    monkeypatch.setattr(mod, "_path_exists", lambda path, opts: False)

    def _fake_rewrite(
        client: object,
        table_path: str,
        new_frame: pl.DataFrame,
        storage_opts: dict[str, str],
        *,
        next_version: int,
        stale_paths: list[str],
    ) -> None:
        recorded["client"] = client
        recorded["table_path"] = table_path
        recorded["rows"] = new_frame.height
        recorded["next_version"] = next_version
        recorded["stale_paths"] = stale_paths

    monkeypatch.setattr(mod, "rewrite_table", _fake_rewrite)
    # The remote write is faked; verification must not hit S3.
    monkeypatch.setattr(mod, "verify_merged_table", lambda *a, **k: True)

    report = merge_broker(
        "ibkr", (snap, events), "s3://test-bucket/raw/ibkr", {}, client=sentinel_client
    )

    assert report.verified is True
    assert recorded["client"] is sentinel_client
    assert recorded["table_path"] == "s3://test-bucket/raw/ibkr"
    assert recorded["rows"] == 2
    assert recorded["next_version"] == 0
    assert recorded["stale_paths"] == []


def test_merge_s3_existing_destination_carries_stale_paths_and_version(
    monkeypatch, tmp_path: Path
) -> None:
    """Re-running against an existing S3 destination removes its live files
    in the rebuilt commit and continues from its current version."""
    import polars as pl

    class _FakeDestDeltaTable:
        def to_pyarrow_table(self) -> pa.Table:
            return _raw_table([]).slice(0, 0)

        def get_add_actions(self, flatten: bool) -> Any:
            assert flatten is True
            return pa.table({"path": ["old-a.parquet", "old-b.parquet"]})

        def version(self) -> int:
            return 4

    monkeypatch.setattr(
        mod, "_path_exists", lambda path, opts: path.startswith("s3://")
    )
    monkeypatch.setattr(mod, "DeltaTable", lambda *a, **k: _FakeDestDeltaTable())
    snap, events, _ = _broker_paths(tmp_path, "ibkr")
    _write(
        Path(snap),
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h1",
                }
            ]
        ),
    )

    recorded: dict[str, object] = {}

    def _fake_rewrite(
        client: object,
        table_path: str,
        new_frame: pl.DataFrame,
        storage_opts: dict[str, str],
        *,
        next_version: int,
        stale_paths: list[str],
    ) -> None:
        recorded["next_version"] = next_version
        recorded["stale_paths"] = stale_paths

    monkeypatch.setattr(mod, "rewrite_table", _fake_rewrite)

    merge_broker(
        "ibkr",
        (snap, events),
        "s3://test-bucket/raw/ibkr",
        {},
        client=object(),
    )

    assert recorded["next_version"] == 5
    assert recorded["stale_paths"] == ["old-a.parquet", "old-b.parquet"]


def test_merge_propagates_non_notfound_errors(monkeypatch, tmp_path: Path) -> None:
    """An auth/region/permission error opening an existing table is NOT
    swallowed as 'absent' (only TableNotFoundError is) -- it propagates so
    main() exits non-zero, the signal a pre-deploy gate needs."""

    def _boom(*args: object, **kwargs: object) -> object:
        raise OSError("simulated S3 auth/region error")

    monkeypatch.setattr(mod, "DeltaTable", _boom)
    snap, events, dest = _broker_paths(tmp_path, "ibkr")

    with pytest.raises(OSError, match="simulated S3 auth/region error"):
        merge_broker("ibkr", (snap, events), dest, {})


# ---------------------------------------------------------------------------
# delete_broker_sources (FakeS3)
# ---------------------------------------------------------------------------


class FakeS3:
    """In-memory S3 double implementing only the methods the delete path uses."""

    def __init__(
        self, objects: dict[str, bytes], fail_keys: set[str] | None = None
    ) -> None:
        self.objects: dict[str, bytes] = objects
        # Keys that delete_objects reports as failed (kept in objects).
        self.fail_keys: set[str] = fail_keys or set()

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        prefix = str(kwargs.get("Prefix", ""))
        contents = [
            {"Key": key, "Size": len(body)}
            for key, body in sorted(self.objects.items())
            if key.startswith(prefix)
        ]
        return {"KeyCount": len(contents), "Contents": contents, "IsTruncated": False}

    def delete_objects(self, **kwargs: object) -> dict[str, object]:
        delete = kwargs["Delete"]
        assert isinstance(delete, dict)
        objects = delete["Objects"]
        assert isinstance(objects, list)
        deleted: list[dict[str, str]] = []
        errors: list[dict[str, str]] = []
        for item in objects:
            key = item["Key"]
            if key in self.fail_keys:
                errors.append(
                    {
                        "Key": key,
                        "Code": "AccessDenied",
                        "Message": "simulated failure",
                    }
                )
                continue
            self.objects.pop(key, None)
            deleted.append({"Key": key})
        return {"Deleted": deleted, "Errors": errors}


def _source_table_objects(broker: str) -> dict[str, bytes]:
    return {
        f"raw/{broker}_snapshot/_delta_log/00000000000000000000.json": b"log",
        f"raw/{broker}_snapshot/part-0.parquet": b"ciphertext",
        f"raw/{broker}_events/_delta_log/00000000000000000000.json": b"log",
        f"raw/{broker}_events/part-0.parquet": b"ciphertext",
    }


def test_delete_removes_per_layer_source_tables() -> None:
    client = FakeS3(_source_table_objects("ibkr"))

    report = delete_broker_sources(client, "test-bucket", "", "ibkr")

    assert report.deleted == 4
    assert client.objects == {}


def test_delete_purges_orphan_xtb_events() -> None:
    # raw/xtb_events was never written but may exist; it is removed too.
    client = FakeS3(
        {
            "raw/xtb_snapshot/_delta_log/00000000000000000000.json": b"log",
            "raw/xtb_snapshot/part-0.parquet": b"ciphertext",
            "raw/xtb_events/_delta_log/00000000000000000000.json": b"log",
        }
    )

    report = delete_broker_sources(client, "test-bucket", "", "xtb")

    assert report.deleted == 3
    assert client.objects == {}


def test_delete_absent_prefix_is_noop() -> None:
    client = FakeS3({})

    report = delete_broker_sources(client, "test-bucket", "", "ibkr")

    assert report.deleted == 0
    assert client.objects == {}


def test_delete_dry_run_does_not_delete() -> None:
    objects = _source_table_objects("ibkr")
    client = FakeS3(dict(objects))

    report = delete_broker_sources(client, "test-bucket", "", "ibkr", dry_run=True)

    assert report.deleted == 4
    assert client.objects == objects


def test_delete_raises_on_partial_errors() -> None:
    # One key fails (throttling/permission); the rest of the batch succeeds.
    client = FakeS3(
        _source_table_objects("ibkr"),
        fail_keys={"raw/ibkr_snapshot/part-0.parquet"},
    )

    with pytest.raises(RuntimeError, match="delete_objects failed"):
        delete_broker_sources(client, "test-bucket", "", "ibkr")

    # Only the failed object remains; the rest of the batch stayed deleted.
    assert client.objects == {"raw/ibkr_snapshot/part-0.parquet": b"ciphertext"}


# ---------------------------------------------------------------------------
# run_migration (S3 delete plan wired to the storage config)
# ---------------------------------------------------------------------------


class _FakeBackend:
    """Minimal S3-like backend double for StorageConfig injection."""

    def __init__(self, bucket: str = "test-bucket", prefix: str = "") -> None:
        self.bucket = bucket
        self.prefix = prefix

    def table_path(self, layer: str, table_name: str) -> str:
        base = f"{self.bucket}/{self.prefix}" if self.prefix else self.bucket
        return f"s3://{base}/{layer}/{table_name}"

    def staging_path(self, segment: str, filename: str) -> str:
        base = f"{self.bucket}/{self.prefix}" if self.prefix else self.bucket
        return f"s3://{base}/{segment}/{filename}"

    def ensure_parent(self, table_path: str) -> None:
        pass

    @property
    def storage_options(self) -> dict[str, str]:
        # Present access-key entries so get_storage_options_with_credentials
        # skips the boto3 default-credentials chain (no AWS in tests).
        return {"aws_access_key_id": "fake", "aws_secret_access_key": "fake"}


def _fake_config(tmp_path: Path) -> StorageConfig:
    return StorageConfig(
        data_dir="s3://test-bucket",
        raw_dir="s3://test-bucket/raw",
        normalized_dir="s3://test-bucket/normalized",
        analytics_dir="s3://test-bucket/analytics",
        secrets_dir=str(tmp_path / ".secrets"),
        encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
        backend=_FakeBackend(),
    )


def _staging_objects() -> dict[str, bytes]:
    objects: dict[str, bytes] = {}
    for broker in _BROKERS:
        objects.update(_source_table_objects(broker))
    return objects


def _verified_merge(
    broker: str,
    source_paths: tuple[str, ...],
    dest_path: str,
    storage_opts: dict[str, str],
    *,
    dry_run: bool = False,
) -> mod.MergeReport:
    return mod.MergeReport(
        broker=broker,
        dest_path=dest_path,
        present_sources=tuple(source_paths),
        merged_rows=2,
        verified=True,
        written=not dry_run,
        dry_run=dry_run,
    )


def test_run_migration_merges_and_deletes_sources(monkeypatch, tmp_path: Path) -> None:
    use_storage(_fake_config(tmp_path))
    client = FakeS3(_staging_objects())
    captured: list[tuple[str, tuple[str, ...], str]] = []

    def _fake_merge(
        broker: str,
        source_paths: tuple[str, ...],
        dest_path: str,
        storage_opts: dict[str, str],
        *,
        client: Any = None,
        dry_run: bool = False,
    ) -> mod.MergeReport:
        captured.append((broker, source_paths, dest_path))
        return _verified_merge(broker, source_paths, dest_path, storage_opts)

    monkeypatch.setattr(mod, "merge_broker", _fake_merge)

    reports = mod.run_migration(client)

    # Source paths are the per-layer tables; destination is raw/{broker}.
    assert captured[0] == (
        "ibkr",
        ("s3://test-bucket/raw/ibkr_snapshot", "s3://test-bucket/raw/ibkr_events"),
        "s3://test-bucket/raw/ibkr",
    )
    # Every broker reports a verified merge.
    assert [r.broker for r in reports] == list(_BROKERS)
    assert all(r.verified for r in reports)
    # Every per-layer source object was deleted (including orphan xtb_events).
    assert client.objects == {}


def test_run_migration_absent_sources_exit_zero(monkeypatch, tmp_path: Path) -> None:
    use_storage(_fake_config(tmp_path))
    client = FakeS3({})
    monkeypatch.setattr(
        mod,
        "merge_broker",
        lambda broker, source_paths, dest_path, storage_opts, **k: mod.MergeReport(
            broker=broker, dest_path=dest_path
        ),
    )

    reports = mod.run_migration(client)  # must not raise

    assert client.objects == {}
    assert all(not r.verified for r in reports)


def test_run_migration_dry_run_writes_nothing(monkeypatch, tmp_path: Path) -> None:
    use_storage(_fake_config(tmp_path))
    objects = _staging_objects()
    client = FakeS3(dict(objects))
    monkeypatch.setattr(
        mod,
        "merge_broker",
        lambda broker, source_paths, dest_path, storage_opts, **k: _verified_merge(
            broker, source_paths, dest_path, storage_opts, dry_run=True
        ),
    )

    mod.run_migration(client, dry_run=True)

    assert client.objects == objects


def test_run_migration_refuses_delete_when_not_verified(
    monkeypatch, tmp_path: Path
) -> None:
    use_storage(_fake_config(tmp_path))
    objects = _staging_objects()
    client = FakeS3(dict(objects))

    def _unverified(
        broker: str,
        source_paths: tuple[str, ...],
        dest_path: str,
        storage_opts: dict[str, str],
        **k: object,
    ) -> mod.MergeReport:
        return mod.MergeReport(
            broker=broker,
            dest_path=dest_path,
            present_sources=tuple(source_paths),
            verified=False,
        )

    monkeypatch.setattr(mod, "merge_broker", _unverified)

    with pytest.raises(RuntimeError, match="(?i)refusing to delete"):
        mod.run_migration(client)

    # Nothing was deleted.
    assert client.objects == objects
