"""Tests for the retention-key raw-table prune migration (deploy gate).

The prune applies ``pipeline.raw.ingest._dedup_by_retention_key`` -- the exact
decision a successful fetch batch makes -- to an accumulated ``raw/{broker}``
table, so the first post-deploy fetch merges a small target instead of
OOM-killing a 512 MB Fargate task. Fixtures carry current ``RAW_SCHEMA``
tables (the prune's inputs), built with real deltalake/polars over local temp
paths -- nothing mocked except storage wiring (the
``test_migrate_raw_account_id.py`` pattern).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

import pipeline.migrations.prune_raw_retention as mod
from pipeline.migrations.migrate_raw_account_id import _BROKERS
from pipeline.migrations.prune_raw_retention import prune_broker
from pipeline.raw.models import RAW_SCHEMA
from pipeline.storage import StorageConfig, use_storage

_TS = pa.timestamp("us", tz="UTC")


def _t(hour: int) -> datetime:
    """A deterministic UTC timestamp for a given hour on 2024-01-01."""
    return datetime(2024, 1, 1, hour, tzinfo=UTC)


def _raw_table(rows: list[dict[str, Any]]) -> pa.Table:
    """Build a current-schema raw table from row dicts."""
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


def _write(path: Path, table: pa.Table) -> None:
    write_deltalake(str(path), table, mode="overwrite", schema_mode="overwrite")


def _read(path: str | Path) -> pa.Table:
    return DeltaTable(str(path)).to_pyarrow_table()


def test_prune_trading212_collapses_pages_to_endpoint_base(tmp_path: Path) -> None:
    """Pages sharing an endpoint base (pre-fetch-strip ``?cursor=...`` values)
    collapse to the newest row per base; other endpoints survive."""
    path = tmp_path / "trading212"
    _write(
        path,
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "Trading 212",
                    "source": "/equity/account/summary",
                    "payload_hash": "h-old",
                },
                {
                    "fetched_at": _t(3),
                    "broker": "Trading 212",
                    "source": "/equity/account/summary?cursor=abc",
                    "payload_hash": "h-page",
                },
                {
                    "fetched_at": _t(3),
                    "broker": "Trading 212",
                    "source": "/equity/account/cash?cursor=def",
                    "payload_hash": "h-cash",
                },
                {
                    "fetched_at": _t(2),
                    "broker": "Trading 212",
                    "source": "/equity/instruments",
                    "payload_hash": "h-instruments",
                },
            ]
        ),
    )

    report = prune_broker("trading212", str(path), {})

    assert report.pruned is True
    assert (report.rows_before, report.rows_after) == (4, 3)
    assert report.written and report.verified
    result = _read(path)
    assert result.num_rows == 3
    assert sorted(result.column("payload_hash").to_pylist()) == [
        "h-cash",
        "h-instruments",
        "h-page",
    ]


def test_prune_keeps_last_row_in_order_on_fetched_at_tie(tmp_path: Path) -> None:
    """Same fetched_at + same key -> the endpoint's final page wins (AC-4):
    the LAST row in batch order."""
    path = tmp_path / "trading212"
    _write(
        path,
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "Trading 212",
                    "source": "/equity/account/summary?page=1",
                    "payload_hash": "h-first",
                },
                {
                    "fetched_at": _t(1),
                    "broker": "Trading 212",
                    "source": "/equity/account/summary?page=2",
                    "payload_hash": "h-last",
                },
            ]
        ),
    )

    report = prune_broker("trading212", str(path), {})

    assert (report.rows_before, report.rows_after) == (2, 1)
    result = _read(path)
    assert result.column("payload_hash").to_pylist() == ["h-last"]


def test_prune_xtb_keys_on_account_id(tmp_path: Path) -> None:
    path = tmp_path / "xtb"
    _write(
        path,
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "XTB",
                    "source": "XTB_REPORT",
                    "payload_hash": "h-a1-old",
                    "account_id": "A1",
                },
                {
                    "fetched_at": _t(2),
                    "broker": "XTB",
                    "source": "XTB_REPORT",
                    "payload_hash": "h-a1-new",
                    "account_id": "A1",
                },
                {
                    "fetched_at": _t(1),
                    "broker": "XTB",
                    "source": "XTB_REPORT",
                    "payload_hash": "h-a2",
                    "account_id": "A2",
                },
            ]
        ),
    )

    report = prune_broker("xtb", str(path), {})

    assert (report.rows_before, report.rows_after) == (3, 2)
    result = _read(path)
    # Newest per account survives; A1's stale row is gone.
    by_account = dict(
        zip(
            result.column("account_id").to_pylist(),
            result.column("payload_hash").to_pylist(),
        )
    )
    assert by_account == {"A1": "h-a1-new", "A2": "h-a2"}


def test_prune_ibkr_keys_on_raw_source_value(tmp_path: Path) -> None:
    """IBKR does not strip pagination: identical source strings collapse,
    different query strings are distinct keys."""
    path = tmp_path / "ibkr"
    _write(
        path,
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex?token=one",
                    "payload_hash": "h-1a",
                },
                {
                    "fetched_at": _t(2),
                    "broker": "IBKR",
                    "source": "flex?token=one",
                    "payload_hash": "h-1b",
                },
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex?token=two",
                    "payload_hash": "h-2",
                },
            ]
        ),
    )

    report = prune_broker("ibkr", str(path), {})

    assert (report.rows_before, report.rows_after) == (3, 2)
    assert sorted(_read(path).column("payload_hash").to_pylist()) == ["h-1b", "h-2"]


def test_prune_already_pruned_is_a_noop(tmp_path: Path) -> None:
    path = tmp_path / "xtb"
    _write(
        path,
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "XTB",
                    "source": "XTB_REPORT",
                    "payload_hash": "h-a1",
                    "account_id": "A1",
                },
                {
                    "fetched_at": _t(1),
                    "broker": "XTB",
                    "source": "XTB_REPORT",
                    "payload_hash": "h-a2",
                    "account_id": "A2",
                },
            ]
        ),
    )
    version_before = DeltaTable(str(path)).version()

    report = prune_broker("xtb", str(path), {})

    assert report.pruned is False
    assert report.written is False
    assert DeltaTable(str(path)).version() == version_before


def test_prune_is_idempotent_across_runs(tmp_path: Path) -> None:
    path = tmp_path / "ibkr"
    _write(
        path,
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h-a",
                },
                {
                    "fetched_at": _t(2),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h-b",
                },
            ]
        ),
    )

    first = prune_broker("ibkr", str(path), {})
    second = prune_broker("ibkr", str(path), {})

    assert first.pruned and first.written
    assert second.pruned is False
    assert second.written is False
    assert second.rows_after == first.rows_after


def test_prune_absent_table_is_skipped(tmp_path: Path) -> None:
    report = prune_broker("ibkr", str(tmp_path / "does_not_exist"), {})

    assert report.pruned is False
    assert report.written is False


def test_prune_conflict_raises_instead_of_clobbering(tmp_path: Path) -> None:
    """A table not readable as RAW_SCHEMA raises and is left untouched."""
    path = tmp_path / "ibkr"
    drifted = _raw_table(
        [
            {
                "fetched_at": _t(1),
                "broker": "IBKR",
                "source": "flex",
                "payload_hash": "h-1",
            }
        ]
    ).append_column("unexpected", pa.array(["x"], type=pa.string()))
    _write(path, drifted)

    with pytest.raises(RuntimeError, match="Conflict"):
        prune_broker("ibkr", str(path), {})

    assert "unexpected" in _read(path).column_names


def test_prune_dry_run_writes_nothing(tmp_path: Path) -> None:
    path = tmp_path / "trading212"
    _write(
        path,
        _raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "Trading 212",
                    "source": f"/equity/account/summary?page={page}",
                    "payload_hash": f"h-{page}",
                }
                for page in (1, 2)
            ]
        ),
    )
    version_before = DeltaTable(str(path)).version()

    report = prune_broker("trading212", str(path), {}, dry_run=True)

    assert report.dry_run is True
    assert report.pruned is True
    assert report.written is False
    assert DeltaTable(str(path)).version() == version_before
    assert _read(path).num_rows == 2


# ---------------------------------------------------------------------------
# run_prune (wiring; FakeS3 / FakeBackend / _fake_config / _LocalBackend)
# ---------------------------------------------------------------------------


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}


class _FakeBackend:
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


def test_run_prune_dispatches_all_brokers(monkeypatch, tmp_path: Path) -> None:
    use_storage(_fake_config(tmp_path))
    client = FakeS3()
    captured: list[tuple[str, str]] = []

    def _fake_prune(
        broker: str,
        table_path: str,
        storage_opts: dict[str, str],
        *,
        client: object = None,
        dry_run: bool = False,
    ) -> mod.PruneReport:
        captured.append((broker, table_path))
        return mod.PruneReport(broker=broker, table_path=table_path)

    monkeypatch.setattr(mod, "prune_broker", _fake_prune)

    reports = mod.run_prune(client)

    assert captured == [
        ("ibkr", "s3://test-bucket/raw/ibkr"),
        ("trading212", "s3://test-bucket/raw/trading212"),
        ("xtb", "s3://test-bucket/raw/xtb"),
    ]
    assert [r.broker for r in reports] == list(_BROKERS)


class _LocalBackend:
    """Local-filesystem backend double so ``run_prune`` runs for real over
    ``tmp_path`` Delta tables (end-to-end, nothing mocked)."""

    def __init__(self, data_dir: Path, bucket: str = "test-bucket") -> None:
        self.data_dir = data_dir.resolve()
        self.bucket = bucket

    def table_path(self, layer: str, table_name: str) -> str:
        return str(self.data_dir / layer / table_name)

    def staging_path(self, segment: str, filename: str) -> str:
        return str(self.data_dir / segment / filename)

    def ensure_parent(self, table_path: str) -> None:
        Path(table_path).parent.mkdir(parents=True, exist_ok=True)

    @property
    def storage_options(self) -> dict[str, str]:
        return {"allow_unsafe_rename": "true"}


def test_run_prune_rewrites_real_local_tables(tmp_path: Path, monkeypatch) -> None:
    backend = _LocalBackend(tmp_path)
    use_storage(
        StorageConfig(
            data_dir=str(tmp_path),
            raw_dir=str(tmp_path / "raw"),
            normalized_dir=str(tmp_path / "normalized"),
            analytics_dir=str(tmp_path / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=backend,
        )
    )
    monkeypatch.setattr(mod, "get_storage_options_with_credentials", dict)

    (tmp_path / "raw").mkdir(parents=True, exist_ok=True)
    _write(
        tmp_path / "raw" / "trading212",
        _raw_table(
            [
                {
                    "fetched_at": _t(1 if page == 1 else 2),
                    "broker": "Trading 212",
                    "source": f"/equity/account/summary?page={page}",
                    "payload_hash": f"h-{page}",
                }
                for page in (1, 2)
            ]
        ),
    )
    # Absent ibkr/xtb tables are skipped without failing the run.
    reports = mod.run_prune(FakeS3())

    t212 = next(r for r in reports if r.broker == "trading212")
    assert t212.written and t212.verified
    assert (t212.rows_before, t212.rows_after) == (2, 1)
    result = _read(tmp_path / "raw" / "trading212")
    assert result.schema.equals(RAW_SCHEMA)
    assert result.column("payload_hash").to_pylist() == ["h-2"]
    assert all(not r.written for r in reports if r.broker != "trading212")


def test_run_prune_s3_requires_client(monkeypatch, tmp_path: Path) -> None:
    use_storage(_fake_config(tmp_path))
    monkeypatch.setattr(mod, "get_storage_options_with_credentials", dict)
    # No client passed for an s3:// table -> explicit error, not a silent skip.
    with pytest.raises(RuntimeError, match="S3 client is required"):
        prune_broker("ibkr", "s3://test-bucket/raw/ibkr", {})
