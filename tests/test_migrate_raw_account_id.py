"""Tests for the raw-schema migration (backfill XTB ``account_id``, drop ``source_file``).

Covers the unit-testable entry points:

- :func:`migrate_broker` against local temp Delta tables (``storage_opts={}``;
  deltalake accepts local paths with empty storage options, mirroring the
  ``tests/test_migrate_single_bronze.py`` pattern).  Real deltalake/polars are
  used -- nothing is mocked.
- :func:`run_migration` wiring: ``_FakeBackend``/``_fake_config`` with
  ``use_storage(...)`` and fake credential storage options so the boto3
  default-credentials chain is skipped, plus a real end-to-end run over local
  Delta tables via ``_LocalBackend``.

The migration's fixtures carry the OLD ``RAW_SCHEMA`` (with ``source_file``,
no ``account_id``) -- the historical inputs the migration rewrites -- and are
exempt from the story's ``source_file`` grep bar (the migration + its test are
the only legitimate live-code references).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError

import pipeline.migrations.migrate_raw_account_id as mod
from pipeline.migrations.migrate_raw_account_id import migrate_broker
from pipeline.raw.models import RAW_SCHEMA
from pipeline.storage import StorageConfig, use_storage

_TS = pa.timestamp("us", tz="UTC")
_BROKERS = ("ibkr", "trading212", "xtb")

# The pre-5-1 raw schema the migration reads: RAW_SCHEMA with the trailing
# ``account_id`` replaced by the retained ``source_file`` filename.
_OLD_RAW_SCHEMA = pa.schema(
    [field for field in RAW_SCHEMA if field.name != "account_id"]
    + [pa.field("source_file", pa.string())]
)


def _t(hour: int) -> datetime:
    """A deterministic UTC timestamp for a given hour on 2024-01-01."""
    return datetime(2024, 1, 1, hour, tzinfo=UTC)


def _old_raw_table(rows: list[dict[str, Any]]) -> pa.Table:
    """Build an old-schema raw table (with ``source_file``) from row dicts."""
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


def _new_raw_table(rows: list[dict[str, Any]]) -> pa.Table:
    """Build a new-schema raw table (with ``account_id``) from row dicts."""
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


def _xtb_row(hour: int, payload_hash: str, source_file: str | None) -> dict[str, Any]:
    return {
        "fetched_at": _t(hour),
        "broker": "XTB",
        "source": "XTB_REPORT",
        "payload_hash": payload_hash,
        "source_file": source_file,
    }


# ---------------------------------------------------------------------------
# migrate_broker (local temp Delta tables, old-schema fixtures)
# ---------------------------------------------------------------------------


def test_migrate_xtb_backfills_account_id_from_source_file(tmp_path: Path) -> None:
    path = tmp_path / "xtb"
    _write(
        path,
        _old_raw_table(
            [
                _xtb_row(1, "h1", "PLN_12345678_2006-01-01_2026-08-03.xlsx"),
                _xtb_row(2, "h2", "EUR_99_2026-01-01_2026-08-03.xlsx"),
            ]
        ),
    )

    report = migrate_broker("xtb", str(path), {})

    assert report.migrated is True
    assert report.rows == 2
    assert report.backfilled == 2
    result = _read(path)
    assert result.schema.equals(RAW_SCHEMA)
    assert "source_file" not in result.column_names
    assert result.column("account_id").to_pylist() == ["12345678", "99"]
    # Row count and payloads survive the rewrite unchanged.
    assert result.column("payload_hash").to_pylist() == ["h1", "h2"]


def test_migrate_xtb_unparseable_filename_yields_null(tmp_path: Path) -> None:
    """Filename-only backfill (adversarial F4 pin): an unparseable or missing
    filename yields NULL account_id -- no payload parsing at migration time."""
    path = tmp_path / "xtb"
    _write(
        path,
        _old_raw_table(
            [
                _xtb_row(1, "h1", "report.xlsx"),
                _xtb_row(2, "h2", "PLN_12345678_2006-01-01_2026-08-03.xlsx"),
                _xtb_row(3, "h3", None),
            ]
        ),
    )

    report = migrate_broker("xtb", str(path), {})

    assert report.backfilled == 1
    result = _read(path)
    assert result.schema.equals(RAW_SCHEMA)
    assert result.column("account_id").to_pylist() == [None, "12345678", None]


def test_migrate_ibkr_and_trading212_get_null_account_id(tmp_path: Path) -> None:
    for broker, display in (("ibkr", "IBKR"), ("trading212", "Trading 212")):
        path = tmp_path / broker
        _write(
            path,
            _old_raw_table(
                [
                    {
                        "fetched_at": _t(1),
                        "broker": display,
                        "source": "flex",
                        "payload_hash": "h1",
                        "source_file": "whatever.xlsx",
                    }
                ]
            ),
        )

        report = migrate_broker(broker, str(path), {})

        assert report.backfilled == 0
        result = _read(path)
        assert result.schema.equals(RAW_SCHEMA)
        assert "source_file" not in result.column_names
        assert result.column("account_id").to_pylist() == [None]


def test_migrate_writes_exact_raw_schema_order(tmp_path: Path) -> None:
    path = tmp_path / "ibkr"
    _write(
        path,
        _old_raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h1",
                    "source_file": "report.xlsx",
                },
                {
                    "fetched_at": _t(2),
                    "broker": "IBKR",
                    "source": "flex_events",
                    "payload_hash": "h2",
                    "source_file": "report.xlsx",
                },
            ]
        ),
    )

    report = migrate_broker("ibkr", str(path), {})

    assert report.rows == 2
    result = _read(path)
    # Order-sensitive equality (mirrors quality.check_schema).
    assert list(result.column_names) == list(RAW_SCHEMA.names)
    assert result.schema.equals(RAW_SCHEMA)
    assert sorted(result.column("source").to_pylist()) == ["flex", "flex_events"]


def test_migrate_is_idempotent_on_already_migrated(tmp_path: Path) -> None:
    path = tmp_path / "xtb"
    _write(
        path,
        _new_raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "XTB",
                    "source": "XTB_REPORT",
                    "payload_hash": "h1",
                    "account_id": "12345678",
                }
            ]
        ),
    )

    report = migrate_broker("xtb", str(path), {})

    assert report.migrated is False
    assert report.written is False
    result = _read(path)
    assert result.schema.equals(RAW_SCHEMA)
    assert result.column("account_id").to_pylist() == ["12345678"]


def test_migrate_absent_table_exits_zero(tmp_path: Path) -> None:
    report = migrate_broker("ibkr", str(tmp_path / "does_not_exist"), {})

    assert report.migrated is False
    assert report.written is False


def test_migrate_dry_run_does_not_write(tmp_path: Path) -> None:
    path = tmp_path / "xtb"
    _write(
        path,
        _old_raw_table([_xtb_row(1, "h1", "PLN_12345678_2006-01-01_2026-08-03.xlsx")]),
    )

    report = migrate_broker("xtb", str(path), {}, dry_run=True)

    assert report.dry_run is True
    assert report.migrated is True
    assert report.written is False
    result = _read(path)
    assert "source_file" in result.column_names
    assert "account_id" not in result.column_names


def test_migrate_conflict_raises_instead_of_clobbering(tmp_path: Path) -> None:
    """A table whose schema is neither the old nor the new RAW_SCHEMA raises
    (ADR 0112 A1 / ADR 0113 A1) and is not overwritten."""
    path = tmp_path / "ibkr"
    bad = _old_raw_table(
        [
            {
                "fetched_at": _t(1),
                "broker": "IBKR",
                "source": "flex",
                "payload_hash": "h1",
                "source_file": "report.xlsx",
            }
        ]
    ).append_column("unexpected", pa.array(["x"], type=pa.string()))
    _write(path, bad)

    with pytest.raises(RuntimeError, match="Conflict"):
        migrate_broker("ibkr", str(path), {})

    # The conflicting table was not clobbered.
    assert "unexpected" in _read(path).column_names


def test_migrate_conflict_raises_on_drifted_new_schema(tmp_path: Path) -> None:
    """An already-migrated table that has since drifted (no source_file but an
    unexpected extra column) is NOT reported as clean -- it raises."""
    path = tmp_path / "xtb"
    drifted = _new_raw_table(
        [
            {
                "fetched_at": _t(1),
                "broker": "XTB",
                "source": "XTB_REPORT",
                "payload_hash": "h1",
                "account_id": "1",
            }
        ]
    ).append_column("unexpected", pa.array(["x"], type=pa.string()))
    _write(path, drifted)

    with pytest.raises(RuntimeError, match="Conflict"):
        migrate_broker("xtb", str(path), {})

    assert "unexpected" in _read(path).column_names


def test_migrate_verification_failure_raises(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "ibkr"
    _write(
        path,
        _old_raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h1",
                    "source_file": "report.xlsx",
                }
            ]
        ),
    )
    monkeypatch.setattr(mod, "verify_migrated_table", lambda *a, **k: False)

    with pytest.raises(RuntimeError, match="(?i)verification failed"):
        migrate_broker("ibkr", str(path), {})


def test_migrate_propagates_non_notfound_errors(monkeypatch, tmp_path: Path) -> None:
    """An auth/region/permission error opening an existing table is NOT
    swallowed as 'absent' (only TableNotFoundError is) -- it propagates so
    main() exits non-zero, the signal a pre-deploy gate needs."""

    def _boom(*args: object, **kwargs: object) -> object:
        raise OSError("simulated S3 auth/region error")

    monkeypatch.setattr(mod, "DeltaTable", _boom)

    with pytest.raises(OSError, match="simulated S3 auth/region error"):
        migrate_broker("ibkr", str(tmp_path / "ibkr"), {})


def test_migrate_retries_transient_write_error_then_succeeds(
    monkeypatch, tmp_path: Path
) -> None:
    """A transient S3 DeltaError on the overwrite is retried; a later attempt
    completes the migration (the failed upload leaves the table untouched)."""
    path = tmp_path / "trading212"
    _write(
        path,
        _old_raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "Trading 212",
                    "source": "/equity/account/summary",
                    "payload_hash": "h1",
                    "source_file": "report.xlsx",
                }
            ]
        ),
    )
    attempts: list[int] = []

    def _flaky_write(*args: object, **kwargs: object) -> None:
        attempts.append(len(attempts))
        if len(attempts) < 3:
            raise DeltaError("Generic S3 error: error sending request")
        write_deltalake(*args, **kwargs)  # type: ignore[arg-type]

    sleeps: list[float] = []
    monkeypatch.setattr(mod, "write_deltalake", _flaky_write)
    monkeypatch.setattr(mod.time, "sleep", sleeps.append)

    report = migrate_broker("trading212", str(path), {})

    assert report.written is True and report.verified is True
    assert len(attempts) == 3
    assert sleeps == [mod._WRITE_RETRY_DELAY_S, mod._WRITE_RETRY_DELAY_S]
    result = _read(path)
    assert result.schema.equals(RAW_SCHEMA)
    assert result.num_rows == 1


def test_migrate_persistent_write_error_reraises_after_retries(
    monkeypatch, tmp_path: Path
) -> None:
    """After exhausting all write attempts the last DeltaError propagates
    (exit non-zero); the old-schema table is left unchanged for a re-run."""
    path = tmp_path / "xtb"
    _write(
        path,
        _old_raw_table([_xtb_row(1, "h1", "PLN_12345678_2006-01-01_2026-08-03.xlsx")]),
    )
    calls: list[int] = []

    def _always_fails(*args: object, **kwargs: object) -> None:
        calls.append(len(calls))
        raise DeltaError("Generic S3 error: error sending request")

    monkeypatch.setattr(mod, "write_deltalake", _always_fails)
    monkeypatch.setattr(mod.time, "sleep", lambda *_: None)

    with pytest.raises(DeltaError, match="error sending request"):
        migrate_broker("xtb", str(path), {})

    assert len(calls) == mod._WRITE_ATTEMPTS
    # The table still carries the OLD schema -- nothing was clobbered.
    assert "source_file" in _read(path).column_names


# ---------------------------------------------------------------------------
# run_migration (wiring; FakeS3 / FakeBackend / _fake_config)
# ---------------------------------------------------------------------------


class FakeS3:
    """In-memory S3 double.  The migration performs no S3 object-level
    operations (the rewrite is an in-place Delta overwrite), so the wiring
    tests assert the double is never touched."""

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects: dict[str, bytes] = objects or {}


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


def test_run_migration_dispatches_all_brokers(monkeypatch, tmp_path: Path) -> None:
    use_storage(_fake_config(tmp_path))
    client = FakeS3()
    captured: list[tuple[str, str]] = []

    def _fake_migrate(
        broker: str,
        table_path: str,
        storage_opts: dict[str, str],
        *,
        dry_run: bool = False,
    ) -> mod.MigrateReport:
        captured.append((broker, table_path))
        return mod.MigrateReport(
            broker=broker,
            table_path=table_path,
            migrated=True,
            verified=True,
            written=True,
        )

    monkeypatch.setattr(mod, "migrate_broker", _fake_migrate)

    reports = mod.run_migration(client)

    # Destination paths are the per-broker raw tables.
    assert captured == [
        ("ibkr", "s3://test-bucket/raw/ibkr"),
        ("trading212", "s3://test-bucket/raw/trading212"),
        ("xtb", "s3://test-bucket/raw/xtb"),
    ]
    assert [r.broker for r in reports] == list(_BROKERS)
    # No S3 object-level operations were performed.
    assert client.objects == {}


def test_run_migration_absent_sources_exit_zero(monkeypatch, tmp_path: Path) -> None:
    use_storage(_fake_config(tmp_path))
    client = FakeS3()
    monkeypatch.setattr(
        mod,
        "migrate_broker",
        lambda broker, table_path, storage_opts, **k: mod.MigrateReport(
            broker=broker, table_path=table_path
        ),
    )

    reports = mod.run_migration(client)  # must not raise

    assert all(not r.migrated for r in reports)
    assert client.objects == {}


def test_run_migration_dry_run_writes_nothing(monkeypatch, tmp_path: Path) -> None:
    use_storage(_fake_config(tmp_path))
    client = FakeS3()
    monkeypatch.setattr(
        mod,
        "migrate_broker",
        lambda broker, table_path, storage_opts, **k: mod.MigrateReport(
            broker=broker, table_path=table_path, migrated=True, dry_run=True
        ),
    )

    reports = mod.run_migration(client, dry_run=True)

    assert all(r.dry_run for r in reports)
    assert client.objects == {}


class _LocalBackend:
    """Local-filesystem backend double so ``run_migration`` runs for real over
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


def test_run_migration_rewrites_real_local_tables(tmp_path: Path, monkeypatch) -> None:
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
    # Local rewrite: empty storage options (the reference local-temp convention).
    monkeypatch.setattr(mod, "get_storage_options_with_credentials", dict)

    (tmp_path / "raw").mkdir(parents=True, exist_ok=True)
    _write(
        tmp_path / "raw" / "ibkr",
        _old_raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "IBKR",
                    "source": "flex",
                    "payload_hash": "h1",
                    "source_file": "report.xlsx",
                }
            ]
        ),
    )
    _write(
        tmp_path / "raw" / "trading212",
        _old_raw_table(
            [
                {
                    "fetched_at": _t(1),
                    "broker": "Trading 212",
                    "source": "/equity/account/summary",
                    "payload_hash": "h2",
                    "source_file": "report.xlsx",
                }
            ]
        ),
    )
    _write(
        tmp_path / "raw" / "xtb",
        _old_raw_table([_xtb_row(1, "h3", "PLN_12345678_2006-01-01_2026-08-03.xlsx")]),
    )

    reports = mod.run_migration(FakeS3())

    assert all(r.written and r.verified for r in reports)
    for broker in _BROKERS:
        table = _read(tmp_path / "raw" / broker)
        assert table.schema.equals(RAW_SCHEMA)
        assert "source_file" not in table.column_names
    xtb = _read(tmp_path / "raw" / "xtb")
    assert xtb.column("account_id").to_pylist() == ["12345678"]
