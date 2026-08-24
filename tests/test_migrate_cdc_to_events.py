"""Tests for the A1 migration (rename CDC tables to events, rewrite flex_cdc).

Covers the two unit-testable entry points:

- :func:`rename_table_dir` against an in-memory :class:`FakeS3` client double
  (no AWS calls), mirroring the ``tests/test_migrate_demo_bucket_to_staging.py``
  pattern for S3-level operations.
- :func:`rewrite_legacy_source` against local temp Delta tables
  (``storage_opts={}``; deltalake accepts local paths with empty storage
  options, mirroring the ``tests/test_migrate_cdc_events_drop_gross_amount.py``
  pattern). The order-sensitive schema assertion mirrors
  ``quality.check_schema``'s order-sensitive ``schema.equals``.

The pre-rename names in this test (``ibkr_cdc``, ``cdc_events``, ``flex_cdc``)
are the migration's historical inputs and are exempt from the rename grep bars.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from botocore.exceptions import ClientError
from deltalake import DeltaTable, write_deltalake

import pipeline.migrations.migrate_cdc_to_events as mod
from pipeline.migrations.migrate_cdc_to_events import (
    rename_table_dir,
    rewrite_legacy_source,
)
from pipeline.raw.models import RAW_SCHEMA
from pipeline.storage import StorageConfig, use_storage

_TS = pa.timestamp("us", tz="UTC")


class FakeS3:
    """In-memory S3 double implementing only the methods the migration uses."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects: dict[str, bytes] = objects
        self.copy_calls: list[tuple[str, str]] = []
        self.skip_copy_write = False  # simulate a copy that does not persist

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        prefix = str(kwargs.get("Prefix", ""))
        contents = [
            {"Key": key, "Size": len(body)}
            for key, body in sorted(self.objects.items())
            if key.startswith(prefix)
        ]
        return {"KeyCount": len(contents), "Contents": contents, "IsTruncated": False}

    def copy_object(self, **kwargs: object) -> dict[str, object]:
        bucket = str(kwargs["Bucket"])
        key = str(kwargs["Key"])
        source = kwargs["CopySource"]
        assert isinstance(source, dict)
        src_key = str(source["Key"])
        if src_key not in self.objects:
            raise ClientError(
                {
                    "Error": {"Code": "NoSuchKey", "Message": "Missing"},
                    "ResponseMetadata": {},
                },
                "CopyObject",
            )
        if not self.skip_copy_write:
            self.objects[key] = self.objects[src_key]
        self.copy_calls.append((bucket, key))
        return {}

    def delete_objects(self, **kwargs: object) -> dict[str, object]:
        delete = kwargs["Delete"]
        assert isinstance(delete, dict)
        objects = delete["Objects"]
        assert isinstance(objects, list)
        deleted = []
        for item in objects:
            key = item["Key"]
            self.objects.pop(key, None)
            deleted.append({"Key": key})
        return {"Deleted": deleted}


def _cdc_table_objects(prefix: str) -> dict[str, bytes]:
    return {
        f"{prefix}_delta_log/00000000000000000000.json": b"log-json",
        f"{prefix}part-0.parquet": b"ciphertext-a",
        f"{prefix}part-1.parquet": b"ciphertext-b",
    }


# ---------------------------------------------------------------------------
# rename_table_dir
# ---------------------------------------------------------------------------


def test_rename_moves_table_directory_and_preserves_delta_log() -> None:
    client = FakeS3(_cdc_table_objects("raw/ibkr_cdc/"))

    report = rename_table_dir(
        client, "test-bucket", "raw/ibkr_cdc/", "raw/ibkr_events/"
    )

    assert report.present is True
    assert report.copied == 3
    assert report.deleted == 3
    assert report.skipped == 0
    # Bytes identical: Fernet ciphertext and _delta_log preserved exactly.
    assert client.objects == _cdc_table_objects("raw/ibkr_events/")


def test_rename_skips_absent_source() -> None:
    client = FakeS3({})

    report = rename_table_dir(client, "test-bucket", "raw/xtb_cdc/", "raw/xtb_events/")

    assert report.present is False
    assert report.copied == 0
    assert report.deleted == 0
    assert client.objects == {}


def test_rename_already_renamed_is_noop() -> None:
    # Source gone, destination present: idempotent no-op (exit 0).
    client = FakeS3(_cdc_table_objects("raw/ibkr_events/"))

    report = rename_table_dir(
        client, "test-bucket", "raw/ibkr_cdc/", "raw/ibkr_events/"
    )

    assert report.present is False
    assert client.copy_calls == []
    assert client.objects == _cdc_table_objects("raw/ibkr_events/")


def test_rename_completes_interrupted_rename() -> None:
    # Copy finished, delete pending: the source is removed, nothing re-copied.
    buckets = _cdc_table_objects("raw/ibkr_events/")
    buckets.update(_cdc_table_objects("raw/ibkr_cdc/"))
    client = FakeS3(buckets)

    report = rename_table_dir(
        client, "test-bucket", "raw/ibkr_cdc/", "raw/ibkr_events/"
    )

    assert report.present is True
    assert report.copied == 0
    assert report.skipped == 3
    assert report.deleted == 3
    assert client.copy_calls == []
    assert client.objects == _cdc_table_objects("raw/ibkr_events/")


def test_rename_dry_run_does_not_write() -> None:
    client = FakeS3(_cdc_table_objects("raw/ibkr_cdc/"))
    before = dict(client.objects)

    report = rename_table_dir(
        client, "test-bucket", "raw/ibkr_cdc/", "raw/ibkr_events/", dry_run=True
    )

    assert report.present is True
    assert report.copied == 3
    assert report.deleted == 3
    assert client.copy_calls == []
    assert client.objects == before


def test_rename_conflict_raises_on_size_mismatch() -> None:
    buckets = _cdc_table_objects("raw/ibkr_cdc/")
    buckets.update(_cdc_table_objects("raw/ibkr_events/"))
    buckets["raw/ibkr_events/_delta_log/00000000000000000000.json"] = b"DIFFERENT-SIZE"
    client = FakeS3(buckets)

    with pytest.raises(RuntimeError, match="Conflict"):
        rename_table_dir(client, "test-bucket", "raw/ibkr_cdc/", "raw/ibkr_events/")

    # The conflicting object was not overwritten.
    assert client.objects["raw/ibkr_events/_delta_log/00000000000000000000.json"] == (
        b"DIFFERENT-SIZE"
    )


def test_rename_refuses_to_delete_on_verification_failure() -> None:
    client = FakeS3(_cdc_table_objects("raw/ibkr_cdc/"))
    client.skip_copy_write = True

    with pytest.raises(RuntimeError, match="refusing to delete"):
        rename_table_dir(client, "test-bucket", "raw/ibkr_cdc/", "raw/ibkr_events/")

    # Source untouched: no partial deletion.
    assert client.objects == _cdc_table_objects("raw/ibkr_cdc/")


# ---------------------------------------------------------------------------
# rewrite_legacy_source
# ---------------------------------------------------------------------------


def _raw_table(source_values: list[str]) -> pa.Table:
    """Build a RAW_SCHEMA raw table with the given ``source`` values."""
    n = len(source_values)
    timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    return pa.table(
        {
            "fetched_at": pa.array([timestamp] * n, type=_TS),
            "broker": pa.array(["ibkr"] * n, type=pa.string()),
            "source": pa.array(source_values, type=pa.string()),
            "payload": pa.array([b"\x01"] * n, type=pa.binary()),
            "payload_hash": pa.array(["hash"] * n, type=pa.string()),
            "account_id": pa.array([None] * n, type=pa.string()),
        }
    ).cast(RAW_SCHEMA)


def _write(path: Path, table: pa.Table) -> None:
    write_deltalake(str(path), table, mode="overwrite", schema_mode="overwrite")


def _read(path: Path) -> pa.Table:
    return DeltaTable(str(path)).to_pyarrow_table()


def test_rewrite_migrates_table(tmp_path: Path) -> None:
    table_path = tmp_path / "ibkr_events"
    _write(table_path, _raw_table(["flex_cdc", "snapshot", "flex_cdc"]))

    rewritten = rewrite_legacy_source(str(table_path), {})

    assert rewritten is True
    result = _read(table_path)
    assert result.column("source").to_pylist() == [
        "flex_events",
        "snapshot",
        "flex_events",
    ]
    # Encrypted payload bytes untouched.
    assert result.column("payload").to_pylist() == [b"\x01"] * 3
    # Order-sensitive equality: quality.check_schema relies on the same
    # order-sensitive schema.equals.
    assert result.schema == RAW_SCHEMA


def test_rewrite_is_idempotent(tmp_path: Path) -> None:
    table_path = tmp_path / "ibkr_events"
    _write(table_path, _raw_table(["flex_events", "snapshot"]))

    rewritten = rewrite_legacy_source(str(table_path), {})

    assert rewritten is False
    assert _read(table_path).column("source").to_pylist() == ["flex_events", "snapshot"]


def test_rewrite_returns_false_for_missing_table(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist"

    rewritten = rewrite_legacy_source(str(missing), {})

    assert rewritten is False


def test_rewrite_dry_run_does_not_write(tmp_path: Path) -> None:
    table_path = tmp_path / "ibkr_events"
    _write(table_path, _raw_table(["flex_cdc"]))

    rewritten = rewrite_legacy_source(str(table_path), {}, dry_run=True)

    assert rewritten is True
    # Unchanged: still flex_cdc.
    assert _read(table_path).column("source").to_pylist() == ["flex_cdc"]


def test_rewrite_raises_on_unexpected_schema(tmp_path: Path) -> None:
    """A raw table with an unexpected extra column raises instead of silently
    being reported clean, so the migration exits non-zero on a real schema
    problem rather than reporting 'nothing to migrate'."""
    table_path = tmp_path / "ibkr_events"
    bad = _raw_table(["flex_cdc"]).append_column(
        "unexpected", pa.array(["x"], type=pa.string())
    )
    _write(table_path, bad)

    with pytest.raises(RuntimeError, match="Schema mismatch"):
        rewrite_legacy_source(str(table_path), {})


def test_rewrite_propagates_non_notfound_errors(monkeypatch, tmp_path: Path) -> None:
    """An auth/region/permission error opening an existing table is NOT
    swallowed as 'absent' (only TableNotFoundError is) -- it propagates so
    main() exits non-zero, the signal a pre-deploy gate needs."""

    def _boom(*args: object, **kwargs: object) -> object:
        raise OSError("simulated S3 auth/region error")

    monkeypatch.setattr(mod, "DeltaTable", _boom)

    with pytest.raises(OSError, match="simulated S3 auth/region error"):
        rewrite_legacy_source(str(tmp_path / "ibkr_events"), {})


# ---------------------------------------------------------------------------
# run_migration (S3 rename plan wired to the storage config)
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
    return {
        "raw/ibkr_cdc/_delta_log/00000000000000000000.json": b"log1",
        "raw/ibkr_cdc/part.parquet": b"cipher-a",
        "raw/trading212_cdc/_delta_log/00000000000000000000.json": b"log2",
        "normalized/ibkr_cdc/_delta_log/00000000000000000000.json": b"log3",
        "normalized/cdc_events/_delta_log/00000000000000000000.json": b"log4",
        "normalized/cdc_events/part.parquet": b"cipher-b",
    }


def test_run_migration_renames_present_tables_and_skips_absent(
    monkeypatch, tmp_path: Path
) -> None:
    use_storage(_fake_config(tmp_path))
    rewritten: list[str] = []

    def _fake_rewrite(
        table_path: str, storage_opts: dict, dry_run: bool = False
    ) -> bool:
        rewritten.append(table_path)
        return False

    monkeypatch.setattr(mod, "rewrite_legacy_source", _fake_rewrite)
    client = FakeS3(_staging_objects())

    reports = mod.run_migration(client)

    # raw ibkr_cdc -> raw/ibkr_events
    assert (
        client.objects["raw/ibkr_events/_delta_log/00000000000000000000.json"]
        == b"log1"
    )
    assert client.objects["raw/ibkr_events/part.parquet"] == b"cipher-a"
    assert "raw/ibkr_cdc/_delta_log/00000000000000000000.json" not in client.objects
    # raw trading212_cdc -> raw/trading212_events
    assert client.objects[
        "raw/trading212_events/_delta_log/00000000000000000000.json"
    ] == (b"log2")
    assert (
        "raw/trading212_cdc/_delta_log/00000000000000000000.json" not in client.objects
    )
    # normalized ibkr_cdc -> normalized/ibkr_events
    assert client.objects[
        "normalized/ibkr_events/_delta_log/00000000000000000000.json"
    ] == (b"log3")
    assert (
        "normalized/ibkr_cdc/_delta_log/00000000000000000000.json" not in client.objects
    )
    # normalized cdc_events -> normalized/events
    assert (
        client.objects["normalized/events/_delta_log/00000000000000000000.json"]
        == b"log4"
    )
    assert client.objects["normalized/events/part.parquet"] == b"cipher-b"
    assert (
        "normalized/cdc_events/_delta_log/00000000000000000000.json"
        not in client.objects
    )
    # absent raw/normalized xtb_cdc were skipped; nothing stray created.
    assert "raw/xtb_events" not in client.objects
    assert "normalized/xtb_events" not in client.objects

    # Every rename reported present/copied; the rewrite is invoked on the
    # post-rename raw events paths.
    assert [r.present for r in reports] == [True, True, False, True, False, False, True]
    assert rewritten == [
        "s3://test-bucket/raw/ibkr_events",
        "s3://test-bucket/raw/trading212_events",
        "s3://test-bucket/raw/xtb_events",
    ]


def test_run_migration_dry_run_does_not_write(monkeypatch, tmp_path: Path) -> None:
    use_storage(_fake_config(tmp_path))
    monkeypatch.setattr(mod, "rewrite_legacy_source", lambda *a, **k: False)
    client = FakeS3(_staging_objects())
    before = dict(client.objects)

    mod.run_migration(client, dry_run=True)

    assert client.objects == before
    assert client.copy_calls == []
