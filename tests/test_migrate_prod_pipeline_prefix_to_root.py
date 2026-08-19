"""Tests for the prod ``pipeline/``-prefix-to-root migration.

Drives ``run_migration`` / ``verify_copy`` / ``strip_source_prefix`` against an
in-memory :class:`FakeS3` client double, so no AWS calls or credentials are
needed.  The ``pipeline/`` prefix is the migration's historical input (ADR 0113
follow-up) and is the project-name-adjacent artifact this one-time migration
strips.
"""

from __future__ import annotations

import io

import pytest
from botocore.exceptions import ClientError

from pipeline.migrations.migrate_prod_pipeline_prefix_to_root import (
    PROD_BUCKET,
    ObjectInfo,
    run_migration,
    strip_source_prefix,
    verify_copy,
)

PROD = PROD_BUCKET


def _not_found() -> ClientError:
    return ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}, "ResponseMetadata": {}},
        "HeadObject",
    )


class FakeS3:
    """In-memory S3 double implementing only the methods the migration uses."""

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects: dict[str, bytes] = objects or {}
        self.copy_calls: list[tuple[str, str]] = []
        self.delete_calls: list[list[str]] = []

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        prefix = str(kwargs.get("Prefix", ""))
        contents = [
            {"Key": key, "Size": len(body)}
            for key, body in sorted(self.objects.items())
            if key.startswith(prefix)
        ]
        return {"KeyCount": len(contents), "Contents": contents, "IsTruncated": False}

    def head_object(self, **kwargs: object) -> dict[str, object]:
        key = str(kwargs["Key"])
        body = self.objects.get(key)
        if body is None:
            raise _not_found()
        return {"ContentLength": len(body)}

    def copy_object(self, **kwargs: object) -> dict[str, object]:
        key = str(kwargs["Key"])
        source = kwargs["CopySource"]
        assert isinstance(source, dict)
        body = self.objects.get(str(source["Key"]))
        if body is None:
            raise ClientError(
                {
                    "Error": {"Code": "NoSuchKey", "Message": "Missing"},
                    "ResponseMetadata": {},
                },
                "CopyObject",
            )
        self.objects[key] = body
        self.copy_calls.append((str(source["Key"]), key))
        return {}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        key = str(kwargs["Key"])
        body = self.objects.get(key)
        if body is None:
            raise ClientError(
                {
                    "Error": {"Code": "NoSuchKey", "Message": "Missing"},
                    "ResponseMetadata": {},
                },
                "GetObject",
            )
        return {"Body": io.BytesIO(body)}

    def delete_objects(self, **kwargs: object) -> dict[str, object]:
        delete = kwargs["Delete"]
        assert isinstance(delete, dict)
        objects = delete["Objects"]
        assert isinstance(objects, list)
        keys = [str(item["Key"]) for item in objects]
        for key in keys:
            self.objects.pop(key, None)
        self.delete_calls.append(keys)
        return {}


def _prod_bucket() -> dict[str, bytes]:
    return {
        "pipeline/raw/ibkr/part.parquet": b"fernettoken-aaaa",
        "pipeline/normalized/events/_delta_log/00000000000000000000.json": (
            b"fernettoken-bbbb"
        ),
    }


def test_dry_run_plans_without_modifying() -> None:
    client = FakeS3(_prod_bucket())

    report = run_migration(client, dry_run=True)

    assert report.total == 2
    assert report.copied == 0
    assert report.deleted == 0
    assert report.dry_run is True
    assert client.copy_calls == []
    assert client.delete_calls == []
    assert client.objects == _prod_bucket()


def test_moves_objects_to_root_and_deletes_sources() -> None:
    client = FakeS3(_prod_bucket())

    report = run_migration(client)

    assert report.total == 2
    assert report.copied == 2
    assert report.skipped == 0
    assert report.deleted == 2
    assert report.sample_verified == 2
    # Prefix stripped entirely: objects land at the bucket root.
    assert client.objects == {
        "raw/ibkr/part.parquet": b"fernettoken-aaaa",
        "normalized/events/_delta_log/00000000000000000000.json": b"fernettoken-bbbb",
    }
    # Bytes identical (Fernet ciphertext preserved).
    assert client.objects["raw/ibkr/part.parquet"] == b"fernettoken-aaaa"


def test_idempotent_skip_completes_interrupted_run() -> None:
    # A prior run copied but was interrupted before delete: sources still
    # exist and root copies already match. Re-running skips the copy but
    # completes the delete (not duplicated).
    objects = _prod_bucket()
    objects["raw/ibkr/part.parquet"] = b"fernettoken-aaaa"
    objects["normalized/events/_delta_log/00000000000000000000.json"] = (
        b"fernettoken-bbbb"
    )
    client = FakeS3(objects)

    report = run_migration(client)

    assert report.total == 2
    assert report.copied == 0
    assert report.skipped == 2
    assert report.deleted == 2
    assert client.copy_calls == []
    # Sources are gone; the root copies from the interrupted run are kept.
    assert client.objects == {
        "raw/ibkr/part.parquet": b"fernettoken-aaaa",
        "normalized/events/_delta_log/00000000000000000000.json": b"fernettoken-bbbb",
    }


def test_conflict_raises_on_size_mismatch() -> None:
    objects = _prod_bucket()
    objects["raw/ibkr/part.parquet"] = b"123"  # wrong size at root
    client = FakeS3(objects)

    with pytest.raises(RuntimeError, match="Conflict"):
        run_migration(client)

    # Nothing was deleted and the conflicting object was not overwritten.
    assert client.delete_calls == []
    assert client.objects["raw/ibkr/part.parquet"] == b"123"


def test_verify_failure_refuses_delete() -> None:
    # Same size, different bytes: size check passes, byte comparison fails.
    objects = _prod_bucket()
    objects["raw/ibkr/part.parquet"] = b"AAAAAAAAAAAAAAAA"
    client = FakeS3(objects)

    with pytest.raises(RuntimeError, match="byte mismatch"):
        run_migration(client)

    # Sources are NOT deleted when verification fails.
    assert client.delete_calls == []
    assert "pipeline/raw/ibkr/part.parquet" in client.objects


def test_empty_source_is_noop() -> None:
    client = FakeS3({})

    report = run_migration(client)

    assert report.total == 0
    assert client.copy_calls == []
    assert client.delete_calls == []


def test_objects_outside_prefix_ignored() -> None:
    # Objects already at the root (outside pipeline/) are untouched.
    client = FakeS3({"raw/ibkr/part.parquet": b"already-root"})

    report = run_migration(client)

    assert report.total == 0
    assert client.copy_calls == []
    assert client.delete_calls == []
    assert client.objects == {"raw/ibkr/part.parquet": b"already-root"}


def test_strip_source_prefix_rejects_foreign_keys() -> None:
    assert strip_source_prefix("pipeline/raw/t") == "raw/t"
    with pytest.raises(ValueError, match="outside the source prefix"):
        strip_source_prefix("other/x")


def test_verify_detects_missing_destination() -> None:
    client = FakeS3({})
    expected = [
        ObjectInfo(key="pipeline/a", size=1),
        ObjectInfo(key="pipeline/b", size=2),
    ]

    result = verify_copy(client, expected)

    assert "missing in destination: a" in result.errors
    assert "missing in destination: b" in result.errors
    assert result.sample_verified == 0
