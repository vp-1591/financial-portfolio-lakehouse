"""Tests for the B1 demo->staging bucket copy migration.

Drives ``run_migration`` / ``verify_copy`` / ``strip_demo_prefix`` against an
in-memory :class:`FakeS3` client double, so no AWS calls or credentials are
needed.  The pre-rename names in this test (``investment-portfolio-pipeline-
demo``, ``pipeline_demo``) are the migration's historical inputs and are
exempt from the rename grep bars.
"""

from __future__ import annotations

import io

import pytest
from botocore.exceptions import ClientError

from pipeline.migrations.migrate_demo_bucket_to_staging import (
    DEMO_BUCKET,
    STAGING_BUCKET,
    ObjectInfo,
    run_migration,
    strip_demo_prefix,
    verify_copy,
)

DEMO = DEMO_BUCKET
STAGING = STAGING_BUCKET


def _not_found() -> ClientError:
    return ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}, "ResponseMetadata": {}},
        "HeadObject",
    )


class FakeS3:
    """In-memory S3 double implementing only the methods the migration uses."""

    def __init__(self, buckets: dict[str, dict[str, bytes]] | None = None) -> None:
        self.buckets: dict[str, dict[str, bytes]] = buckets or {}
        self.copy_calls: list[tuple[str, str]] = []

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        bucket = str(kwargs["Bucket"])
        prefix = str(kwargs.get("Prefix", ""))
        contents = [
            {"Key": key, "Size": len(body)}
            for key, body in sorted(self.buckets.get(bucket, {}).items())
            if key.startswith(prefix)
        ]
        return {"KeyCount": len(contents), "Contents": contents, "IsTruncated": False}

    def head_object(self, **kwargs: object) -> dict[str, object]:
        bucket = str(kwargs["Bucket"])
        key = str(kwargs["Key"])
        body = self.buckets.get(bucket, {}).get(key)
        if body is None:
            raise _not_found()
        return {"ContentLength": len(body)}

    def copy_object(self, **kwargs: object) -> dict[str, object]:
        bucket = str(kwargs["Bucket"])
        key = str(kwargs["Key"])
        source = kwargs["CopySource"]
        assert isinstance(source, dict)
        body = self.buckets.get(str(source["Bucket"]), {}).get(str(source["Key"]))
        if body is None:
            raise ClientError(
                {
                    "Error": {"Code": "NoSuchKey", "Message": "Missing"},
                    "ResponseMetadata": {},
                },
                "CopyObject",
            )
        self.buckets.setdefault(bucket, {})[key] = body
        self.copy_calls.append((bucket, key))
        return {}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        bucket = str(kwargs["Bucket"])
        key = str(kwargs["Key"])
        body = self.buckets.get(bucket, {}).get(key)
        if body is None:
            raise ClientError(
                {
                    "Error": {"Code": "NoSuchKey", "Message": "Missing"},
                    "ResponseMetadata": {},
                },
                "GetObject",
            )
        return {"Body": io.BytesIO(body)}


def _demo_bucket() -> dict[str, dict[str, bytes]]:
    return {
        DEMO: {
            "pipeline_demo/raw/ibkr/part.parquet": b"fernettoken-aaaa",
            "pipeline_demo/normalized/events/_delta_log/00000000000000000000.json": (
                b"fernettoken-bbbb"
            ),
        }
    }


def test_dry_run_plans_without_modifying() -> None:
    client = FakeS3(_demo_bucket())

    report = run_migration(client, dry_run=True)

    assert report.total == 2
    assert report.copied == 0
    assert report.dry_run is True
    assert client.copy_calls == []
    assert client.buckets.get(STAGING) in (None, {})


def test_copy_moves_objects_to_root_and_preserves_bytes() -> None:
    client = FakeS3(_demo_bucket())

    report = run_migration(client)

    assert report.total == 2
    assert report.copied == 2
    assert report.skipped == 0
    assert report.sample_verified == 2
    dest = client.buckets[STAGING]
    # Prefix stripped entirely: objects land at the bucket root.
    assert dest == {
        "raw/ibkr/part.parquet": b"fernettoken-aaaa",
        "normalized/events/_delta_log/00000000000000000000.json": b"fernettoken-bbbb",
    }
    # Old bucket untouched, bytes identical (Fernet ciphertext preserved).
    assert client.buckets[DEMO] == _demo_bucket()[DEMO]


def test_idempotent_skip_when_already_copied() -> None:
    buckets = _demo_bucket()
    buckets[STAGING] = {
        "raw/ibkr/part.parquet": b"fernettoken-aaaa",
        "normalized/events/_delta_log/00000000000000000000.json": b"fernettoken-bbbb",
    }
    client = FakeS3(buckets)

    report = run_migration(client)

    assert report.total == 2
    assert report.copied == 0
    assert report.skipped == 2
    assert client.copy_calls == []
    assert report.sample_verified == 2


def test_conflict_raises_on_size_mismatch() -> None:
    buckets = _demo_bucket()
    buckets[STAGING] = {"raw/ibkr/part.parquet": b"123"}  # wrong size
    client = FakeS3(buckets)

    with pytest.raises(RuntimeError, match="Conflict"):
        run_migration(client)

    # The conflicting object was not overwritten.
    assert client.buckets[STAGING]["raw/ibkr/part.parquet"] == b"123"


def test_verify_detects_byte_mismatch() -> None:
    # Same size, different bytes: size check passes, byte comparison fails.
    buckets = _demo_bucket()
    buckets[STAGING] = {"raw/ibkr/part.parquet": b"AAAAAAAAAAAAAAAA"}
    client = FakeS3(buckets)

    with pytest.raises(RuntimeError, match="byte mismatch"):
        run_migration(client)


def test_empty_source_is_noop() -> None:
    client = FakeS3({DEMO: {}})

    report = run_migration(client)

    assert report.total == 0
    assert client.copy_calls == []
    assert client.buckets.get(STAGING) in (None, {})


def test_objects_outside_prefix_ignored() -> None:
    buckets = {
        DEMO: {
            "pipeline_demo/data.bin": b"in-scope",
            "other-prefix/x.bin": b"out-of-scope",
        }
    }
    client = FakeS3(buckets)

    report = run_migration(client)

    assert report.total == 1
    assert client.buckets[STAGING] == {"data.bin": b"in-scope"}


def test_strip_demo_prefix_rejects_foreign_keys() -> None:
    assert strip_demo_prefix("pipeline_demo/raw/t") == "raw/t"
    with pytest.raises(ValueError, match="outside the source prefix"):
        strip_demo_prefix("other/x")


def test_verify_detects_missing_destination() -> None:
    client = FakeS3({DEMO: _demo_bucket()[DEMO]})
    expected = [
        ObjectInfo(key="pipeline_demo/a", size=1),
        ObjectInfo(key="pipeline_demo/b", size=2),
    ]

    result = verify_copy(client, expected)

    assert "missing in destination: a" in result.errors
    assert "missing in destination: b" in result.errors
    assert result.sample_verified == 0
