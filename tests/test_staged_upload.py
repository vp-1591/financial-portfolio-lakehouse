"""Tests for the shared staged S3 Delta-table rewrite.

:func:`stage_and_upload` is exercised against a :class:`_RecordingS3` client
double (no AWS calls): it must push parquet data files first and the rebuilt
commit last, skip objects that already landed (resume), and produce a valid
create commit for a fresh destination at version 0.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pyarrow as pa
from botocore.exceptions import ClientError

from pipeline.migrations._staged_upload import stage_and_upload
from pipeline.raw.models import RAW_SCHEMA


def _t(hour: int) -> datetime:
    """A deterministic UTC timestamp for a given hour on 2024-01-01."""
    return datetime(2024, 1, 1, hour, tzinfo=UTC)


class _RecordingS3:
    """S3 client double for the staged upload: tracks head/upload calls."""

    def __init__(self, existing: set[str] | None = None) -> None:
        self.existing: set[str] = existing or set()
        self.uploads: dict[str, bytes] = {}

    def head_object(self, Bucket: str, Key: str) -> dict[str, object]:
        if Key not in self.existing:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            )
        return {"ContentLength": 1}

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None:
        # Copy immediately: the staging temp dir is deleted after the call.
        self.uploads[Key] = Path(Filename).read_bytes()


def _new_frame() -> pl.DataFrame:
    """A one-row migrated frame matching RAW_SCHEMA exactly."""
    table = pa.table(
        {
            "fetched_at": [_t(1)],
            "broker": ["Trading 212"],
            "source": ["/equity/account/summary"],
            "payload": [b"\x01"],
            "payload_hash": ["h1"],
            "account_id": [None],
        },
        schema=RAW_SCHEMA,
    )
    frame = pl.from_arrow(table)
    assert isinstance(frame, pl.DataFrame)
    return frame


def test_stage_upload_pushes_data_then_commit_with_shifted_version() -> None:
    """The staged S3 rewrite writes the migrated frame locally, then uploads
    each parquet file and finally the rebuilt commit (staged actions plus a
    remove per stale file), renamed to continue the remote version history."""
    client = _RecordingS3()

    stage_and_upload(
        client,
        "s3://test-bucket/raw/trading212",
        _new_frame(),
        next_version=5,
        stale_paths=["old-part-a.parquet", "old-part-b.parquet"],
    )

    assert set(client.uploads) == {
        "raw/trading212/_delta_log/00000000000000000005.json",
        next(k for k in client.uploads if k.endswith(".parquet")),
    }
    json_keys = [k for k in client.uploads if k.endswith(".json")]
    parquet_keys = [k for k in client.uploads if k.endswith(".parquet")]
    assert json_keys == ["raw/trading212/_delta_log/00000000000000000005.json"]
    assert len(parquet_keys) == 1

    actions = [
        json.loads(line)
        for line in client.uploads[json_keys[0]].decode("utf-8").splitlines()
        if line.strip()
    ]
    removes = [a["remove"]["path"] for a in actions if "remove" in a]
    adds = [a for a in actions if "add" in a]
    metas = [a for a in actions if "metaData" in a]
    # The remote snapshot drops BOTH superseded files and keeps only the new
    # one; the schema change rides in metaData.
    assert removes == ["old-part-a.parquet", "old-part-b.parquet"]
    assert len(adds) == 1
    assert len(metas) == 1


def test_stage_upload_skips_objects_already_uploaded() -> None:
    """Object keys are unique per write, so anything already present remotely
    is from an earlier interrupted attempt -- re-runs resume without
    re-uploading it."""
    client = _RecordingS3(existing={"raw/xtb/_delta_log/00000000000000000007.json"})

    stage_and_upload(
        client,
        "s3://test-bucket/raw/xtb",
        _new_frame(),
        next_version=7,
        stale_paths=["old.parquet"],
    )

    # The commit was already present, so only the parquet was pushed.
    assert [k for k in client.uploads if k.endswith(".json")] == []
    assert len(client.uploads) == 1


def test_stage_upload_fresh_destination_version_zero() -> None:
    """A fresh destination starts at version 0, so the rebuilt commit's name
    collides with the staged commit's.  The rebuilt commit (not an unlinked
    empty shell) must survive and be uploaded -- regression: unlinking the
    staged commit AFTER the rebuild deleted the file and crashed the upload."""
    client = _RecordingS3()

    stage_and_upload(
        client,
        "s3://test-bucket/raw/trading212",
        _new_frame(),
        next_version=0,
        stale_paths=[],
    )

    commit_key = "raw/trading212/_delta_log/00000000000000000000.json"
    assert commit_key in client.uploads
    actions = [
        json.loads(line)
        for line in client.uploads[commit_key].decode("utf-8").splitlines()
        if line.strip()
    ]
    # A real create commit: schema metaData plus the data add, no removes.
    assert len([a for a in actions if "metaData" in a]) == 1
    assert len([a for a in actions if "add" in a]) == 1
    assert [a for a in actions if "remove" in a] == []
