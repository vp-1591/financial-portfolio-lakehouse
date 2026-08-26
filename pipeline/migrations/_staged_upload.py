"""Staged S3 Delta-table rewrite shared by raw-layer migrations.

delta-rs' S3 uploader aborts sustained uploads after a 180s wall-clock retry
budget, so large raw rewrites never land over a slow uplink.  These helpers
instead write the new frame to a local Delta table (no network) and push its
files with boto3's transfer manager, whose multipart PUTs have no such
budget.  Migrations rewriting ``raw/{broker}`` tables call
:func:`rewrite_table`; local (non-S3) paths fall through to a plain
``write_deltalake``.
"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path
from typing import Any

import polars as pl
from botocore.exceptions import ClientError
from deltalake import write_deltalake

# Transient S3 failures during the staged upload are retried with a fixed
# delay; each attempt skips objects already uploaded, so only the remainder
# is pushed.  The commit lands last and atomically.
WRITE_ATTEMPTS = 3
WRITE_RETRY_DELAY_S = 10.0


def split_s3_uri(table_path: str) -> tuple[str, str]:
    """Split ``s3://bucket/prefix`` into ``(bucket, prefix)``, no trailing slash."""
    remainder = table_path.removeprefix("s3://")
    bucket, _, prefix = remainder.partition("/")
    return bucket, prefix.rstrip("/")


def object_exists(client: Any, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return True


def stage_and_upload(
    client: Any,
    table_path: str,
    new_frame: pl.DataFrame,
    *,
    next_version: int,
    stale_paths: list[str],
) -> None:
    """Rewrite the S3 table at *table_path* via a local Delta staging copy.

    A fresh staging table's own commit cannot serve as the remote commit: it
    has no ``remove`` actions, so the remote snapshot would keep the
    superseded parquet files (observed: row count doubled).  The commit is
    therefore rebuilt as the staging actions plus a ``remove`` per *stale_paths*
    (the remote table's live files), renamed to *next_version* continuing the
    remote history, and uploaded LAST -- a crash mid-upload leaves the
    old-schema table untouched.  Parquet keys are random-UUID named, so an
    object that already exists remotely is from an interrupted attempt and is
    skipped; re-runs resume where they stopped.  Superseded files stay as
    unreferenced orphans until the next VACUUM removes them.
    """
    import json

    bucket, prefix = split_s3_uri(table_path)

    with tempfile.TemporaryDirectory(prefix="raw-migration-") as tmp:
        local = Path(tmp) / "table"
        write_deltalake(
            str(local), new_frame, mode="overwrite", schema_mode="overwrite"
        )

        log_dir = local / "_delta_log"
        staged_commit = log_dir / "00000000000000000000.json"
        actions = [
            json.loads(line)
            for line in staged_commit.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        # Unlink before rebuilding: for a FRESH destination next_version is 0,
        # making the rebuilt commit's name identical to the staged commit's --
        # unlinking afterwards would delete the rebuilt commit itself.
        staged_commit.unlink()
        now_ms = int(time.time() * 1000)
        actions.extend(
            {
                "remove": {
                    "path": path,
                    "deletionTimestamp": now_ms,
                    "dataChange": True,
                }
            }
            for path in stale_paths
        )

        commit = log_dir / f"{next_version:020d}.json"
        commit.write_text(
            "".join(json.dumps(action) + "\n" for action in actions),
            encoding="utf-8",
        )

        data_files = sorted(local.rglob("*.parquet"))
        for path in [*data_files, commit]:
            rel = path.relative_to(local).as_posix()
            key = f"{prefix}/{rel}"
            if object_exists(client, bucket, key):
                print(f"  already uploaded, skipping: {key}")
                continue
            print(f"  uploading: {key}")
            client.upload_file(str(path), bucket, key)


def rewrite_table(
    client: Any | None,
    table_path: str,
    new_frame: pl.DataFrame,
    storage_opts: dict[str, str],
    *,
    next_version: int,
    stale_paths: list[str],
) -> None:
    """Overwrite *table_path* with *new_frame*.

    Local tables are rewritten in place.  S3 tables go through the staged
    boto3 upload (:func:`stage_and_upload`) with bounded retries; each
    retry resumes from the objects that already landed.
    """
    if not table_path.startswith("s3://"):
        write_deltalake(
            table_path, new_frame, mode="overwrite", schema_mode="overwrite"
        )
        return

    for attempt in range(1, WRITE_ATTEMPTS + 1):
        try:
            stage_and_upload(
                client,
                table_path,
                new_frame,
                next_version=next_version,
                stale_paths=stale_paths,
            )
            return
        except (ClientError, OSError) as exc:
            if attempt == WRITE_ATTEMPTS:
                raise
            print(
                f"  Upload attempt {attempt}/{WRITE_ATTEMPTS} failed "
                f"(transient error: {exc}); retrying in "
                f"{WRITE_RETRY_DELAY_S:.0f}s..."
            )
            time.sleep(WRITE_RETRY_DELAY_S)
