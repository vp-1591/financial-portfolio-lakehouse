"""Migration: collapse each ``raw/{broker}`` to its retention-key survivors.

One-off deploy gate for epic 5. The bounded merge-on-key retention (AD-1)
prunes superseded bronze rows only when a connector run *succeeds* -- but a
run against an accumulated pre-retention raw table OOM-kills the 512 MB
Fargate task before it can prune anything: delta-rs' MERGE reads the whole
target (trading212: ~113 MB across 121 rows), and the encrypt/dedup path
materializes several payload copies on top. Staging deploys #160/#168/#169
all failed this way (exit 137, OutOfMemoryError); ADR 0115 measured the same
ceiling locally at ~1 GB peak.

This script applies the exact end-state a first successful fetch would
produce -- newest row per retention key, ties resolved to the last row in
batch order (the same ``_dedup_by_retention_key`` semantics the ingest uses,
F1.2/AC-4) -- via the staged boto3 rewrite proven in
``migrate_raw_account_id`` (local Delta write, object upload without a
wall-clock budget, commit rebuilt with ``remove`` actions and uploaded last).
After pruning, every subsequent run merges a small target and fits the task
limit; per-run merge + VACUUM keeps it there.

Usage::

    python -m pipeline.migrations.prune_raw_retention \
        --mode docker|staging|prod [--dry-run]

Run BEFORE deploying the epic-5 connector code in each environment.
Idempotent: a table whose row count already equals its distinct-key count is
skipped, so re-runs (and post-prune re-runs) are no-ops.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Any

import polars as pl
from botocore.exceptions import ClientError
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from pipeline.migrations._staged_upload import rewrite_table
from pipeline.migrations.migrate_raw_account_id import (
    _BROKER_DISPLAY,
    _BROKERS,
    _build_s3_client,
    get_storage_options_with_credentials,
    verify_migrated_table,
)
from pipeline.raw.ingest import _dedup_by_retention_key
from pipeline.raw.models import RAW_SCHEMA


@dataclass
class PruneReport:
    """Outcome of pruning one ``raw/{broker}`` table."""

    broker: str
    table_path: str
    rows_before: int = 0
    rows_after: int = 0
    pruned: bool = False
    verified: bool = False
    written: bool = False
    dry_run: bool = False


def prune_broker(
    broker: str,
    table_path: str,
    storage_opts: dict[str, str],
    *,
    client: Any | None = None,
    dry_run: bool = False,
) -> PruneReport:
    """Collapse one ``raw/{broker}`` to one row per retention key.

    Keeps, per pagination-stripped key value, the row with the latest
    ``fetched_at`` (tie -> last in batch order) -- byte-for-byte the decision
    :func:`pipeline.raw.ingest._dedup_by_retention_key` makes for a fetch
    batch, applied to the accumulated table. Idempotent: a table already at
    one row per key is skipped. A table not readable as ``RAW_SCHEMA``
    raises (:class:`RuntimeError`) rather than being rewritten.
    """
    if table_path.startswith("s3://") and client is None:
        raise RuntimeError(
            f"An S3 client is required to prune {table_path} (staged boto3 "
            "upload); pass client= from run_prune."
        )
    try:
        dt = DeltaTable(table_path, storage_options=storage_opts)
    except TableNotFoundError:
        print(f"  Table not found (absent), skipping: {table_path}")
        return PruneReport(broker=broker, table_path=table_path)

    table = dt.to_pyarrow_table()
    if not table.schema.equals(RAW_SCHEMA):
        raise RuntimeError(
            f"Conflict: {table_path} has schema {table.schema}, expected "
            f"{RAW_SCHEMA}. Refusing to overwrite; investigate before "
            "re-running."
        )

    display = _BROKER_DISPLAY.get(broker, broker)
    survivors = _dedup_by_retention_key(table, broker)
    if survivors.num_rows == table.num_rows:
        print(
            f"  Already pruned ({display}: {table.num_rows} row(s), one per "
            f"retention key): {table_path}"
        )
        return PruneReport(
            broker=broker,
            table_path=table_path,
            rows_before=table.num_rows,
            rows_after=table.num_rows,
        )

    print(
        f"  Pruning {display}: {table.num_rows} -> {survivors.num_rows} row(s) "
        f"at {table_path}"
    )
    if dry_run:
        print(f"  [DRY RUN] would rewrite {survivors.num_rows} row(s)")
        return PruneReport(
            broker=broker,
            table_path=table_path,
            rows_before=table.num_rows,
            rows_after=survivors.num_rows,
            pruned=True,
            dry_run=True,
        )

    new_frame = pl.from_arrow(survivors)
    assert isinstance(new_frame, pl.DataFrame)
    if new_frame.columns != list(RAW_SCHEMA.names):
        raise RuntimeError(
            f"Schema mismatch after prune for {table_path}: expected columns "
            f"{list(RAW_SCHEMA.names)}, got {new_frame.columns}"
        )
    stale_paths: list[str] = (
        dt.get_add_actions(flatten=True)["path"].to_pylist()
        if table_path.startswith("s3://")
        else []
    )
    rewrite_table(
        client,
        table_path,
        new_frame,
        storage_opts,
        next_version=dt.version() + 1,
        stale_paths=stale_paths,
    )

    if not verify_migrated_table(
        table_path, storage_opts, expected_rows=new_frame.height
    ):
        raise RuntimeError(
            f"Post-prune verification FAILED for {table_path}: expected "
            f"{new_frame.height} row(s) with {RAW_SCHEMA}."
        )

    print(f"  Done: {table_path}")
    return PruneReport(
        broker=broker,
        table_path=table_path,
        rows_before=table.num_rows,
        rows_after=new_frame.height,
        pruned=True,
        verified=True,
        written=True,
    )


def run_prune(client: Any, *, dry_run: bool = False) -> list[PruneReport]:
    """Execute (or plan) the retention prune against the active storage.

    *client* is the boto3 S3 client used for staged uploads (see
    ``_staged_upload.stage_and_upload``). Errors propagate for
    ``main()`` to exit non-zero (ADR 0112 A1 convention).
    """
    from pipeline.storage import get_storage

    storage = get_storage()
    storage_opts = get_storage_options_with_credentials()

    print("Prune: collapse raw/{broker} to one row per retention key")
    print(f"  Bucket: {storage.backend.bucket}")
    if dry_run:
        print("[DRY RUN MODE - no changes will be made]")
    print()

    reports: list[PruneReport] = []
    for broker in _BROKERS:
        table_path = storage.raw_path(broker)
        print(f"Pruning {broker}...")
        reports.append(
            prune_broker(
                broker, table_path, storage_opts, client=client, dry_run=dry_run
            )
        )
        print()
    return reports


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collapse each raw/{broker} to its retention-key survivors "
        "(the end-state of the first successful epic-5 fetch). Run BEFORE "
        "deploying the epic-5 connector code: the first fetch against an "
        "accumulated table OOM-kills a 512 MB Fargate task.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=("docker", "staging", "prod"),
        help="Execution mode (which S3/env to prune).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the prune plan without making any changes",
    )
    args = parser.parse_args()

    from pipeline.secrets import load_env, set_mode

    load_env()
    set_mode(args.mode)

    client = _build_s3_client()
    try:
        run_prune(client, dry_run=args.dry_run)
    except ClientError as exc:
        print(f"FATAL: AWS request failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    except RuntimeError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print("\nPrune complete.")
    if args.dry_run:
        print("[DRY RUN - no changes were made]")


if __name__ == "__main__":
    main()
