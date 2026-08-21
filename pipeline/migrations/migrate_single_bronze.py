"""Migration: merge the per-layer raw tables into one ``raw/{broker}`` bronze.

Part of the single-bronze-per-broker change (AD-7).  The pipeline currently
lands every raw payload in two Delta tables per broker --
``raw/{broker}_snapshot`` and ``raw/{broker}_events`` -- both carrying the
identical :data:`RAW_SCHEMA`.  This migration merges them into one
``raw/{broker}`` table discriminated by ``source``, then removes every
per-layer raw path (including the orphaned ``raw/xtb_events`` that was never
written but may still exist).

What this script does
---------------------
1. For each broker (``ibkr``, ``trading212``, ``xtb``), read
   ``raw/{broker}_snapshot`` and ``raw/{broker}_events`` (absent tables are
   skipped), concat them with polars, and dedup on ``(source, payload_hash)``
   keeping the latest ``fetched_at`` row -- and its
   ``source_file``, so XTB's ``account_id`` derivation (ADR 0108 D18)
   survives.  The deduped frame is written to ``raw/{broker}`` with
   ``write_deltalake(mode="overwrite", schema_mode="overwrite")``.  The
   ``pl.DataFrame`` is passed directly (project rule: never convert to
   ``pa.Table`` for writes).
2. After the merged table is written and verified (readable, exact
   ``RAW_SCHEMA`` in order, expected row count), every per-layer raw path for
   that broker is deleted from S3 via batched ``delete_objects``
   (``_DELETE_CHUNK=1000``).  This also purges the orphaned ``raw/xtb_events``.

Safety and idempotency
----------------------
- Idempotent: the merge is a deterministic overwrite of ``raw/{broker}`` --
  re-running against the still-present sources with the same dedup key
  reproduces the same destination, so an interrupted run re-succeeds.  An
  already-migrated broker (sources absent, merged table present) is skipped
  and exits 0.
- Never clobbers: a destination ``raw/{broker}`` is overwritten only when it
  is empty or its rows are a subset of the source tables (matched on the
  dedup key).  A destination whose schema is not exactly ``RAW_SCHEMA``
  (order-sensitive ``schema.equals``) or that holds rows not present in the
  sources raises instead of being overwritten (ADR 0113 A1 conflict
  convention) -- rows appended after a partially failed earlier run are never
  silently discarded.
- Source tables are deleted only after the merged table was written and
  verified for that broker.  A verification failure raises and refuses to
  delete the per-layer sources.  A partial source deletion (boto3 per-key
  ``Errors``: throttling, permission gaps) also raises, so the migration
  cannot report success while per-layer tables remain on S3.
- A genuinely absent per-layer table is skipped (exit 0).  An existing but
  unreadable table (auth/region/permission error) or an unexpected schema
  raises and exits non-zero, so a pre-deploy gate cannot mistake a real
  failure for "nothing to migrate".

Usage:
    .venv/Scripts/python -m pipeline.migrations.migrate_single_bronze \
        --mode (docker|staging|prod) [--dry-run]

Run this script BEFORE the renamed-path code (which reads and writes
``raw/{broker}``) deploys per environment (AD-7 migration-first ordering).
Per environment, in a maintenance window while connectors are idle:

1. ``--dry-run``: inspect the printed merge + delete plan, no writes.
2. Verify the plan, then run the migration for real.
3. Confirm ``raw/{broker}`` counts equal the sum of the two source tables,
   e.g.:

       pipeline.run query "SELECT broker, source, COUNT(*) FROM {broker}_raw \\
           GROUP BY broker, source" --decrypt --mode staging

   Per-broker distinct ``source`` values must match the single-bronze
   vocabulary (``flex``/``flex_events``, the five Trading 212 request paths,
   ``XTB_REPORT``).
4. Only then deploy the renamed-path code to that environment.  Running the
   migration AFTER the code means ``transform_connector`` reads only the
   merged table while per-layer tables still exist, and a live fetch running
   old code can recreate a per-layer path the migration already removed.

Requires the same environment variables as the main pipeline (ENCRYPTION_KEY,
S3_BUCKET or PIPELINE_DATA_DIR, etc.).

Exit codes
----------
0 -- merge/delete complete, or a dry-run, or an idempotent no-op.
1 -- AWS request failure, destination schema or row conflict, verification
    failure, or partial source-table deletion failure.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Any

import polars as pl
from botocore.exceptions import ClientError
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from pipeline.migrations._storage_options import get_storage_options_with_credentials
from pipeline.raw.models import RAW_SCHEMA
from pipeline.storage import get_storage

# Table paths use the lowercase connector names; the ``broker`` column values
# in the data are the display names ("IBKR", "Trading 212", "XTB").
_BROKERS: tuple[str, ...] = ("ibkr", "trading212", "xtb")
_BROKER_DISPLAY: dict[str, str] = {
    "ibkr": "IBKR",
    "trading212": "Trading 212",
    "xtb": "XTB",
}

# Every per-layer raw table merged into ``raw/{broker}``.  For XTB this also
# covers the orphaned ``raw/xtb_events`` that was never written.
_SOURCE_TABLE_SUFFIXES: tuple[str, ...] = ("_snapshot", "_events")

# Dedup key of each broker-scoped raw table (ADR 0047, ingest.py:46-52).
_DEDUP_KEY: tuple[str, ...] = ("source", "payload_hash")

# boto3 delete_objects accepts at most this many keys per call.
_DELETE_CHUNK = 1000


@dataclass
class MergeReport:
    """Outcome of merging one broker's per-layer raw tables into ``raw/{broker}``."""

    broker: str
    dest_path: str
    present_sources: tuple[str, ...] = ()
    merged_rows: int = 0
    verified: bool = False
    written: bool = False
    dry_run: bool = False


@dataclass
class DeleteReport:
    """Outcome of deleting one broker's per-layer raw paths from S3."""

    broker: str
    prefixes: tuple[str, ...] = ()
    deleted: int = 0
    dry_run: bool = False


def _path_exists(table_path: str, storage_opts: dict[str, str]) -> bool:
    """Return True when a Delta table exists at *table_path*.

    Auth/region/permission errors are not ``TableNotFoundError`` and
    propagate -- an existing but unreadable table must not be treated as
    absent (mirrors the A1 migration contract).
    """
    try:
        DeltaTable(table_path, storage_options=storage_opts)
        return True
    except TableNotFoundError:
        return False


def _dedup_merged(frames: list[pl.DataFrame]) -> pl.DataFrame:
    """Concat source frames and dedup on ``(source, payload_hash)``.

    For each dedup key the row with the latest ``fetched_at`` wins (descending
    sort, then ``unique(keep="first")`` -- mirrors ``dedup_events`` ADR 0105),
    preserving the winning row's ``source_file`` so XTB's per-``account_id``
    derivation (ADR 0108 D18) survives the merge.
    """
    merged = pl.concat(frames)
    return (
        merged.sort("fetched_at", descending=True)
        .unique(subset=list(_DEDUP_KEY), keep="first")
        .sort("fetched_at")
    )


def verify_merged_table(
    dest_path: str,
    storage_opts: dict[str, str],
    *,
    expected_rows: int,
) -> bool:
    """Return True when *dest_path* is readable with exactly RAW_SCHEMA.

    The schema comparison is order-sensitive (``pa.schema.equals`` fails on
    any unexpected column or a column out of order, mirroring
    ``quality.check_schema``), and the row count must match the merged frame.
    """
    try:
        table = DeltaTable(dest_path, storage_options=storage_opts).to_pyarrow_table()
    except TableNotFoundError:
        return False
    return table.schema.equals(RAW_SCHEMA) and table.num_rows == expected_rows


def merge_broker(
    broker: str,
    source_paths: tuple[str, ...],
    dest_path: str,
    storage_opts: dict[str, str],
    *,
    dry_run: bool = False,
) -> MergeReport:
    """Merge *source_paths* (snapshot + events) into the single table *dest_path*.

    Returns a :class:`MergeReport`.  ``verified`` is True only when the merged
    table was written and re-verified (or already existed as a valid
    ``RAW_SCHEMA`` table while all sources are gone).  ``--dry-run`` never
    writes.

    Raises :class:`RuntimeError` when the destination already exists with a
    valid ``RAW_SCHEMA`` but holds rows the source tables do not (never
    clobbers -- ADR 0113 A1).
    """
    frames: list[pl.DataFrame] = []
    present: list[str] = []
    for path in source_paths:
        try:
            dt = DeltaTable(path, storage_options=storage_opts)
        except TableNotFoundError:
            # Absent table (e.g. a broker not yet onboarded): expected, skip.
            # Auth/region/permission/I-O errors are not TableNotFoundError and
            # propagate so main() exits non-zero rather than silently skipping
            # a table that exists but is unreadable.
            print(f"  Table not found (absent), skipping: {path}")
            continue
        frames.append(pl.from_arrow(dt.to_pyarrow_table()))
        present.append(path)

    dest_table: Any | None = None
    if _path_exists(dest_path, storage_opts):
        dest_table = DeltaTable(
            dest_path, storage_options=storage_opts
        ).to_pyarrow_table()
        # Destination conflict guard (ADR 0113 A1): a destination whose schema
        # is NOT exactly RAW_SCHEMA (order-sensitive) raises rather than being
        # clobbered -- including in a dry-run.
        if not dest_table.schema.equals(RAW_SCHEMA):
            raise RuntimeError(
                f"Conflict: {dest_path} already exists with schema {dest_table.schema}, "
                "expected RAW_SCHEMA. Refusing to overwrite; investigate before "
                "re-running."
            )

    if not frames:
        if dest_table is not None:
            # Already migrated: sources gone, merged table present with the
            # exact schema.  Idempotent no-op.
            print(f"  Already migrated (merged table exists): {dest_path}")
            return MergeReport(broker=broker, dest_path=dest_path, verified=True)
        print(f"  Nothing to merge (all sources absent): {dest_path}")
        return MergeReport(broker=broker, dest_path=dest_path)

    merged = _dedup_merged(frames)

    # Decision: docs/adr/0114-single-bronze-raw-table-per-broker.md
    # Row conflict guard (never-clobber, ADR 0113 A1): a destination that
    # already holds rows must be empty or a subset of the source tables
    # (matched on the dedup key).  Overwriting otherwise would discard rows
    # appended to raw/{broker} after an earlier run -- e.g. a fetch that wrote
    # the renamed path while a partial source deletion left per-layer tables
    # behind.  Refuse and let the operator reconcile.
    if dest_table is not None:
        dest_rows = pl.from_arrow(dest_table)
        if dest_rows.height:
            orphaned = dest_rows.select(list(_DEDUP_KEY)).join(
                merged.select(list(_DEDUP_KEY)),
                on=list(_DEDUP_KEY),
                how="anti",
            )
            if orphaned.height:
                raise RuntimeError(
                    f"Conflict: {dest_path} already holds {dest_rows.height} "
                    f"row(s), {orphaned.height} of which are not present in the "
                    "per-layer source tables. Overwriting would lose rows "
                    "appended after a previous run; investigate before "
                    "re-running."
                )

    display = _BROKER_DISPLAY.get(broker, broker)
    print(
        f"  Merging {display}: {len(frames)} source table(s) -> "
        f"{merged.height} row(s) at {dest_path}"
    )

    if dry_run:
        print(f"  [DRY RUN] would write {merged.height} row(s) to {dest_path}")
        return MergeReport(
            broker=broker,
            dest_path=dest_path,
            present_sources=tuple(present),
            merged_rows=merged.height,
            dry_run=True,
        )

    write_deltalake(
        dest_path,
        merged,
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=storage_opts,
    )

    if not verify_merged_table(dest_path, storage_opts, expected_rows=merged.height):
        raise RuntimeError(
            f"Post-merge verification FAILED for {dest_path}; refusing to delete "
            f"source tables for {display}."
        )

    print(f"  Done: {dest_path}")
    return MergeReport(
        broker=broker,
        dest_path=dest_path,
        present_sources=tuple(present),
        merged_rows=merged.height,
        verified=True,
        written=True,
    )


def list_objects(client: Any, bucket: str, prefix: str) -> list[str]:
    """List all object keys under *prefix* in *bucket* (paginated)."""
    keys: list[str] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        response = client.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            keys.append(item["Key"])
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")
        if not token:
            raise RuntimeError(
                f"list_objects_v2 for {bucket} truncated without NextContinuationToken"
            )
    return keys


def _delete_objects(client: Any, bucket: str, keys: list[str]) -> None:
    """Delete *keys* from *bucket* in bounded batches.

    Per-key failures surface in the response's ``Errors`` list (throttling,
    permission gaps, missing keys).  Any failed key raises, so a partial
    deletion cannot be reported as success while per-layer tables remain on
    S3.  Keys in earlier batches are already gone; re-running the migration
    completes the remainder (idempotent).
    """
    for i in range(0, len(keys), _DELETE_CHUNK):
        chunk = keys[i : i + _DELETE_CHUNK]
        response = client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in chunk]},
        )
        # Decision: docs/adr/0114-single-bronze-raw-table-per-broker.md
        errors = response.get("Errors", [])
        if errors:
            failed = "; ".join(
                f"{err.get('Key')} ({err.get('Code')}: "
                f"{err.get('Message', '').strip()})"
                for err in errors
            )
            raise RuntimeError(
                f"delete_objects failed for {len(errors)} of {len(chunk)} "
                f"object(s): {failed}"
            )


def delete_broker_sources(
    client: Any,
    bucket: str,
    prefix: str,
    broker: str,
    *,
    dry_run: bool = False,
) -> DeleteReport:
    """Delete every per-layer raw path for *broker* from S3.

    Removes ``raw/{broker}_snapshot`` and ``raw/{broker}_events`` (which
    covers the orphaned ``raw/xtb_events``).  Callers MUST only invoke this
    after the merged ``raw/{broker}`` was written and verified.
    """
    prefixes = tuple(
        _table_prefix(prefix, "raw", f"{broker}{suffix}")
        for suffix in _SOURCE_TABLE_SUFFIXES
    )
    to_delete: list[str] = []
    for table_prefix in prefixes:
        to_delete.extend(list_objects(client, bucket, table_prefix))

    report = DeleteReport(broker=broker, prefixes=prefixes, dry_run=dry_run)
    if not to_delete:
        print(f"  No per-layer raw tables to delete for {broker}")
        return report

    print(
        f"  Deleting {len(to_delete)} object(s) for {broker}: "
        f"{', '.join(p.rstrip('/') for p in prefixes)}"
    )
    if dry_run:
        print(f"  [DRY RUN] would delete {len(to_delete)} object(s)")
        report.deleted = len(to_delete)
        return report

    _delete_objects(client, bucket, to_delete)
    report.deleted = len(to_delete)
    print(f"  Done: removed per-layer raw tables for {broker}")
    return report


def _table_prefix(prefix: str, layer: str, table_name: str) -> str:
    """Return the S3 key prefix of a Delta table directory."""
    parts = [p for p in (prefix, layer, table_name) if p]
    return "/".join(parts) + "/"


def run_migration(client: Any, *, dry_run: bool = False) -> list[MergeReport]:
    """Execute (or plan) the single-bronze merge against the active storage."""
    storage = get_storage()
    storage_opts = get_storage_options_with_credentials()
    bucket = storage.backend.bucket
    # All environments store at the bucket root (the storage-prefix concept
    # was removed); _table_prefix("", layer, name) yields raw/{table}/ etc.
    prefix = ""

    print("Migration: single bronze (raw) table per broker")
    print(f"  Bucket: {bucket}  (prefix {prefix or '(none)'})")
    if dry_run:
        print("[DRY RUN MODE - no changes will be made]")
    print()

    reports: list[MergeReport] = []
    for broker in _BROKERS:
        source_paths = tuple(
            storage.raw_path(f"{broker}{suffix}") for suffix in _SOURCE_TABLE_SUFFIXES
        )
        dest_path = storage.raw_path(broker)
        print(f"Merging {broker}...")
        report = merge_broker(
            broker, source_paths, dest_path, storage_opts, dry_run=dry_run
        )
        reports.append(report)

        if report.verified or report.dry_run:
            # Sources removed only after the merged table is written/verified;
            # a dry-run prints the delete plan without touching anything.
            delete_broker_sources(client, bucket, prefix, broker, dry_run=dry_run)
        elif report.present_sources:
            raise RuntimeError(
                f"Refusing to delete source tables for {broker}: merged table "
                f"{dest_path} was not written/verified."
            )
        print()
    return reports


def _build_s3_client() -> Any:
    """Build a boto3 S3 client using the project's consolidated credentials."""
    import boto3

    from pipeline.secrets import resolve_aws_credentials

    creds = resolve_aws_credentials()
    kwargs: dict[str, Any] = {"region_name": creds.region}
    if creds.key_id is not None or creds.secret_key is not None:
        kwargs["aws_access_key_id"] = creds.key_id or ""
        kwargs["aws_secret_access_key"] = creds.secret_key or ""
    if creds.session_token:
        kwargs["aws_session_token"] = creds.session_token
    if creds.endpoint_url:
        kwargs["endpoint_url"] = creds.endpoint_url
    return boto3.client("s3", **kwargs)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge per-layer raw tables into one bronze table per "
        "broker (raw/{broker}_snapshot + raw/{broker}_events -> raw/{broker}, "
        "deduped on (source, payload_hash)) and remove every per-layer "
        "raw path. Run BEFORE deploying the renamed-path code in each "
        "environment.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=("docker", "staging", "prod"),
        help="Execution mode (which S3/env to migrate). Run BEFORE the "
        "renamed-path code deploys to this env.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the merge/delete plan without making any changes",
    )
    args = parser.parse_args()

    from pipeline.secrets import load_env, set_mode

    load_env()
    set_mode(args.mode)

    client = _build_s3_client()
    try:
        run_migration(client, dry_run=args.dry_run)
    except ClientError as exc:
        print(f"FATAL: AWS request failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    except RuntimeError as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print("\nMigration complete.")
    if args.dry_run:
        print("[DRY RUN - no changes were made]")


if __name__ == "__main__":
    main()
