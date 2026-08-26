"""Migration: rewrite each ``raw/{broker}`` to the new ``RAW_SCHEMA`` (AD-7).

Story 5-1 changed ``RAW_SCHEMA`` from ``fetched_at, broker, source, payload,
payload_hash, source_file`` to ``fetched_at, broker, source, payload,
payload_hash, account_id`` (nullable).  Every live ``raw/{broker}`` table that
predates that change still carries the OLD schema, so deploying the merge-on-
``account_id`` code (5-2) against them would make every legacy XTB row NULL-
keyed and re-insert it on every run instead of replacing it.  This migration
is the deploy gate of the bounded-bronze epic: it rewrites each ``raw/{broker}``
to the new ``RAW_SCHEMA``, backfilling XTB's ``account_id`` by parsing the
retained ``source_file`` filename before the column is dropped.

What this script does
---------------------
For each broker (``ibkr``, ``trading212``, ``xtb``), read ``raw/{broker}``:

1. XTB rows get ``account_id`` from the ``source_file`` filename via
   ``_account_id_from_filename`` (``{CCY}_{account_id}_{from}_{to}.xlsx``
   ``->`` ``account_id``).  The backfill parses the filename ONLY -- no payload
   parsing at migration time (adversarial F4 pin) -- so an unparseable (or
   missing) filename yields ``NULL``, matching AD-1's append-for-null-key
   rule; the XTB transform's payload-parse recovery stays the sole recovery
   path.  IBKR and Trading 212 rows get ``NULL`` ``account_id`` (they never
   merge on it).
2. ``source_file`` is dropped and the frame is written back with
   ``write_deltalake(mode="overwrite", schema_mode="overwrite")``.  The
   ``pl.DataFrame`` is passed directly (project rule: never convert to
   ``pa.Table`` for writes); ``storage_options`` come from
   ``get_storage_options_with_credentials()``.  For S3 tables the rewrite is
   staged to a local Delta table first and then pushed object-by-object with
   boto3's transfer manager: delta-rs' S3 uploader aborts sustained uploads
   after a 180s wall-clock retry budget, so a large raw table (T212 carries
   ~100 MB of encrypted payloads) never lands over a slow uplink -- every
   attempt died at ~180s mid-multipart in staging.  boto3 multipart PUTs have
   no such budget; objects whose key already exists remotely are skipped, so
   an interrupted run resumes where it stopped.  The new ``_delta_log`` entry
   is renamed to continue the remote version history and is uploaded LAST,
   so a crash mid-upload leaves the old-schema table untouched.
3. After the rewrite the table is re-read and verified (readable, exact
   ``RAW_SCHEMA`` in order, expected row count).

Safety and idempotency
----------------------
- Transient S3 write failures are retried with a fixed delay; because each
  attempt skips objects already uploaded, a retry only pushes what is left.
  The commit (log entry) lands last and atomically, so no intermediate state
  is ever visible and a retry never duplicates rows.
- Idempotent: a table already carrying the new ``RAW_SCHEMA`` is skipped and
  exits 0; a genuinely absent table is skipped and exits 0.
- Never clobbers (ADR 0113 A1): a table whose schema is neither the old
  ``RAW_SCHEMA`` (with ``source_file``) nor the new one raises instead of
  being overwritten -- an unexpected schema means the table drifted and must
  be investigated before re-running.
- Auth/region/permission errors (``ClientError``, ``OSError``, anything that
  is not ``TableNotFoundError``) propagate and exit non-zero, so a
  pre-deploy gate cannot mistake a real failure for "nothing to migrate"
  (ADR 0112 A1 convention).
- ``--dry-run`` prints the plan (per-broker backfill and rewrite counts) and
  writes nothing.

Deploy sequencing (AD-7, migration-first; ADR 0110)
---------------------------------------------------
Run this script BEFORE the 5-1/5-2 code deploys in each environment.  Per
environment, in a maintenance window while the connectors are idle:

1. Pause the scheduled Step Functions executions (the ``schedule_connectors``
   trigger for the orchestrator state machine).  Connectors idle means the
   file-arrival rule cannot fire either -- for XTB, ADR 0110's
   EventBridge file-arrival task is paused together with the schedule (both
   invoke the same state machine), so no new ``raw/xtb`` row can land mid-
   migration.  In terraform this means disabling/suspending the event rule
   and schedule targets (or pausing the state machine) for the migration
   window.
2. ``--dry-run``: inspect the printed plan, no writes.
3. Verify the plan, then run the migration for real.
4. Confirm ``raw/{broker}`` row counts and the ``account_id`` backfill, e.g.:

       PYTHONIOENCODING=utf-8 pipeline.run query \\
           "SELECT broker, source, account_id, COUNT(*) FROM {broker}_raw \\
            GROUP BY broker, source, account_id" --decrypt --mode <env>

   Per-broker counts must be unchanged from before the migration (the
   rewrite never changes row counts) and XTB's non-null ``account_id``
   values must match the ``source_file`` parse.
5. Resume the scheduled executions / file-arrival rule, then deploy the new
   code (5-1's schema + 5-2's merge) last.

If the migration runs AFTER the new code, the merge-on-``account_id`` code
reads tables that predate the column, every legacy XTB row becomes NULL-keyed
and is re-inserted every run instead of being replaced.

Requires the same environment variables as the main pipeline (ENCRYPTION_KEY,
S3_BUCKET or PIPELINE_DATA_DIR, etc.).

Usage:
    .venv/Scripts/python -m pipeline.migrations.migrate_raw_account_id \\
        --mode (docker|staging|prod) [--dry-run]

Exit codes
----------
0 -- rewrite complete, or a dry-run, or an idempotent no-op (absent or
    already-migrated tables).
1 -- AWS request failure, unexpected schema conflict, or verification
    failure.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
from botocore.exceptions import ClientError
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from pipeline.connectors.xtb.fetch import _account_id_from_filename
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

# The pre-5-1 raw schema, identical to RAW_SCHEMA except the trailing
# ``account_id`` field is the retained ``source_file`` filename (ADR 0117).
_OLD_RAW_SCHEMA = pa.schema(
    [
        *[field for field in RAW_SCHEMA if field.name != "account_id"],
        pa.field("source_file", pa.string()),
    ]
)


# Transient S3 failures during the staged upload are retried with a fixed
# delay; each attempt skips objects already uploaded, so only the remainder
# is pushed.  The commit lands last and atomically.
_WRITE_ATTEMPTS = 3
_WRITE_RETRY_DELAY_S = 10.0


@dataclass
class MigrateReport:
    """Outcome of migrating one ``raw/{broker}`` table."""

    broker: str
    table_path: str
    rows: int = 0
    backfilled: int = 0
    migrated: bool = False
    verified: bool = False
    written: bool = False
    dry_run: bool = False


def _backfill_account_id(source_file: object) -> str | None:
    """Derive the raw ``account_id`` from a legacy ``source_file`` value.

    Filename-only (adversarial F4 pin): an unparseable or missing filename
    yields ``None`` so the row keeps a NULL ``account_id`` and the XTB
    transform's payload-parse recovery (R1 ``Account number``) remains the
    sole recovery path.
    """
    if not isinstance(source_file, str):
        return None
    return _account_id_from_filename(source_file)


def _split_s3_uri(table_path: str) -> tuple[str, str]:
    """Split ``s3://bucket/prefix`` into ``(bucket, prefix)``, no trailing slash."""
    remainder = table_path.removeprefix("s3://")
    bucket, _, prefix = remainder.partition("/")
    return bucket, prefix.rstrip("/")


def _object_exists(client: Any, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return True


def _stage_and_upload(
    client: Any,
    table_path: str,
    new_frame: pl.DataFrame,
    *,
    next_version: int,
    stale_paths: list[str],
) -> None:
    """Rewrite the S3 table at *table_path* via a local Delta staging copy.

    delta-rs' S3 uploader aborts sustained uploads after a 180s wall-clock
    retry budget, so large raw rewrites never land over a slow uplink.  The
    migrated frame is instead written to a local Delta table (no network),
    and its files are pushed with boto3's transfer manager, whose multipart
    PUTs have no such budget.

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

    bucket, prefix = _split_s3_uri(table_path)

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
            if _object_exists(client, bucket, key):
                print(f"  already uploaded, skipping: {key}")
                continue
            print(f"  uploading: {key}")
            client.upload_file(str(path), bucket, key)


def _rewrite_table(
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
    boto3 upload (:func:`_stage_and_upload`) with bounded retries; each
    retry resumes from the objects that already landed.
    """
    if not table_path.startswith("s3://"):
        write_deltalake(
            table_path, new_frame, mode="overwrite", schema_mode="overwrite"
        )
        return

    for attempt in range(1, _WRITE_ATTEMPTS + 1):
        try:
            _stage_and_upload(
                client,
                table_path,
                new_frame,
                next_version=next_version,
                stale_paths=stale_paths,
            )
            return
        except (ClientError, OSError) as exc:
            if attempt == _WRITE_ATTEMPTS:
                raise
            print(
                f"  Upload attempt {attempt}/{_WRITE_ATTEMPTS} failed "
                f"(transient error: {exc}); retrying in "
                f"{_WRITE_RETRY_DELAY_S:.0f}s..."
            )
            time.sleep(_WRITE_RETRY_DELAY_S)


def verify_migrated_table(
    table_path: str,
    storage_opts: dict[str, str],
    *,
    expected_rows: int,
) -> bool:
    """Return True when *table_path* is readable with exactly ``RAW_SCHEMA``.

    The schema comparison is order-sensitive (``pa.schema.equals`` fails on
    any unexpected column or a column out of order, mirroring
    ``quality.check_schema``), and the row count must match the migrated
    frame -- the migration must never drop or duplicate rows.
    """
    try:
        table = DeltaTable(table_path, storage_options=storage_opts).to_pyarrow_table()
    except TableNotFoundError:
        return False
    return table.schema.equals(RAW_SCHEMA) and table.num_rows == expected_rows


def migrate_broker(
    broker: str,
    table_path: str,
    storage_opts: dict[str, str],
    *,
    client: Any | None = None,
    dry_run: bool = False,
) -> MigrateReport:
    """Rewrite one ``raw/{broker}`` from the old schema to ``RAW_SCHEMA``.

    Backfills XTB's ``account_id`` from the retained ``source_file`` filename
    (unparseable -> NULL), then drops ``source_file``.  Idempotent: an absent
    table or a table already in ``RAW_SCHEMA`` is skipped.  A table whose
    schema is neither old nor new raises (:class:`RuntimeError`) rather than
    being clobbered (ADR 0112 A1 / ADR 0113 A1).  ``--dry-run`` prints the
    plan and writes nothing.  S3 tables require *client* (boto3 S3 client)
    for the staged upload.
    """
    try:
        dt = DeltaTable(table_path, storage_options=storage_opts)
    except TableNotFoundError:
        # Absent table (e.g. a broker not yet onboarded): expected, skip.
        # Auth/region/permission errors are not TableNotFoundError and
        # propagate so main() exits non-zero rather than silently skipping a
        # table that exists but is unreadable.
        print(f"  Table not found (absent), skipping: {table_path}")
        return MigrateReport(broker=broker, table_path=table_path)

    old_table = dt.to_pyarrow_table()

    if old_table.schema.equals(RAW_SCHEMA):
        # Already migrated: skip.  Nothing else to check -- the schema is
        # exactly the new RAW_SCHEMA (order-sensitive).
        print(f"  Already migrated (RAW_SCHEMA): {table_path}")
        return MigrateReport(broker=broker, table_path=table_path)

    # Destination conflict guard (ADR 0112 A1 / ADR 0113 A1): a table whose
    # schema is neither the old nor the new RAW_SCHEMA raises rather than
    # being overwritten -- including in a dry-run.
    if not old_table.schema.equals(_OLD_RAW_SCHEMA):
        raise RuntimeError(
            f"Conflict: {table_path} has schema {old_table.schema}, expected "
            f"the old RAW_SCHEMA (with source_file) or the new {RAW_SCHEMA}. "
            "Refusing to overwrite; investigate before re-running."
        )

    frame = pl.from_arrow(old_table)
    if broker == "xtb":
        account_ids = [
            _backfill_account_id(value) for value in frame["source_file"].to_list()
        ]
        backfilled = sum(1 for value in account_ids if value is not None)
    else:
        # IBKR and Trading 212 never merge on account_id (AD-2): NULL.
        account_ids = [None] * frame.height
        backfilled = 0

    # Drop source_file, append account_id at the end -> RAW_SCHEMA column
    # order (fetched_at, broker, source, payload, payload_hash, account_id).
    new_frame = frame.drop("source_file").with_columns(
        pl.Series("account_id", account_ids, dtype=pl.String)
    )
    # Column-order guard.  (A pa-schema equality check is NOT meaningful on the
    # polars frame: polars' arrow view types strings/binary as
    # ``large_string``/``large_binary``, so the authoritative schema check is
    # ``verify_migrated_table`` reading the written Delta table back.)
    if new_frame.columns != list(RAW_SCHEMA.names):
        raise RuntimeError(
            f"Schema mismatch after migration for {table_path}: expected "
            f"columns {list(RAW_SCHEMA.names)}, got {new_frame.columns}"
        )

    display = _BROKER_DISPLAY.get(broker, broker)
    print(
        f"  Migrating {display}: {frame.height} row(s) at {table_path} "
        f"(backfilling account_id, dropping source_file)"
    )
    if broker == "xtb":
        print(
            f"    account_id backfill: {backfilled} row(s) from filename, "
            f"{frame.height - backfilled} row(s) remain NULL"
        )

    if dry_run:
        print(f"  [DRY RUN] would rewrite {new_frame.height} row(s)")
        return MigrateReport(
            broker=broker,
            table_path=table_path,
            rows=new_frame.height,
            backfilled=backfilled,
            migrated=True,
            dry_run=True,
        )

    if table_path.startswith("s3://") and client is None:
        raise RuntimeError(
            f"An S3 client is required to migrate {table_path} (staged boto3 "
            "upload); pass client= from run_migration."
        )
    stale_paths: list[str] = (
        dt.get_add_actions(flatten=True)["path"].to_pylist()
        if table_path.startswith("s3://")
        else []
    )
    _rewrite_table(
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
            f"Post-migration verification FAILED for {table_path}: expected "
            f"{new_frame.height} row(s) with {RAW_SCHEMA}."
        )

    print(f"  Done: {table_path}")
    return MigrateReport(
        broker=broker,
        table_path=table_path,
        rows=new_frame.height,
        backfilled=backfilled,
        migrated=True,
        verified=True,
        written=True,
    )


def run_migration(client: Any, *, dry_run: bool = False) -> list[MigrateReport]:
    """Execute (or plan) the raw-schema migration against the active storage.

    S3 rewrites are staged through boto3 (see :func:`_stage_and_upload`), so
    *client* is the boto3 S3 client used for the object uploads.  Errors
    propagate for ``main()`` to exit non-zero (ADR 0112 A1 convention:
    ``ClientError`` -> exit 1).
    """
    storage = get_storage()
    storage_opts = get_storage_options_with_credentials()

    print("Migration: backfill XTB account_id, drop source_file (AD-7)")
    print(f"  Bucket: {storage.backend.bucket}")
    if dry_run:
        print("[DRY RUN MODE - no changes will be made]")
    print()

    reports: list[MigrateReport] = []
    for broker in _BROKERS:
        table_path = storage.raw_path(broker)
        print(f"Migrating {broker}...")
        reports.append(
            migrate_broker(
                broker, table_path, storage_opts, client=client, dry_run=dry_run
            )
        )
        print()
    return reports


def _build_s3_client() -> Any:
    """Build a boto3 S3 client using the project's consolidated credentials.

    Adaptive retries with a high attempt count: the staged upload pushes
    ~100 MB of parquet over the caller's uplink, and boto3's transfer manager
    (unlike delta-rs') has no wall-clock budget -- it just needs each request
    to eventually get through.
    """
    import boto3
    from botocore.config import Config

    from pipeline.secrets import resolve_aws_credentials

    creds = resolve_aws_credentials()
    kwargs: dict[str, Any] = {
        "region_name": creds.region,
        "config": Config(retries={"max_attempts": 10, "mode": "adaptive"}),
    }
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
        description="Rewrite each raw/{broker} to the new RAW_SCHEMA: backfill "
        "XTB account_id from the retained source_file filename, then drop "
        "source_file. Run BEFORE deploying the 5-1/5-2 code in each "
        "environment (AD-7 deploy gate).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=("docker", "staging", "prod"),
        help="Execution mode (which S3/env to migrate). Run BEFORE the "
        "schema-change code deploys to this env.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the migration plan without making any changes",
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
