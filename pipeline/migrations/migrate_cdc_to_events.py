"""Migration A1: rename the CDC Delta tables to events and rewrite ``flex_cdc``.

Part of the ``cdc`` -> ``events`` rename (Track A, AD-2).  The pipeline code
now reads and writes ``{broker}_events`` / ``events`` table paths, but any
environment that ran the pre-rename code still has tables at the historical
``{broker}_cdc`` / ``cdc_events`` locations.  Without this migration those
environments would silently start with empty tables and lose history.

What this script does
---------------------
1. For each broker (``ibkr``, ``trading212``, ``xtb``), rename the raw Delta
   table ``raw/{broker}_cdc`` -> ``raw/{broker}_events`` when present.  Absent
   tables are skipped.
2. Rename the normalized Delta tables ``normalized/{broker}_cdc`` ->
   ``normalized/{broker}_events`` and the consolidated
   ``normalized/cdc_events`` -> ``normalized/events``.  Absent tables are
   skipped.
3. Rewrite historical raw ``source`` values ``flex_cdc`` -> ``flex_events`` in
   place (AD-2(d)): a code-only rename would silently skip every historical
   IBKR Flex row.  The raw ``source`` column is plain text (not Fernet-
   encrypted), so the values are rewritten directly without touching the
   encrypted ``payload`` bytes.

Rename mechanics
----------------
A rename is a server-side S3 copy of the whole table directory (``_delta_log``
included) to the new prefix followed by a delete of the source objects.  The
copy is byte-for-byte, so the Fernet ciphertext inside the Delta/Parquet
payloads is never decrypted, re-encrypted, or otherwise touched; the
``_delta_log`` is preserved exactly.  (This pipeline is path-addressed, not
catalog-addressed, so Delta ``ALTER TABLE RENAME`` would only rewrite logical
metadata and is not sufficient.)

Safety and idempotency
----------------------
- Idempotent: re-running skips source tables that no longer exist and skips
  copy objects already present in the destination with a matching size.  An
  interrupted run (copy done, delete pending) is completed, not duplicated.
  Exits 0 when all target names already exist or all sources are absent.
- Never overwrites: a destination object with the same key but a *different*
  size is a conflict and raises instead of clobbering data.
- Source objects are deleted only after a post-copy verification confirms every
  source object is present in the destination with a matching size.
- A genuinely absent table is skipped (exit 0).  An existing but unreadable
  table (auth/region/permission error) or an unexpected raw schema raises and
  exits non-zero, so a pre-deploy gate cannot mistake a real failure for
  "nothing to migrate".

Sequencing (PR order)
---------------------
A1 runs only AFTER ``migrate_cdc_events_drop_gross_amount.py`` has been applied
per environment.  That script's ``_CDC_TABLES`` are the pre-rename names
(``cdc_events``, ``{broker}_cdc``; exempt historical artifact), and after A1
those paths no longer exist, so the drop must run first.  A1's schema guard on
the raw tables expects the post-drop raw schema (raw tables never carried
``gross_amount``; the guard verifies against ``RAW_SCHEMA``).

Usage:
    .venv/Scripts/python -m pipeline.migrations.migrate_cdc_to_events \
        --mode (docker|staging|prod) [--dry-run]

Run this script BEFORE deploying the renamed code, so the renamed tables exist
when the first renamed pipeline run looks for them.  After the run, verify with
``pipeline.run query "SELECT count(*) FROM events" --decrypt --mode staging``
(pre-migration ``cdc_events`` count) and, for the data rewrite, count
``source``-gated ``events`` rows.

Requires the same environment variables as the main pipeline (ENCRYPTION_KEY,
S3_BUCKET or PIPELINE_DATA_DIR, etc.).

Exit codes
----------
0 -- rename/rewrite complete, or a dry-run, or an idempotent no-op.
1 -- AWS request failure, destination conflict, or verification/schema failure.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from botocore.exceptions import ClientError
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from pipeline.migrations._storage_options import get_storage_options_with_credentials
from pipeline.raw.models import RAW_SCHEMA
from pipeline.storage import get_storage

# Historical (pre-rename) names are intentional inputs to this one-time
# migration and are exempt from the rename grep bars.
_BROKERS: tuple[str, ...] = ("ibkr", "trading212", "xtb")

# Table renames per layer: (old_name, new_name).
_RAW_RENAMES: tuple[tuple[str, str], ...] = tuple(
    (f"{broker}_cdc", f"{broker}_events") for broker in _BROKERS
)
_NORMALIZED_RENAMES: tuple[tuple[str, str], ...] = _RAW_RENAMES + (
    ("cdc_events", "events"),
)

# Data-value rewrite (AD-2(d)): historical raw IBKR Flex payloads carry
# source="flex_cdc"; the renamed pipeline reads source="flex_events".
_LEGACY_SOURCE_VALUE = "flex_cdc"
_SOURCE_VALUE = "flex_events"

# boto3 delete_objects accepts at most this many keys per call.
_DELETE_CHUNK = 1000


@dataclass(frozen=True)
class ObjectInfo:
    """A single S3 object key plus its size in bytes."""

    key: str
    size: int


@dataclass
class RenameReport:
    """Outcome of renaming one table directory (S3 copy + delete)."""

    src_prefix: str
    dst_prefix: str
    present: bool = False
    copied: int = 0
    skipped: int = 0
    deleted: int = 0
    dry_run: bool = False


def list_objects(client: Any, bucket: str, prefix: str) -> list[ObjectInfo]:
    """List all objects under *prefix* in *bucket* (paginated)."""
    objects: list[ObjectInfo] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        response = client.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            objects.append(ObjectInfo(key=item["Key"], size=int(item["Size"])))
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")
        if not token:
            raise RuntimeError(
                f"list_objects_v2 for {bucket} truncated without NextContinuationToken"
            )
    return objects


def _relative_key(key: str, src_prefix: str) -> str:
    """Return *key* relative to *src_prefix* (the destination key suffix)."""
    if not key.startswith(src_prefix):
        raise ValueError(f"Key {key!r} is outside the source prefix {src_prefix!r}")
    return key[len(src_prefix) :]


def _delete_objects(client: Any, bucket: str, keys: list[str]) -> None:
    """Delete *keys* from *bucket* in bounded batches."""
    for i in range(0, len(keys), _DELETE_CHUNK):
        chunk = keys[i : i + _DELETE_CHUNK]
        client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in chunk]},
        )


def rename_table_dir(
    client: Any,
    bucket: str,
    src_prefix: str,
    dst_prefix: str,
    *,
    dry_run: bool = False,
) -> RenameReport:
    """Rename a Delta table directory within *bucket* (server-side copy + delete).

    Returns a :class:`RenameReport`.  ``present`` is False (idempotent no-op)
    when the source prefix holds no objects.  When the destination already
    holds every source object with a matching size (a run interrupted between
    copy and delete), the copy is skipped and the source is deleted to finish
    the rename.

    A destination object with the same key but a different size is a conflict
    and raises.  Auth/region/permission errors are not ``NoSuchKey`` and
    propagate so ``main()`` exits non-zero rather than silently skipping a
    table that exists but is unreadable.
    """
    source_objects = list_objects(client, bucket, src_prefix)
    report = RenameReport(
        src_prefix=src_prefix,
        dst_prefix=dst_prefix,
        present=bool(source_objects),
        dry_run=dry_run,
    )

    if not source_objects:
        print(f"  Table not found (absent), skipping: {src_prefix.rstrip('/')}")
        return report

    dst_by_key = {
        _relative_key(obj.key, dst_prefix): obj.size
        for obj in list_objects(client, bucket, dst_prefix)
    }

    print(
        f"  Renaming: {src_prefix.rstrip('/')} -> {dst_prefix.rstrip('/')} "
        f"({len(source_objects)} object(s))"
    )

    to_copy: list[ObjectInfo] = []
    for obj in source_objects:
        dst_key = _relative_key(obj.key, src_prefix)
        existing = dst_by_key.get(dst_key)
        if existing is None:
            to_copy.append(obj)
        elif existing == obj.size:
            report.skipped += 1
        else:
            raise RuntimeError(
                f"Conflict: {dst_prefix.rstrip('/')}/{dst_key} already exists with "
                f"size {existing} but the source has size {obj.size}. "
                "Refusing to overwrite; investigate before re-running."
            )

    if dry_run:
        print(
            f"  [DRY RUN] would copy {len(to_copy)} object(s) and delete "
            f"{len(source_objects)} source object(s)"
        )
        report.copied = len(to_copy)
        report.deleted = len(source_objects)
        return report

    for obj in to_copy:
        dst_key = _relative_key(obj.key, src_prefix)
        print(f"  Copying: {obj.key} -> {dst_prefix}{dst_key} ({obj.size} bytes)")
        client.copy_object(
            Bucket=bucket,
            Key=f"{dst_prefix}{dst_key}",
            CopySource={"Bucket": bucket, "Key": obj.key},
        )
        report.copied += 1

    # Verify every source object is present at the destination with a matching
    # size before deleting anything.
    final_dest = {
        _relative_key(obj.key, dst_prefix): obj.size
        for obj in list_objects(client, bucket, dst_prefix)
    }
    missing = [
        _relative_key(obj.key, src_prefix)
        for obj in source_objects
        if final_dest.get(_relative_key(obj.key, src_prefix)) != obj.size
    ]
    if missing:
        raise RuntimeError(
            "Post-copy verification FAILED; refusing to delete the source:\n- "
            + "\n- ".join(missing)
        )

    _delete_objects(client, bucket, [obj.key for obj in source_objects])
    report.deleted = len(source_objects)
    print(f"  Done: {src_prefix.rstrip('/')} -> {dst_prefix.rstrip('/')}")
    return report


def rewrite_legacy_source(
    table_path: str,
    storage_opts: dict[str, str],
    dry_run: bool = False,
) -> bool:
    """Rewrite raw ``source`` values ``flex_cdc`` -> ``flex_events`` in place.

    Returns True if the table was rewritten, False if it was absent or had no
    ``flex_cdc`` source values (already migrated).
    """
    try:
        dt = DeltaTable(table_path, storage_options=storage_opts)
    except TableNotFoundError:
        # Absent table (e.g. a broker not yet onboarded): expected, skip.
        # Auth/region/permission/I-O errors are not TableNotFoundError and
        # propagate so main() exits non-zero rather than silently skipping a
        # table that exists but is unreadable.
        print(f"  Table not found (absent), skipping: {table_path}")
        return False

    table = dt.to_pyarrow_table()
    if not table.schema.equals(RAW_SCHEMA):
        raise RuntimeError(
            f"Schema mismatch for {table_path}: expected {RAW_SCHEMA}, "
            f"got {table.schema}"
        )

    mask = pc.equal(table["source"], _LEGACY_SOURCE_VALUE)
    count = int(pc.sum(mask).as_py())
    if count == 0:
        # Already migrated; still verify the full schema so a drifted table
        # (other missing/extra/out-of-order column) is not reported as clean —
        # the docstring contract is "unexpected schema raises".
        print(
            f"  Already migrated (no {_LEGACY_SOURCE_VALUE} source values): {table_path}"
        )
        return False

    new_source = pc.if_else(mask, _SOURCE_VALUE, table["source"])
    new_table: pa.Table = table.set_column(
        table.schema.get_field_index("source"), "source", new_source
    )

    # Order-sensitive guard: schema.equals fails on any unexpected column or a
    # column placed out of order (mirrors quality.check_schema).
    if not new_table.schema.equals(RAW_SCHEMA):
        raise RuntimeError(
            f"Schema mismatch after migration for {table_path}: expected "
            f"{RAW_SCHEMA}, got {new_table.schema}"
        )

    print(
        f"  Rewriting {count} row(s) source={_LEGACY_SOURCE_VALUE!r} -> "
        f"{_SOURCE_VALUE!r} in {table_path} ({table.num_rows} rows)"
    )

    if dry_run:
        print(f"  [DRY RUN] Would overwrite with {new_table.num_rows} rows")
        return True

    write_deltalake(
        table_path,
        new_table,
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=storage_opts,
    )
    print(f"  Done: {table_path}")
    return True


def _table_prefix(prefix: str, layer: str, table_name: str) -> str:
    """Return the S3 key prefix of a Delta table directory."""
    parts = [p for p in (prefix, layer, table_name) if p]
    return "/".join(parts) + "/"


def run_migration(client: Any, *, dry_run: bool = False) -> list[RenameReport]:
    """Execute (or plan) migration A1 against the active storage environment."""
    storage = get_storage()
    storage_opts = get_storage_options_with_credentials()
    bucket = storage.backend.bucket
    prefix = storage.backend.prefix.rstrip("/")

    print("Migration A1: CDC -> events table renames (AD-2)")
    print(f"  Bucket: {bucket}  (prefix {prefix or '(none)'})")
    if dry_run:
        print("[DRY RUN MODE - no changes will be made]")
    print()

    reports: list[RenameReport] = []

    print("Renaming raw tables...")
    for old, new in _RAW_RENAMES:
        src = _table_prefix(prefix, "raw", old)
        dst = _table_prefix(prefix, "raw", new)
        reports.append(rename_table_dir(client, bucket, src, dst, dry_run=dry_run))

    print()
    print("Renaming normalized tables...")
    for old, new in _NORMALIZED_RENAMES:
        src = _table_prefix(prefix, "normalized", old)
        dst = _table_prefix(prefix, "normalized", new)
        reports.append(rename_table_dir(client, bucket, src, dst, dry_run=dry_run))

    print()
    print("Rewriting historical raw source values...")
    for broker in _BROKERS:
        print(f"Checking {broker}_events...")
        rewrite_legacy_source(
            storage.raw_path(f"{broker}_events"), storage_opts, dry_run=dry_run
        )

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
        description="Migration A1: rename the CDC Delta tables to events "
        "(raw/{broker}_cdc -> raw/{broker}_events, "
        "normalized/{broker}_cdc -> normalized/{broker}_events, "
        "normalized/cdc_events -> normalized/events) and rewrite historical "
        "raw source values flex_cdc -> flex_events in place (AD-2). Run AFTER "
        "migrate_cdc_events_drop_gross_amount.py has been applied per env.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=("docker", "staging", "prod"),
        help="Execution mode (which S3/env to migrate). Run AFTER "
        "migrate_cdc_events_drop_gross_amount.py on this env, and BEFORE "
        "deploying the renamed code.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the rename/rewrite plan without making any changes",
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

    print("\nMigration A1 complete.")
    if args.dry_run:
        print("[DRY RUN - no changes were made]")


if __name__ == "__main__":
    main()
