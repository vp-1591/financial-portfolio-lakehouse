"""Migration script: Unify snapshot schemas (rename ``name`` -> ``description``).

Stage 1 collapsed the three per-broker snapshot schemas
(``ibkr_snapshot_normalized_schema``, ``trading212_snapshot_normalized_schema``,
``xtb_snapshot_normalized_schema``) into a single shared
``snapshot_normalized_schema`` whose field set is IBKR's (it already has a
``description`` column and stores ``security_value`` in the instrument
currency).

The existing ``trading212_snapshot`` and ``xtb_snapshot`` normalized Delta
tables still have a ``name`` column (the old T212/XTB field name) and no
``description`` column. Without this migration, a deploy's ``validate`` step
would FAIL on the stale ``name``-column table before the next
``transform_snapshot`` re-run overwrites it with the unified schema.

This migration renames ``name`` -> ``description`` in the T212 and XTB
normalized snapshot tables only (IBKR already has ``description``, so it is
untouched). It does NOT recompute T212 ``security_ccy`` / ``security_value``
from raw — that happens automatically when ``transform_snapshot`` re-runs
(``mode="overwrite"``) on the next pipeline run.

Idempotent: skips tables that are absent and tables that already have
``description`` (already migrated).

Usage:
    .venv/Scripts/python -m pipeline.migrations.migrate_snapshot_schema_unify [--dry-run]

Requires the same environment variables as the main pipeline (ENCRYPTION_KEY,
S3_BUCKET or PIPELINE_DATA_DIR, etc.).
"""

from __future__ import annotations

import argparse

import boto3
from deltalake import DeltaTable, write_deltalake

from pipeline.normalized.models import snapshot_normalized_schema
from pipeline.storage import get_storage


def _get_storage_options_with_credentials() -> dict[str, str]:
    """Resolve storage options using boto3 for credential discovery.

    The deltalake Rust backend (object_store) cannot read AWS credential
    files on all platforms.  Use boto3 (which handles credential chains
    correctly) to discover credentials and pass them explicitly.
    """
    storage = get_storage()
    opts = dict(storage.storage_options or {})

    if "aws_access_key_id" not in opts:
        session = boto3.Session(region_name=opts.get("aws_region", "eu-west-1"))
        creds = session.get_credentials()
        if creds:
            frozen = creds.get_frozen_credentials()
            if frozen.access_key and frozen.secret_key:
                opts["aws_access_key_id"] = frozen.access_key
                opts["aws_secret_access_key"] = frozen.secret_key
                if frozen.token:
                    opts["aws_session_token"] = frozen.token

    return opts


# Snapshot tables to migrate (IBKR already has `description`, so it is omitted).
_RENAME_NAME_TO_DESCRIPTION = {
    "trading212_snapshot": snapshot_normalized_schema,
    "xtb_snapshot": snapshot_normalized_schema,
}


def rename_name_to_description(
    table_name: str,
    table_path: str,
    storage_opts: dict[str, str],
    target_schema: object,
    dry_run: bool = False,
) -> bool:
    """Rename ``name`` -> ``description`` in a normalized snapshot Delta table.

    Returns True if the table was migrated, False if it was already migrated
    or doesn't exist.
    """
    try:
        dt = DeltaTable(table_path, storage_options=storage_opts)
    except Exception as exc:
        print(f"  Table not found or unreadable: {table_path}")
        print(f"  Error: {exc}")
        return False

    table = dt.to_pyarrow_table()
    if "description" in table.column_names and "name" not in table.column_names:
        print(f"  Already migrated (has description, no name): {table_path}")
        return False

    if "name" not in table.column_names:
        print(f"  Already migrated (no name column): {table_path}")
        return False

    print(f"  Migrating: {table_path} ({table.num_rows} rows)")
    new_table = table.rename_columns({"name": "description"})

    if new_table.schema != target_schema:
        print("  ERROR: Schema mismatch after migration!")
        print(f"  Expected: {target_schema}")
        print(f"  Got: {new_table.schema}")
        return False

    if dry_run:
        print(f"  [DRY RUN] Would overwrite with {new_table.num_rows} rows")
        print(f"  [DRY RUN] New columns: {new_table.column_names}")
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Unify snapshot schemas: rename name -> description "
        "for trading212_snapshot and xtb_snapshot"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    args = parser.parse_args()

    storage = get_storage()
    storage_opts = _get_storage_options_with_credentials()

    print("Migrating normalized snapshot tables: rename name -> description...")
    if args.dry_run:
        print("[DRY RUN MODE - no changes will be made]")
    print()

    migrated = 0
    for table_name, target_schema in _RENAME_NAME_TO_DESCRIPTION.items():
        table_path = storage.normalized_path(table_name)
        print(f"Checking {table_name}...")
        if rename_name_to_description(
            table_name,
            table_path,
            storage_opts,
            target_schema,
            dry_run=args.dry_run,
        ):
            migrated += 1

    print(f"\nMigration complete. {migrated} table(s) migrated.")
    if args.dry_run:
        print("[DRY RUN - no changes were made]")


if __name__ == "__main__":
    main()
