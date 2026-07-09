"""Migration script: Drop conid column from normalized ibkr_snapshot Delta table.

The normalized ibkr_snapshot table in S3 has a legacy ``conid`` column (12 fields)
that the current code no longer produces (11 fields). Without migration, Delta Lake
rejects writes due to schema mismatch:

    SchemaMismatchError: Cannot cast schema, number of fields does not match: 11 vs 12

Run this script BEFORE deploying code that removed the conid column.

Usage:
    .venv/Scripts/python scripts/migrate_drop_conid.py [--dry-run]

Requires the same environment variables as the main pipeline (ENCRYPTION_KEY,
S3_BUCKET or PIPELINE_DATA_DIR, etc.).
"""

from __future__ import annotations

import argparse

import boto3
from deltalake import DeltaTable, write_deltalake

from pipeline.storage import get_storage


def _get_storage_options_with_credentials() -> dict[str, str]:
    """Resolve storage options using boto3 for credential discovery.

    The deltalake Rust backend (object_store) cannot read AWS credential
    files on all platforms.  Use boto3 (which handles credential chains
    correctly) to discover credentials and pass them explicitly.
    """
    storage = get_storage()
    opts = dict(storage.storage_options or {})

    # Only inject credentials if they aren't already present.
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


def drop_conid_from_table(
    table_path: str, storage_opts: dict[str, str], dry_run: bool = False
) -> bool:
    """Drop the conid column from the normalized ibkr_snapshot Delta table.

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
    if "conid" not in table.column_names:
        print(f"  Already migrated (no conid column): {table_path}")
        return False

    print(f"  Migrating: {table_path} ({table.num_rows} rows)")
    new_table = table.drop(["conid"])

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
        description="Drop conid column from normalized ibkr_snapshot Delta table"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    args = parser.parse_args()

    storage = get_storage()
    storage_opts = _get_storage_options_with_credentials()

    table_name = "ibkr_snapshot"
    table_path = storage.normalized_path(table_name)

    print(f"Migrating normalized/{table_name} to drop conid column...")
    if args.dry_run:
        print("[DRY RUN MODE - no changes will be made]")
    print()

    drop_conid_from_table(table_path, storage_opts, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
