"""Migration script: Drop account_id column from raw Delta tables.

Run this script BEFORE deploying the code changes from ADR 0047.
Existing raw Delta tables have 7 columns (including account_id), but the
new code produces 6-column tables. Without migration, Delta Lake will
reject appends due to schema mismatch.

Usage:
    .venv/Scripts/python scripts/migrate_drop_account_id.py [--dry-run]

Requires the same environment variables as the main pipeline (ENCRYPTION_KEY,
S3_BUCKET or PIPELINE_DATA_DIR, etc.).
"""

from __future__ import annotations

import argparse

import boto3
from deltalake import write_deltalake

from pipeline.raw.models import RAW_SCHEMA
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


def drop_account_id_from_table(
    table_path: str, storage_opts: dict[str, str], dry_run: bool = False
) -> bool:
    """Drop the account_id column from a raw Delta table.

    Returns True if the table was migrated, False if it was already migrated
    or doesn't exist.
    """
    try:
        from deltalake import DeltaTable

        dt = DeltaTable(table_path, storage_options=storage_opts)
    except Exception as exc:
        print(f"  Table not found or unreadable: {table_path}")
        print(f"  Error: {exc}")
        return False

    table = dt.to_pyarrow_table()
    if "account_id" not in table.column_names:
        print(f"  Already migrated (no account_id column): {table_path}")
        return False

    print(f"  Migrating: {table_path} ({table.num_rows} rows)")
    new_table = table.drop(["account_id"])

    # Verify the new schema matches RAW_SCHEMA
    if new_table.schema != RAW_SCHEMA:
        print("  ERROR: Schema mismatch after migration!")
        print(f"  Expected: {RAW_SCHEMA}")
        print(f"  Got: {new_table.schema}")
        return False

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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Drop account_id column from raw Delta tables (ADR 0047)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    args = parser.parse_args()

    storage = get_storage()
    storage_opts = _get_storage_options_with_credentials()
    brokers_tables = [
        ("ibkr", "snapshot"),
        ("ibkr", "cdc"),
        ("trading212", "snapshot"),
        ("trading212", "cdc"),
        ("xtb", "snapshot"),
        ("xtb", "cdc"),
    ]

    print("Migrating raw Delta tables to drop account_id column...")
    if args.dry_run:
        print("[DRY RUN MODE - no changes will be made]")
    print()

    migrated = 0
    for broker, table_type in brokers_tables:
        table_name = f"{broker}_{table_type}"
        table_path = storage.raw_path(table_name)
        print(f"Checking {table_name}...")
        if drop_account_id_from_table(table_path, storage_opts, dry_run=args.dry_run):
            migrated += 1

    print(f"\nMigration complete. {migrated} table(s) migrated.")
    if args.dry_run:
        print("[DRY RUN - no changes were made]")


if __name__ == "__main__":
    main()
