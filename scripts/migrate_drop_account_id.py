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

from deltalake import write_deltalake

from pipeline.raw.models import RAW_SCHEMA
from pipeline.storage import get_storage


def drop_account_id_from_table(table_path: str, dry_run: bool = False) -> bool:
    """Drop the account_id column from a raw Delta table.

    Returns True if the table was migrated, False if it was already migrated
    or doesn't exist.
    """
    try:
        from deltalake import DeltaTable

        storage_opts = get_storage().storage_options
        dt = DeltaTable(table_path, storage_options=storage_opts)
    except Exception:
        print(f"  Table not found or unreadable: {table_path}")
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
        table_path, new_table, mode="overwrite", storage_options=storage_opts
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
        if drop_account_id_from_table(table_path, dry_run=args.dry_run):
            migrated += 1

    print(f"\nMigration complete. {migrated} table(s) migrated.")
    if args.dry_run:
        print("[DRY RUN - no changes were made]")


if __name__ == "__main__":
    main()
