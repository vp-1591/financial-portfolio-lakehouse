"""Migration script: Rename overloaded `currency` columns across all Delta tables.

Run this script AFTER deploying the code changes from ADR 0074.
Existing Delta tables still have a `currency` column; the new code expects
`value_currency`, `base_currency`, or no `currency` at all (snapshots).

Without this migration, Delta Lake will reject appends due to schema mismatch
and `pa.concat_tables(..., schema=...)` will fail on CDC consolidation.

Usage:
    .venv/Scripts/python scripts/migrate_rename_currency_columns.py [--dry-run]

Requires the same environment variables as the main pipeline (ENCRYPTION_KEY,
S3_BUCKET or PIPELINE_DATA_DIR, etc.).
"""

from __future__ import annotations

import argparse

import boto3
from deltalake import write_deltalake

from pipeline.normalized.models import (
    cdc_events_normalized_schema,
    consolidated_holdings_schema,
    ibkr_snapshot_normalized_schema,
    trading212_snapshot_normalized_schema,
    xtb_snapshot_normalized_schema,
)
from pipeline.analytics.models import (
    cash_flow_summary_schema,
    dividend_income_schema,
    interest_income_schema,
    portfolio_holdings_schema,
)
from pipeline.storage import get_storage


def _get_storage_options_with_credentials() -> dict[str, str]:
    """Resolve storage options using boto3 for credential discovery."""
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


# Tables that need `currency` dropped (no replacement column).
_DROP_CURRENCY = {
    "ibkr_snapshot": (ibkr_snapshot_normalized_schema, "currency"),
    "trading212_snapshot": (trading212_snapshot_normalized_schema, "currency"),
    "xtb_snapshot": (xtb_snapshot_normalized_schema, "currency"),
}

# Tables where `currency` is renamed to `value_currency`.
_RENAME_TO_VALUE_CURRENCY = {
    "ibkr_cdc": (cdc_events_normalized_schema, "currency", "value_currency"),
    "trading212_cdc": (cdc_events_normalized_schema, "currency", "value_currency"),
    "xtb_cdc": (cdc_events_normalized_schema, "currency", "value_currency"),
    "cdc_events": (cdc_events_normalized_schema, "currency", "value_currency"),
    "portfolio_holdings": (portfolio_holdings_schema, "currency", "value_currency"),
    "dividend_income": (dividend_income_schema, "currency", "value_currency"),
    "interest_income": (interest_income_schema, "currency", "value_currency"),
    "cash_flow_summary": (cash_flow_summary_schema, "currency", "value_currency"),
}

# Tables where `currency` is renamed to `base_currency`.
_RENAME_TO_BASE_CURRENCY = {
    "consolidated_holdings": (
        consolidated_holdings_schema,
        "currency",
        "base_currency",
    ),
}


def migrate_table_drop(
    table_name: str,
    table_path: str,
    storage_opts: dict[str, str],
    target_schema: object,
    drop_col: str,
    dry_run: bool = False,
) -> bool:
    """Drop a column from a Delta table and overwrite with the target schema."""
    from deltalake import DeltaTable

    try:
        dt = DeltaTable(table_path, storage_options=storage_opts)
    except Exception as exc:
        print(f"  Table not found or unreadable: {table_path}")
        print(f"  Error: {exc}")
        return False

    table = dt.to_pyarrow_table()
    if drop_col not in table.column_names:
        print(f"  Already migrated (no {drop_col} column): {table_path}")
        return False

    print(f"  Migrating: {table_path} ({table.num_rows} rows)")
    new_table = table.drop([drop_col])

    if new_table.schema != target_schema:
        print("  ERROR: Schema mismatch after migration!")
        print(f"  Expected: {target_schema}")
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


def migrate_table_rename(
    table_name: str,
    table_path: str,
    storage_opts: dict[str, str],
    target_schema: object,
    old_col: str,
    new_col: str,
    dry_run: bool = False,
) -> bool:
    """Rename a column in a Delta table and overwrite with the target schema."""
    from deltalake import DeltaTable

    try:
        dt = DeltaTable(table_path, storage_options=storage_opts)
    except Exception as exc:
        print(f"  Table not found or unreadable: {table_path}")
        print(f"  Error: {exc}")
        return False

    table = dt.to_pyarrow_table()
    if old_col not in table.column_names:
        print(f"  Already migrated (no {old_col} column): {table_path}")
        return False

    if new_col in table.column_names:
        print(f"  Already migrated ({new_col} already exists): {table_path}")
        return False

    print(f"  Migrating: {table_path} ({table.num_rows} rows)")
    new_table = table.rename_columns({old_col: new_col})

    if new_table.schema != target_schema:
        print("  ERROR: Schema mismatch after migration!")
        print(f"  Expected: {target_schema}")
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
        description="Rename overloaded currency columns across Delta tables (ADR 0074)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    args = parser.parse_args()

    storage = get_storage()
    storage_opts = _get_storage_options_with_credentials()

    print("Migrating Delta tables: rename/drop overloaded currency columns...")
    if args.dry_run:
        print("[DRY RUN MODE - no changes will be made]")
    print()

    migrated = 0

    # Drop `currency` from snapshot tables
    for table_name, (target_schema, drop_col) in _DROP_CURRENCY.items():
        table_path = storage.normalized_path(table_name)
        print(f"Checking {table_name}...")
        if migrate_table_drop(
            table_name,
            table_path,
            storage_opts,
            target_schema,
            drop_col,
            dry_run=args.dry_run,
        ):
            migrated += 1

    # Rename `currency` → `value_currency`
    for table_name, (
        target_schema,
        old_col,
        new_col,
    ) in _RENAME_TO_VALUE_CURRENCY.items():
        table_path = (
            storage.analytics_path(table_name)
            if table_name
            in (
                "portfolio_holdings",
                "dividend_income",
                "interest_income",
                "cash_flow_summary",
            )
            else storage.normalized_path(table_name)
        )
        print(f"Checking {table_name}...")
        if migrate_table_rename(
            table_name,
            table_path,
            storage_opts,
            target_schema,
            old_col,
            new_col,
            dry_run=args.dry_run,
        ):
            migrated += 1

    # Rename `currency` → `base_currency`
    for table_name, (
        target_schema,
        old_col,
        new_col,
    ) in _RENAME_TO_BASE_CURRENCY.items():
        table_path = storage.normalized_path(table_name)
        print(f"Checking {table_name}...")
        if migrate_table_rename(
            table_name,
            table_path,
            storage_opts,
            target_schema,
            old_col,
            new_col,
            dry_run=args.dry_run,
        ):
            migrated += 1

    print(f"\nMigration complete. {migrated} table(s) migrated.")
    if args.dry_run:
        print("[DRY RUN - no changes were made]")


if __name__ == "__main__":
    main()
