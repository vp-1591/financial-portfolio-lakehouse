"""Migration script: Apply Phase 2 + Phase 3 schema changes to Delta tables.

Phase 2 (ADR 0077) renamed overloaded column names:
  - ``value_currency`` / ``security_currency`` → ``security_ccy``
  - ``base_currency`` → ``target_ccy``
  - ``value`` / ``value_base`` → ``security_value`` / ``target_value``
  - ``fx_rate_to_base`` → ``target_fx_rate``
  - ``amount_base`` → ``target_value``
  - Dropped ``net_amount`` from CDC events

Phase 3 (ADR 0078/0079/0080) added new columns:
  - ``instrument_ccy`` in CDC events (nullable, populated by normalize_currency)
  - ``security_value`` and ``position_type`` in consolidated holdings
  - ``instrument_ccy`` in dividend_income

Run this script BEFORE deploying the Phase 2+3 code.  Existing Delta tables
still have the old column names; the new code expects the new names.

Usage:
    .venv/Scripts/python scripts/migrate_phase2_phase3_schema.py [--dry-run]

Requires the same environment variables as the main pipeline (ENCRYPTION_KEY,
S3_BUCKET or PIPELINE_DATA_DIR, etc.).
"""

from __future__ import annotations

import argparse

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from pipeline.analytics.models import (
    cash_flow_summary_schema,
    dividend_income_schema,
    interest_income_schema,
    portfolio_allocation_schema,
    portfolio_holdings_schema,
)
from pipeline.normalized.models import (
    cdc_events_normalized_schema,
    consolidated_holdings_schema,
    ibkr_snapshot_normalized_schema,
    trading212_snapshot_normalized_schema,
    xtb_snapshot_normalized_schema,
)
from pipeline.storage import get_storage


def _get_storage_options_with_credentials() -> dict[str, str]:
    """Resolve storage options using boto3 for credential discovery."""
    storage = get_storage()
    opts = dict(storage.storage_options or {})

    if "aws_access_key_id" not in opts:
        import boto3

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


# ---------------------------------------------------------------------------
# Table definitions: (storage_subpath, target_schema, layer)
# layer = "normalized" | "analytics"
# ---------------------------------------------------------------------------

_SNAPSHOTS = {
    "ibkr_snapshot": (ibkr_snapshot_normalized_schema, "normalized"),
    "trading212_snapshot": (trading212_snapshot_normalized_schema, "normalized"),
    "xtb_snapshot": (xtb_snapshot_normalized_schema, "normalized"),
}

_CDC = {
    "ibkr_cdc": (cdc_events_normalized_schema, "normalized"),
    "trading212_cdc": (cdc_events_normalized_schema, "normalized"),
    "xtb_cdc": (cdc_events_normalized_schema, "normalized"),
    "cdc_events": (cdc_events_normalized_schema, "normalized"),
}

_ANALYTICS = {
    "portfolio_allocation": (portfolio_allocation_schema, "analytics"),
    "portfolio_holdings": (portfolio_holdings_schema, "analytics"),
    "dividend_income": (dividend_income_schema, "analytics"),
    "interest_income": (interest_income_schema, "analytics"),
    "cash_flow_summary": (cash_flow_summary_schema, "analytics"),
}


def _resolve_path(table_name: str, layer: str, storage) -> str:
    """Return the S3 or local path for a given table."""
    if layer == "analytics":
        return storage.analytics_path(table_name)
    return storage.normalized_path(table_name)


def _rename_and_migrate(
    table: pa.Table,
    renames: dict[str, str],
    drops: list[str],
    adds: dict[str, pa.Field],
    target_schema: pa.schema,
) -> pa.Table:
    """Apply renames, drops, and adds to align a table with target_schema.

    1. Rename columns (old → new names).
    2. Drop columns that no longer exist.
    3. Add new nullable columns with null values.
    4. Cast to target_schema to enforce column order and types.

    Returns the migrated table.
    """
    # Step 1: rename
    existing_renames = {
        old: new for old, new in renames.items() if old in table.column_names
    }
    if existing_renames:
        table = table.rename_columns(
            [existing_renames.get(c, c) for c in table.column_names]
        )

    # Step 2: drop
    cols_to_drop = [c for c in drops if c in table.column_names]
    if cols_to_drop:
        table = table.drop(cols_to_drop)

    # Step 3: add new nullable columns
    for col_name, field in adds.items():
        if col_name not in table.column_names:
            null_col = pa.array([None] * table.num_rows, type=field.type)
            table = table.append_column(field.name, null_col)

    # Step 4: reorder + cast to target schema
    result = {}
    for field in target_schema:
        if field.name in table.column_names:
            col = table.column(field.name)
            if col.type != field.type:
                col = col.cast(field.type)
            result[field.name] = col
        else:
            # Nullable column missing from the source — fill with nulls
            result[field.name] = pa.array([None] * table.num_rows, type=field.type)

    return pa.table(result, schema=target_schema)


def migrate_table(
    table_name: str,
    table_path: str,
    storage_opts: dict[str, str],
    target_schema: pa.schema,
    renames: dict[str, str],
    drops: list[str],
    adds: dict[str, pa.Field],
    dry_run: bool = False,
) -> bool:
    """Migrate a single Delta table to the target schema."""
    try:
        dt = DeltaTable(table_path, storage_options=storage_opts)
    except Exception as exc:
        print(f"  Table not found or unreadable: {table_path}")
        print(f"  Error: {exc}")
        return False

    table = dt.to_pyarrow_table()
    current_names = set(table.column_names)
    target_names = {f.name for f in target_schema}

    if current_names == target_names:
        # Also check types match
        try:
            table.cast(target_schema)
            print(f"  Already migrated: {table_name}")
            return False
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            # Types don't match yet — need migration
            pass

    print(f"  Migrating: {table_name} ({table.num_rows} rows)")
    print(f"    Current columns: {list(table.column_names)}")
    print(f"    Target columns:  {[f.name for f in target_schema]}")

    new_table = _rename_and_migrate(table, renames, drops, adds, target_schema)

    if dry_run:
        print(f"  [DRY RUN] Would overwrite with {new_table.num_rows} rows")
        print(f"  [DRY RUN] New columns: {list(new_table.column_names)}")
        return True

    write_deltalake(
        table_path,
        new_table,
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=storage_opts,
    )
    print(f"  Done: {table_name}")
    return True


# ---------------------------------------------------------------------------
# Per-table migration specs
# ---------------------------------------------------------------------------

# Snapshot tables: drop value, value_currency, security_currency; add security_value, security_ccy
_SNAPSHOT_RENAMES: dict[
    str, str
] = {}  # no renames — old cols are dropped, new ones added
_SNAPSHOT_DROPS = ["value", "value_currency", "security_currency"]
_SNAPSHOT_ADDS: dict[str, pa.Field] = {
    "security_value": pa.field("security_value", pa.binary()),
    "security_ccy": pa.field("security_ccy", pa.string()),
}

# CDC tables: rename + drop + add instrument_ccy
_CDC_RENAMES = {
    "value_currency": "security_ccy",
    "base_currency": "target_ccy",
    "fx_rate_to_base": "target_fx_rate",
    "amount_base": "target_value",
}
_CDC_DROPS = ["net_amount"]
_CDC_ADDS: dict[str, pa.Field] = {
    "instrument_ccy": pa.field("instrument_ccy", pa.string()),
}

# consolidated_holdings: rename + add security_value, position_type; drop value, security_currency
_HOLDINGS_RENAMES = {
    "base_currency": "target_ccy",
    "value": "target_value",
    "security_currency": "security_ccy",
}
_HOLDINGS_DROPS: list[str] = []  # all old cols are renamed, not dropped
_HOLDINGS_ADDS: dict[str, pa.Field] = {
    "security_value": pa.field("security_value", pa.binary()),
    "position_type": pa.field("position_type", pa.string()),
}

# portfolio_allocation: rename security_currency → security_ccy
_ALLOCATION_RENAMES = {"security_currency": "security_ccy"}
_ALLOCATION_DROPS: list[str] = []
_ALLOCATION_ADDS: dict[str, pa.Field] = {}

# portfolio_holdings: rename + drop security_currency (merged into security_ccy via value_currency)
_HOLDINGS_GOLD_RENAMES = {
    "value_currency": "security_ccy",
    "value": "security_value",
    "value_base": "target_value",
    "base_currency": "target_ccy",
}
_HOLDINGS_GOLD_DROPS = [
    "security_currency"
]  # value_currency renamed to security_ccy supersedes this
_HOLDINGS_GOLD_ADDS: dict[str, pa.Field] = {}

# dividend_income: rename + add instrument_ccy
_DIVIDEND_RENAMES = {
    "value_currency": "security_ccy",
    "amount_base": "target_value",
    "base_currency": "target_ccy",
}
_DIVIDEND_ADDS: dict[str, pa.Field] = {
    "instrument_ccy": pa.field("instrument_ccy", pa.string()),
}

# interest_income: rename
_INTEREST_RENAMES = {
    "value_currency": "security_ccy",
    "amount_base": "target_value",
    "base_currency": "target_ccy",
}
_INTEREST_ADDS: dict[str, pa.Field] = {}

# cash_flow_summary: rename
_CASHFLOW_RENAMES = {
    "value_currency": "security_ccy",
    "amount_base": "target_value",
    "base_currency": "target_ccy",
}
_CASHFLOW_ADDS: dict[str, pa.Field] = {}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apply Phase 2+3 schema changes to Delta tables (ADR 0077/0078/0080)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    args = parser.parse_args()

    storage = get_storage()
    storage_opts = _get_storage_options_with_credentials()

    print("Migrating Delta tables: Phase 2+3 schema changes (ADR 0077/0078/0080)...")
    if args.dry_run:
        print("[DRY RUN MODE - no changes will be made]")
    print()

    migrated = 0

    # Snapshot tables
    for table_name, (target_schema, layer) in _SNAPSHOTS.items():
        table_path = _resolve_path(table_name, layer, storage)
        print(f"Checking {table_name}...")
        if migrate_table(
            table_name,
            table_path,
            storage_opts,
            target_schema,
            _SNAPSHOT_RENAMES,
            _SNAPSHOT_DROPS,
            _SNAPSHOT_ADDS,
            dry_run=args.dry_run,
        ):
            migrated += 1

    # CDC tables
    for table_name, (target_schema, layer) in _CDC.items():
        table_path = _resolve_path(table_name, layer, storage)
        print(f"Checking {table_name}...")
        if migrate_table(
            table_name,
            table_path,
            storage_opts,
            target_schema,
            _CDC_RENAMES,
            _CDC_DROPS,
            _CDC_ADDS,
            dry_run=args.dry_run,
        ):
            migrated += 1

    # consolidated_holdings
    print("Checking consolidated_holdings...")
    if migrate_table(
        "consolidated_holdings",
        storage.normalized_path("consolidated_holdings"),
        storage_opts,
        consolidated_holdings_schema,
        _HOLDINGS_RENAMES,
        _HOLDINGS_DROPS,
        _HOLDINGS_ADDS,
        dry_run=args.dry_run,
    ):
        migrated += 1

    # portfolio_allocation
    print("Checking portfolio_allocation...")
    if migrate_table(
        "portfolio_allocation",
        storage.analytics_path("portfolio_allocation"),
        storage_opts,
        portfolio_allocation_schema,
        _ALLOCATION_RENAMES,
        _ALLOCATION_DROPS,
        _ALLOCATION_ADDS,
        dry_run=args.dry_run,
    ):
        migrated += 1

    # portfolio_holdings
    print("Checking portfolio_holdings...")
    if migrate_table(
        "portfolio_holdings",
        storage.analytics_path("portfolio_holdings"),
        storage_opts,
        portfolio_holdings_schema,
        _HOLDINGS_GOLD_RENAMES,
        _HOLDINGS_GOLD_DROPS,
        _HOLDINGS_GOLD_ADDS,
        dry_run=args.dry_run,
    ):
        migrated += 1

    # dividend_income
    print("Checking dividend_income...")
    if migrate_table(
        "dividend_income",
        storage.analytics_path("dividend_income"),
        storage_opts,
        dividend_income_schema,
        _DIVIDEND_RENAMES,
        [],
        _DIVIDEND_ADDS,
        dry_run=args.dry_run,
    ):
        migrated += 1

    # interest_income
    print("Checking interest_income...")
    if migrate_table(
        "interest_income",
        storage.analytics_path("interest_income"),
        storage_opts,
        interest_income_schema,
        _INTEREST_RENAMES,
        [],
        _INTEREST_ADDS,
        dry_run=args.dry_run,
    ):
        migrated += 1

    # cash_flow_summary
    print("Checking cash_flow_summary...")
    if migrate_table(
        "cash_flow_summary",
        storage.analytics_path("cash_flow_summary"),
        storage_opts,
        cash_flow_summary_schema,
        _CASHFLOW_RENAMES,
        [],
        _CASHFLOW_ADDS,
        dry_run=args.dry_run,
    ):
        migrated += 1

    print(f"\nMigration complete. {migrated} table(s) migrated.")
    if args.dry_run:
        print("[DRY RUN - no changes were made]")


if __name__ == "__main__":
    main()
