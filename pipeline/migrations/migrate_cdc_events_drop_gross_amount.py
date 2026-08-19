"""Migration script: Drop the YAGNI ``gross_amount`` column from CDC tables.

The broker-neutral ``cdc_events_normalized_schema`` no longer carries
``gross_amount`` (YAGNI: ``cash_amount`` already holds the signed cash impact).
The four CDC normalized Delta tables that ``quality.check_schema`` validates
against that schema (``cdc_events``, ``ibkr_cdc``, ``trading212_cdc``,
``xtb_cdc``) still have the stale column from pre-change runs. Without this
migration, a deploy's validate step would FAIL, flagging ``gross_amount`` as
an "extra field" in those tables before the next pipeline run's transform
overwrites them with the new schema.

This migration drops ``gross_amount`` from each table so the existing CDC
tables match ``cdc_events_normalized_schema`` again.

Idempotent: skips the table when it is absent and skips the overwrite when
the table already lacks the ``gross_amount`` column (already migrated).

A genuinely absent table is skipped (exit 0). An existing but unreadable
table (auth/region/permission/I-O error) or an unexpected schema raises and
exits non-zero, so a pre-deploy gate cannot mistake a real failure for
"nothing to migrate".

Run this script BEFORE deploying the schema-change code, so the existing CDC
tables do not fail ``quality.check_schema`` with an "extra field" on deploy.
The connector writes the normalized CDC with ``write_deltalake(mode="overwrite")``
and no ``schema_mode``, so overwriting an existing ``gross_amount``-column
table without the column raises ``SchemaMismatchError``; this migration drops
the column first so the next pipeline run's transform write succeeds.

Usage:
    .venv/Scripts/python -m pipeline.migrations.migrate_cdc_events_drop_gross_amount \
        --mode (docker|staging|prod) [--dry-run]

Requires the same environment variables as the main pipeline (ENCRYPTION_KEY,
S3_BUCKET or PIPELINE_DATA_DIR, etc.).
"""

from __future__ import annotations

import argparse

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from pipeline.normalized.models import cdc_events_normalized_schema
from pipeline.secrets import _boto3_default_chain_credentials
from pipeline.storage import get_storage


def _get_storage_options_with_credentials() -> dict[str, str]:
    """Resolve storage options, injecting AWS credentials via boto3.

    The deltalake Rust backend (object_store) cannot read AWS credential
    files on all platforms.  Use
    :func:`pipeline.secrets._boto3_default_chain_credentials` (boto3's
    default chain) to discover credentials and pass them explicitly
    when not already present in the storage options.
    """
    storage = get_storage()
    opts = dict(storage.storage_options or {})

    if "aws_access_key_id" not in opts:
        boto = _boto3_default_chain_credentials(opts.get("aws_region", "eu-west-1"))
        if boto is not None:
            key_id, secret_key, token = boto
            opts["aws_access_key_id"] = key_id
            opts["aws_secret_access_key"] = secret_key
            if token:
                opts["aws_session_token"] = token

    return opts


# Broker-neutral CDC normalized tables that quality.check_schema validates
# against cdc_events_normalized_schema (pipeline/analytics/quality.py).
_CDC_TABLES: tuple[str, ...] = ("cdc_events", "ibkr_cdc", "trading212_cdc", "xtb_cdc")


def drop_gross_amount(
    table_path: str,
    storage_opts: dict[str, str],
    dry_run: bool = False,
) -> bool:
    """Drop the ``gross_amount`` column from a CDC normalized Delta table.

    Returns True if the table was rewritten, False if it was absent or had no
    ``gross_amount`` column (already migrated).
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
    if "gross_amount" not in table.column_names:
        print(f"  Already migrated (no gross_amount column): {table_path}")
        return False

    new_table: pa.Table = table.drop(["gross_amount"])

    # Order-sensitive guard: schema.equals (like quality.check_schema) fails
    # on any leftover unexpected column or a column placed out of order.
    if not new_table.schema.equals(cdc_events_normalized_schema):
        raise RuntimeError(
            f"Schema mismatch after migration for {table_path}: "
            f"expected {cdc_events_normalized_schema}, got {new_table.schema}"
        )

    print(f"  Migrating: {table_path} ({table.num_rows} rows, dropping gross_amount)")

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
        description="Drop the YAGNI gross_amount column from the CDC "
        "normalized tables (cdc_events, ibkr_cdc, trading212_cdc, xtb_cdc)."
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=("docker", "staging", "prod"),
        help="Execution mode (which S3/env to migrate). Run BEFORE deploying "
        "the schema-change code, otherwise the deploy's validate step flags "
        "gross_amount as an extra field in the CDC tables.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    args = parser.parse_args()

    from pipeline.secrets import load_env, set_mode

    load_env()
    set_mode(args.mode)

    storage = get_storage()
    storage_opts = _get_storage_options_with_credentials()

    print("Dropping gross_amount from CDC normalized tables...")
    if args.dry_run:
        print("[DRY RUN MODE - no changes will be made]")
    print()

    migrated = 0
    for table_name in _CDC_TABLES:
        table_path = storage.normalized_path(table_name)
        print(f"Checking {table_name}...")
        if drop_gross_amount(table_path, storage_opts, dry_run=args.dry_run):
            migrated += 1

    print(f"\nMigration complete. {migrated} table(s) migrated.")
    if args.dry_run:
        print("[DRY RUN - no changes were made]")


if __name__ == "__main__":
    main()
