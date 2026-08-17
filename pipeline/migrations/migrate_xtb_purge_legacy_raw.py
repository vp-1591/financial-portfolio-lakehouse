"""Migration script: Purge legacy XTB raw rows (source in OPEN POSITION / CASH OPERATION).

Stage 3 of the XTB overhaul (D17). The new parser handles only the new-format
workbook, and both transforms gate on ``source == "XTB_REPORT"`` (shared
bronze, D17). Legacy raw rows with ``source == "OPEN POSITION"`` (the old
``fetch_snapshot``) or ``source == "CASH OPERATION"`` (the old ``fetch_cdc``,
removed by D17) are skipped by the transforms but linger in the
``xtb_snapshot`` raw table from pre-overhaul runs. This migration purges them
so the raw table contains only new-format rows.

The orphaned ``xtb_cdc`` raw table (``raw/xtb_cdc``) is no longer written or
read (D17 — CDC is now produced from the snapshot raw via
``cdc_raw_layer = "snapshot"``). No action is taken against it here; it is
abandoned in place. It can be deleted out-of-band if desired, but leaving it
is harmless (the pipeline never references it again).

Idempotent: skips the table when it is absent and skips the overwrite when
no legacy rows are present (already migrated).

Run this script BEFORE deploying the code that reads the shared bronze raw,
so the legacy rows do not accumulate. The transforms already skip them, so
this is cleanup, not a correctness gate.

Usage:
    .venv/Scripts/python -m pipeline.migrations.migrate_xtb_purge_legacy_raw \
        --mode (docker|staging|prod) [--dry-run]

Requires the same environment variables as the main pipeline (ENCRYPTION_KEY,
S3_BUCKET or PIPELINE_DATA_DIR, etc.).
"""

from __future__ import annotations

import argparse

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from pipeline.raw.models import RAW_SCHEMA
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


# Legacy source values to purge (the old fetch_snapshot / fetch_cdc sources).
_LEGACY_SOURCES = {"OPEN POSITION", "CASH OPERATION"}


def purge_legacy_raw(
    table_path: str,
    storage_opts: dict[str, str],
    dry_run: bool = False,
) -> bool:
    """Purge legacy-source rows from a raw Delta table.

    Returns True if the table was rewritten, False if it was absent or had no
    legacy rows (already migrated).
    """
    try:
        dt = DeltaTable(table_path, storage_options=storage_opts)
    except TableNotFoundError:
        print(f"  Table not found (absent), skipping: {table_path}")
        return False

    table = dt.to_pyarrow_table()
    sources = table.column("source").to_pylist()
    legacy_mask = [src in _LEGACY_SOURCES for src in sources]
    legacy_count = sum(legacy_mask)

    if legacy_count == 0:
        print(f"  No legacy rows (already migrated): {table_path}")
        return False

    # Keep only non-legacy rows.
    keep_mask = [not m for m in legacy_mask]
    keep_indices = [i for i, keep in enumerate(keep_mask) if keep]
    new_table = (
        table.take(keep_indices)
        if keep_indices
        else pa.table(
            {field.name: pa.array([], type=field.type) for field in RAW_SCHEMA},
            schema=RAW_SCHEMA,
        )
    )

    print(
        f"  Purging {legacy_count} legacy row(s) from {table_path} "
        f"({table.num_rows} -> {new_table.num_rows} rows)"
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Purge legacy XTB raw rows (source in OPEN POSITION / "
        "CASH OPERATION) from xtb_snapshot raw."
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=("docker", "staging", "prod"),
        help="Execution mode (which S3/env to migrate). Run BEFORE deploying "
        "the shared-bronze transform code, so legacy rows do not accumulate.",
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

    print("Purging legacy XTB raw rows (source in OPEN POSITION / CASH OPERATION)...")
    if args.dry_run:
        print("[DRY RUN MODE - no changes will be made]")
    print()

    # xtb_snapshot raw: purge legacy-source rows. The orphaned xtb_cdc raw
    # table (raw/xtb_cdc) is abandoned per D17 — no action needed.
    table_path = storage.raw_path("xtb_snapshot")
    print(f"Checking xtb_snapshot raw at {table_path}...")
    migrated = purge_legacy_raw(table_path, storage_opts, dry_run=args.dry_run)

    print(
        f"\nMigration complete. {'1 table migrated.' if migrated else 'No tables needed migration.'}"
    )
    if args.dry_run:
        print("[DRY RUN - no changes were made]")


if __name__ == "__main__":
    main()
