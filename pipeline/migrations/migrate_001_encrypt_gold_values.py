"""Migration 001: Encrypt gold-layer value columns and add percentage.

Prior to ADR 0084, gold tables stored value columns (security_value,
target_value, cash_amount) as plaintext ``pa.float64()``.  ADR 0084
changed the schemas to store them as ``pa.binary()`` (Fernet-encrypted).
ADR 0082 added a ``percentage`` column to ``portfolio_holdings``.

Existing Delta tables on S3 still have the old schema.  This migration
rewrites them in-place to match the current schemas defined in
``pipeline.analytics.models``.

The migration is **idempotent**: if a column already has the target type
(``binary``) or the ``percentage`` column already exists, it is skipped.
"""

from __future__ import annotations

import logging

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from pipeline.analytics.models import (
    cash_flow_summary_schema,
    dividend_income_schema,
    interest_income_schema,
    portfolio_holdings_schema,
)
from pipeline.crypto import encrypt_float, load_key
from pipeline.storage import get_storage

logger = logging.getLogger(__name__)

# Tables that need value-column encryption, keyed by table name.
# Each value maps old column name → (old_type, new_schema_field).
# We only encrypt float → binary; we skip columns that are already binary.
GOLD_TABLES: dict[str, pa.Schema] = {
    "portfolio_holdings": portfolio_holdings_schema,
    "dividend_income": dividend_income_schema,
    "interest_income": interest_income_schema,
    "cash_flow_summary": cash_flow_summary_schema,
}

# Value columns to encrypt, per table.  Only float64 columns that should
# become binary are listed here.
ENCRYPT_COLUMNS: dict[str, list[str]] = {
    "portfolio_holdings": ["security_value", "target_value"],
    "dividend_income": ["cash_amount", "target_value"],
    "interest_income": ["cash_amount", "target_value"],
    "cash_flow_summary": ["cash_amount", "target_value"],
}


def _needs_migration(
    table: pa.Table, schema: pa.Schema, encrypt_cols: list[str]
) -> bool:
    """Return True if the table needs any migration work."""
    # Check for missing columns (e.g. percentage in portfolio_holdings).
    for field in schema:
        if field.name not in table.column_names:
            return True

    # Check for type mismatches on encrypted columns.
    for col_name in encrypt_cols:
        if col_name in table.column_names:
            actual_type = table.schema.field(col_name).type
            expected_type = schema.field(col_name).type
            if actual_type != expected_type:
                return True

    return False


def migrate_table(table_name: str, fernet_key: bytes) -> bool:
    """Migrate a single gold table to the current schema.

    Returns True if the table was migrated, False if it was already
    up-to-date or didn't exist.
    """
    storage = get_storage()
    storage_opts = storage.storage_options
    table_path = storage.analytics_path(table_name)
    schema = GOLD_TABLES[table_name]
    encrypt_cols = ENCRYPT_COLUMNS.get(table_name, [])

    try:
        dt = DeltaTable(table_path, storage_options=storage_opts)
    except Exception:
        logger.info("%s: table not found at %s — skipping", table_name, table_path)
        return False

    arrow = dt.to_pyarrow_table()
    logger.info(
        "%s: loaded %d rows, schema has %d columns",
        table_name,
        arrow.num_rows,
        len(arrow.schema),
    )

    if not _needs_migration(arrow, schema, encrypt_cols):
        logger.info("%s: already up-to-date — skipping", table_name)
        return False

    import polars as pl

    result = pl.from_arrow(arrow)

    # --- Encrypt float columns that should be binary ---
    for col_name in encrypt_cols:
        if col_name not in result.columns:
            # Column doesn't exist at all — will be handled below
            # when we add missing columns.
            continue

        actual_type = arrow.schema.field(col_name).type
        expected_type = schema.field(col_name).type

        if actual_type == expected_type:
            # Already the correct type (binary) — skip.
            continue

        if pa.types.is_float64(actual_type) and pa.types.is_binary(expected_type):
            logger.info(
                "%s: encrypting column %s (float64 → binary)", table_name, col_name
            )
            result = result.with_columns(
                pl.col(col_name)
                .map_elements(
                    lambda v: encrypt_float(v, fernet_key) if v is not None else None,
                    return_dtype=pl.Binary,
                )
                .alias(col_name),
            )
        else:
            logger.warning(
                "%s: unexpected type for %s: actual=%s, expected=%s — skipping encryption",
                table_name,
                col_name,
                actual_type,
                expected_type,
            )

    # --- Add missing columns ---
    # Special handling for portfolio_holdings.percentage
    if table_name == "portfolio_holdings" and "percentage" not in result.columns:
        logger.info("%s: adding percentage column", table_name)
        if "target_value" in result.columns:
            # target_value is already encrypted — we need to decrypt it first
            # to compute percentage. But if we just encrypted it above, it's
            # now binary. If the original was float64, we already replaced it.
            # Either way, we need the float value.
            from pipeline.crypto import decrypt_float

            tv_col = result["target_value"]
            if (
                pa.types.is_binary(arrow.schema.field("target_value").type)
                and tv_col.dtype == pl.Binary
            ):
                # Was already encrypted in the source — decrypt for computation.
                target_values = [
                    decrypt_float(v, fernet_key) if v is not None else 0.0
                    for v in tv_col.to_list()
                ]
            else:
                # Was just encrypted (float64 → binary in this migration) or
                # still float64.
                # If we just encrypted it, the column is now binary.
                # Check the current Polars dtype.
                if tv_col.dtype == pl.Binary:
                    target_values = [
                        decrypt_float(v, fernet_key) if v is not None else 0.0
                        for v in tv_col.to_list()
                    ]
                else:
                    target_values = tv_col.to_list()

            total = sum(v for v in target_values if v is not None)
            if total == 0:
                percentages = [0.0] * len(target_values)
            else:
                percentages = [round(v / total * 100, 4) for v in target_values]

            result = result.with_columns(
                pl.Series("percentage", percentages, dtype=pl.Float64)
            )
        else:
            # No target_value at all — fill with 0.0
            result = result.with_columns(pl.lit(0.0).alias("percentage"))

    # --- Cast to match the expected schema ---
    arrow_result = result.to_arrow()
    casted = {}
    for field in schema:
        col_name = field.name
        if col_name in arrow_result.column_names:
            casted[col_name] = arrow_result.column(col_name).cast(field.type)
        else:
            # Column missing even after migration — fill with nulls.
            logger.warning(
                "%s: filling missing column %s with nulls", table_name, col_name
            )
            casted[col_name] = pa.nulls(arrow_result.num_rows, field.type)

    final = pa.table(casted, schema=schema)

    storage.backend.ensure_parent(table_path)
    write_deltalake(table_path, final, mode="overwrite", storage_options=storage_opts)
    logger.info(
        "%s: migration complete — %d rows written with schema v%d",
        table_name,
        final.num_rows,
        len(schema),
    )
    return True


def run_migration(fernet_key: bytes | None = None) -> int:
    """Run all gold-table migrations.  Returns 0 on success, 1 on failure."""
    if fernet_key is None:
        fernet_key = load_key()

    migrated = 0
    failed = 0

    for table_name in GOLD_TABLES:
        try:
            if migrate_table(table_name, fernet_key):
                migrated += 1
        except Exception:
            logger.exception("%s: migration failed", table_name)
            failed += 1

    logger.info(
        "Migration summary: %d migrated, %d failed, %d skipped",
        migrated,
        failed,
        len(GOLD_TABLES) - migrated - failed,
    )
    return 1 if failed else 0
