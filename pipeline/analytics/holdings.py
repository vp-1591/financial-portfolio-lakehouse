"""Consolidated holdings → portfolio holdings gold table.

Reads ``consolidated_holdings`` (which includes both ``security_value`` in native
currency and ``target_value`` in EUR, plus ``position_type``) and produces a gold
table suitable for the report's portfolio summary section.

Previously this function re-read broker snapshots to recover ``security_value``
and ``position_type`` that were dropped during consolidation.  Now these columns
are stored directly in ``consolidated_holdings``, so no snapshot join is needed.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from pipeline.analytics.models import portfolio_holdings_schema
from pipeline.crypto import decrypt_float

logger = logging.getLogger(__name__)


def build_portfolio_holdings(
    table_path: str | None = None,
    fernet_key: bytes | None = None,
    analytics_path: str | None = None,
) -> pa.Table:
    """Build the ``portfolio_holdings`` analytics table.

    Reads ``consolidated_holdings`` (which now includes ``security_value``,
    ``security_ccy``, and ``position_type``) and produces a gold table with
    both native and target values plus ``position_type``.

    Parameters
    ----------
    table_path:
        Path to the ``consolidated_holdings`` Delta table.
        Defaults to the normalized-layer path from storage config.
    fernet_key:
        Fernet key for decrypting value columns.
        When *None*, loaded from the default location.
    analytics_path:
        Path to write the ``portfolio_holdings`` Delta table.
        Defaults to the analytics-layer path from storage config.
    """
    from pipeline.storage import get_storage

    storage = get_storage()
    storage_opts = storage.storage_options

    if table_path is None:
        table_path = storage.normalized_path("consolidated_holdings")

    if fernet_key is None:
        from pipeline.crypto import load_key

        fernet_key = load_key()

    if analytics_path is None:
        analytics_path = storage.analytics_path("portfolio_holdings")

    # ------------------------------------------------------------------
    # 1. Read consolidated_holdings and decrypt both value columns
    # ------------------------------------------------------------------
    try:
        dt = DeltaTable(table_path, storage_options=storage_opts)
    except Exception as exc:
        raise FileNotFoundError(
            f"Consolidated holdings table not found at {table_path}. "
            "Run the consolidate step first to populate the table."
        ) from exc

    arrow_table = dt.to_pyarrow_table()
    cons = pl.from_arrow(arrow_table)

    # Decrypt both value columns (native-currency and target-currency)
    cons = cons.with_columns(
        pl.col("security_value")
        .map_elements(
            lambda v: decrypt_float(v, fernet_key),
            return_dtype=pl.Float64,
        )
        .alias("security_value_decrypted"),
        pl.col("target_value")
        .map_elements(
            lambda v: decrypt_float(v, fernet_key),
            return_dtype=pl.Float64,
        )
        .alias("target_value_decrypted"),
    )

    # ------------------------------------------------------------------
    # 2. Build the final table matching the schema
    # ------------------------------------------------------------------
    now = datetime.now(timezone.utc)

    result = cons.select(
        [
            pl.lit(now).alias("calculated_at"),
            "broker",
            "ticker",
            "security_ccy",
            "security_value_decrypted",
            "target_value_decrypted",
            "target_ccy",
            "position_type",
            "identifier",
            "description",
        ]
    )

    # Rename final columns to match schema
    result = result.rename(
        {
            "security_value_decrypted": "security_value",
            "target_value_decrypted": "target_value",
        }
    )

    # Compute percentage: (target_value / total_target_value) * 100, rounded to 4 dp
    total_target = result["target_value"].sum()
    result = result.with_columns(
        ((pl.col("target_value") / total_target) * 100).round(4).alias("percentage")
    )

    # Convert to PyArrow and cast to match the schema
    arrow_result = result.to_arrow()
    casted = {}
    for i, field in enumerate(portfolio_holdings_schema):
        col_name = field.name
        if col_name in arrow_result.column_names:
            casted[col_name] = arrow_result.column(col_name).cast(field.type)
        else:
            raise ValueError(f"Missing column {col_name} in result table")

    final = pa.table(casted, schema=portfolio_holdings_schema)

    storage.backend.ensure_parent(analytics_path)
    write_deltalake(
        analytics_path, final, mode="overwrite", storage_options=storage_opts
    )
    return final
