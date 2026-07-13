"""Consolidated holdings + per-broker snapshots → portfolio holdings gold table.

Reads ``consolidated_holdings`` (base-currency values, already FX-converted)
and each broker's ``*_snapshot_normalized`` table (native-currency values,
position type).  Joins on ``(broker, ticker)`` to produce a gold table with
both native and base values plus ``position_type`` (EQUITY / CASH / UNKNOWN).
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

    Reads ``consolidated_holdings`` (for base-currency value) and each broker's
    normalized snapshot (for native-currency value, currency, and position type),
    then joins them on ``(broker, ticker)`` to produce a gold table suitable
    for the report's portfolio summary section.

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
    from pipeline.connectors.registry import all as all_connectors
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
    # 1. Read consolidated_holdings and decrypt value → value_base
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

    # Decrypt the value column (already base-currency from consolidate step)
    cons = cons.with_columns(
        pl.col("value")
        .map_elements(
            lambda v: decrypt_float(v, fernet_key),
            return_dtype=pl.Float64,
        )
        .alias("value_base")
    )

    # ------------------------------------------------------------------
    # 2. Read each broker snapshot for native value, currency, position_type
    # ------------------------------------------------------------------
    snapshot_frames: list[pl.DataFrame] = []
    for connector in all_connectors():
        snap_path = storage.normalized_path(f"{connector.name}_snapshot")
        try:
            snap_dt = DeltaTable(str(snap_path), storage_options=storage_opts)
        except Exception:
            logger.debug(
                "Skipping %s: no normalized snapshot data", connector.display_name
            )
            continue

        snap_arrow = snap_dt.to_pyarrow_table()
        snap = pl.from_arrow(snap_arrow)

        # Determine the label column name (IBKR/T212 use "label", XTB also has it)
        label_col = "label" if "label" in snap.columns else "name"

        # Decrypt the value column for native currency amount
        snap = snap.with_columns(
            pl.col("value")
            .map_elements(
                lambda v: decrypt_float(v, fernet_key),
                return_dtype=pl.Float64,
            )
            .alias("value_native")
        )

        # Determine native currency: prefer value_currency if present, fall back
        # to the generic "currency" column.
        currency_col = (
            "value_currency" if "value_currency" in snap.columns else "currency"
        )

        snap = snap.select(
            [
                pl.lit(connector.display_name).alias("broker"),
                pl.col(label_col).alias("ticker"),
                pl.col("position_type"),
                pl.col(currency_col).alias("currency"),
                pl.col("value_native").alias("value"),
            ]
        )
        snapshot_frames.append(snap)

    snapshots: pl.DataFrame
    if snapshot_frames:
        snapshots = pl.concat(snapshot_frames)
    else:
        logger.warning("No broker snapshots found; position_type will be UNKNOWN")
        snapshots = pl.DataFrame(
            {
                "broker": pl.Series([], dtype=pl.String),
                "ticker": pl.Series([], dtype=pl.String),
                "position_type": pl.Series([], dtype=pl.String),
                "currency": pl.Series([], dtype=pl.String),
                "value": pl.Series([], dtype=pl.Float64),
            }
        )

    # ------------------------------------------------------------------
    # 3. Left join: consolidated (source of truth) ← snapshot (enrichment)
    # ------------------------------------------------------------------
    cons_selected = cons.select(
        [
            "broker",
            "ticker",
            # consolidated.currency is the base/target currency (always EUR in
            # test fixtures).  Rename early so the left join doesn't collide
            # with the snapshot's "currency" column.
            pl.col("currency").alias("base_currency"),
            "value_base",
            "identifier",
            "security_currency",
            "description",
        ]
    )

    result = cons_selected.join(snapshots, on=["broker", "ticker"], how="left")

    # Fill in defaults for unmatched rows (no snapshot match)
    result = result.with_columns(
        [
            # Native value: use snapshot value if matched, else fall back to value_base
            pl.when(pl.col("value").is_null())
            .then(pl.col("value_base"))
            .otherwise(pl.col("value"))
            .alias("value"),
            # Native currency: use snapshot currency if matched, else fall back to base
            pl.when(pl.col("currency").is_null())
            .then(pl.col("base_currency"))
            .otherwise(pl.col("currency"))
            .alias("currency"),
            # Position type: UNKNOWN for unmatched rows
            pl.when(pl.col("position_type").is_null())
            .then(pl.lit("UNKNOWN"))
            .otherwise(pl.col("position_type"))
            .alias("position_type"),
        ]
    )

    # ------------------------------------------------------------------
    # 4. Build the final table matching the schema
    # ------------------------------------------------------------------
    now = datetime.now(timezone.utc)

    # Select columns in schema order and cast types
    result = result.select(
        [
            pl.lit(now).alias("calculated_at"),
            "broker",
            "ticker",
            "currency",
            "value",
            "value_base",
            "base_currency",
            "position_type",
            "identifier",
            "security_currency",
            "description",
        ]
    )

    # Log warnings for unmatched rows
    unknown_count = result.filter(pl.col("position_type") == "UNKNOWN").height
    if unknown_count > 0:
        logger.warning(
            "%d holdings had no snapshot match; position_type set to UNKNOWN",
            unknown_count,
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
