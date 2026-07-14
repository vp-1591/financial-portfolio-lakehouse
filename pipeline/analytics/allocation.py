"""Consolidated holdings → portfolio allocation percentages.

Reads the ``consolidated_holdings`` Delta table, decrypts values,
and calculates percentage allocation per ticker and broker.
"""

from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pyarrow as pa
from deltalake import write_deltalake

from pipeline.crypto import decrypt_float, load_key


def allocate_percentages(
    table_path: str | None = None,
    fernet_key: bytes | None = None,
    analytics_path: str | None = None,
) -> pa.Table:
    """Calculate portfolio allocation percentages from consolidated holdings.

    Parameters
    ----------
    table_path:
        Path to the ``consolidated_holdings`` Delta table.
        Defaults to ``NORMALIZED_CONSOLIDATED_HOLDINGS``.
    fernet_key:
        Fernet key for decrypting value columns.
        When *None*, loaded from the default location.
    analytics_path:
        Path to write the ``portfolio_allocation`` Delta table.
        Defaults to ``ANALYTICS_PORTFOLIO_ALLOCATION``.
    """

    if table_path is None:
        from pipeline.storage import get_storage

        table_path = get_storage().normalized_path("consolidated_holdings")

    if fernet_key is None:
        fernet_key = load_key()

    if analytics_path is None:
        from pipeline.storage import get_storage

        analytics_path = get_storage().analytics_path("portfolio_allocation")

    from pipeline.storage import get_storage

    storage_opts = get_storage().storage_options
    get_storage().backend.ensure_parent(analytics_path)

    from deltalake import DeltaTable

    try:
        dt = DeltaTable(table_path, storage_options=storage_opts)
    except Exception as exc:
        raise FileNotFoundError(
            f"Consolidated holdings table not found at {table_path}. "
            "Run the fetch and transform steps first to populate the table."
        ) from exc

    # Read via Arrow to preserve schema, then convert to Polars
    arrow_table = dt.to_pyarrow_table()
    df = pl.from_arrow(arrow_table)

    # Decrypt target_value column
    df = df.with_columns(
        pl.col("target_value")
        .map_elements(
            lambda v: decrypt_float(v, fernet_key),
            return_dtype=pl.Float64,
        )
        .alias("target_value_decrypted")
    )

    # Calculate percentages
    net_worth = df["target_value_decrypted"].sum()
    if net_worth == 0:
        raise ValueError("Net worth is zero; cannot calculate percentages.")

    df = df.with_columns(
        (pl.col("target_value_decrypted") / net_worth * 100)
        .round(4)
        .alias("percentage")
    )

    # Aggregate by ticker + broker (sum duplicates)
    agg = (
        df.group_by(["ticker", "broker"])
        .agg(
            [
                pl.col("percentage").sum(),
                pl.col("identifier").first(),
                pl.col("security_ccy").first(),
                pl.col("description").first(),
            ]
        )
        .sort("percentage", descending=True)
    )

    now = datetime.now(timezone.utc)

    from pipeline.analytics.models import portfolio_allocation_schema

    result = pa.table(
        {
            "calculated_at": [now] * len(agg),
            "ticker": agg["ticker"].to_list(),
            "percentage": agg["percentage"].to_list(),
            "broker": agg["broker"].to_list(),
            "identifier": agg["identifier"].to_list(),
            "security_ccy": agg["security_ccy"].to_list(),
            "description": agg["description"].to_list(),
        },
        schema=portfolio_allocation_schema,
    )

    write_deltalake(
        analytics_path, result, mode="overwrite", storage_options=storage_opts
    )
    return result
