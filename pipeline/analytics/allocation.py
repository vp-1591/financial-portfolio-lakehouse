"""Consolidated holdings → portfolio allocation percentages.

Reads the ``consolidated_holdings`` Delta table, decrypts values,
and calculates percentage allocation per ticker and broker.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
from deltalake import write_deltalake

from pipeline.crypto import decrypt_float, load_key
from pipeline.paths import ANALYTICS_PORTFOLIO_ALLOCATION


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
    from pipeline.normalized.models import consolidated_holdings_schema

    if table_path is None:
        from pipeline.paths import NORMALIZED_CONSOLIDATED_HOLDINGS

        table_path = str(NORMALIZED_CONSOLIDATED_HOLDINGS)

    if fernet_key is None:
        fernet_key = load_key()

    if analytics_path is None:
        analytics_path = str(ANALYTICS_PORTFOLIO_ALLOCATION)

    from deltalake import DeltaTable

    dt = DeltaTable(table_path)
    df = dt.to_pandas()

    # Decrypt value column
    df["value_decrypted"] = df["value"].apply(lambda v: decrypt_float(v, fernet_key))

    # Calculate percentages
    net_worth = df["value_decrypted"].sum()
    if net_worth == 0:
        raise ValueError("Net worth is zero; cannot calculate percentages.")

    df["percentage"] = (df["value_decrypted"] / net_worth * 100).round(4)

    # Aggregate by ticker + broker (sum duplicates)
    agg = df.groupby(["ticker", "broker"], as_index=False).agg(
        {"percentage": "sum", "identifier": "first", "security_currency": "first", "description": "first"}
    )

    now = datetime.now(timezone.utc)

    from pipeline.analytics.models import portfolio_allocation_schema

    result = pa.table(
        {
            "calculated_at": [now] * len(agg),
            "ticker": agg["ticker"].tolist(),
            "percentage": agg["percentage"].tolist(),
            "broker": agg["broker"].tolist(),
            "identifier": agg["identifier"].tolist(),
            "security_currency": agg["security_currency"].tolist(),
            "description": agg["description"].tolist(),
        },
        schema=portfolio_allocation_schema,
    )

    write_deltalake(analytics_path, result, mode="overwrite")
    return result