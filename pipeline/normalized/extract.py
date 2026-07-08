"""Extract Holding objects from normalized broker snapshot tables.

Delegates broker-specific extraction logic to each connector's
:meth:`~pipeline.connectors.base.BrokerConnector.extract_holdings` method.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
from deltalake import DeltaTable

from pipeline.crypto import decrypt_float, load_key
from pipeline.normalized.consolidate import Holding


def extract_holdings(
    broker: str,
    table_path: str | Path,
    fernet_key: bytes | None = None,
) -> list[Holding]:
    """Read a normalized broker snapshot and return a list of :class:`Holding` objects.

    Delegates the per-broker extraction logic to the connector registered
    under *broker* via :func:`pipeline.connectors.registry.get`.

    Parameters
    ----------
    broker:
        Connector name, e.g. ``"ibkr"``, ``"trading212"``, ``"xtb"``.
    table_path:
        Path to the normalized Delta table.
    fernet_key:
        Fernet key for decrypting value columns.
        When *None*, loaded from the default location.
    """
    if fernet_key is None:
        fernet_key = load_key()

    # For S3 paths, skip the local Path.exists() check and go
    # straight to DeltaTable which handles cloud storage.
    from pipeline.storage import get_storage

    storage_opts = get_storage().storage_options
    # Do not wrap table_path in Path() — S3 URIs like s3://bucket/...
    # would be collapsed to s3:/bucket/... by pathlib.
    if not storage_opts and not Path(table_path).exists():
        return []

    try:
        dt = DeltaTable(str(table_path), storage_options=storage_opts)
    except Exception:
        return []

    # Read via Arrow to preserve schema, then convert to Polars
    table = dt.to_pyarrow_table()
    if table.num_rows == 0:
        return []

    df = pl.from_arrow(table)

    # Decrypt value column using Polars map_elements (batch operation)
    df = df.with_columns(
        pl.col("value")
        .map_elements(
            lambda v: decrypt_float(v, fernet_key),
            return_dtype=pl.Float64,
        )
        .alias("value_decrypted")
    )

    # Delegate to the connector's extract_holdings method
    from pipeline.connectors.registry import get as get_connector

    connector = get_connector(broker)
    return connector.extract_holdings(df, fernet_key)
