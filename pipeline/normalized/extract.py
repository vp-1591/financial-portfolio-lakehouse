"""Extract Holding objects from normalized broker snapshot tables.

Each broker's normalized snapshot has its own schema, but all share a
common subset of columns that map to :class:`Holding`:

- ``label`` → ``ticker``
- ``value`` → encrypted float (decrypted here)
- ``value_currency`` → ``currency``
- ``isin`` → ``identifier`` (formatted as ``ISIN:...``)
- ``security_currency`` → ``security_currency``
- ``description`` → ``description``
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
from deltalake import DeltaTable

from pipeline.crypto import decrypt_float, load_key
from pipeline.normalized.consolidate import Holding


def _label_column_for(broker: str) -> str:
    """Return the ticker/label column name for a broker's normalized schema."""
    if broker == "ibkr":
        return "label"
    return "label"


def extract_holdings(
    broker: str,
    table_path: str | Path,
    fernet_key: bytes | None = None,
) -> list[Holding]:
    """Read a normalized broker snapshot and return a list of :class:`Holding` objects.

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

    holdings: list[Holding] = []

    if broker == "ibkr":
        for row in df.iter_rows(named=True):
            value = decrypt_float(row["value"], fernet_key)
            isin = str(row.get("isin", "") or "").strip()
            identifier = f"ISIN:{isin}" if isin else ""
            holdings.append(
                Holding(
                    broker="IBKR",
                    ticker=str(row["label"]),
                    currency=str(row.get("value_currency", row.get("currency", ""))),
                    value=value,
                    identifier=identifier,
                    security_currency=str(row.get("security_currency", "")),
                    description=str(row.get("description", "")),
                )
            )

    elif broker == "trading212":
        for row in df.iter_rows(named=True):
            value = decrypt_float(row["value"], fernet_key)
            isin = str(row.get("isin", "") or "").strip()
            identifier = f"ISIN:{isin}" if isin else ""
            holdings.append(
                Holding(
                    broker="Trading 212",
                    ticker=str(row["label"]),
                    currency=str(row.get("value_currency", row.get("currency", ""))),
                    value=value,
                    identifier=identifier,
                    security_currency=str(row.get("security_currency", "")),
                    description=str(row.get("name", "")),
                )
            )

    elif broker == "xtb":
        for row in df.iter_rows(named=True):
            value = decrypt_float(row["value"], fernet_key)
            isin = str(row.get("isin", "") or "").strip()
            identifier = f"ISIN:{isin}" if isin else ""
            holdings.append(
                Holding(
                    broker="XTB",
                    ticker=str(row["label"]),
                    currency=str(row.get("value_currency", row.get("currency", ""))),
                    value=value,
                    identifier=identifier,
                    security_currency=str(
                        row.get("value_currency", row.get("currency", ""))
                    ),
                    description=str(row.get("name", "")),
                )
            )

    return holdings
