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

from deltalake import DeltaTable

from pipeline.crypto import decrypt_float, load_key
from pipeline.normalized.consolidate import Holding
from pipeline.normalized.models import (
    ibkr_snapshot_normalized_schema,
    trading212_snapshot_normalized_schema,
    xtb_snapshot_normalized_schema,
)


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

    path = Path(table_path)
    if not path.exists():
        return []

    try:
        dt = DeltaTable(str(path))
    except Exception:
        return []

    df = dt.to_pandas()
    if df.empty:
        return []

    holdings: list[Holding] = []

    if broker == "ibkr":
        for _, row in df.iterrows():
            value = decrypt_float(row["value"], fernet_key)
            isin = str(row.get("isin", "") or "").strip()
            identifier = f"ISIN:{isin}" if isin else ""
            conid = str(row.get("conid", "") or "").strip()
            if not identifier and conid:
                identifier = f"CONID:{conid}"
            holdings.append(Holding(
                broker="IBKR",
                ticker=str(row["label"]),
                currency=str(row.get("value_currency", row.get("currency", ""))),
                value=value,
                identifier=identifier,
                security_currency=str(row.get("security_currency", "")),
                description=str(row.get("description", "")),
            ))

    elif broker == "trading212":
        for _, row in df.iterrows():
            value = decrypt_float(row["value"], fernet_key)
            isin = str(row.get("isin", "") or "").strip()
            identifier = f"ISIN:{isin}" if isin else ""
            holdings.append(Holding(
                broker="Trading 212",
                ticker=str(row["label"]),
                currency=str(row.get("value_currency", row.get("currency", ""))),
                value=value,
                identifier=identifier,
                security_currency=str(row.get("security_currency", "")),
                description=str(row.get("name", "")),
            ))

    elif broker == "xtb":
        for _, row in df.iterrows():
            value = decrypt_float(row["value"], fernet_key)
            isin = str(row.get("isin", "") or "").strip()
            identifier = f"ISIN:{isin}" if isin else ""
            holdings.append(Holding(
                broker="XTB",
                ticker=str(row["label"]),
                currency=str(row.get("value_currency", row.get("currency", ""))),
                value=value,
                identifier=identifier,
                security_currency=str(row.get("value_currency", row.get("currency", ""))),
                description=str(row.get("name", "")),
            ))

    return holdings