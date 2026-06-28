"""DuckDB query helpers with decryption support."""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from pipeline.crypto import decrypt_float, decrypt_string, load_key
from pipeline.storage import get_storage

# Columns that contain Fernet-encrypted binary data in normalized tables.
_ENCRYPTED_COLUMNS = frozenset({"value", "quantity", "amount"})


def query(
    table_path: str | Path,
    sql: str,
    *,
    decrypt: bool = True,
    key: bytes | None = None,
) -> duckdb.DuckDBPyRelation:
    """Run a SQL query against a Delta table with automatic decryption.

    Parameters
    ----------
    table_path:
        Path to the Delta table directory.
    sql:
        SQL expression.  The table is available as ``delta_table``.
    decrypt:
        If True (default), decrypt Fernet-encrypted binary columns
        (``value``, ``quantity``, ``amount``) into floats.  When False,
        the raw binary is preserved.
    key:
        Fernet key.  When *None*, loaded from the default location.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Query result relation.  When *decrypt* is True, encrypted
        columns are replaced with decrypted floats.

    Example
    -------
    >>> result = query(
    ...     "data/normalized/trading212_snapshot",
    ...     "SELECT * FROM delta_table ORDER BY label",
    ... )
    >>> result.pl()  # Polars DataFrame with decrypted values
    """
    # Convert to absolute path for DuckDB
    path = Path(table_path)
    if path.is_absolute():
        abs_path = path.resolve()
    else:
        abs_path = (get_storage().data_dir / path).resolve()
    conn = duckdb.connect()
    conn.execute(
        f"CREATE VIEW delta_table AS SELECT * FROM delta_scan('{abs_path}')"
    )
    return conn.sql(sql)


def load_decrypted(
    table_path: str | Path,
    encrypted_cols: list[str] | None = None,
    key: bytes | None = None,
) -> list[dict]:
    """Read a Delta table and decrypt specified columns.

    Parameters
    ----------
    table_path:
        Path to the Delta table directory.
    encrypted_cols:
        Column names that contain Fernet-encrypted binary values.
        Defaults to ``["value"]``.
    key:
        Fernet key.  When *None*, loaded from the default location.

    Returns
    -------
    list[dict]
        Rows as dictionaries with encrypted columns replaced by decrypted floats.
    """
    if encrypted_cols is None:
        encrypted_cols = ["value"]
    if key is None:
        key = load_key()

    # Convert to absolute path for DuckDB
    path = Path(table_path)
    if path.is_absolute():
        abs_path = path.resolve()
    else:
        abs_path = (get_storage().data_dir / path).resolve()

    conn = duckdb.connect()
    conn.execute(
        f"CREATE VIEW delta_table AS SELECT * FROM delta_scan('{abs_path}')"
    )
    result = conn.execute("SELECT * FROM delta_table").fetchall()
    columns = [desc[0] for desc in conn.execute("SELECT * FROM delta_table").description]
    encrypted_indices = {col: columns.index(col) for col in encrypted_cols if col in columns}

    rows = []
    for row in result:
        row_dict = dict(zip(columns, row))
        for col, idx in encrypted_indices.items():
            raw = row[idx]
            if raw is not None:
                # DuckDB returns BLOB as bytearray; convert to bytes for Fernet
                if isinstance(raw, (bytes, bytearray, memoryview)):
                    raw = bytes(raw)
                try:
                    row_dict[col] = decrypt_float(raw, key)
                except Exception:
                    try:
                        row_dict[col] = decrypt_string(raw, key)
                    except Exception:
                        row_dict[col] = raw
        rows.append(row_dict)
    return rows


def _decrypt_value(v, key: bytes):
    """Decrypt a single Fernet-encrypted value; return None for nulls."""
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray, memoryview)):
        raw = bytes(v)
        try:
            return decrypt_float(raw, key)
        except Exception:
            try:
                return decrypt_string(raw, key)
            except Exception:
                return v
    return v


def decrypted_df(
    table_path: str | Path,
    sql: str | None = None,
    encrypted_cols: list[str] | None = None,
    key: bytes | None = None,
) -> pl.DataFrame:
    """Run a query and return a Polars DataFrame with decrypted values.

    This is the recommended way to inspect normalized Delta tables.
    Encrypted binary columns (``value``, ``quantity``, ``amount``) are
    automatically decrypted into floats for easy viewing.

    Parameters
    ----------
    table_path:
        Path to the Delta table directory.
    sql:
        SQL expression.  The table is available as ``delta_table``.
        Defaults to ``SELECT * FROM delta_table``.
    encrypted_cols:
        Column names that contain Fernet-encrypted binary values.
        Defaults to ``["value"]``.
    key:
        Fernet key.  When *None*, loaded from the default location.

    Returns
    -------
    polars.DataFrame
        Query result with encrypted columns replaced by decrypted floats.

    Example
    -------
    >>> df = decrypted_df("data/normalized/trading212_snapshot")
    >>> df[["label", "value", "currency"]].head()
    >>> df.filter(pl.col("position_type") == "EQUITY")  # Polars filter
    """
    if sql is None:
        sql = "SELECT * FROM delta_table"
    result = query(table_path, sql, decrypt=False)
    df = result.pl()

    if encrypted_cols is None:
        encrypted_cols = list(_ENCRYPTED_COLUMNS)
    if key is None:
        key = load_key()

    for col in encrypted_cols:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).map_elements(
                    lambda v: _decrypt_value(v, key),
                    return_dtype=pl.Float64,
                )
            )

    # Round decrypted float columns to 2 decimal places.
    for col in encrypted_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).round(2))

    return df