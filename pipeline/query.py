"""DuckDB query helpers with decryption support."""

from __future__ import annotations

from pathlib import Path

import duckdb

from pipeline.crypto import decrypt_float, decrypt_string, load_key


def query(table_path: str | Path, sql: str) -> duckdb.DuckDBPyRelation:
    """Run a SQL query against a Delta table.

    Parameters
    ----------
    table_path:
        Path to the Delta table directory.
    sql:
        SQL expression.  The table is available as ``delta_table``.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Query result relation.

    Example
    -------
    >>> result = query(
    ...     "data/analytics/portfolio_allocation",
    ...     "SELECT ticker, percentage FROM delta_table ORDER BY percentage DESC",
    ... )
    """
    conn = duckdb.connect()
    conn.execute(
        "CREATE VIEW delta_table AS SELECT * FROM delta_scan(?)",
        [str(table_path)],
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

    conn = duckdb.connect()
    conn.execute(
        "CREATE VIEW delta_table AS SELECT * FROM delta_scan(?)",
        [str(table_path)],
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
                try:
                    row_dict[col] = decrypt_float(raw, key)
                except Exception:
                    try:
                        row_dict[col] = decrypt_string(raw, key)
                    except Exception:
                        row_dict[col] = raw
        rows.append(row_dict)
    return rows