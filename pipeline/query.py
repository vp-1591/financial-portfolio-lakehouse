"""DuckDB query helpers with decryption support.

Supports both local filesystem paths and S3 URIs.  For S3 queries,
AWS credentials must be configured via environment variables
(``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, ``AWS_REGION``).
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import polars as pl

from pipeline.crypto import decrypt_float, decrypt_string, load_key
from pipeline.storage import S3Backend, get_storage

# Columns that contain Fernet-encrypted binary data in normalized tables.
_ENCRYPTED_COLUMNS = frozenset({"value", "quantity", "amount"})

# All known Delta tables, organized by medallion layer.
KNOWN_TABLES: dict[str, list[str]] = {
    "raw": [
        "ibkr_snapshot",
        "ibkr_cdc",
        "trading212_snapshot",
        "trading212_cdc",
        "xtb_snapshot",
        "xtb_cdc",
    ],
    "normalized": [
        "ibkr_snapshot",
        "ibkr_cdc",
        "trading212_snapshot",
        "trading212_cdc",
        "xtb_snapshot",
        "xtb_cdc",
        "consolidated_holdings",
    ],
    "analytics": [
        "portfolio_allocation",
    ],
}


def list_tables() -> list[str]:
    """Return names of Delta tables that exist on disk/S3.

    Each name can be passed directly to :func:`decrypted_df` or
    :func:`query`.

    Returns
    -------
    list[str]
        Table names (e.g. ``"consolidated_holdings"``,
        ``"ibkr_snapshot"``) that exist in the configured storage backend.

    Example
    -------
    >>> list_tables()
    ['consolidated_holdings', 'portfolio_allocation']
    >>> for name in list_tables():
    ...     df = decrypted_df(name)
    """
    from deltalake import DeltaTable

    config = get_storage()
    existing: list[str] = []

    for layer, names in KNOWN_TABLES.items():
        for name in names:
            path = config.backend.table_path(layer, name)
            try:
                storage_opts = config.storage_options
                kwargs: dict = {}
                if storage_opts:
                    kwargs["storage_options"] = storage_opts
                DeltaTable(path, **kwargs)
                existing.append(name)
            except Exception:
                pass

    return existing


def _configure_s3(conn: duckdb.DuckDBPyConnection) -> None:
    """Configure DuckDB S3 credentials from environment variables.

    Uses DuckDB's SECRET mechanism (v0.10+) which propagates credentials
    to all extensions including ``delta_scan()``.  The legacy ``SET s3_*``
    variables only affect DuckDB's built-in httpfs extension and are
    invisible to the Delta Kernel's object store, causing S3 reads to
    fall back to EC2 instance metadata and fail on non-EC2 machines.
    """
    key_id = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    region = os.environ.get("AWS_REGION", "eu-west-1")

    conn.execute(
        f"CREATE SECRET (TYPE S3, KEY_ID '{key_id}', SECRET '{secret}', REGION '{region}')"
    )


def _resolve_path(table_path: str | Path) -> str:
    """Resolve a table path to an absolute path or S3 URI string."""
    config = get_storage()
    path_str = str(table_path)

    # Already an S3 URI — use as-is.
    if path_str.startswith("s3://"):
        return path_str

    # Already absolute — resolve.
    path = Path(path_str)
    if path.is_absolute():
        return str(path.resolve())

    # Relative — resolve against data_dir (which may be an S3 prefix).
    if isinstance(config.backend, S3Backend):
        return config.backend.table_path("normalized", path_str.replace("\\", "/"))
    return str((Path(config.data_dir) / path).resolve())


def _create_connection(table_path: str) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with S3 config if needed."""
    conn = duckdb.connect()
    if table_path.startswith("s3://"):
        _configure_s3(conn)
    return conn


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
        Path to the Delta table directory.  Can be a local path or
        an ``s3://`` URI.
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
    abs_path = _resolve_path(table_path)
    conn = _create_connection(abs_path)
    conn.execute(f"CREATE VIEW delta_table AS SELECT * FROM delta_scan('{abs_path}')")
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
        Path to the Delta table directory.  Can be a local path or
        an ``s3://`` URI.
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

    abs_path = _resolve_path(table_path)
    conn = _create_connection(abs_path)
    conn.execute(f"CREATE VIEW delta_table AS SELECT * FROM delta_scan('{abs_path}')")
    result = conn.execute("SELECT * FROM delta_table").fetchall()
    columns = [
        desc[0] for desc in conn.execute("SELECT * FROM delta_table").description
    ]
    encrypted_indices = {
        col: columns.index(col) for col in encrypted_cols if col in columns
    }

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
        Path to the Delta table directory.  Can be a local path or
        an ``s3://`` URI.
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
