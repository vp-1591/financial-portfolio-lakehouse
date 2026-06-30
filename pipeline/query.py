"""DuckDB query helpers with decryption support.

Supports both local filesystem paths and S3 URIs.  For S3 queries,
AWS credentials must be configured via environment variables
(``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, ``AWS_REGION``).

Tables are identified by **aliases** that follow the
``{name}_{layer}`` convention:

- ``ibkr_snapshot_raw`` — raw layer
- ``ibkr_snapshot_normalized`` — normalized layer
- ``portfolio_allocation_analytics`` — analytics layer

Aliases can be passed to :func:`decrypted_df`, :func:`query`, or used
as table names in :func:`sql`.
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

# Medallion layers to scan for Delta tables.
LAYERS = ("raw", "normalized", "analytics")

# Layer suffixes used in aliases, in reverse-length order so that longer
# suffixes are matched first (e.g. "_analytics" before "_raw").
_LAYER_SUFFIXES = ("_analytics", "_normalized", "_raw")


# ---------------------------------------------------------------------------
# Table discovery
# ---------------------------------------------------------------------------


def _discover_tables_local(data_dir: Path) -> list[tuple[str, str]]:
    """Discover Delta tables under a local data directory.

    Scans ``data_dir/{layer}/`` subdirectories and yields ``(layer, name)``
    tuples for each directory that contains a ``_delta_log/`` subdirectory.
    """
    tables: list[tuple[str, str]] = []
    for layer in LAYERS:
        layer_dir = data_dir / layer
        if not layer_dir.is_dir():
            continue
        for table_dir in sorted(layer_dir.iterdir()):
            if table_dir.is_dir() and (table_dir / "_delta_log").is_dir():
                tables.append((layer, table_dir.name))
    return tables


def _discover_tables_s3(bucket: str, prefix: str) -> list[tuple[str, str]]:
    """Discover Delta tables under an S3 prefix.

    Uses PyArrow's S3FileSystem to list directories under each layer
    prefix and checks for ``_delta_log/`` objects to identify Delta tables.
    """
    import pyarrow.fs as pafs

    region = os.environ.get("AWS_REGION", "eu-west-1")
    fs = pafs.S3FileSystem(region=region)

    tables: list[tuple[str, str]] = []
    for layer in LAYERS:
        base = f"{bucket}/{prefix}/{layer}/" if prefix else f"{bucket}/{layer}/"
        try:
            selector = pafs.FileSelector(base, recursive=False)
            entries = fs.get_file_info(selector)
        except Exception:
            continue

        for entry in entries:
            if entry.type != pafs.FileType.Directory:
                continue
            name = entry.path.rsplit("/", 1)[-1]
            # Verify _delta_log exists under this table directory.
            log_path = f"{base}{name}/_delta_log"
            try:
                log_info = fs.get_file_info(log_path)
                if log_info.type in (pafs.FileType.Directory, pafs.FileType.NotFound):
                    # On S3, _delta_log may appear as a "directory" or
                    # may not be directly listable.  Try listing its
                    # contents to confirm it's a real Delta log.
                    try:
                        log_selector = pafs.FileSelector(
                            f"{base}{name}/_delta_log/", recursive=False
                        )
                        log_entries = fs.get_file_info(log_selector)
                        if log_entries:
                            tables.append((layer, name))
                    except Exception:
                        pass
            except Exception:
                pass

    return tables


def list_tables() -> list[str]:
    """Discover Delta tables on disk/S3 and return layer-qualified aliases.

    Each returned alias follows the ``{name}_{layer}`` convention
    (e.g. ``ibkr_snapshot_raw``, ``portfolio_allocation_analytics``).
    Empty tables (Delta log exists but no Parquet files) are excluded
    because they would fail at query time with ``No files in log segment``.

    Aliases can be passed directly to :func:`decrypted_df`,
    :func:`query`, or used as table names in :func:`sql`.

    Returns
    -------
    list[str]
        Layer-qualified aliases sorted alphabetically.

    Example
    -------
    >>> list_tables()
    ['consolidated_holdings_normalized', 'ibkr_snapshot_normalized',
     'ibkr_snapshot_raw', 'portfolio_allocation_analytics']
    >>> for alias in list_tables():
    ...     df = decrypted_df(alias)
    """
    from deltalake import DeltaTable

    config = get_storage()

    if isinstance(config.backend, S3Backend):
        raw_tables = _discover_tables_s3(config.backend.bucket, config.backend.prefix)
    else:
        raw_tables = _discover_tables_local(Path(config.data_dir))

    # Filter out empty tables (no Parquet files in the Delta log).
    existing: list[str] = []
    for layer, name in raw_tables:
        path = config.backend.table_path(layer, name)
        try:
            storage_opts = config.storage_options
            kwargs: dict = {}
            if storage_opts:
                kwargs["storage_options"] = storage_opts
            dt = DeltaTable(path, **kwargs)
            if not dt.file_uris():
                continue  # empty table — no data to query
        except Exception:
            continue  # can't open table — skip it
        existing.append(f"{name}_{layer}")

    return sorted(existing)


# ---------------------------------------------------------------------------
# Alias parsing and path resolution
# ---------------------------------------------------------------------------


def parse_alias(alias: str) -> tuple[str, str] | None:
    """Parse a layer-qualified alias into ``(name, layer)``.

    Aliases follow the ``{name}_{layer}`` convention where *layer* is
    one of ``raw``, ``normalized``, or ``analytics``.

    Parameters
    ----------
    alias:
        A layer-qualified alias such as ``ibkr_snapshot_raw``.

    Returns
    -------
    tuple[str, str] | None
        ``(name, layer)`` if *alias* ends with a known layer suffix,
        ``None`` otherwise.

    Example
    -------
    >>> parse_alias("ibkr_snapshot_raw")
    ('ibkr_snapshot', 'raw')
    >>> parse_alias("portfolio_allocation_analytics")
    ('portfolio_allocation', 'analytics')
    >>> parse_alias("some_random_string")
    None
    """
    for suffix in _LAYER_SUFFIXES:
        if alias.endswith(suffix):
            name = alias[: -len(suffix)]
            layer = suffix[1:]  # strip leading "_"
            return (name, layer)
    return None


def _resolve_path(table_path: str | Path) -> str:
    """Resolve a table path or alias to an absolute path or S3 URI.

    Resolution order:

    1. **Layer-qualified alias** — if the input ends with ``_raw``,
       ``_normalized``, or ``_analytics``, parse the suffix and resolve
       via :meth:`StorageBackend.table_path`.
    2. **S3 URI** — if the input starts with ``s3://``, use as-is.
    3. **Absolute path** — if the input is an absolute filesystem path,
       resolve it.
    4. **Bare name** — treat as a normalized-layer table name (legacy
       behaviour for backward compatibility).
    """
    config = get_storage()
    path_str = str(table_path).replace("\\", "/")

    # Layer-qualified alias (e.g. "ibkr_snapshot_raw").
    parsed = parse_alias(path_str)
    if parsed is not None:
        name, layer = parsed
        return config.backend.table_path(layer, name)

    # Already an S3 URI — use as-is.
    if path_str.startswith("s3://"):
        return path_str

    # Already absolute — resolve.
    path = Path(path_str)
    if path.is_absolute():
        return str(path.resolve())

    # Bare name — resolve against normalized layer (legacy default).
    if isinstance(config.backend, S3Backend):
        return config.backend.table_path("normalized", path_str)
    return str((Path(config.data_dir) / "normalized" / path_str).resolve())


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


def _create_connection(table_path: str) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with S3 config if needed."""
    conn = duckdb.connect()
    if table_path.startswith("s3://"):
        _configure_s3(conn)
    return conn


# ---------------------------------------------------------------------------
# Public query API
# ---------------------------------------------------------------------------


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
        A layer-qualified alias (e.g. ``ibkr_snapshot_raw``),
        an S3 URI, or a local path to the Delta table directory.
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
    ...     "ibkr_snapshot_raw",
    ...     "SELECT * FROM delta_table ORDER BY label",
    ... )
    >>> result.pl()  # Polars DataFrame with decrypted values
    """
    abs_path = _resolve_path(table_path)
    conn = _create_connection(abs_path)
    conn.execute(f"CREATE VIEW delta_table AS SELECT * FROM delta_scan('{abs_path}')")
    return conn.sql(sql)


def sql(query: str) -> duckdb.DuckDBPyRelation:
    """Run a SQL query against all discovered Delta tables.

    Creates a DuckDB connection, configures S3 credentials, registers
    all existing Delta tables as views (using their layer-qualified
    aliases), and executes the query.  This is the simplest way to
    query multiple tables in a single call.

    Tables are available by their layer-qualified alias names
    (e.g. ``ibkr_snapshot_raw``, ``ibkr_snapshot_normalized``,
    ``portfolio_allocation_analytics``).

    Parameters
    ----------
    query:
        SQL expression referencing tables by alias name.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Query result relation.

    Example
    -------
    >>> sql("SELECT * FROM ibkr_snapshot_raw LIMIT 5")
    >>> sql(
    ...     "SELECT * FROM consolidated_holdings_normalized "
    ...     "WHERE value > 1000"
    ... )
    """
    config = get_storage()
    conn = duckdb.connect()

    # Configure S3 if needed.
    if isinstance(config.backend, S3Backend):
        _configure_s3(conn)

    # Register all discovered tables as views.
    for alias in list_tables():
        parsed = parse_alias(alias)
        if parsed is None:
            continue
        name, layer = parsed
        path = config.backend.table_path(layer, name)
        conn.execute(f"CREATE VIEW {alias} AS SELECT * FROM delta_scan('{path}')")

    return conn.sql(query)


def load_decrypted(
    table_path: str | Path,
    encrypted_cols: list[str] | None = None,
    key: bytes | None = None,
) -> list[dict]:
    """Read a Delta table and decrypt specified columns.

    Parameters
    ----------
    table_path:
        A layer-qualified alias (e.g. ``ibkr_snapshot_raw``),
        an S3 URI, or a local path to the Delta table directory.
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
        A layer-qualified alias (e.g. ``ibkr_snapshot_raw``),
        an S3 URI, or a local path to the Delta table directory.
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
    >>> df = decrypted_df("ibkr_snapshot_raw")
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
