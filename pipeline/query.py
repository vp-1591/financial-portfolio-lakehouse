"""DuckDB query helpers with decryption support.

Supports both local filesystem paths and S3 URIs.  For S3 queries,
AWS credentials must be configured via environment variables
(``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, ``AWS_REGION``).

Tables are identified by **aliases** that follow the
``{name}_{layer}`` convention:

- ``ibkr_snapshot_raw`` — raw layer
- ``ibkr_snapshot_normalized`` — normalized layer
- ``portfolio_allocation_analytics`` — analytics layer

Usage::

    from pipeline.query import get_connection, list_tables, decrypt_df

    # Get a pre-configured DuckDB connection
    db = get_connection()

    # List available tables
    list_tables()

    # Query using native DuckDB API
    db.sql("SELECT * FROM ibkr_snapshot_raw LIMIT 5").pl()

    # Decrypt encrypted columns
    df = db.sql("SELECT * FROM ibkr_snapshot_raw").pl()
    decrypt_df(df)

    # Refresh after writing new tables
    refresh()
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import duckdb
import polars as pl

from pipeline.crypto import decrypt_float, decrypt_string, load_key
from pipeline.storage import S3Backend, get_storage

logger = logging.getLogger(__name__)

# Columns that contain Fernet-encrypted binary data in normalized tables.
_ENCRYPTED_COLUMNS = frozenset({"value", "quantity", "amount"})

# Medallion layers to scan for Delta tables.
LAYERS = ("raw", "normalized", "analytics")

# Layer suffixes used in aliases, in reverse-length order so that longer
# suffixes are matched first (e.g. "_analytics" before "_raw").
_LAYER_SUFFIXES = ("_analytics", "_normalized", "_raw")

# Module-level cache for list_tables().
_TABLE_CACHE: list[str] | None = None

# Module-level DuckDB connection (created lazily by get_connection()).
_connection: duckdb.DuckDBPyConnection | None = None


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
    import pyarrow as pa
    import pyarrow.fs as pafs

    region = os.environ.get("AWS_REGION", "eu-west-1")
    fs = pafs.S3FileSystem(region=region)

    # Exceptions raised by PyArrow S3 operations.  We catch these
    # specifically so that programming errors (e.g. TypeError, ValueError)
    # are not silently swallowed.
    _s3_errors: tuple[type[Exception], ...] = (OSError, pa.ArrowInvalid)

    tables: list[tuple[str, str]] = []
    for layer in LAYERS:
        base = f"{bucket}/{prefix}/{layer}/" if prefix else f"{bucket}/{layer}/"
        try:
            selector = pafs.FileSelector(base, recursive=False)
            entries = fs.get_file_info(selector)
        except _s3_errors as exc:
            logger.warning("S3 discovery failed for %s: %s", base, exc)
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
                    except _s3_errors:
                        pass
            except _s3_errors:
                pass

    return tables


def list_tables(*, refresh: bool = False) -> list[str]:
    """Discover Delta tables on disk/S3 and return layer-qualified aliases.

    Each returned alias follows the ``{name}_{layer}`` convention
    (e.g. ``ibkr_snapshot_raw``, ``portfolio_allocation_analytics``).

    Results are cached in memory for the lifetime of the process.
    Pass ``refresh=True`` to force re-discovery.

    Returns
    -------
    list[str]
        Layer-qualified aliases sorted alphabetically.

    Example
    -------
    >>> list_tables()
    ['consolidated_holdings_normalized', 'ibkr_snapshot_normalized',
     'ibkr_snapshot_raw', 'portfolio_allocation_analytics']
    """
    global _TABLE_CACHE
    if _TABLE_CACHE is not None and not refresh:
        return _TABLE_CACHE

    config = get_storage()

    if isinstance(config.backend, S3Backend):
        raw_tables = _discover_tables_s3(config.backend.bucket, config.backend.prefix)
    else:
        raw_tables = _discover_tables_local(Path(config.data_dir))

    _TABLE_CACHE = sorted(f"{name}_{layer}" for layer, name in raw_tables)
    return _TABLE_CACHE


def clear_table_cache() -> None:
    """Clear the in-memory table discovery cache.

    Call this after writing new tables if you need ``list_tables()`` to
    reflect the change immediately.
    """
    global _TABLE_CACHE
    _TABLE_CACHE = None


# ---------------------------------------------------------------------------
# Alias parsing
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


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection configured for Delta table queries.

    Returns a cached connection with S3 credentials (if needed) and
    all discovered Delta tables registered as views.  Call
    :func:`refresh` to re-discover tables after schema changes.

    Returns
    -------
    duckdb.DuckDBPyConnection
        A DuckDB connection ready for queries.

    Example
    -------
    >>> db = get_connection()
    >>> db.sql("SELECT * FROM ibkr_snapshot_raw LIMIT 5").pl()
    """
    global _connection
    if _connection is None:
        _connection = duckdb.connect()
        _setup_connection(_connection)
    return _connection


def _setup_connection(conn: duckdb.DuckDBPyConnection) -> None:
    """Configure S3 credentials and register all discovered tables as views."""
    config = get_storage()
    if isinstance(config.backend, S3Backend):
        _configure_s3(conn)

    for alias in list_tables():
        parsed = parse_alias(alias)
        if parsed is None:
            continue
        name, layer = parsed
        path = config.backend.table_path(layer, name)
        # Escape single quotes in path to prevent SQL injection via delta_scan().
        escaped_path = path.replace("'", "''")
        # Double-quote the alias so DuckDB treats it as a delimited identifier.
        conn.execute(
            f"CREATE OR REPLACE VIEW \"{alias}\" AS SELECT * FROM delta_scan('{escaped_path}')"
        )


def refresh() -> None:
    """Re-discover tables and recreate the DuckDB connection.

    Call this after writing new tables if you need them to appear
    in query results.  Closes the existing connection, clears the
    table cache, and the next :func:`get_connection` call will
    re-discover and re-register everything.
    """
    global _connection
    if _connection is not None:
        _connection.close()
    _connection = None
    clear_table_cache()


# ---------------------------------------------------------------------------
# S3 configuration
# ---------------------------------------------------------------------------


def _configure_s3(conn: duckdb.DuckDBPyConnection) -> None:
    """Configure DuckDB S3 credentials from environment variables.

    Uses DuckDB's SECRET mechanism (v0.10+) which propagates credentials
    to all extensions including ``delta_scan()``.

    When explicit credentials (``AWS_ACCESS_KEY_ID`` and
    ``AWS_SECRET_ACCESS_KEY``) are present, they are registered as a
    DuckDB SECRET.  When they are absent, no SECRET is created so that
    DuckDB / Delta Kernel can fall back to IAM instance metadata or
    other credential providers — mirroring the empty-credential logic
    in ``S3Backend.storage_options``.
    """
    key_id = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    region = os.environ.get("AWS_REGION", "eu-west-1")

    if key_id and secret:
        # Escape single quotes to prevent SQL injection via env vars.
        safe_key_id = key_id.replace("'", "''")
        safe_secret = secret.replace("'", "''")
        conn.execute(
            f"CREATE SECRET (TYPE S3, KEY_ID '{safe_key_id}', "
            f"SECRET '{safe_secret}', REGION '{region}')"
        )
    else:
        # No explicit credentials — rely on IAM role / instance metadata.
        logger.debug(
            "No AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY set; "
            "skipping DuckDB S3 SECRET (expecting IAM role fallback)"
        )


# ---------------------------------------------------------------------------
# Decryption
# ---------------------------------------------------------------------------


def decrypt_df(
    df: pl.DataFrame,
    columns: list[str] | None = None,
    key: bytes | None = None,
) -> pl.DataFrame:
    """Decrypt Fernet-encrypted columns in a Polars DataFrame.

    Parameters
    ----------
    df:
        Polars DataFrame with encrypted binary columns.
    columns:
        Column names to decrypt.  Defaults to
        ``["value", "quantity", "amount"]``.
    key:
        Fernet key.  When *None*, loaded from the default location.

    Returns
    -------
    pl.DataFrame
        DataFrame with encrypted columns decrypted and rounded to
        2 decimal places.

    Example
    -------
    >>> db = get_connection()
    >>> df = db.sql("SELECT * FROM ibkr_snapshot_raw").pl()
    >>> decrypt_df(df)
    """
    encrypted_cols = columns if columns is not None else list(_ENCRYPTED_COLUMNS)
    decrypt_key = key if key is not None else load_key()

    result = df
    for col in encrypted_cols:
        if col in result.columns:
            result = result.with_columns(
                pl.col(col).map_elements(
                    lambda v: _decrypt_value(v, decrypt_key),
                    return_dtype=pl.Float64,
                )
            )

    # Round decrypted float columns to 2 decimal places.
    for col in encrypted_cols:
        if col in result.columns:
            result = result.with_columns(pl.col(col).round(2))

    return result


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
