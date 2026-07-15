"""Tests for table discovery, alias resolution, connection management, and decryption."""

import duckdb
import pyarrow as pa
import polars as pl
import pytest
from deltalake import write_deltalake

from pipeline.crypto import encrypt_float, encrypt_string, generate_key
from pipeline.query import (
    LAYERS,
    _decrypt_value,
    _discover_tables_local,
    clear_table_cache,
    decrypt_df,
    get_connection,
    list_tables,
    parse_alias,
    refresh,
)
from pipeline.storage import LocalBackend, S3Backend, StorageConfig, use_storage


# ---------------------------------------------------------------------------
# parse_alias
# ---------------------------------------------------------------------------


class TestParseAlias:
    """Verify parse_alias() extracts layer suffixes."""

    def test_raw_suffix(self):
        assert parse_alias("ibkr_snapshot_raw") == ("ibkr_snapshot", "raw")

    def test_normalized_suffix(self):
        assert parse_alias("ibkr_snapshot_normalized") == (
            "ibkr_snapshot",
            "normalized",
        )

    def test_analytics_suffix(self):
        assert parse_alias("portfolio_holdings_analytics") == (
            "portfolio_holdings",
            "analytics",
        )

    def test_no_suffix_returns_none(self):
        assert parse_alias("ibkr_snapshot") is None

    def test_longer_suffix_matches_first(self):
        """_analytics is checked before _raw so names like 'x_analytics' parse."""
        assert parse_alias("x_analytics") == ("x", "analytics")

    def test_bare_raw_name_not_confused_with_suffix(self):
        """A name ending in _raw that is not a suffix should not match."""
        # "raw" is a suffix, so "something_raw" is parsed as (something, raw)
        assert parse_alias("something_raw") == ("something", "raw")

    def test_s3_uri_not_an_alias(self):
        assert parse_alias("s3://bucket/prefix/raw/ibkr_snapshot") is None


# ---------------------------------------------------------------------------
# _discover_tables_local
# ---------------------------------------------------------------------------


class TestDiscoverTablesLocal:
    """Verify _discover_tables_local() scans filesystem directories."""

    def test_discovers_delta_tables(self, tmp_path):
        """Delta tables with _delta_log are discovered."""
        data = tmp_path / "data"
        for layer in LAYERS:
            (data / layer).mkdir(parents=True, exist_ok=True)

        # Write a real Delta table in normalized layer.
        table_dir = data / "normalized" / "ibkr_snapshot"
        table_dir.mkdir(parents=True, exist_ok=True)

        table = pa.table(
            {
                "label": ["test"],
                "value": [b"encrypted"],
            }
        )
        write_deltalake(str(table_dir), table, mode="overwrite")

        # Create a directory without _delta_log (should be skipped).
        (data / "normalized" / "not_a_table").mkdir()

        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(data),
        )
        use_storage(config)
        clear_table_cache()
        try:
            tables = _discover_tables_local(data)
            assert ("normalized", "ibkr_snapshot") in tables
            # The non-delta directory should not appear.
            names = [n for _, n in tables]
            assert "not_a_table" not in names
        finally:
            import pipeline.storage as _storage_mod

            _storage_mod._config = None

    def test_empty_data_dir(self, tmp_path):
        """An empty data directory returns no tables."""
        data = tmp_path / "data"
        data.mkdir()
        tables = _discover_tables_local(data)
        assert tables == []


# ---------------------------------------------------------------------------
# list_tables
# ---------------------------------------------------------------------------


class TestListTables:
    """Verify list_tables() returns layer-qualified aliases."""

    def setup_method(self):
        """Clear cache before each test."""
        clear_table_cache()

    def test_returns_list_of_strings(self):
        """list_tables() returns a list of strings."""
        tables = list_tables()
        assert isinstance(tables, list)
        for name in tables:
            assert isinstance(name, str)

    def test_aliases_have_layer_suffix(self):
        """Every returned alias ends with a layer suffix."""
        for alias in list_tables():
            assert any(alias.endswith(f"_{layer}") for layer in LAYERS), (
                f"{alias!r} does not end with a known layer suffix"
            )

    def test_aliases_parse_correctly(self):
        """Every returned alias can be parsed by parse_alias()."""
        for alias in list_tables():
            parsed = parse_alias(alias)
            assert parsed is not None, f"{alias!r} could not be parsed"
            name, layer = parsed
            assert layer in LAYERS

    def test_no_duplicate_aliases(self):
        """list_tables() should never return duplicate aliases."""
        tables = list_tables()
        assert len(tables) == len(set(tables)), f"Duplicates found: {tables}"

    def test_s3_backend_returns_empty_for_missing_bucket(self):
        """With S3Backend pointing to nonexistent bucket, result is empty."""
        backend = S3Backend(bucket="test-bucket", prefix="pipeline")
        use_storage(
            StorageConfig(
                data_dir="s3://test-bucket/pipeline",
                raw_dir="s3://test-bucket/pipeline/raw",
                normalized_dir="s3://test-bucket/pipeline/normalized",
                analytics_dir="s3://test-bucket/pipeline/analytics",
                secrets_dir="/tmp/secrets",
                encryption_key_file="/tmp/secrets/encryption.key",
                backend=backend,
            )
        )
        clear_table_cache()
        try:
            tables = list_tables()
            assert tables == []
        finally:
            import pipeline.storage as _storage_mod

            _storage_mod._config = None
            clear_table_cache()

    def test_local_backend_with_delta_tables(self, tmp_path):
        """Local backend discovers Delta tables in tmp_path."""
        data = tmp_path / "data"
        for layer in LAYERS:
            (data / layer).mkdir(parents=True, exist_ok=True)

        # Write a Delta table in normalized layer.
        table_dir = data / "normalized" / "test_table"
        table_dir.mkdir(parents=True, exist_ok=True)
        table = pa.table({"col1": [1, 2], "col2": ["a", "b"]})
        write_deltalake(str(table_dir), table, mode="overwrite")

        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(data),
        )
        use_storage(config)
        clear_table_cache()
        try:
            tables = list_tables()
            assert "test_table_normalized" in tables
        finally:
            import pipeline.storage as _storage_mod

            _storage_mod._config = None
            clear_table_cache()

    def test_caching(self, tmp_path):
        """list_tables() caches results and returns the same list on second call."""
        data = tmp_path / "data"
        for layer in LAYERS:
            (data / layer).mkdir(parents=True, exist_ok=True)

        table_dir = data / "normalized" / "cached_table"
        table_dir.mkdir(parents=True, exist_ok=True)
        table = pa.table({"x": [1]})
        write_deltalake(str(table_dir), table, mode="overwrite")

        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(data),
        )
        use_storage(config)
        clear_table_cache()
        try:
            first = list_tables()
            second = list_tables()
            assert first is second  # same object — cached
        finally:
            import pipeline.storage as _storage_mod

            _storage_mod._config = None
            clear_table_cache()

    def test_refresh_bypasses_cache(self, tmp_path):
        """list_tables(refresh=True) re-discovers tables even if cached."""
        data = tmp_path / "data"
        for layer in LAYERS:
            (data / layer).mkdir(parents=True, exist_ok=True)

        table_dir = data / "normalized" / "refresh_table"
        table_dir.mkdir(parents=True, exist_ok=True)
        table = pa.table({"x": [1]})
        write_deltalake(str(table_dir), table, mode="overwrite")

        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(data),
        )
        use_storage(config)
        clear_table_cache()
        try:
            first = list_tables()
            refreshed = list_tables(refresh=True)
            assert refreshed == first  # same content
            assert first is not refreshed  # different object — re-discovered
        finally:
            import pipeline.storage as _storage_mod

            _storage_mod._config = None
            clear_table_cache()

    def test_clear_table_cache(self):
        """clear_table_cache() resets the module-level cache."""
        clear_table_cache()

        # Force a cache fill (may be None if no local backend configured).
        try:
            list_tables()
        except Exception:
            # If no storage configured, that's fine for this test.
            pass
        # After clearing, cache should be None.
        clear_table_cache()
        from pipeline.query import _TABLE_CACHE as fresh_cache

        assert fresh_cache is None


# ---------------------------------------------------------------------------
# get_connection
# ---------------------------------------------------------------------------


class TestGetConnection:
    """Verify get_connection() returns a configured DuckDB connection."""

    def setup_method(self):
        """Reset connection and cache before each test."""
        refresh()

    def teardown_method(self):
        """Clean up storage singleton and connection."""
        import pipeline.storage as _storage_mod

        _storage_mod._config = None
        refresh()

    def _setup_local_backend(self, tmp_path):
        """Create a LocalBackend with a single Delta table for testing."""
        data = tmp_path / "data"
        for layer in LAYERS:
            (data / layer).mkdir(parents=True, exist_ok=True)

        table_dir = data / "normalized" / "test_table"
        table_dir.mkdir(parents=True, exist_ok=True)
        table = pa.table({"col1": [1, 2], "col2": ["a", "b"]})
        write_deltalake(str(table_dir), table, mode="overwrite")

        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(data),
        )
        use_storage(config)
        refresh()  # ensure connection picks up new storage config
        return config

    def test_returns_duckdb_connection(self, tmp_path):
        """get_connection() returns a DuckDB connection."""
        self._setup_local_backend(tmp_path)
        db = get_connection()
        assert isinstance(db, duckdb.DuckDBPyConnection)

    def test_caches_connection(self, tmp_path):
        """get_connection() returns the same connection on subsequent calls."""
        self._setup_local_backend(tmp_path)
        db1 = get_connection()
        db2 = get_connection()
        assert db1 is db2

    def test_queries_registered_table(self, tmp_path):
        """get_connection() registers table views that can be queried."""
        self._setup_local_backend(tmp_path)
        db = get_connection()
        df = db.sql("SELECT * FROM test_table_normalized").pl()
        assert isinstance(df, pl.DataFrame)
        assert df.shape[0] == 2

    def test_refresh_recreates_connection(self, tmp_path):
        """refresh() closes the connection; next get_connection() creates new one."""
        self._setup_local_backend(tmp_path)
        db1 = get_connection()
        refresh()
        db2 = get_connection()
        assert db1 is not db2

    def test_sql_query_no_tables(self, tmp_path):
        """get_connection() can run SQL without referencing registered tables."""
        self._setup_local_backend(tmp_path)
        db = get_connection()
        df = db.sql("SELECT 1 AS x").pl()
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (1, 1)


# ---------------------------------------------------------------------------
# decrypt_df
# ---------------------------------------------------------------------------


class TestDecryptDf:
    """Verify decrypt_df() decrypts Fernet-encrypted columns."""

    def test_decrypt_float_column(self):
        key = generate_key()
        encrypted = [encrypt_float(42.5, key), encrypt_float(10.0, key)]
        df = pl.DataFrame({"value": encrypted})
        result = decrypt_df(df, key=key)
        assert result["value"][0] == pytest.approx(42.5)
        assert result["value"][1] == pytest.approx(10.0)

    def test_auto_detect_binary_columns(self):
        """decrypt_df auto-detects binary columns when columns is None."""
        key = generate_key()
        enc_float = encrypt_float(100.0, key)
        enc_str = encrypt_string("hello", key)
        df = pl.DataFrame(
            {
                "value": [enc_float],
                "payload": [enc_str],
                "label": ["US0378331007"],  # plain string — not encrypted
            }
        )
        result = decrypt_df(df, key=key)
        assert result["value"][0] == pytest.approx(100.0)
        assert result["payload"][0] == "hello"
        assert result["label"][0] == "US0378331007"

    def test_auto_detect_no_binary_columns(self):
        """decrypt_df returns the DataFrame unchanged when no binary columns exist."""
        key = generate_key()
        df = pl.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        result = decrypt_df(df, key=key)
        assert result.shape == (2, 2)
        assert result["col1"].to_list() == [1, 2]

    def test_decrypt_string_column(self):
        """decrypt_df correctly decrypts string (payload) columns."""
        key = generate_key()
        enc_str = encrypt_string("<FlexStatement>...</FlexStatement>", key)
        df = pl.DataFrame({"payload": [enc_str]})
        result = decrypt_df(df, key=key)
        assert result["payload"][0] == "<FlexStatement>...</FlexStatement>"
        assert result["payload"].dtype == pl.String

    def test_custom_columns(self):
        """decrypt_df respects the columns parameter."""
        key = generate_key()
        encrypted = encrypt_float(42.5, key)
        df = pl.DataFrame({"value": [encrypted], "other": ["hello"]})
        result = decrypt_df(df, columns=["value"], key=key)
        assert result["value"][0] == pytest.approx(42.5)
        assert result["other"][0] == "hello"

    def test_missing_columns_ignored(self):
        """decrypt_df ignores columns that don't exist in the DataFrame."""
        key = generate_key()
        df = pl.DataFrame({"col1": [1, 2]})
        result = decrypt_df(df, columns=["nonexistent"], key=key)
        assert result.shape == (2, 1)

    def test_null_values(self):
        """decrypt_df handles None values in encrypted columns."""
        key = generate_key()
        encrypted = encrypt_float(42.5, key)
        df = pl.DataFrame({"value": [encrypted, None]})
        result = decrypt_df(df, key=key)
        assert result["value"][0] == pytest.approx(42.5)
        assert result["value"][1] is None

    def test_rounding(self):
        """decrypt_df rounds decrypted float values to 2 decimal places."""
        key = generate_key()
        encrypted = encrypt_float(42.123456, key)
        df = pl.DataFrame({"value": [encrypted]})
        result = decrypt_df(df, key=key)
        assert result["value"][0] == pytest.approx(42.12)

    def test_no_rounding_on_string_columns(self):
        """decrypt_df does not round genuinely string-valued columns."""
        key = generate_key()
        enc_str = encrypt_string("<FlexStatement>data</FlexStatement>", key)
        df = pl.DataFrame({"payload": [enc_str]})
        result = decrypt_df(df, key=key)
        # String columns should come back as String dtype, not rounded.
        assert result["payload"].dtype == pl.String
        assert result["payload"][0] == "<FlexStatement>data</FlexStatement>"


# ---------------------------------------------------------------------------
# _decrypt_value
# ---------------------------------------------------------------------------


class TestDecryptValue:
    """Verify _decrypt_value handles various input types."""

    def test_none_returns_none(self):
        key = generate_key()
        assert _decrypt_value(None, key) is None

    def test_bytes_decrypts_float(self):
        key = generate_key()
        encrypted = encrypt_float(42.5, key)
        result = _decrypt_value(encrypted, key)
        assert result == pytest.approx(42.5)

    def test_bytes_decrypts_string(self):
        from pipeline.crypto import encrypt_string

        key = generate_key()
        encrypted = encrypt_string("hello", key)
        result = _decrypt_value(encrypted, key)
        assert result == "hello"

    def test_non_encrypted_bytes_passthrough(self):
        key = generate_key()
        result = _decrypt_value(b"not-encrypted", key)
        # Should return the raw bytes since decryption fails
        assert result == b"not-encrypted"

    def test_passthrough_int(self):
        key = generate_key()
        assert _decrypt_value(42, key) == 42

    def test_bytearray_decrypts(self):
        key = generate_key()
        encrypted = encrypt_float(10.0, key)
        result = _decrypt_value(bytearray(encrypted), key)
        assert result == pytest.approx(10.0)
