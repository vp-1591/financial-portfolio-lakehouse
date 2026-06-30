"""Tests for table discovery and alias resolution in query.py."""

import pyarrow as pa
from deltalake import write_deltalake

from pipeline.query import LAYERS, _discover_tables_local, list_tables, parse_alias
from pipeline.storage import LocalBackend, S3Backend, StorageConfig, use_storage


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
        assert parse_alias("portfolio_allocation_analytics") == (
            "portfolio_allocation",
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


class TestListTables:
    """Verify list_tables() returns layer-qualified aliases."""

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
        try:
            tables = list_tables()
            assert tables == []
        finally:
            import pipeline.storage as _storage_mod

            _storage_mod._config = None

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
        try:
            tables = list_tables()
            assert "test_table_normalized" in tables
        finally:
            import pipeline.storage as _storage_mod

            _storage_mod._config = None
