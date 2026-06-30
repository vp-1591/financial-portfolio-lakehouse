"""Tests for list_tables in query.py."""

from pipeline.query import KNOWN_TABLES, list_tables
from pipeline.storage import S3Backend, StorageConfig, use_storage


class TestKnownTables:
    """Verify the KNOWN_TABLES constant has the expected structure."""

    def test_has_three_layers(self):
        assert set(KNOWN_TABLES.keys()) == {"raw", "normalized", "analytics"}

    def test_raw_has_six_tables(self):
        assert len(KNOWN_TABLES["raw"]) == 6

    def test_normalized_has_seven_tables(self):
        assert len(KNOWN_TABLES["normalized"]) == 7

    def test_analytics_has_one_table(self):
        assert len(KNOWN_TABLES["analytics"]) == 1

    def test_consolidated_holdings_in_normalized(self):
        assert "consolidated_holdings" in KNOWN_TABLES["normalized"]

    def test_portfolio_allocation_in_analytics(self):
        assert "portfolio_allocation" in KNOWN_TABLES["analytics"]


class TestListTables:
    """Verify list_tables returns existing table names."""

    def test_returns_list_of_strings(self):
        """list_tables() returns a list of strings."""
        tables = list_tables()
        assert isinstance(tables, list)
        for name in tables:
            assert isinstance(name, str)

    def test_names_are_valid_table_aliases(self):
        """Every returned name appears in KNOWN_TABLES."""
        all_known = {n for names in KNOWN_TABLES.values() for n in names}
        for name in list_tables():
            assert name in all_known, f"{name!r} not in KNOWN_TABLES"

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

    def test_all_raw_tables_have_broker_names(self):
        """All raw tables are prefixed with a broker name."""
        for name in KNOWN_TABLES["raw"]:
            assert any(name.startswith(b) for b in ("ibkr_", "trading212_", "xtb_")), (
                f"Raw table {name!r} not prefixed with a known broker"
            )
