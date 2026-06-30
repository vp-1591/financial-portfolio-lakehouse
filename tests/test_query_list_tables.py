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
    """Verify list_tables returns correct structure."""

    def test_returns_all_tables_by_default(self):
        """list_tables() returns all 14 known tables."""
        tables = list_tables()
        assert len(tables) == 14

    def test_each_entry_has_required_keys(self):
        """Every entry has layer, name, path, exists."""
        tables = list_tables()
        for t in tables:
            assert "layer" in t
            assert "name" in t
            assert "path" in t
            assert "exists" in t

    def test_existing_only_filters_missing(self):
        """existing_only=True returns only tables that exist."""
        tables = list_tables(existing_only=True)
        for t in tables:
            assert t["exists"] is True

    def test_paths_use_s3_backend_when_configured(self):
        """With an S3Backend, paths are s3:// URIs."""
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
            for t in tables:
                assert t["path"].startswith("s3://"), (
                    f"Expected S3 path, got: {t['path']}"
                )
        finally:
            # Reset so other tests don't see the S3Backend
            import pipeline.storage as _storage_mod

            _storage_mod._config = None

    def test_all_raw_tables_have_broker_names(self):
        """All raw tables are prefixed with a broker name."""
        for name in KNOWN_TABLES["raw"]:
            assert any(name.startswith(b) for b in ("ibkr_", "trading212_", "xtb_")), (
                f"Raw table {name!r} not prefixed with a known broker"
            )
