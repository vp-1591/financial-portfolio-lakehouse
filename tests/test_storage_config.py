"""Tests for the storage configuration system.

Verifies that:
- ``resolve_storage`` uses ``PIPELINE_DATA_DIR`` env var or project default
- ``resolve_storage`` uses ``S3_BUCKET`` env var for S3Backend
- ``use_storage`` injects custom configs
- ``get_storage`` returns the active config
- ``pipeline.paths`` module delegates to the active ``StorageConfig``
- ``S3Backend`` generates correct URIs and has no-op ensure_parent
- ``LocalBackend.ensure_parent`` rescues orphaned parquet files
  from failed writes (e.g. Docker volume mount rename failures)
  to a ``.rescue/`` directory under the data directory
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.storage import (
    LocalBackend,
    S3Backend,
    S3_DEFAULT_PREFIX,
    StorageConfig,
    get_storage,
    resolve_storage,
    use_storage,
)


class TestResolveStorage:
    """Test resolve_storage() data directory resolution."""

    def setup_method(self):
        """Reset module-level singleton before each test."""
        import pipeline.storage

        pipeline.storage._config = None

    def teardown_method(self):
        """Reset module-level singleton after each test."""
        import pipeline.storage

        pipeline.storage._config = None

    def test_default_uses_project_data_dir(self, monkeypatch):
        monkeypatch.delenv("PIPELINE_DATA_DIR", raising=False)
        monkeypatch.delenv("S3_BUCKET", raising=False)
        config = resolve_storage()
        assert isinstance(config.backend, LocalBackend)
        assert "data" in config.data_dir

    def test_pipeline_data_dir_env_var(self, monkeypatch, tmp_path: Path):
        custom = tmp_path / "my-data"
        custom.mkdir()
        monkeypatch.delenv("S3_BUCKET", raising=False)
        monkeypatch.setenv("PIPELINE_DATA_DIR", str(custom))
        config = resolve_storage()
        assert isinstance(config.backend, LocalBackend)
        assert config.data_dir == str(custom)

    def test_pipeline_data_dir_absolute_path(self, monkeypatch, tmp_path: Path):
        custom = tmp_path / "absolute-path-data"
        custom.mkdir()
        monkeypatch.delenv("S3_BUCKET", raising=False)
        monkeypatch.setenv("PIPELINE_DATA_DIR", str(custom.resolve()))
        config = resolve_storage()
        assert isinstance(config.backend, LocalBackend)

    def test_s3_bucket_env_var(self, monkeypatch):
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.delenv("S3_PREFIX", raising=False)
        config = resolve_storage()
        assert isinstance(config.backend, S3Backend)
        assert config.data_dir.startswith("s3://")
        assert "test-bucket" in config.data_dir

    def test_s3_prefix_env_var(self, monkeypatch):
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv("S3_PREFIX", "custom-prefix")
        config = resolve_storage()
        assert isinstance(config.backend, S3Backend)
        assert (
            config.raw_path("ibkr_snapshot")
            == "s3://test-bucket/custom-prefix/raw/ibkr_snapshot"
        )

    def test_s3_does_not_use_pipeline_data_dir(self, monkeypatch):
        """When S3_BUCKET is set, PIPELINE_DATA_DIR is ignored."""
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv("PIPELINE_DATA_DIR", "/tmp/should-be-ignored")
        config = resolve_storage()
        assert isinstance(config.backend, S3Backend)
        assert "/tmp/should-be-ignored" not in config.data_dir

    def test_secrets_dir_at_project_root(self, monkeypatch):
        from pipeline.storage import PROJECT_ROOT

        monkeypatch.delenv("S3_BUCKET", raising=False)
        monkeypatch.delenv("PIPELINE_DATA_DIR", raising=False)
        config = resolve_storage()
        # secrets_dir should be at project root, not inside data dir
        assert config.secrets_dir == str(PROJECT_ROOT / ".secrets")
        assert config.encryption_key_file == str(
            PROJECT_ROOT / ".secrets" / "encryption.key"
        )

    def test_s3_secrets_dir_at_project_root(self, monkeypatch):

        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        config = resolve_storage()
        assert ".secrets" in config.secrets_dir
        assert not config.secrets_dir.startswith("s3://")


class TestUseStorage:
    """Test use_storage() injection."""

    def test_use_storage_overrides(self, tmp_path: Path):
        data = tmp_path / "custom-data"
        data.mkdir()
        secrets = tmp_path / ".secrets"
        secrets.mkdir()
        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(secrets),
            encryption_key_file=str(secrets / "encryption.key"),
            backend=LocalBackend(data),
        )
        use_storage(config)
        assert get_storage().data_dir == str(data)

    def test_use_storage_with_tmp_path(self, tmp_path: Path):
        data = tmp_path / "data"
        (data / "raw").mkdir(parents=True)
        (data / "normalized").mkdir(parents=True)
        (data / "analytics").mkdir(parents=True)
        secrets = tmp_path / ".secrets"
        secrets.mkdir()
        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(secrets),
            encryption_key_file=str(secrets / "encryption.key"),
            backend=LocalBackend(data),
        )
        use_storage(config)
        assert get_storage() is config
        assert get_storage().raw_dir == str(data / "raw")


class TestLocalBackend:
    """Test LocalBackend path generation."""

    def test_table_path(self, tmp_path: Path):
        backend = LocalBackend(tmp_path)
        result = backend.table_path("raw", "ibkr_snapshot")
        assert result == str(tmp_path.resolve() / "raw" / "ibkr_snapshot")

    def test_ensure_parent(self, tmp_path: Path):
        backend = LocalBackend(tmp_path)
        path = str(tmp_path / "raw" / "ibkr_snapshot")
        backend.ensure_parent(path)
        assert (tmp_path / "raw").exists()

    def test_ensure_parent_creates_parent_dirs(self, tmp_path: Path):
        backend = LocalBackend(tmp_path)
        path = str(tmp_path / "raw" / "ibkr_snapshot")
        backend.ensure_parent(path)
        assert (tmp_path / "raw").is_dir()

    def test_ensure_parent_rescues_orphaned_parquets(self, tmp_path: Path):
        """Corrupted table dir (parquet files, no _delta_log) is moved to .rescue/."""
        backend = LocalBackend(tmp_path)
        table_dir = tmp_path / "raw" / "trading212_snapshot"
        table_dir.mkdir(parents=True)
        # Simulate a failed write: parquet files but no _delta_log
        (table_dir / "part-00000-abc.snappy.parquet").write_bytes(b"\x00")
        (table_dir / "part-00001-def.snappy.parquet").write_bytes(b"\x00")

        backend.ensure_parent(str(table_dir))

        # Original table directory should be removed so write_deltalake starts fresh
        assert not table_dir.exists()
        # Orphaned files should be rescued to .rescue/ under data_dir
        rescue_dir = tmp_path / ".rescue"
        assert rescue_dir.is_dir()
        rescued = list(rescue_dir.iterdir())
        assert len(rescued) == 1
        assert rescued[0].name.startswith("trading212_snapshot_")
        # The rescued directory should contain the orphaned parquet files
        assert len(list(rescued[0].glob("*.parquet"))) == 2

    def test_ensure_parent_preserves_valid_table(self, tmp_path: Path):
        """A valid Delta table (with _delta_log) is left intact."""
        backend = LocalBackend(tmp_path)
        table_dir = tmp_path / "raw" / "ibkr_snapshot"
        table_dir.mkdir(parents=True)
        delta_log = table_dir / "_delta_log"
        delta_log.mkdir()
        (delta_log / "00000000000000000000.json").write_text("{}")
        (table_dir / "part-00000-abc.snappy.parquet").write_bytes(b"\x00")

        backend.ensure_parent(str(table_dir))

        # Valid table should be untouched
        assert (table_dir / "_delta_log").is_dir()
        assert (table_dir / "part-00000-abc.snappy.parquet").exists()

    def test_ensure_parent_rescues_empty_dir(self, tmp_path: Path):
        """An empty table directory is moved to .rescue/ so write_deltalake starts fresh."""
        backend = LocalBackend(tmp_path)
        table_dir = tmp_path / "raw" / "ibkr_snapshot"
        table_dir.mkdir(parents=True)

        backend.ensure_parent(str(table_dir))

        # Empty dir moved to rescue
        assert not table_dir.exists()
        # But parent dir created
        assert (tmp_path / "raw").is_dir()
        # Rescue dir contains the moved empty directory
        rescue_dir = tmp_path / ".rescue"
        assert rescue_dir.is_dir()
        rescued = list(rescue_dir.iterdir())
        assert len(rescued) == 1
        assert rescued[0].name.startswith("ibkr_snapshot_")

    def test_ensure_parent_noop_for_nonexistent_path(self, tmp_path: Path):
        """A path that doesn't exist yet is simply prepared (parent created)."""
        backend = LocalBackend(tmp_path)
        path = str(tmp_path / "raw" / "new_table")

        backend.ensure_parent(path)

        assert (tmp_path / "raw").is_dir()
        assert not (tmp_path / "raw" / "new_table").exists()


class TestS3Backend:
    """Test S3Backend URI generation and no-op ensure_parent."""

    def test_table_path_with_prefix(self):
        backend = S3Backend(bucket="my-bucket", prefix="pipeline")
        assert (
            backend.table_path("raw", "ibkr_snapshot")
            == "s3://my-bucket/pipeline/raw/ibkr_snapshot"
        )

    def test_table_path_default_prefix(self):
        backend = S3Backend(bucket="my-bucket")
        assert (
            backend.table_path("raw", "ibkr_snapshot")
            == f"s3://my-bucket/{S3_DEFAULT_PREFIX}/raw/ibkr_snapshot"
        )

    def test_table_path_custom_prefix(self):
        backend = S3Backend(bucket="my-bucket", prefix="data")
        assert (
            backend.table_path("normalized", "consolidated_holdings")
            == "s3://my-bucket/data/normalized/consolidated_holdings"
        )

    def test_table_path_strips_trailing_slash(self):
        backend = S3Backend(bucket="my-bucket", prefix="pipeline/")
        assert (
            backend.table_path("raw", "ibkr_snapshot")
            == "s3://my-bucket/pipeline/raw/ibkr_snapshot"
        )

    def test_table_path_no_prefix(self):
        backend = S3Backend(bucket="my-bucket", prefix="")
        assert (
            backend.table_path("raw", "ibkr_snapshot")
            == "s3://my-bucket/raw/ibkr_snapshot"
        )

    def test_ensure_parent_is_noop(self):
        backend = S3Backend(bucket="my-bucket")
        # Should not raise — S3 doesn't need parent dirs
        backend.ensure_parent("s3://my-bucket/pipeline/raw/ibkr_snapshot")

    def test_storage_options_lowercase_keys(self):
        """S3Backend.storage_options returns lowercase keys for deltalake."""
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "test-key-id",
                "AWS_SECRET_ACCESS_KEY": "test-secret",
                "AWS_REGION": "us-east-1",
            },
        ):
            backend = S3Backend(bucket="my-bucket")
            opts = backend.storage_options
            # Keys must be lowercase — deltalake's object_store only
            # recognizes lowercase keys.
            assert "aws_access_key_id" in opts
            assert "aws_secret_access_key" in opts
            assert "aws_region" in opts
            # Uppercase keys must NOT be present.
            assert "AWS_ACCESS_KEY_ID" not in opts
            assert "AWS_SECRET_ACCESS_KEY" not in opts
            assert "AWS_REGION" not in opts
            # Values come from environment variables.
            assert opts["aws_access_key_id"] == "test-key-id"
            assert opts["aws_secret_access_key"] == "test-secret"
            assert opts["aws_region"] == "us-east-1"

    def test_storage_options_omits_empty_credentials(self):
        """Empty credential strings are omitted so object_store can fall back."""
        with patch.dict(
            os.environ,
            {"AWS_REGION": "eu-west-1"},
            clear=False,
        ):
            os.environ.pop("AWS_ACCESS_KEY_ID", None)
            os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
            backend = S3Backend(bucket="my-bucket")
            opts = backend.storage_options
            # Empty credentials should be absent, not present with "".
            assert "aws_access_key_id" not in opts
            assert "aws_secret_access_key" not in opts
            # Region is always present (has default).
            assert opts["aws_region"] == "eu-west-1"

    def test_storage_options_includes_endpoint_url(self):
        """S3_ENDPOINT_URL is included in storage options for MinIO."""
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "minioadmin",
                "AWS_SECRET_ACCESS_KEY": "minioadmin",
                "AWS_REGION": "us-east-1",
                "S3_ENDPOINT_URL": "http://minio:9000",
            },
        ):
            backend = S3Backend(bucket="pipeline")
            opts = backend.storage_options
            assert opts["aws_endpoint_url"] == "http://minio:9000"

    def test_storage_options_includes_allow_http(self):
        """S3_ALLOW_HTTP=true adds aws_allow_http to storage options."""
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "minioadmin",
                "AWS_SECRET_ACCESS_KEY": "minioadmin",
                "AWS_REGION": "us-east-1",
                "S3_ALLOW_HTTP": "true",
            },
        ):
            backend = S3Backend(bucket="pipeline")
            opts = backend.storage_options
            assert opts["aws_allow_http"] == "true"

    def test_storage_options_omits_endpoint_url_when_not_set(self):
        """aws_endpoint_url is absent when S3_ENDPOINT_URL is not set."""
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "test-key",
                "AWS_SECRET_ACCESS_KEY": "test-secret",
                "AWS_REGION": "us-east-1",
            },
            clear=False,
        ):
            os.environ.pop("S3_ENDPOINT_URL", None)
            os.environ.pop("S3_ALLOW_HTTP", None)
            backend = S3Backend(bucket="my-bucket")
            opts = backend.storage_options
            assert "aws_endpoint_url" not in opts
            assert "aws_allow_http" not in opts


class TestPathsModule:
    """Test that pipeline.paths delegates to StorageConfig."""

    def setup_method(self):
        import pipeline.storage

        pipeline.storage._config = None

    def teardown_method(self):
        import pipeline.storage

        pipeline.storage._config = None

    def test_paths_module_delegates_to_storage(self, tmp_path: Path):
        import pipeline.paths

        data = tmp_path / "test-data"
        (data / "raw").mkdir(parents=True)
        (data / "normalized").mkdir(parents=True)
        (data / "analytics").mkdir(parents=True)
        secrets = tmp_path / ".secrets"
        secrets.mkdir()
        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(secrets),
            encryption_key_file=str(secrets / "encryption.key"),
            backend=LocalBackend(data),
        )
        use_storage(config)

        assert pipeline.paths.DATA_DIR == str(data)
        assert pipeline.paths.RAW_DIR == str(data / "raw")
        assert pipeline.paths.NORMALIZED_DIR == str(data / "normalized")

    def test_paths_table_paths(self, tmp_path: Path):
        import pipeline.paths

        data = tmp_path / "test-data"
        (data / "raw").mkdir(parents=True)
        (data / "normalized").mkdir(parents=True)
        (data / "analytics").mkdir(parents=True)
        secrets = tmp_path / ".secrets"
        secrets.mkdir()
        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(secrets),
            encryption_key_file=str(secrets / "encryption.key"),
            backend=LocalBackend(data),
        )
        use_storage(config)

        assert pipeline.paths.RAW_IBKR_SNAPSHOT == str(data / "raw" / "ibkr_snapshot")
        assert pipeline.paths.NORMALIZED_CONSOLIDATED_HOLDINGS == str(
            data / "normalized" / "consolidated_holdings"
        )
        assert pipeline.paths.ANALYTICS_PORTFOLIO_ALLOCATION == str(
            data / "analytics" / "portfolio_allocation"
        )

    def test_paths_unknown_attribute_raises(self):
        import pipeline.paths

        with pytest.raises(AttributeError, match="has no attribute"):
            _ = pipeline.paths.NONEXISTENT_PATH


class TestStorageConfigHelpers:
    """Test StorageConfig convenience methods."""

    def test_raw_path(self, tmp_path: Path):
        data = tmp_path / "data"
        secrets = tmp_path / ".secrets"
        secrets.mkdir()
        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(secrets),
            encryption_key_file=str(secrets / "encryption.key"),
            backend=LocalBackend(data),
        )
        assert config.raw_path("ibkr_snapshot") == str(data / "raw" / "ibkr_snapshot")

    def test_normalized_path(self, tmp_path: Path):
        data = tmp_path / "data"
        secrets = tmp_path / ".secrets"
        secrets.mkdir()
        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(secrets),
            encryption_key_file=str(secrets / "encryption.key"),
            backend=LocalBackend(data),
        )
        assert config.normalized_path("ibkr_snapshot") == str(
            data / "normalized" / "ibkr_snapshot"
        )

    def test_analytics_path(self, tmp_path: Path):
        data = tmp_path / "data"
        secrets = tmp_path / ".secrets"
        secrets.mkdir()
        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(secrets),
            encryption_key_file=str(secrets / "encryption.key"),
            backend=LocalBackend(data),
        )
        assert config.analytics_path("portfolio_allocation") == str(
            data / "analytics" / "portfolio_allocation"
        )


class TestDemoStorage:
    """Test resolve_storage() in demo mode.

    When DEMO=true, storage paths are isolated from production.
    No cross-mode fallback — demo mode uses demo paths exclusively.
    """

    def setup_method(self):
        import pipeline.storage

        pipeline.storage._config = None

    def teardown_method(self):
        import pipeline.storage

        pipeline.storage._config = None

    def test_local_demo_mode_appends_demo_suffix(self, monkeypatch, tmp_path):
        """In demo mode with local storage, data dir gets _demo suffix."""
        monkeypatch.delenv("S3_BUCKET", raising=False)
        monkeypatch.delenv("PIPELINE_DATA_DIR", raising=False)
        monkeypatch.delenv("PIPELINE_DATA_DIR_DEMO", raising=False)
        monkeypatch.setenv("DEMO", "true")
        monkeypatch.setattr("pipeline.storage.PROJECT_ROOT", tmp_path)
        config = resolve_storage()
        assert isinstance(config.backend, LocalBackend)
        assert config.data_dir.endswith("_demo")
        assert "data_demo" in config.data_dir

    def test_local_demo_mode_custom_dir(self, monkeypatch, tmp_path):
        """PIPELINE_DATA_DIR_DEMO overrides the default demo data dir."""
        custom = tmp_path / "custom-demo-data"
        custom.mkdir()
        monkeypatch.delenv("S3_BUCKET", raising=False)
        monkeypatch.setenv("PIPELINE_DATA_DIR_DEMO", str(custom))
        monkeypatch.setenv("DEMO", "true")
        config = resolve_storage()
        assert isinstance(config.backend, LocalBackend)
        assert config.data_dir == str(custom)

    def test_s3_demo_mode_default_bucket_suffix(self, monkeypatch):
        """In demo mode, S3 bucket gets _demo suffix by default."""
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        monkeypatch.delenv("S3_BUCKET_DEMO", raising=False)
        monkeypatch.delenv("S3_PREFIX_DEMO", raising=False)
        monkeypatch.setenv("DEMO", "true")
        config = resolve_storage()
        assert isinstance(config.backend, S3Backend)
        assert config.backend.bucket == "my-bucket_demo"
        assert config.backend.prefix == "pipeline_demo"

    def test_s3_demo_mode_explicit_bucket(self, monkeypatch):
        """S3_BUCKET_DEMO and S3_PREFIX_DEMO override defaults."""
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        monkeypatch.setenv("S3_BUCKET_DEMO", "explicit-demo-bucket")
        monkeypatch.setenv("S3_PREFIX_DEMO", "custom-demo-prefix")
        monkeypatch.setenv("DEMO", "true")
        config = resolve_storage()
        assert isinstance(config.backend, S3Backend)
        assert config.backend.bucket == "explicit-demo-bucket"
        assert config.backend.prefix == "custom-demo-prefix"

    def test_non_demo_unchanged(self, monkeypatch):
        """Without DEMO, storage config is unchanged from production."""
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        monkeypatch.delenv("DEMO", raising=False)
        config = resolve_storage()
        assert isinstance(config.backend, S3Backend)
        assert config.backend.bucket == "my-bucket"
        assert config.backend.prefix == "pipeline"

    def test_local_non_demo_unchanged(self, monkeypatch, tmp_path):
        """Without DEMO, local storage uses the default data dir."""
        monkeypatch.delenv("S3_BUCKET", raising=False)
        monkeypatch.delenv("PIPELINE_DATA_DIR", raising=False)
        monkeypatch.delenv("DEMO", raising=False)
        monkeypatch.setattr("pipeline.storage.PROJECT_ROOT", tmp_path)
        config = resolve_storage()
        assert isinstance(config.backend, LocalBackend)
        assert config.data_dir == str(tmp_path / "data")

    def test_s3_demo_paths_include_prefix(self, monkeypatch):
        """Demo S3 paths use pipeline_demo prefix by default."""
        monkeypatch.setenv("S3_BUCKET", "pipeline")
        monkeypatch.delenv("S3_BUCKET_DEMO", raising=False)
        monkeypatch.delenv("S3_PREFIX_DEMO", raising=False)
        monkeypatch.setenv("DEMO", "true")
        config = resolve_storage()
        assert config.raw_dir == "s3://pipeline_demo/pipeline_demo/raw"
        assert config.normalized_dir == "s3://pipeline_demo/pipeline_demo/normalized"
        assert config.analytics_dir == "s3://pipeline_demo/pipeline_demo/analytics"
