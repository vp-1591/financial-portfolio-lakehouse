"""Tests for the storage configuration system.

Verifies that:
- ``resolve_storage`` uses ``PIPELINE_DATA_DIR`` env var or project default
- ``resolve_storage`` uses ``S3_BUCKET`` env var for S3Backend
- ``use_storage`` injects custom configs
- ``get_storage`` returns the active config
- ``pipeline.paths`` module delegates to the active ``StorageConfig``
- ``S3Backend`` generates correct URIs and has no-op ensure_parent
- ``LocalBackend`` creates directories on ensure_parent
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
