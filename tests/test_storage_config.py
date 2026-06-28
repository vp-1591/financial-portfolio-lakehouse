"""Tests for the storage configuration system.

Verifies that:
- ``resolve_storage`` maps environments to correct directories
- ``use_storage`` injects custom configs
- ``get_storage`` returns the active config
- ``pipeline.paths`` module delegates to the active ``StorageConfig``
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.storage import (
    LocalBackend,
    StorageConfig,
    get_storage,
    resolve_storage,
    use_storage,
)


class TestResolveStorage:
    """Test resolve_storage() environment resolution."""

    def test_prod_env_uses_data_dir(self):
        config = resolve_storage("prod")
        assert config.data_dir.name == "data"
        assert config.env == "prod"

    def test_dev_env_uses_data_dev_dir(self):
        config = resolve_storage("dev")
        assert config.data_dir.name == "data-dev"
        assert config.env == "dev"

    def test_test_env_raises(self):
        with pytest.raises(ValueError, match="Unknown environment 'test'"):
            resolve_storage("test")

    def test_unknown_env_raises(self):
        with pytest.raises(ValueError, match="Unknown environment 'staging'"):
            resolve_storage("staging")

    def test_pipe_env_var_dev(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_ENV", "dev")
        config = resolve_storage()  # no explicit env
        assert config.data_dir.name == "data-dev"

    def test_pipe_env_var_prod(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_ENV", "prod")
        config = resolve_storage()  # no explicit env
        assert config.data_dir.name == "data"

    def test_explicit_env_overrides_pipe_env_var(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_ENV", "dev")
        config = resolve_storage("prod")  # explicit overrides env var
        assert config.data_dir.name == "data"

    def test_default_is_prod(self):
        config = resolve_storage()
        assert config.env == "prod"
        assert config.data_dir.name == "data"

    def test_dirs_are_subpaths_of_data_dir(self):
        config = resolve_storage("prod")
        assert config.raw_dir == config.data_dir / "raw"
        assert config.normalized_dir == config.data_dir / "normalized"
        assert config.analytics_dir == config.data_dir / "analytics"


class TestUseStorage:
    """Test use_storage() injection."""

    def test_use_storage_overrides(self, tmp_path: Path):
        data = tmp_path / "custom-data"
        data.mkdir()
        config = StorageConfig(
            env="custom",
            data_dir=data,
            raw_dir=data / "raw",
            normalized_dir=data / "normalized",
            analytics_dir=data / "analytics",
            secrets_dir=tmp_path / ".secrets",
            encryption_key_file=tmp_path / ".secrets" / "encryption.key",
            backend=LocalBackend(data),
        )
        use_storage(config)
        assert get_storage().data_dir == data
        assert get_storage().env == "custom"

    def test_use_storage_with_tmp_path(self, tmp_path: Path):
        data = tmp_path / "data"
        (data / "raw").mkdir(parents=True)
        (data / "normalized").mkdir(parents=True)
        (data / "analytics").mkdir(parents=True)
        config = StorageConfig(
            env="test",
            data_dir=data,
            raw_dir=data / "raw",
            normalized_dir=data / "normalized",
            analytics_dir=data / "analytics",
            secrets_dir=tmp_path / ".secrets",
            encryption_key_file=tmp_path / ".secrets" / "encryption.key",
            backend=LocalBackend(data),
        )
        use_storage(config)
        assert get_storage() is config
        assert get_storage().raw_dir == data / "raw"


class TestPathsDelegation:
    """Test that pipeline.paths delegates to the active StorageConfig."""

    def test_paths_module_delegates_to_storage(self, tmp_path: Path):
        import pipeline.paths

        data = tmp_path / "test-data"
        (data / "raw").mkdir(parents=True)
        (data / "normalized").mkdir(parents=True)
        (data / "analytics").mkdir(parents=True)
        config = StorageConfig(
            env="test",
            data_dir=data,
            raw_dir=data / "raw",
            normalized_dir=data / "normalized",
            analytics_dir=data / "analytics",
            secrets_dir=tmp_path / ".secrets",
            encryption_key_file=tmp_path / ".secrets" / "encryption.key",
            backend=LocalBackend(data),
        )
        use_storage(config)

        assert pipeline.paths.DATA_DIR == data
        assert pipeline.paths.RAW_DIR == data / "raw"
        assert pipeline.paths.NORMALIZED_DIR == data / "normalized"

    def test_paths_table_paths(self, tmp_path: Path):
        import pipeline.paths

        data = tmp_path / "test-data"
        (data / "raw").mkdir(parents=True)
        (data / "normalized").mkdir(parents=True)
        (data / "analytics").mkdir(parents=True)
        config = StorageConfig(
            env="test",
            data_dir=data,
            raw_dir=data / "raw",
            normalized_dir=data / "normalized",
            analytics_dir=data / "analytics",
            secrets_dir=tmp_path / ".secrets",
            encryption_key_file=tmp_path / ".secrets" / "encryption.key",
            backend=LocalBackend(data),
        )
        use_storage(config)

        assert pipeline.paths.RAW_IBKR_SNAPSHOT == data / "raw" / "ibkr_snapshot"
        assert pipeline.paths.NORMALIZED_CONSOLIDATED_HOLDINGS == data / "normalized" / "consolidated_holdings"
        assert pipeline.paths.ANALYTICS_PORTFOLIO_ALLOCATION == data / "analytics" / "portfolio_allocation"

    def test_paths_unknown_attribute_raises(self):
        import pipeline.paths

        with pytest.raises(AttributeError, match="has no attribute"):
            _ = pipeline.paths.NONEXISTENT_PATH


class TestLocalBackend:
    """Test LocalBackend path generation."""

    def test_table_path(self, tmp_path: Path):
        backend = LocalBackend(tmp_path)
        result = backend.table_path("raw", "ibkr_snapshot")
        assert result == str(tmp_path / "raw" / "ibkr_snapshot")

    def test_ensure_parent(self, tmp_path: Path):
        backend = LocalBackend(tmp_path)
        path = str(tmp_path / "raw" / "ibkr_snapshot")
        backend.ensure_parent(path)
        assert (tmp_path / "raw").exists()


class TestStorageConfigHelpers:
    """Test StorageConfig convenience methods."""

    def test_raw_path(self, tmp_path: Path):
        data = tmp_path / "data"
        config = StorageConfig(
            env="test",
            data_dir=data,
            raw_dir=data / "raw",
            normalized_dir=data / "normalized",
            analytics_dir=data / "analytics",
            secrets_dir=tmp_path / ".secrets",
            encryption_key_file=tmp_path / ".secrets" / "encryption.key",
            backend=LocalBackend(data),
        )
        assert config.raw_path("ibkr_snapshot") == str(data / "raw" / "ibkr_snapshot")

    def test_normalized_path(self, tmp_path: Path):
        data = tmp_path / "data"
        config = StorageConfig(
            env="test",
            data_dir=data,
            raw_dir=data / "raw",
            normalized_dir=data / "normalized",
            analytics_dir=data / "analytics",
            secrets_dir=tmp_path / ".secrets",
            encryption_key_file=tmp_path / ".secrets" / "encryption.key",
            backend=LocalBackend(data),
        )
        assert config.normalized_path("ibkr_snapshot") == str(data / "normalized" / "ibkr_snapshot")

    def test_analytics_path(self, tmp_path: Path):
        data = tmp_path / "data"
        config = StorageConfig(
            env="test",
            data_dir=data,
            raw_dir=data / "raw",
            normalized_dir=data / "normalized",
            analytics_dir=data / "analytics",
            secrets_dir=tmp_path / ".secrets",
            encryption_key_file=tmp_path / ".secrets" / "encryption.key",
            backend=LocalBackend(data),
        )
        assert config.analytics_path("portfolio_allocation") == str(data / "analytics" / "portfolio_allocation")