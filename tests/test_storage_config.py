"""Tests for the storage configuration system.

Verifies that:
- ``resolve_storage`` uses ``PIPELINE_DATA_DIR`` env var or project default
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
        config = resolve_storage()
        assert config.data_dir.name == "data"
        assert config.secrets_dir.name == ".secrets"
        assert config.encryption_key_file.name == "encryption.key"

    def test_pipeline_data_dir_env_var(self, monkeypatch, tmp_path: Path):
        custom = tmp_path / "my-data"
        custom.mkdir()
        monkeypatch.setenv("PIPELINE_DATA_DIR", str(custom))
        config = resolve_storage()
        assert config.data_dir == custom

    def test_pipeline_data_dir_absolute_path(self, monkeypatch, tmp_path: Path):
        custom = tmp_path / "absolute-path-data"
        custom.mkdir()
        monkeypatch.setenv("PIPELINE_DATA_DIR", str(custom.resolve()))
        config = resolve_storage()
        assert config.data_dir.resolve() == custom.resolve()

    def test_dirs_are_subpaths_of_data_dir(self):
        config = resolve_storage()
        assert config.raw_dir == config.data_dir / "raw"
        assert config.normalized_dir == config.data_dir / "normalized"
        assert config.analytics_dir == config.data_dir / "analytics"

    def test_secrets_dir_at_project_root(self):
        from pipeline.storage import PROJECT_ROOT

        config = resolve_storage()
        # secrets_dir should be at project root, not inside data dir
        assert config.secrets_dir == PROJECT_ROOT / ".secrets"
        assert config.encryption_key_file == PROJECT_ROOT / ".secrets" / "encryption.key"


class TestUseStorage:
    """Test use_storage() injection."""

    def test_use_storage_overrides(self, tmp_path: Path):
        data = tmp_path / "custom-data"
        data.mkdir()
        secrets = tmp_path / ".secrets"
        secrets.mkdir()
        config = StorageConfig(
            data_dir=data,
            raw_dir=data / "raw",
            normalized_dir=data / "normalized",
            analytics_dir=data / "analytics",
            secrets_dir=secrets,
            encryption_key_file=secrets / "encryption.key",
            backend=LocalBackend(data),
        )
        use_storage(config)
        assert get_storage().data_dir == data

    def test_use_storage_with_tmp_path(self, tmp_path: Path):
        data = tmp_path / "data"
        (data / "raw").mkdir(parents=True)
        (data / "normalized").mkdir(parents=True)
        (data / "analytics").mkdir(parents=True)
        secrets = tmp_path / ".secrets"
        secrets.mkdir()
        config = StorageConfig(
            data_dir=data,
            raw_dir=data / "raw",
            normalized_dir=data / "normalized",
            analytics_dir=data / "analytics",
            secrets_dir=secrets,
            encryption_key_file=secrets / "encryption.key",
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
        secrets = tmp_path / ".secrets"
        secrets.mkdir()
        config = StorageConfig(
            data_dir=data,
            raw_dir=data / "raw",
            normalized_dir=data / "normalized",
            analytics_dir=data / "analytics",
            secrets_dir=secrets,
            encryption_key_file=secrets / "encryption.key",
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
        secrets = tmp_path / ".secrets"
        secrets.mkdir()
        config = StorageConfig(
            data_dir=data,
            raw_dir=data / "raw",
            normalized_dir=data / "normalized",
            analytics_dir=data / "analytics",
            secrets_dir=secrets,
            encryption_key_file=secrets / "encryption.key",
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
        secrets = tmp_path / ".secrets"
        secrets.mkdir()
        config = StorageConfig(
            data_dir=data,
            raw_dir=data / "raw",
            normalized_dir=data / "normalized",
            analytics_dir=data / "analytics",
            secrets_dir=secrets,
            encryption_key_file=secrets / "encryption.key",
            backend=LocalBackend(data),
        )
        assert config.raw_path("ibkr_snapshot") == str(data / "raw" / "ibkr_snapshot")

    def test_normalized_path(self, tmp_path: Path):
        data = tmp_path / "data"
        secrets = tmp_path / ".secrets"
        secrets.mkdir()
        config = StorageConfig(
            data_dir=data,
            raw_dir=data / "raw",
            normalized_dir=data / "normalized",
            analytics_dir=data / "analytics",
            secrets_dir=secrets,
            encryption_key_file=secrets / "encryption.key",
            backend=LocalBackend(data),
        )
        assert config.normalized_path("ibkr_snapshot") == str(data / "normalized" / "ibkr_snapshot")

    def test_analytics_path(self, tmp_path: Path):
        data = tmp_path / "data"
        secrets = tmp_path / ".secrets"
        secrets.mkdir()
        config = StorageConfig(
            data_dir=data,
            raw_dir=data / "raw",
            normalized_dir=data / "normalized",
            analytics_dir=data / "analytics",
            secrets_dir=secrets,
            encryption_key_file=secrets / "encryption.key",
            backend=LocalBackend(data),
        )
        assert config.analytics_path("portfolio_allocation") == str(data / "analytics" / "portfolio_allocation")