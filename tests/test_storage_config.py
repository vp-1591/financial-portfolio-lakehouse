"""Tests for the storage configuration system.

Verifies that:
- ``resolve_storage`` uses execution mode (``set_mode``) to select backend
- ``resolve_storage`` requires ``S3_BUCKET`` in docker/prod modes
- ``resolve_storage`` uses ``S3_BUCKET`` in staging mode
- ``use_storage`` injects custom configs
- ``get_storage`` returns the active config
- ``pipeline.paths`` module delegates to the active ``StorageConfig``
- ``S3Backend`` generates correct URIs and has no-op ensure_parent
- ``LocalBackend.ensure_parent`` rescues orphaned parquet files
  from failed writes (e.g. Docker volume mount rename failures)
  to a ``.rescue/`` directory under the data directory
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.secrets import reset_mode, set_mode
from pipeline.storage import (
    S3Backend,
    S3_DEFAULT_PREFIX,
    StorageConfig,
    get_storage,
    resolve_storage,
    use_storage,
)
from tests.local_backend import LocalBackend


class TestResolveStorage:
    """Test resolve_storage() with execution modes."""

    def setup_method(self):
        """Reset module-level singletons before each test."""
        import pipeline.storage

        pipeline.storage._config = None
        reset_mode()

    def teardown_method(self):
        """Reset module-level singletons after each test."""
        import pipeline.storage

        pipeline.storage._config = None
        reset_mode()

    def test_docker_mode_with_s3_bucket(self, monkeypatch):
        """In docker mode, resolve_storage creates S3Backend with MinIO config."""
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        set_mode("docker")
        config = resolve_storage()
        assert isinstance(config.backend, S3Backend)
        assert config.data_dir.startswith("s3://")
        assert "test-bucket" in config.data_dir

    def test_docker_mode_with_endpoint_url(self, monkeypatch):
        """Docker mode with S3_ENDPOINT_URL sets up MinIO endpoint."""
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
        set_mode("docker")
        config = resolve_storage()
        assert isinstance(config.backend, S3Backend)
        assert config.backend.bucket == "test-bucket"

    def test_docker_mode_requires_s3_bucket(self, monkeypatch):
        """Docker mode raises ValueError when S3_BUCKET is not set."""
        monkeypatch.delenv("S3_BUCKET", raising=False)
        set_mode("docker")
        with pytest.raises(ValueError, match="S3_BUCKET is required"):
            resolve_storage()

    def test_staging_mode_uses_s3_bucket_directly(self, monkeypatch):
        """In staging mode, S3 bucket is read from S3_BUCKET (no suffix fallback)."""
        monkeypatch.setenv("S3_BUCKET", "my-staging-bucket")
        set_mode("staging")
        config = resolve_storage()
        assert isinstance(config.backend, S3Backend)
        assert config.backend.bucket == "my-staging-bucket"
        assert config.backend.prefix == "pipeline_demo"

    def test_staging_mode_explicit_prefix(self, monkeypatch):
        """S3_BUCKET and S3_PREFIX override defaults in staging mode."""
        monkeypatch.setenv("S3_BUCKET", "my-staging-bucket")
        monkeypatch.setenv("S3_PREFIX", "custom-prefix")
        set_mode("staging")
        config = resolve_storage()
        assert isinstance(config.backend, S3Backend)
        assert config.backend.bucket == "my-staging-bucket"
        assert config.backend.prefix == "custom-prefix"

    def test_staging_mode_paths_include_prefix(self, monkeypatch):
        """Staging mode uses pipeline_demo prefix in S3 paths."""
        monkeypatch.setenv("S3_BUCKET", "pipeline-staging")
        set_mode("staging")
        config = resolve_storage()
        assert config.raw_dir == "s3://pipeline-staging/pipeline_demo/raw"
        assert config.normalized_dir == "s3://pipeline-staging/pipeline_demo/normalized"
        assert config.analytics_dir == "s3://pipeline-staging/pipeline_demo/analytics"

    def test_staging_mode_s3_bucket_alone(self, monkeypatch):
        """S3_BUCKET alone works in staging mode."""
        monkeypatch.setenv("S3_BUCKET", "my-staging-bucket")
        set_mode("staging")
        config = resolve_storage()
        assert isinstance(config.backend, S3Backend)
        assert config.backend.bucket == "my-staging-bucket"
        assert config.backend.prefix == "pipeline_demo"

    def test_staging_mode_requires_bucket(self, monkeypatch):
        """Staging mode raises ValueError when no S3 bucket is configured."""
        monkeypatch.delenv("S3_BUCKET", raising=False)
        set_mode("staging")
        with pytest.raises(ValueError, match="Staging mode requires"):
            resolve_storage()

    def test_staging_prefix_empty_falls_back(self, monkeypatch):
        """Empty S3_PREFIX falls back to pipeline_demo in staging mode."""
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        monkeypatch.setenv("S3_PREFIX", "")
        set_mode("staging")
        config = resolve_storage()
        assert config.backend.prefix == "pipeline_demo"

    def test_staging_empty_bucket_raises(self, monkeypatch):
        """Empty S3_BUCKET in staging mode raises ValueError (no suffix fallback)."""
        monkeypatch.setenv("S3_BUCKET", "")
        set_mode("staging")
        with pytest.raises(ValueError, match="Staging mode requires"):
            resolve_storage()

    def test_prod_mode_with_s3_bucket(self, monkeypatch):
        """In prod mode, resolve_storage creates S3Backend with production bucket."""
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        set_mode("prod")
        config = resolve_storage()
        assert isinstance(config.backend, S3Backend)
        assert config.backend.bucket == "my-bucket"
        assert config.backend.prefix == "pipeline"

    def test_prod_mode_requires_s3_bucket(self, monkeypatch):
        """Prod mode raises ValueError when S3_BUCKET is not set."""
        monkeypatch.delenv("S3_BUCKET", raising=False)
        set_mode("prod")
        with pytest.raises(ValueError, match="S3_BUCKET is required"):
            resolve_storage()

    def test_prod_prefix_empty_falls_back(self, monkeypatch):
        """Empty S3_PREFIX falls back to 'pipeline'."""
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        monkeypatch.setenv("S3_PREFIX", "")
        set_mode("prod")
        config = resolve_storage()
        assert config.backend.prefix == "pipeline"

    def test_docker_prefix_empty_falls_back(self, monkeypatch):
        """Empty S3_PREFIX falls back to 'pipeline' in docker mode."""
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv("S3_PREFIX", "")
        set_mode("docker")
        config = resolve_storage()
        assert config.backend.prefix == "pipeline"

    def test_mode_not_set_raises(self):
        """resolve_storage raises RuntimeError when mode is not set."""
        import pipeline.storage

        pipeline.storage._config = None
        reset_mode()
        with pytest.raises(RuntimeError, match="Mode not set"):
            resolve_storage()

    def test_secrets_dir_at_project_root(self, monkeypatch):
        """secrets_dir is always at project root, not inside data dir."""
        from pipeline.storage import PROJECT_ROOT

        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        set_mode("docker")
        config = resolve_storage()
        assert config.secrets_dir == str(PROJECT_ROOT / ".secrets")
        assert config.encryption_key_file == str(
            PROJECT_ROOT / ".secrets" / "encryption.key"
        )

    def test_s3_secrets_dir_not_in_s3(self, monkeypatch):
        """secrets_dir is a local path even when data is on S3."""
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        set_mode("docker")
        config = resolve_storage()
        assert ".secrets" in config.secrets_dir
        assert not config.secrets_dir.startswith("s3://")

    def test_s3_does_not_use_pipeline_data_dir(self, monkeypatch):
        """When S3_BUCKET is set, PIPELINE_DATA_DIR is ignored."""
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv("PIPELINE_DATA_DIR", "/tmp/should-be-ignored")
        set_mode("docker")
        config = resolve_storage()
        assert isinstance(config.backend, S3Backend)
        assert "/tmp/should-be-ignored" not in config.data_dir


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
        # Should not raise -- S3 doesn't need parent dirs
        backend.ensure_parent("s3://my-bucket/pipeline/raw/ibkr_snapshot")

    def test_staging_path_with_prefix(self):
        backend = S3Backend(bucket="my-bucket", prefix="pipeline")
        assert (
            backend.staging_path("staging", "xtb", "report.xlsx")
            == "s3://my-bucket/pipeline/staging/xtb/report.xlsx"
        )

    def test_staging_path_no_prefix(self):
        backend = S3Backend(bucket="my-bucket", prefix="")
        assert (
            backend.staging_path("staging", "xtb", "report.xlsx")
            == "s3://my-bucket/staging/xtb/report.xlsx"
        )

    def test_staging_path_demo_prefix(self):
        backend = S3Backend(bucket="my-bucket-demo", prefix="pipeline_demo")
        assert (
            backend.staging_path("staging_demo", "xtb", "report.xlsx")
            == "s3://my-bucket-demo/pipeline_demo/staging_demo/xtb/report.xlsx"
        )

    def test_storage_options_lowercase_keys(self, monkeypatch):
        """S3Backend.storage_options returns lowercase keys for deltalake."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-key-id")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        set_mode("docker")
        backend = S3Backend(bucket="my-bucket")
        opts = backend.storage_options
        # Keys must be lowercase -- deltalake's object_store only
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

    def test_storage_options_no_credentials_omitted_for_iam_role_fallback(
        self, monkeypatch
    ):
        """When both credentials are None, keys are omitted to allow IAM role fallback.

        On ECS with IAM task roles, omitting credential keys allows the SDK to
        fall through its default credential chain. In CI, step-level conditionals
        ensure production env vars are absent in demo runs.
        """
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        monkeypatch.setenv("AWS_REGION", "eu-west-1")
        set_mode("docker")
        backend = S3Backend(bucket="my-bucket")
        opts = backend.storage_options
        # Credential keys are omitted entirely -- not empty strings --
        # allowing object_store to fall through to IAM role metadata.
        assert "aws_access_key_id" not in opts
        assert "aws_secret_access_key" not in opts
        # Region is always present (has default).
        assert opts["aws_region"] == "eu-west-1"

    def test_storage_options_includes_endpoint_url(self, monkeypatch):
        """S3_ENDPOINT_URL is included in storage options for MinIO."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
        set_mode("docker")
        backend = S3Backend(bucket="pipeline")
        opts = backend.storage_options
        assert opts["aws_endpoint_url"] == "http://minio:9000"

    def test_storage_options_includes_allow_http(self, monkeypatch):
        """S3_ALLOW_HTTP=true adds aws_allow_http to storage options."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        monkeypatch.setenv("S3_ALLOW_HTTP", "true")
        set_mode("docker")
        backend = S3Backend(bucket="pipeline")
        opts = backend.storage_options
        assert opts["aws_allow_http"] == "true"

    def test_storage_options_omits_endpoint_url_when_not_set(self, monkeypatch):
        """aws_endpoint_url is absent when S3_ENDPOINT_URL is not set."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
        monkeypatch.delenv("S3_ALLOW_HTTP", raising=False)
        set_mode("docker")
        backend = S3Backend(bucket="my-bucket")
        opts = backend.storage_options
        assert "aws_endpoint_url" not in opts
        assert "aws_allow_http" not in opts

    def test_storage_options_staging_mode_uses_credentials(self, monkeypatch):
        """In staging mode, storage_options uses AWS credentials from env vars."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "staging-key-id")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "staging-secret")
        monkeypatch.setenv("AWS_REGION", "eu-west-1")
        set_mode("staging")
        backend = S3Backend(bucket="my-bucket")
        opts = backend.storage_options
        assert opts["aws_access_key_id"] == "staging-key-id"
        assert opts["aws_secret_access_key"] == "staging-secret"

    def test_storage_options_staging_mode_no_credentials_omitted(self, monkeypatch):
        """In staging mode, missing credentials result in omitted keys, not empty strings.

        Omitting keys allows IAM role fallback on ECS. In CI, step-level
        conditionals ensure production env vars are absent in staging runs
        (ADR 0041 Decision #1), so there is nothing to fall back to.
        """
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        monkeypatch.setenv("AWS_REGION", "eu-west-1")
        set_mode("staging")
        backend = S3Backend(bucket="my-bucket")
        opts = backend.storage_options
        # Should omit credential keys entirely, allowing IAM role fallback.
        assert "aws_access_key_id" not in opts
        assert "aws_secret_access_key" not in opts


class TestPathsModule:
    """Test that pipeline.paths delegates to StorageConfig."""

    def setup_method(self):
        import pipeline.storage

        pipeline.storage._config = None
        reset_mode()

    def teardown_method(self):
        import pipeline.storage

        pipeline.storage._config = None
        reset_mode()

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
        assert pipeline.paths.ANALYTICS_PORTFOLIO_HOLDINGS == str(
            data / "analytics" / "portfolio_holdings"
        )

    def test_paths_unknown_attribute_raises(self, monkeypatch):
        import pipeline.paths

        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        set_mode("docker")
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
        assert config.analytics_path("portfolio_holdings") == str(
            data / "analytics" / "portfolio_holdings"
        )

    def test_staging_path_local_backend(self, tmp_path: Path) -> None:

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
        # Production mode -- staging_path uses "staging" prefix
        set_mode("prod")
        result = config.staging_path("xtb", "report.xlsx")
        assert result == str(data / "staging" / "xtb" / "report.xlsx")

    def test_staging_path_local_backend_staging(self, tmp_path: Path) -> None:

        data = tmp_path / "data_demo"
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
        # Staging mode -- staging_path uses "staging_demo" prefix
        set_mode("staging")
        result = config.staging_path("xtb", "report.xlsx")
        assert result == str(data / "staging_demo" / "xtb" / "report.xlsx")

    def test_staging_path_s3_backend(self, monkeypatch) -> None:
        backend = S3Backend(bucket="my-bucket", prefix="pipeline")
        config = StorageConfig(
            data_dir="s3://my-bucket/pipeline",
            raw_dir="s3://my-bucket/pipeline/raw",
            normalized_dir="s3://my-bucket/pipeline/normalized",
            analytics_dir="s3://my-bucket/pipeline/analytics",
            secrets_dir="/app/.secrets",
            encryption_key_file="/app/.secrets/encryption.key",
            backend=backend,
        )
        set_mode("docker")
        result = config.staging_path("xtb", "report.xlsx")
        assert result == "s3://my-bucket/pipeline/staging/xtb/report.xlsx"

    def test_staging_path_s3_backend_staging(self, monkeypatch) -> None:
        backend = S3Backend(bucket="my-bucket-demo", prefix="pipeline_demo")
        config = StorageConfig(
            data_dir="s3://my-bucket-demo/pipeline_demo",
            raw_dir="s3://my-bucket-demo/pipeline_demo/raw",
            normalized_dir="s3://my-bucket-demo/pipeline_demo/normalized",
            analytics_dir="s3://my-bucket-demo/pipeline_demo/analytics",
            secrets_dir="/app/.secrets",
            encryption_key_file="/app/.secrets/encryption.key",
            backend=backend,
        )
        set_mode("staging")
        result = config.staging_path("xtb", "report.xlsx")
        assert (
            result == "s3://my-bucket-demo/pipeline_demo/staging_demo/xtb/report.xlsx"
        )
