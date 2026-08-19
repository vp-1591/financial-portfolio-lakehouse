"""Tests for mode resolution: set_mode, get_mode, is_staging, resolve_storage."""

from __future__ import annotations

import pytest

from pipeline.secrets import get_mode, is_staging, reset_mode, set_mode
from pipeline.storage import S3_DEFAULT_PROD_BUCKET, resolve_storage, use_storage


class TestSetMode:
    """set_mode validates and stores the mode."""

    def test_set_mode_valid(self) -> None:
        for mode in ("docker", "staging", "prod"):
            set_mode(mode)
            assert get_mode() == mode
            reset_mode()

    def test_set_mode_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid mode"):
            set_mode("invalid")

    def test_set_mode_overwrites(self) -> None:
        set_mode("docker")
        set_mode("prod")
        assert get_mode() == "prod"
        reset_mode()


class TestGetMode:
    """get_mode raises RuntimeError when mode is not set."""

    def test_get_mode_unset_raises(self) -> None:
        reset_mode()
        with pytest.raises(RuntimeError, match="Mode not set"):
            get_mode()

    def test_get_mode_after_set(self) -> None:
        set_mode("docker")
        assert get_mode() == "docker"
        reset_mode()


class TestIsStaging:
    """is_staging returns True only in staging mode."""

    def test_staging_is_staging(self) -> None:
        set_mode("staging")
        assert is_staging() is True
        reset_mode()

    def test_docker_is_not_staging(self) -> None:
        set_mode("docker")
        assert is_staging() is False
        reset_mode()

    def test_prod_is_not_staging(self) -> None:
        set_mode("prod")
        assert is_staging() is False
        reset_mode()


class TestResolveStorage:
    """resolve_storage dispatches on mode to select S3 backend."""

    def test_docker_mode_uses_minio_endpoint(self, monkeypatch) -> None:

        set_mode("docker")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://localhost:9000")
        monkeypatch.setenv("S3_ALLOW_HTTP", "true")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        config = resolve_storage()
        assert config.backend.bucket == "test-bucket"
        # Exact path assertion (A5 C6): the prefix must appear in every path
        # so a bug dropping the prefix from the docker base is detected.
        assert config.data_dir == "s3://test-bucket/pipeline"
        assert config.raw_dir == "s3://test-bucket/pipeline/raw"
        assert config.normalized_dir == "s3://test-bucket/pipeline/normalized"
        assert config.analytics_dir == "s3://test-bucket/pipeline/analytics"
        use_storage(config)  # just verify it doesn't crash
        reset_mode()

    def test_staging_mode_uses_s3_bucket(self, monkeypatch) -> None:

        set_mode("staging")
        monkeypatch.setenv("S3_BUCKET", "staging-bucket")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "staging-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "staging-secret")
        monkeypatch.setenv("AWS_REGION", "eu-west-1")
        config = resolve_storage()
        assert config.backend.bucket == "staging-bucket"
        assert config.backend.prefix == ""
        reset_mode()

    def test_prod_mode_uses_prod_bucket(self, monkeypatch) -> None:

        set_mode("prod")
        monkeypatch.setenv("S3_BUCKET", "prod-bucket")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "prod-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "prod-secret")
        monkeypatch.setenv("AWS_REGION", "eu-west-1")
        config = resolve_storage()
        assert config.backend.bucket == "prod-bucket"
        assert config.backend.prefix == "pipeline"
        # Exact path assertion (A5 C6): the prod prefix must appear in every
        # path so a bug dropping the prefix from the prod base is detected.
        assert config.data_dir == "s3://prod-bucket/pipeline"
        assert config.raw_dir == "s3://prod-bucket/pipeline/raw"
        assert config.normalized_dir == "s3://prod-bucket/pipeline/normalized"
        assert config.analytics_dir == "s3://prod-bucket/pipeline/analytics"
        reset_mode()

    def test_docker_mode_missing_bucket_raises(self, monkeypatch) -> None:

        set_mode("docker")
        # S3_BUCKET not set
        with pytest.raises(ValueError, match="S3_BUCKET is required"):
            resolve_storage()
        reset_mode()

    def test_prod_mode_missing_bucket_defaults(self, monkeypatch) -> None:

        set_mode("prod")
        # S3_BUCKET not set — defaults to the production bucket
        config = resolve_storage()
        assert config.backend.bucket == S3_DEFAULT_PROD_BUCKET
        assert config.backend.prefix == "pipeline"
        reset_mode()

    def test_staging_mode_missing_bucket_raises(self, monkeypatch) -> None:

        set_mode("staging")
        # S3_BUCKET not set
        with pytest.raises(ValueError, match="Staging mode requires"):
            resolve_storage()
        reset_mode()
