"""Tests for S3 credential configuration in query.py."""

import os
from unittest.mock import patch

import duckdb
import pytest

from pipeline.query import _configure_s3


class TestConfigureS3:
    """Verify _configure_s3 uses DuckDB SECRET mechanism."""

    def test_creates_s3_secret(self):
        """_configure_s3 should create a DuckDB S3 secret when credentials are present."""
        conn = duckdb.connect()
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "test-key-id",
                "AWS_SECRET_ACCESS_KEY": "test-secret",
                "AWS_REGION": "eu-west-1",
            },
        ):
            _configure_s3(conn)

        secrets = conn.execute("SELECT * FROM duckdb_secrets()").fetchall()
        # DuckDB stores the type as lowercase 's3'
        s3_secrets = [s for s in secrets if s[1] == "s3"]
        assert len(s3_secrets) >= 1, f"Expected at least one S3 secret, got: {secrets}"

        # Verify the secret contains our key ID
        secret_row = s3_secrets[0]
        assert "test-key-id" in str(secret_row), (
            f"Secret should contain key ID: {secret_row}"
        )
        conn.close()

    def test_uses_region_from_env(self):
        """_configure_s3 should use AWS_REGION env var."""
        conn = duckdb.connect()
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "test-key-id",
                "AWS_SECRET_ACCESS_KEY": "test-secret",
                "AWS_REGION": "us-east-1",
            },
        ):
            _configure_s3(conn)

        secrets = conn.execute(
            "SELECT * FROM duckdb_secrets() WHERE type = 's3'"
        ).fetchall()
        assert len(secrets) >= 1
        assert "us-east-1" in str(secrets[0]), (
            f"Secret should contain region: {secrets[0]}"
        )
        conn.close()

    def test_default_region(self):
        """_configure_s3 should default to eu-west-1 when AWS_REGION is unset."""
        conn = duckdb.connect()
        env = {
            "AWS_ACCESS_KEY_ID": "test-key-id",
            "AWS_SECRET_ACCESS_KEY": "test-secret",
        }
        # Ensure AWS_REGION is absent so default kicks in
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("AWS_REGION", None)
            _configure_s3(conn)

        secrets = conn.execute(
            "SELECT * FROM duckdb_secrets() WHERE type = 's3'"
        ).fetchall()
        assert len(secrets) >= 1
        assert "eu-west-1" in str(secrets[0]), (
            f"Secret should default to eu-west-1: {secrets[0]}"
        )
        conn.close()

    def test_secret_propagates_to_s3_settings(self):
        """DuckDB SECRET credentials should be accessible to delta_scan().

        Verify that CREATE SECRET propagates to the s3_* settings that
        extensions can read, unlike the legacy SET approach which only
        affected DuckDB's built-in httpfs.
        """
        conn = duckdb.connect()
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "test-key-id",
                "AWS_SECRET_ACCESS_KEY": "test-secret",
                "AWS_REGION": "eu-west-1",
            },
        ):
            _configure_s3(conn)

        # After CREATE SECRET, DuckDB should propagate credentials
        # so they're available to extensions like delta_scan()
        key_id = conn.execute("SELECT current_setting('s3_access_key_id')").fetchone()[
            0
        ]
        assert key_id == "test-key-id", f"S3 key should come from secret, got: {key_id}"

        region = conn.execute("SELECT current_setting('s3_region')").fetchone()[0]
        assert region == "eu-west-1", (
            f"S3 region should come from secret, got: {region}"
        )
        conn.close()

    def test_raises_when_credentials_absent(self):
        """When AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are not set,
        _configure_s3 should raise RuntimeError with an actionable message.

        DuckDB's delta_scan() cannot resolve credentials from
        ~/.aws/credentials or AWS SSO, so silently skipping SECRET
        creation leads to confusing IMDS timeout errors.  Raising an
        error with a clear message tells the user exactly what to do.
        """
        conn = duckdb.connect()
        with patch.dict(os.environ, {"AWS_REGION": "eu-west-1"}, clear=False):
            os.environ.pop("AWS_ACCESS_KEY_ID", None)
            os.environ.pop("AWS_SECRET_ACCESS_KEY", None)
            with pytest.raises(RuntimeError, match="AWS credentials not found"):
                _configure_s3(conn)
        conn.close()

    def test_raises_when_credentials_empty(self):
        """When AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are set to empty
        strings, _configure_s3 should raise RuntimeError.

        Empty-string credentials are normalized to None by
        resolve_aws_credentials(), so the same missing-credential
        error applies.
        """
        conn = duckdb.connect()
        with patch.dict(
            os.environ,
            {
                "AWS_ACCESS_KEY_ID": "",
                "AWS_SECRET_ACCESS_KEY": "",
                "AWS_REGION": "eu-west-1",
            },
        ):
            with pytest.raises(RuntimeError, match="AWS credentials not found"):
                _configure_s3(conn)
        conn.close()

    def test_demo_mode_uses_demo_credentials(self):
        """In demo mode, _configure_s3 uses _DEMO AWS credentials."""
        conn = duckdb.connect()
        with patch.dict(
            os.environ,
            {
                "DEMO": "true",
                "AWS_ACCESS_KEY_ID_DEMO": "demo-key-id",
                "AWS_SECRET_ACCESS_KEY_DEMO": "demo-secret",
                "AWS_ACCESS_KEY_ID": "prod-key-id",
                "AWS_SECRET_ACCESS_KEY": "prod-secret",
                "AWS_REGION": "eu-west-1",
            },
        ):
            _configure_s3(conn)

        secrets = conn.execute(
            "SELECT * FROM duckdb_secrets() WHERE type = 's3'"
        ).fetchall()
        assert len(secrets) >= 1, f"Expected S3 secret in demo mode, got: {secrets}"
        secret_str = str(secrets[0])
        assert "demo-key-id" in secret_str, (
            f"Secret should use demo credentials, got: {secret_str}"
        )
        conn.close()

    def test_demo_mode_no_fallback_to_prod(self):
        """In demo mode with missing _DEMO creds, a SECRET with empty
        credentials is created to prevent DuckDB from falling back to
        production credentials in environment variables.

        This tests the core isolation guarantee: if _DEMO AWS credentials
        are missing, the pipeline must NOT fall back to production
        credentials from environment variables.
        """
        conn = duckdb.connect()
        with patch.dict(os.environ, {"AWS_REGION": "eu-west-1"}, clear=False):
            os.environ["DEMO"] = "true"
            os.environ.pop("AWS_ACCESS_KEY_ID_DEMO", None)
            os.environ.pop("AWS_SECRET_ACCESS_KEY_DEMO", None)
            # Production creds are present — must NOT be used
            os.environ["AWS_ACCESS_KEY_ID"] = "prod-key-id"
            os.environ["AWS_SECRET_ACCESS_KEY"] = "prod-secret"
            _configure_s3(conn)

        # A SECRET should be created with empty credentials,
        # preventing DuckDB from falling back to production env vars.
        secrets = conn.execute(
            "SELECT * FROM duckdb_secrets() WHERE type = 's3'"
        ).fetchall()
        assert len(secrets) >= 1, (
            f"Expected at least one S3 secret with empty credentials, got: {secrets}"
        )
        # Verify the secret does NOT contain production credentials
        secret_str = str(secrets[0])
        assert "prod-key-id" not in secret_str, (
            f"SECRET must not contain production credentials: {secret_str}"
        )
        conn.close()
