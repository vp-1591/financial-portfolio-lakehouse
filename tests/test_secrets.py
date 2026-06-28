"""Tests for pipeline.secrets module — env-var validation and .env loading."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.secrets import (
    REQUIRED_SECRET_NAMES,
    OPTIONAL_SECRET_NAMES,
    get_secret,
    inject_secrets,
)


class TestInjectSecrets:
    """Test inject_secrets() with .env file and environment variables."""

    def setup_method(self):
        """Reset module-level state before each test."""
        import pipeline.storage
        pipeline.storage._config = None

    def teardown_method(self):
        """Clean up env vars after each test."""
        import pipeline.storage
        pipeline.storage._config = None

    def test_inject_returns_available_secrets(self, monkeypatch):
        """Already-set env vars are returned by inject_secrets."""
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "test-token")
        monkeypatch.setenv("PORTFOLIO_ENCRYPTION_KEY", "test-key")
        secrets = inject_secrets()
        assert secrets["IBKR_FLEX_TOKEN"] == "test-token"
        assert secrets["PORTFOLIO_ENCRYPTION_KEY"] == "test-key"

    def test_inject_warns_on_missing_required(self, monkeypatch, caplog):
        """Missing required secrets are logged as warnings."""
        for name in REQUIRED_SECRET_NAMES:
            monkeypatch.delenv(name, raising=False)
        secrets = inject_secrets()
        for name in REQUIRED_SECRET_NAMES:
            assert name not in secrets
        assert any(name in msg for msg in caplog.messages for name in REQUIRED_SECRET_NAMES)

    def test_inject_loads_dotenv(self, tmp_path, monkeypatch):
        """inject_secrets loads variables from .env file."""
        env_file = tmp_path / ".env"
        env_file.write_text("IBKR_FLEX_TOKEN=from-dotenv\n")
        # Patch PROJECT_ROOT to point at tmp_path so .env is found
        monkeypatch.setattr("pipeline.secrets.PROJECT_ROOT", tmp_path)
        # Make sure env var is NOT already set
        monkeypatch.delenv("IBKR_FLEX_TOKEN", raising=False)
        secrets = inject_secrets()
        assert secrets.get("IBKR_FLEX_TOKEN") == "from-dotenv"

    def test_env_overrides_dotenv(self, tmp_path, monkeypatch):
        """Environment variables take priority over .env file."""
        env_file = tmp_path / ".env"
        env_file.write_text("IBKR_FLEX_TOKEN=from-dotenv\n")
        monkeypatch.setattr("pipeline.secrets.PROJECT_ROOT", tmp_path)
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "from-env")
        secrets = inject_secrets()
        assert secrets["IBKR_FLEX_TOKEN"] == "from-env"

    def test_optional_secrets_not_required(self, monkeypatch):
        """Optional secrets are not required to be present."""
        for name in OPTIONAL_SECRET_NAMES:
            monkeypatch.delenv(name, raising=False)
        for name in REQUIRED_SECRET_NAMES:
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "x")
        monkeypatch.setenv("T212_API_KEY", "x")
        monkeypatch.setenv("T212_API_SECRET", "x")
        monkeypatch.setenv("PORTFOLIO_ENCRYPTION_KEY", "x")
        secrets = inject_secrets()
        # No S3 or AWS vars — should not error
        assert "S3_BUCKET" not in secrets


class TestGetSecret:
    """Test get_secret() reads from os.environ."""

    def test_get_existing_secret(self, monkeypatch):
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "my-token")
        assert get_secret("IBKR_FLEX_TOKEN") == "my-token"

    def test_get_missing_secret(self):
        assert get_secret("NONEXISTENT_SECRET_XYZ") is None