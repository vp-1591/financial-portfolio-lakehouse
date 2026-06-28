"""Tests for pipeline.secrets module."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from pipeline.secrets import _BW_SECRET_MAP, get_secret, inject_secrets, resolve_secrets


class TestResolveSecrets:
    """Test resolve_secrets() with env vars and mocked Bitwarden."""

    def test_env_vars_take_priority(self, monkeypatch):
        """Already-set env vars are used directly; bw is only called for missing ones."""
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "from-env")
        monkeypatch.setenv("BW_SESSION", "fake-session")

        mock_result = MagicMock()
        mock_result.stdout = "bw-value\n"
        with patch("pipeline.secrets.subprocess.run", return_value=mock_result) as mock_run:
            secrets = resolve_secrets()
            # IBKR_FLEX_TOKEN comes from env var, not bw
            assert secrets["IBKR_FLEX_TOKEN"] == "from-env"
            # bw IS called for other secrets that don't have env vars set
            assert mock_run.call_count > 0

    def test_bw_lookup_when_env_not_set(self, monkeypatch):
        """If env var is not set but BW_SESSION is, bw is called."""
        monkeypatch.delenv("IBKR_FLEX_TOKEN", raising=False)
        monkeypatch.setenv("BW_SESSION", "test-session")

        mock_result = MagicMock()
        mock_result.stdout = "token-from-bw\n"
        with patch("pipeline.secrets.subprocess.run", return_value=mock_result):
            secrets = resolve_secrets()
            assert secrets["IBKR_FLEX_TOKEN"] == "token-from-bw"

    def test_bw_failure_is_skipped(self, monkeypatch):
        """If bw fails for a secret, it's skipped gracefully."""
        monkeypatch.delenv("IBKR_FLEX_TOKEN", raising=False)
        monkeypatch.setenv("BW_SESSION", "test-session")

        import subprocess
        with patch("pipeline.secrets.subprocess.run", side_effect=subprocess.CalledProcessError(1, "bw")):
            secrets = resolve_secrets()
            assert "IBKR_FLEX_TOKEN" not in secrets

    def test_no_bw_session_no_lookup(self, monkeypatch):
        """If BW_SESSION is not set, no bw lookup is attempted."""
        monkeypatch.delenv("BW_SESSION", raising=False)
        monkeypatch.delenv("IBKR_FLEX_TOKEN", raising=False)

        with patch("pipeline.secrets.subprocess.run") as mock_run:
            secrets = resolve_secrets()
            mock_run.assert_not_called()
            assert "IBKR_FLEX_TOKEN" not in secrets

    def test_all_env_vars_skip_bw(self, monkeypatch):
        """When all secrets are set via env vars, bw is never called."""
        for env_name in _BW_SECRET_MAP:
            monkeypatch.setenv(env_name, f"{env_name}-value")
        monkeypatch.setenv("BW_SESSION", "test-session")

        with patch("pipeline.secrets.subprocess.run") as mock_run:
            secrets = resolve_secrets()
            mock_run.assert_not_called()
            for env_name in _BW_SECRET_MAP:
                assert secrets[env_name] == f"{env_name}-value"


class TestInjectSecrets:
    """Test inject_secrets() sets env vars."""

    def test_inject_sets_env_vars(self, monkeypatch):
        """Resolved secrets are injected into os.environ."""
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "injected-token")
        monkeypatch.delenv("BW_SESSION", raising=False)

        secrets = inject_secrets()
        assert os.environ.get("IBKR_FLEX_TOKEN") == "injected-token"
        assert secrets["IBKR_FLEX_TOKEN"] == "injected-token"

    def test_inject_from_bw(self, monkeypatch):
        """Secrets fetched from bw are injected into os.environ."""
        monkeypatch.delenv("T212_API_KEY", raising=False)
        monkeypatch.setenv("BW_SESSION", "test-session")

        mock_result = MagicMock()
        mock_result.stdout = "bw-api-key\n"
        with patch("pipeline.secrets.subprocess.run", return_value=mock_result):
            secrets = inject_secrets()

        assert os.environ.get("T212_API_KEY") == "bw-api-key"
        assert secrets["T212_API_KEY"] == "bw-api-key"

        # Clean up
        monkeypatch.delenv("T212_API_KEY", raising=False)


class TestGetSecret:
    """Test get_secret() reads from os.environ."""

    def test_get_existing_secret(self, monkeypatch):
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "my-token")
        assert get_secret("IBKR_FLEX_TOKEN") == "my-token"

    def test_get_missing_secret(self):
        assert get_secret("NONEXISTENT_SECRET_XYZ") is None