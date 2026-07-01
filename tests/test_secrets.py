"""Tests for pipeline.secrets module — env-var validation, .env loading, and config helpers."""

from __future__ import annotations


from pipeline.secrets import (
    REQUIRED_SECRETS,
    get_config,
    get_secret,
    inject_secrets,
    is_enabled,
    parse_bool,
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
        for name in REQUIRED_SECRETS:
            monkeypatch.delenv(name, raising=False)
        secrets = inject_secrets()
        for name in REQUIRED_SECRETS:
            assert name not in secrets
        assert any(name in msg for msg in caplog.messages for name in REQUIRED_SECRETS)

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
        """Optional secrets like S3_BUCKET are not required to be present."""
        monkeypatch.delenv("S3_BUCKET", raising=False)
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        for name in REQUIRED_SECRETS:
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "x")
        monkeypatch.setenv("T212_API_KEY", "x")
        monkeypatch.setenv("T212_API_SECRET", "x")
        monkeypatch.setenv("PORTFOLIO_ENCRYPTION_KEY", "x")
        secrets = inject_secrets()
        # No S3 vars — should not error
        assert "S3_BUCKET" not in secrets


class TestGetSecret:
    """Test get_secret() reads from os.environ."""

    def test_get_existing_secret(self, monkeypatch):
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "my-token")
        assert get_secret("IBKR_FLEX_TOKEN") == "my-token"

    def test_get_missing_secret(self):
        assert get_secret("NONEXISTENT_SECRET_XYZ") is None


class TestGetConfig:
    """Test get_config() reads env vars with defaults."""

    def test_get_config_with_value(self, monkeypatch):
        monkeypatch.setenv("T212_BASE_URL", "https://custom.api")
        assert get_config("T212_BASE_URL") == "https://custom.api"

    def test_get_config_with_default(self, monkeypatch):
        monkeypatch.delenv("T212_BASE_URL", raising=False)
        assert (
            get_config("T212_BASE_URL", "https://live.trading212.com/api/v0")
            == "https://live.trading212.com/api/v0"
        )

    def test_get_config_no_default(self, monkeypatch):
        monkeypatch.delenv("NONEXISTENT_CONFIG_XYZ", raising=False)
        assert get_config("NONEXISTENT_CONFIG_XYZ") is None


class TestIsEnabled:
    """Test is_enabled() reads boolean env vars."""

    def test_enabled_by_default(self, monkeypatch):
        """Connectors are enabled when the env var is not set."""
        monkeypatch.delenv("IBKR_ENABLED", raising=False)
        assert is_enabled("IBKR_ENABLED") is True

    def test_explicitly_enabled(self, monkeypatch):
        monkeypatch.setenv("IBKR_ENABLED", "true")
        assert is_enabled("IBKR_ENABLED") is True

    def test_explicitly_disabled_zero(self, monkeypatch):
        monkeypatch.setenv("IBKR_ENABLED", "0")
        assert is_enabled("IBKR_ENABLED") is False

    def test_explicitly_disabled_false(self, monkeypatch):
        monkeypatch.setenv("T212_ENABLED", "false")
        assert is_enabled("T212_ENABLED") is False

    def test_explicitly_disabled_no(self, monkeypatch):
        monkeypatch.setenv("XTB_ENABLED", "no")
        assert is_enabled("XTB_ENABLED") is False

    def test_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("IBKR_ENABLED", "False")
        assert is_enabled("IBKR_ENABLED") is False

    def test_empty_string_is_enabled(self, monkeypatch):
        """Empty string (var set but empty) means enabled."""
        monkeypatch.setenv("IBKR_ENABLED", "")
        assert is_enabled("IBKR_ENABLED") is True


class TestParseBool:
    """Test parse_bool() reads boolean env vars with explicit default."""

    def test_default_false_when_unset(self, monkeypatch):
        """Returns False when env var is not set and default is False."""
        monkeypatch.delenv("MY_FLAG", raising=False)
        assert parse_bool("MY_FLAG") is False

    def test_default_true_when_unset(self, monkeypatch):
        """Returns True when env var is not set and default is True."""
        monkeypatch.delenv("MY_FLAG", raising=False)
        assert parse_bool("MY_FLAG", default=True) is True

    def test_true_values(self, monkeypatch):
        for val in ("true", "True", "TRUE", "1", "yes", "Yes"):
            monkeypatch.setenv("MY_FLAG", val)
            assert parse_bool("MY_FLAG") is True, f"Failed for {val!r}"

    def test_false_values(self, monkeypatch):
        for val in ("false", "False", "0", "no", "random"):
            monkeypatch.setenv("MY_FLAG", val)
            assert parse_bool("MY_FLAG") is False, f"Failed for {val!r}"
