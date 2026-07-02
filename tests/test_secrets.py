"""Tests for pipeline.secrets module — env-var validation, .env loading, and config helpers."""

from __future__ import annotations

import pytest

from pipeline.secrets import (
    DEMO_SECRET_MAP,
    REQUIRED_SECRETS,
    REQUIRED_SECRETS_DEMO,
    get_config,
    get_secret,
    inject_secrets,
    is_demo,
    is_enabled,
    parse_bool,
    resolve_secret,
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
        monkeypatch.setenv("ENCRYPTION_KEY", "test-key")
        secrets = inject_secrets()
        assert secrets["IBKR_FLEX_TOKEN"] == "test-token"
        assert secrets["ENCRYPTION_KEY"] == "test-key"

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
        monkeypatch.setenv("ENCRYPTION_KEY", "x")
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


class TestIsDemo:
    """Test is_demo() reads the DEMO env var."""

    def test_demo_false_by_default(self, monkeypatch):
        monkeypatch.delenv("DEMO", raising=False)
        assert is_demo() is False

    def test_demo_true(self, monkeypatch):
        monkeypatch.setenv("DEMO", "true")
        assert is_demo() is True

    def test_demo_one(self, monkeypatch):
        monkeypatch.setenv("DEMO", "1")
        assert is_demo() is True

    def test_demo_yes(self, monkeypatch):
        monkeypatch.setenv("DEMO", "yes")
        assert is_demo() is True

    def test_demo_false_explicit(self, monkeypatch):
        monkeypatch.setenv("DEMO", "false")
        assert is_demo() is False

    def test_demo_zero(self, monkeypatch):
        monkeypatch.setenv("DEMO", "0")
        assert is_demo() is False


class TestResolveSecret:
    """Test resolve_secret() in demo and non-demo modes.

    Strict isolation: demo mode must NOT fall back to base secrets,
    and production mode must NOT read _DEMO secrets.
    """

    def test_returns_base_secret_in_non_demo(self, monkeypatch):
        monkeypatch.delenv("DEMO", raising=False)
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "live-token")
        monkeypatch.delenv("IBKR_FLEX_TOKEN_DEMO", raising=False)
        assert resolve_secret("IBKR_FLEX_TOKEN") == "live-token"

    def test_returns_demo_secret_in_demo_mode(self, monkeypatch):
        monkeypatch.setenv("DEMO", "true")
        monkeypatch.setenv("IBKR_FLEX_TOKEN_DEMO", "demo-token")
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "live-token")
        assert resolve_secret("IBKR_FLEX_TOKEN") == "demo-token"

    def test_raises_error_when_demo_secret_missing(self, monkeypatch):
        """In demo mode, missing _DEMO variant is a hard error."""
        monkeypatch.setenv("DEMO", "true")
        monkeypatch.delenv("IBKR_FLEX_TOKEN_DEMO", raising=False)
        # Even if base secret is set, it must NOT be used in demo mode
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "live-token")
        with pytest.raises(EnvironmentError, match="IBKR_FLEX_TOKEN_DEMO"):
            resolve_secret("IBKR_FLEX_TOKEN")

    def test_returns_none_when_base_secret_missing_in_non_demo(self, monkeypatch):
        monkeypatch.delenv("DEMO", raising=False)
        monkeypatch.delenv("IBKR_FLEX_TOKEN", raising=False)
        assert resolve_secret("IBKR_FLEX_TOKEN") is None

    def test_pass_through_for_unknown_names(self, monkeypatch):
        """Names not in DEMO_SECRET_MAP are returned from env directly."""
        monkeypatch.setenv("DEMO", "true")
        monkeypatch.setenv("SOME_OTHER_VAR", "value")
        # Not in DEMO_SECRET_MAP, so demo mode has no effect
        assert resolve_secret("SOME_OTHER_VAR") == "value"

    def test_non_demo_never_reads_demo_secrets(self, monkeypatch):
        """In production mode, _DEMO secrets must NOT be used."""
        monkeypatch.delenv("DEMO", raising=False)
        monkeypatch.delenv("IBKR_FLEX_TOKEN", raising=False)
        monkeypatch.setenv("IBKR_FLEX_TOKEN_DEMO", "demo-token")
        assert resolve_secret("IBKR_FLEX_TOKEN") is None

    def test_all_secrets_in_demo_secret_map(self):
        """Every REQUIRED_SECRETS entry must have a _DEMO mapping."""
        for name in REQUIRED_SECRETS:
            assert name in DEMO_SECRET_MAP, f"{name} missing from DEMO_SECRET_MAP"

    def test_all_demo_secrets_in_required_demo(self):
        """Every _DEMO variant must be in REQUIRED_SECRETS_DEMO."""
        for base, demo in DEMO_SECRET_MAP.items():
            assert demo in REQUIRED_SECRETS_DEMO, (
                f"{demo} missing from REQUIRED_SECRETS_DEMO"
            )


class TestInjectSecretsDemoMode:
    """Test inject_secrets() validation in demo mode."""

    def setup_method(self):
        import pipeline.storage

        pipeline.storage._config = None

    def teardown_method(self):
        import pipeline.storage

        pipeline.storage._config = None

    def test_demo_mode_validates_demo_secrets(self, monkeypatch):
        """In demo mode, _DEMO variants are validated, not base secrets."""
        monkeypatch.setenv("DEMO", "true")
        # Set all _DEMO variants
        for demo_name in REQUIRED_SECRETS_DEMO:
            monkeypatch.setenv(demo_name, "demo-value")
        # Do NOT set base secrets — they should not be required
        for name in REQUIRED_SECRETS:
            monkeypatch.delenv(name, raising=False)

        secrets = inject_secrets()
        for demo_name in REQUIRED_SECRETS_DEMO:
            assert demo_name in secrets
            assert secrets[demo_name] == "demo-value"

    def test_demo_mode_warns_on_missing_demo_secrets(self, monkeypatch, caplog):
        """Missing _DEMO secrets generate warnings in demo mode."""
        monkeypatch.setenv("DEMO", "true")
        for name in REQUIRED_SECRETS_DEMO:
            monkeypatch.delenv(name, raising=False)
        for name in REQUIRED_SECRETS:
            monkeypatch.delenv(name, raising=False)

        secrets = inject_secrets()
        # No secrets should be found
        assert not secrets
        # Warnings should mention demo secrets
        for demo_name in REQUIRED_SECRETS_DEMO:
            assert any(demo_name in msg for msg in caplog.messages), (
                f"Expected warning for {demo_name}"
            )

    def test_non_demo_mode_does_not_warn_about_demo_secrets(self, monkeypatch, caplog):
        """In production mode, missing _DEMO secrets are not warned about."""
        monkeypatch.delenv("DEMO", raising=False)
        # Set all base secrets
        for name in REQUIRED_SECRETS:
            monkeypatch.setenv(name, "value")
        # _DEMO variants are not set
        for demo_name in REQUIRED_SECRETS_DEMO:
            monkeypatch.delenv(demo_name, raising=False)

        inject_secrets()
        # Warnings should NOT mention _DEMO variants
        for demo_name in REQUIRED_SECRETS_DEMO:
            assert not any(demo_name in msg for msg in caplog.messages), (
                f"Unexpected warning for {demo_name} in non-demo mode"
            )
