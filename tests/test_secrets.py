"""Tests for pipeline.secrets module -- env-var validation, .env loading, and config helpers."""

from __future__ import annotations

import pytest

from pipeline.secrets import (
    DEMO_SECRET_MAP,
    REQUIRED_SECRETS,
    REQUIRED_SECRETS_DEMO,
    REQUIRED_SECRETS_DEMO_NON_AWS,
    REQUIRED_SECRETS_S3_DEMO,
    AwsCredentials,
    get_env,
    get_mode,
    get_secret,
    inject_secrets,
    is_demo,
    is_enabled,
    load_env,
    parse_bool,
    reset_mode,
    resolve_aws_credentials,
    resolve_secret,
    set_mode,
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
        set_mode("docker")
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "test-token")
        monkeypatch.setenv("ENCRYPTION_KEY", "test-key")
        secrets = inject_secrets()
        assert secrets["IBKR_FLEX_TOKEN"] == "test-token"
        assert secrets["ENCRYPTION_KEY"] == "test-key"

    def test_inject_warns_on_missing_required(self, monkeypatch, caplog):
        """Missing required secrets are logged as warnings."""
        set_mode("docker")
        for name in REQUIRED_SECRETS:
            monkeypatch.delenv(name, raising=False)
        secrets = inject_secrets()
        for name in REQUIRED_SECRETS:
            assert name not in secrets
        assert any(name in msg for msg in caplog.messages for name in REQUIRED_SECRETS)

    def test_inject_loads_dotenv(self, tmp_path, monkeypatch):
        """inject_secrets loads variables from .env file."""
        set_mode("docker")
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
        set_mode("docker")
        env_file = tmp_path / ".env"
        env_file.write_text("IBKR_FLEX_TOKEN=from-dotenv\n")
        monkeypatch.setattr("pipeline.secrets.PROJECT_ROOT", tmp_path)
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "from-env")
        secrets = inject_secrets()
        assert secrets["IBKR_FLEX_TOKEN"] == "from-env"

    def test_optional_secrets_not_required(self, monkeypatch):
        """Optional secrets like S3_BUCKET are not required to be present."""
        set_mode("docker")
        monkeypatch.delenv("S3_BUCKET", raising=False)
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        for name in REQUIRED_SECRETS:
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "x")
        monkeypatch.setenv("T212_API_KEY", "x")
        monkeypatch.setenv("T212_API_SECRET", "x")
        monkeypatch.setenv("ENCRYPTION_KEY", "x")
        secrets = inject_secrets()
        # No S3 vars -- should not error
        assert "S3_BUCKET" not in secrets


class TestLoadEnv:
    """Test load_env() silently loads .env without warnings."""

    def setup_method(self):
        """Reset module-level state before each test."""
        import pipeline.storage

        pipeline.storage._config = None

    def teardown_method(self):
        """Clean up env vars after each test."""
        import pipeline.storage

        pipeline.storage._config = None

    def test_load_env_loads_dotenv(self, tmp_path, monkeypatch):
        """load_env loads variables from .env file without warnings."""
        env_file = tmp_path / ".env"
        env_file.write_text("IBKR_FLEX_TOKEN=from-dotenv\n")
        monkeypatch.setattr("pipeline.secrets.PROJECT_ROOT", tmp_path)
        monkeypatch.delenv("IBKR_FLEX_TOKEN", raising=False)
        load_env()
        assert get_secret("IBKR_FLEX_TOKEN") == "from-dotenv"

    def test_load_env_no_warnings(self, tmp_path, monkeypatch, caplog):
        """load_env does not log warnings about missing secrets."""
        env_file = tmp_path / ".env"
        env_file.write_text("SOME_OTHER_VAR=hello\n")
        monkeypatch.setattr("pipeline.secrets.PROJECT_ROOT", tmp_path)
        for name in REQUIRED_SECRETS:
            monkeypatch.delenv(name, raising=False)
        load_env()
        warnings = [m for m in caplog.messages if "secret" in m.lower()]
        assert not warnings, f"Expected no warnings from load_env, got: {warnings}"

    def test_load_env_idempotent(self, tmp_path, monkeypatch):
        """Calling load_env twice is safe and produces the same result."""
        env_file = tmp_path / ".env"
        env_file.write_text("T212_API_KEY=from-dotenv\n")
        monkeypatch.setattr("pipeline.secrets.PROJECT_ROOT", tmp_path)
        monkeypatch.delenv("T212_API_KEY", raising=False)
        load_env()
        load_env()
        assert get_secret("T212_API_KEY") == "from-dotenv"

    def test_load_env_no_dotenv_file(self, tmp_path, monkeypatch, caplog):
        """load_env does nothing when no .env file exists."""
        monkeypatch.setattr("pipeline.secrets.PROJECT_ROOT", tmp_path)
        load_env()
        # No "Loaded environment" message -- silently skipped
        assert not any("Loaded environment" in m for m in caplog.messages)


class TestGetSecret:
    """Test get_secret() reads from os.environ."""

    def test_get_existing_secret(self, monkeypatch):
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "my-token")
        assert get_secret("IBKR_FLEX_TOKEN") == "my-token"

    def test_get_missing_secret(self):
        assert get_secret("NONEXISTENT_SECRET_XYZ") is None


class TestGetEnv:
    """Test get_env() reads env vars, treating empty strings as unset."""

    def test_get_env_with_value(self, monkeypatch):
        monkeypatch.setenv("T212_BASE_URL", "https://custom.api")
        assert get_env("T212_BASE_URL") == "https://custom.api"

    def test_get_env_with_default(self, monkeypatch):
        monkeypatch.delenv("T212_BASE_URL", raising=False)
        assert (
            get_env("T212_BASE_URL", "https://live.trading212.com/api/v0")
            == "https://live.trading212.com/api/v0"
        )

    def test_get_env_no_default(self, monkeypatch):
        monkeypatch.delenv("NONEXISTENT_CONFIG_XYZ", raising=False)
        assert get_env("NONEXISTENT_CONFIG_XYZ") is None

    def test_get_env_empty_string_falls_back_to_default(self, monkeypatch):
        """Empty string env var falls back to default, unlike os.environ.get."""
        monkeypatch.setenv("S3_PREFIX", "")
        assert get_env("S3_PREFIX", "pipeline") == "pipeline"

    def test_get_env_empty_string_no_default_returns_none(self, monkeypatch):
        """Empty string env var with no default returns None."""
        monkeypatch.setenv("NONEXISTENT_CONFIG_XYZ", "")
        assert get_env("NONEXISTENT_CONFIG_XYZ") is None


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


class TestSetMode:
    """Test set_mode() and get_mode() for execution mode control."""

    def test_set_mode_docker(self):
        set_mode("docker")
        assert get_mode() == "docker"

    def test_set_mode_staging(self):
        set_mode("staging")
        assert get_mode() == "staging"

    def test_set_mode_prod(self):
        set_mode("prod")
        assert get_mode() == "prod"

    def test_set_mode_invalid_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid mode"):
            set_mode("invalid")

    def test_get_mode_raises_when_not_set(self):
        """get_mode() raises RuntimeError if set_mode() was never called."""
        with pytest.raises(RuntimeError, match="Mode not set"):
            get_mode()

    def test_reset_mode_clears_mode(self):
        set_mode("docker")
        assert get_mode() == "docker"
        reset_mode()
        with pytest.raises(RuntimeError, match="Mode not set"):
            get_mode()


class TestIsDemo:
    """Test is_demo() returns True only for staging mode."""

    def test_is_demo_true_for_staging(self):
        set_mode("staging")
        assert is_demo() is True

    def test_is_demo_false_for_docker(self):
        set_mode("docker")
        assert is_demo() is False

    def test_is_demo_false_for_prod(self):
        set_mode("prod")
        assert is_demo() is False

    def test_is_demo_raises_when_mode_not_set(self):
        """is_demo() raises RuntimeError if mode has not been set."""
        with pytest.raises(RuntimeError, match="Mode not set"):
            is_demo()


class TestResolveSecret:
    """Test resolve_secret() in staging (demo) and docker (non-demo) modes.

    Strict isolation: demo mode must NOT fall back to base secrets,
    and docker/prod mode must NOT read _DEMO secrets.
    """

    def test_returns_base_secret_in_docker_mode(self, monkeypatch):
        set_mode("docker")
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "live-token")
        monkeypatch.delenv("IBKR_FLEX_TOKEN_DEMO", raising=False)
        assert resolve_secret("IBKR_FLEX_TOKEN") == "live-token"

    def test_returns_demo_secret_in_staging_mode(self, monkeypatch):
        set_mode("staging")
        monkeypatch.setenv("IBKR_FLEX_TOKEN_DEMO", "demo-token")
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "live-token")
        assert resolve_secret("IBKR_FLEX_TOKEN") == "demo-token"

    def test_returns_none_when_demo_secret_missing(self, monkeypatch, caplog):
        """In staging mode, missing _DEMO variant returns None with a warning."""
        set_mode("staging")
        monkeypatch.delenv("IBKR_FLEX_TOKEN_DEMO", raising=False)
        # Even if base secret is set, it must NOT be used in staging mode
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "live-token")
        result = resolve_secret("IBKR_FLEX_TOKEN")
        assert result is None
        # Warning should mention the missing demo secret
        assert any("IBKR_FLEX_TOKEN_DEMO" in msg for msg in caplog.messages), (
            "Expected warning about missing IBKR_FLEX_TOKEN_DEMO"
        )

    def test_returns_none_when_base_secret_missing_in_docker_mode(self, monkeypatch):
        set_mode("docker")
        monkeypatch.delenv("IBKR_FLEX_TOKEN", raising=False)
        assert resolve_secret("IBKR_FLEX_TOKEN") is None

    def test_pass_through_for_unknown_names(self, monkeypatch):
        """Names not in DEMO_SECRET_MAP are returned from env directly."""
        set_mode("staging")
        monkeypatch.setenv("SOME_OTHER_VAR", "value")
        # Not in DEMO_SECRET_MAP, so demo mode has no effect
        assert resolve_secret("SOME_OTHER_VAR") == "value"

    def test_docker_mode_never_reads_demo_secrets(self, monkeypatch):
        """In docker mode, _DEMO secrets must NOT be used."""
        set_mode("docker")
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

    def test_required_secrets_demo_non_aws_excludes_aws(self):
        """REQUIRED_SECRETS_DEMO_NON_AWS must not contain AWS credential names."""
        for name in REQUIRED_SECRETS_S3_DEMO:
            assert name not in REQUIRED_SECRETS_DEMO_NON_AWS, (
                f"{name} should not be in REQUIRED_SECRETS_DEMO_NON_AWS"
            )

    def test_required_secrets_demo_non_aws_union_equals_demo(self):
        """Non-AWS + S3 demo lists must equal the full REQUIRED_SECRETS_DEMO."""
        combined = sorted(REQUIRED_SECRETS_DEMO_NON_AWS + REQUIRED_SECRETS_S3_DEMO)
        expected = sorted(REQUIRED_SECRETS_DEMO)
        assert combined == expected


class TestInjectSecretsDemoMode:
    """Test inject_secrets() validation in staging (demo) mode."""

    def setup_method(self):
        import pipeline.storage

        pipeline.storage._config = None

    def teardown_method(self):
        import pipeline.storage

        pipeline.storage._config = None

    def test_staging_mode_validates_demo_secrets(self, monkeypatch):
        """In staging mode, _DEMO variants are validated, not base secrets.

        Staging mode validates all demo secrets (non-AWS in the general loop
        and AWS in the S3-specific section).
        """
        set_mode("staging")
        # Set all _DEMO variants (non-AWS and AWS S3)
        for demo_name in REQUIRED_SECRETS_DEMO:
            monkeypatch.setenv(demo_name, "demo-value")
        # Do NOT set base secrets -- they should not be required in staging mode
        for name in REQUIRED_SECRETS:
            monkeypatch.delenv(name, raising=False)

        secrets = inject_secrets()
        for demo_name in REQUIRED_SECRETS_DEMO:
            assert demo_name in secrets
            assert secrets[demo_name] == "demo-value"

    def test_staging_mode_warns_on_missing_demo_secrets(self, monkeypatch, caplog):
        """Missing _DEMO secrets generate warnings in staging mode."""
        set_mode("staging")
        for name in REQUIRED_SECRETS_DEMO:
            monkeypatch.delenv(name, raising=False)
        for name in REQUIRED_SECRETS:
            monkeypatch.delenv(name, raising=False)

        secrets = inject_secrets()
        # No secrets should be found
        assert not secrets
        # Warnings should mention all demo secrets (non-AWS and S3)
        for demo_name in REQUIRED_SECRETS_DEMO:
            assert any(demo_name in msg for msg in caplog.messages), (
                f"Expected warning for {demo_name}"
            )

    def test_docker_mode_does_not_warn_about_demo_secrets(self, monkeypatch, caplog):
        """In docker mode, missing _DEMO secrets are not warned about."""
        set_mode("docker")
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
                f"Unexpected warning for {demo_name} in docker mode"
            )


class TestResolveSecretLogging:
    """Test that resolve_secret logs which variant is used."""

    def test_logs_demo_variant_when_resolved(self, monkeypatch, caplog):
        """In staging mode, resolve_secret logs that it used the _DEMO variant."""
        caplog.set_level("INFO")
        set_mode("staging")
        monkeypatch.setenv("IBKR_FLEX_TOKEN_DEMO", "demo-token")
        result = resolve_secret("IBKR_FLEX_TOKEN")
        assert result == "demo-token"
        assert any(
            "Resolved IBKR_FLEX_TOKEN from IBKR_FLEX_TOKEN_DEMO" in msg
            for msg in caplog.messages
        ), f"Expected info log about demo variant, got: {caplog.messages}"

    def test_logs_base_variant_when_resolved(self, monkeypatch, caplog):
        """In docker mode, resolve_secret logs that it used the base variant."""
        caplog.set_level("INFO")
        set_mode("docker")
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "live-token")
        result = resolve_secret("IBKR_FLEX_TOKEN")
        assert result == "live-token"
        assert any(
            "Resolved IBKR_FLEX_TOKEN from IBKR_FLEX_TOKEN" in msg
            for msg in caplog.messages
        ), f"Expected info log about base variant, got: {caplog.messages}"

    def test_logs_debug_when_secret_missing(self, monkeypatch, caplog):
        """In docker mode, missing secrets are logged at debug level."""
        caplog.set_level("DEBUG")
        set_mode("docker")
        monkeypatch.delenv("IBKR_FLEX_TOKEN", raising=False)
        result = resolve_secret("IBKR_FLEX_TOKEN")
        assert result is None
        assert any("IBKR_FLEX_TOKEN is not set" in msg for msg in caplog.messages), (
            f"Expected debug log about missing secret, got: {caplog.messages}"
        )


class TestAWSSecretsInDemoMap:
    """Test that AWS credentials are in DEMO_SECRET_MAP."""

    def test_aws_access_key_in_demo_map(self):
        assert "AWS_ACCESS_KEY_ID" in DEMO_SECRET_MAP
        assert DEMO_SECRET_MAP["AWS_ACCESS_KEY_ID"] == "AWS_ACCESS_KEY_ID_DEMO"

    def test_aws_secret_key_in_demo_map(self):
        assert "AWS_SECRET_ACCESS_KEY" in DEMO_SECRET_MAP
        assert DEMO_SECRET_MAP["AWS_SECRET_ACCESS_KEY"] == "AWS_SECRET_ACCESS_KEY_DEMO"

    def test_aws_creds_resolve_demo_variant(self, monkeypatch):
        """In staging mode, AWS creds use _DEMO variants."""
        set_mode("staging")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID_DEMO", "demo-key")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "prod-key")
        assert resolve_secret("AWS_ACCESS_KEY_ID") == "demo-key"

    def test_aws_creds_no_fallback_in_demo(self, monkeypatch):
        """In staging mode, missing _DEMO AWS creds return None, not base creds."""
        set_mode("staging")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID_DEMO", raising=False)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "prod-key")
        assert resolve_secret("AWS_ACCESS_KEY_ID") is None


class TestInjectSecretsS3Validation:
    """Test that inject_secrets validates S3 secrets only for staging/prod modes."""

    def setup_method(self):
        import pipeline.storage

        pipeline.storage._config = None

    def teardown_method(self):
        import pipeline.storage

        pipeline.storage._config = None

    def test_s3_secrets_validated_for_prod(self, monkeypatch, caplog):
        """In prod mode, missing AWS creds generate a warning."""
        set_mode("prod")
        for name in REQUIRED_SECRETS:
            monkeypatch.setenv(name, "value")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

        inject_secrets()
        assert any(
            "AWS_ACCESS_KEY_ID" in msg and "cloud storage" in msg
            for msg in caplog.messages
        ), f"Expected S3 warning, got: {caplog.messages}"

    def test_s3_secrets_not_required_for_docker(self, monkeypatch, caplog):
        """In docker mode, missing AWS creds do NOT generate S3 warnings."""
        set_mode("docker")
        for name in REQUIRED_SECRETS:
            monkeypatch.setenv(name, "value")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

        inject_secrets()
        assert not any("cloud storage" in msg for msg in caplog.messages), (
            f"Unexpected S3 warning in docker mode: {caplog.messages}"
        )

    def test_s3_secrets_not_required_for_docker_with_minio_config(
        self, monkeypatch, caplog
    ):
        """In docker mode with MinIO config, missing AWS creds do NOT generate S3 warnings."""
        set_mode("docker")
        for name in REQUIRED_SECRETS:
            monkeypatch.setenv(name, "value")
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

        inject_secrets()
        assert not any("cloud storage" in msg for msg in caplog.messages), (
            f"Unexpected S3 warning in docker mode: {caplog.messages}"
        )

    def test_staging_s3_secrets_validated(self, monkeypatch, caplog):
        """In staging mode, missing _DEMO AWS creds generate a warning."""
        set_mode("staging")
        for name in REQUIRED_SECRETS_DEMO:
            monkeypatch.setenv(name, "demo-value")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID_DEMO", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY_DEMO", raising=False)

        inject_secrets()
        assert any(
            "AWS_ACCESS_KEY_ID_DEMO" in msg and "cloud storage" in msg
            for msg in caplog.messages
        ), f"Expected demo S3 warning, got: {caplog.messages}"

    def test_docker_mode_no_aws_demo_warnings(self, monkeypatch, caplog):
        """In docker mode, missing AWS _DEMO creds do NOT generate warnings.

        Docker mode validates base secrets only and skips S3 validation.
        No _DEMO variants are checked, so missing AWS _DEMO credentials
        should not produce any warnings.
        """
        set_mode("docker")
        # Set base secrets
        for name in REQUIRED_SECRETS:
            monkeypatch.setenv(name, "value")
        # Explicitly do NOT set AWS credentials (base or demo)
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        monkeypatch.delenv("AWS_ACCESS_KEY_ID_DEMO", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY_DEMO", raising=False)

        inject_secrets()
        # No warning should mention AWS credentials at all
        aws_warnings = [
            msg
            for msg in caplog.messages
            if "AWS_ACCESS_KEY_ID" in msg or "AWS_SECRET_ACCESS_KEY" in msg
        ]
        assert not aws_warnings, (
            f"Unexpected AWS warnings in docker mode: {aws_warnings}"
        )

    def test_staging_single_aws_demo_warning(self, monkeypatch, caplog):
        """In staging mode, each missing AWS _DEMO cred is warned about once.

        Previously, AWS demo credentials were warned about twice -- once
        in the general demo loop and once in the S3-specific section.
        Now they should only appear in the S3-specific section.
        """
        set_mode("staging")
        # Set non-AWS demo secrets
        for name in REQUIRED_SECRETS_DEMO_NON_AWS:
            monkeypatch.setenv(name, "demo-value")
        # Do NOT set AWS _DEMO creds
        monkeypatch.delenv("AWS_ACCESS_KEY_ID_DEMO", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY_DEMO", raising=False)

        inject_secrets()
        # Count how many times AWS_ACCESS_KEY_ID_DEMO appears in warnings
        key_warnings = [
            msg for msg in caplog.messages if "AWS_ACCESS_KEY_ID_DEMO" in msg
        ]
        assert len(key_warnings) == 1, (
            f"Expected exactly 1 warning for AWS_ACCESS_KEY_ID_DEMO, "
            f"got {len(key_warnings)}: {key_warnings}"
        )


class TestResolveAwsCredentials:
    """Test resolve_aws_credentials() and AwsCredentials dataclass."""

    def test_docker_mode_credentials(self, monkeypatch):
        """In docker mode, base AWS credentials are used."""
        set_mode("docker")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "prod-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "prod-secret")

        creds = resolve_aws_credentials()
        assert creds.key_id == "prod-key"
        assert creds.secret_key == "prod-secret"

    def test_staging_credentials(self, monkeypatch):
        """In staging mode, _DEMO AWS credentials are used."""
        set_mode("staging")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID_DEMO", "demo-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY_DEMO", "demo-secret")

        creds = resolve_aws_credentials()
        assert creds.key_id == "demo-key"
        assert creds.secret_key == "demo-secret"

    def test_staging_no_fallback_to_base(self, monkeypatch):
        """In staging mode, missing _DEMO AWS creds return None, not base creds."""
        set_mode("staging")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID_DEMO", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY_DEMO", raising=False)
        # Base creds are set -- must NOT be used
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "prod-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "prod-secret")

        creds = resolve_aws_credentials()
        assert creds.key_id is None
        assert creds.secret_key is None

    def test_docker_mode_missing_creds(self, monkeypatch):
        """In docker mode, missing AWS creds return None."""
        set_mode("docker")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

        creds = resolve_aws_credentials()
        assert creds.key_id is None
        assert creds.secret_key is None

    def test_defaults(self, monkeypatch):
        """Region defaults to eu-west-1, endpoint_url and allow_http default."""
        set_mode("docker")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
        monkeypatch.delenv("S3_ALLOW_HTTP", raising=False)

        creds = resolve_aws_credentials()
        assert creds.region == "eu-west-1"
        assert creds.endpoint_url is None
        assert creds.allow_http is False

    def test_region_override(self, monkeypatch):
        """AWS_REGION can be overridden."""
        set_mode("docker")
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

        creds = resolve_aws_credentials()
        assert creds.region == "us-east-1"

    def test_endpoint_url(self, monkeypatch):
        """S3_ENDPOINT_URL is read from env."""
        set_mode("docker")
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

        creds = resolve_aws_credentials()
        assert creds.endpoint_url == "http://minio:9000"

    def test_allow_http(self, monkeypatch):
        """S3_ALLOW_HTTP is parsed as boolean."""
        set_mode("docker")
        monkeypatch.setenv("S3_ALLOW_HTTP", "true")
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

        creds = resolve_aws_credentials()
        assert creds.allow_http is True


class TestAwsCredentialsDataclass:
    """Test AwsCredentials helper methods."""

    def test_to_storage_options_with_credentials(self):
        creds = AwsCredentials(
            key_id="key",
            secret_key="secret",
            region="eu-west-1",
            endpoint_url=None,
            allow_http=False,
        )
        opts = creds.to_storage_options()
        assert opts == {
            "aws_access_key_id": "key",
            "aws_secret_access_key": "secret",
            "aws_region": "eu-west-1",
        }

    def test_to_storage_options_none_credentials_omitted(self):
        """When both credentials are None, keys are omitted to allow IAM role fallback.

        Omitting credential keys allows object_store to fall through its default
        credential chain (including ECS IAM task roles). When either credential
        is set, both keys are included (with the missing one as empty string) to
        block fallback to environment variables.
        """
        creds = AwsCredentials(
            key_id=None,
            secret_key=None,
            region="eu-west-1",
            endpoint_url=None,
            allow_http=False,
        )
        opts = creds.to_storage_options()
        assert "aws_access_key_id" not in opts
        assert "aws_secret_access_key" not in opts
        assert opts["aws_region"] == "eu-west-1"

    def test_to_storage_options_partial_credentials_includes_empty(self):
        """When only one credential is set, both keys are included with empty string
        for the missing one. This prevents SDK fallback to environment variables.
        """
        creds = AwsCredentials(
            key_id="AKID",
            secret_key=None,
            region="eu-west-1",
            endpoint_url=None,
            allow_http=False,
        )
        opts = creds.to_storage_options()
        assert opts["aws_access_key_id"] == "AKID"
        assert opts["aws_secret_access_key"] == ""

    def test_to_storage_options_with_endpoint(self):
        creds = AwsCredentials(
            key_id="key",
            secret_key="secret",
            region="eu-west-1",
            endpoint_url="http://minio:9000",
            allow_http=True,
        )
        opts = creds.to_storage_options()
        assert opts["aws_endpoint_url"] == "http://minio:9000"
        assert opts["aws_allow_http"] == "true"

    def test_to_pyarrow_kwargs_with_credentials(self):
        creds = AwsCredentials(
            key_id="key",
            secret_key="secret",
            region="eu-west-1",
            endpoint_url=None,
            allow_http=False,
        )
        kwargs = creds.to_pyarrow_kwargs()
        assert kwargs["access_key"] == "key"
        assert kwargs["secret_key"] == "secret"
        assert kwargs["region"] == "eu-west-1"

    def test_to_pyarrow_kwargs_none_credentials_omitted(self):
        """When both credentials are None, keys are omitted to allow IAM role fallback.

        Omitting credential keys allows PyArrow to fall through its default
        credential chain (including ECS IAM task roles). When either credential
        is set, both keys are included (with the missing one as empty string) to
        block fallback to environment variables.
        """
        creds = AwsCredentials(
            key_id=None,
            secret_key=None,
            region="eu-west-1",
            endpoint_url=None,
            allow_http=False,
        )
        kwargs = creds.to_pyarrow_kwargs()
        assert "access_key" not in kwargs
        assert "secret_key" not in kwargs
        assert kwargs["region"] == "eu-west-1"

    def test_to_pyarrow_kwargs_partial_credentials_includes_empty(self):
        """When only one credential is set, both keys are included with empty string
        for the missing one. This prevents SDK fallback to environment variables.
        """
        creds = AwsCredentials(
            key_id="AKID",
            secret_key=None,
            region="eu-west-1",
            endpoint_url=None,
            allow_http=False,
        )
        kwargs = creds.to_pyarrow_kwargs()
        assert kwargs["access_key"] == "AKID"
        assert kwargs["secret_key"] == ""

    def test_to_pyarrow_kwargs_endpoint_url(self):
        creds = AwsCredentials(
            key_id="key",
            secret_key="secret",
            region="eu-west-1",
            endpoint_url="http://minio:9000",
            allow_http=False,
        )
        kwargs = creds.to_pyarrow_kwargs()
        assert kwargs["endpoint_override"] == "minio:9000"
        assert kwargs["scheme"] == "http"

    def test_to_pyarrow_kwargs_allow_http_no_endpoint(self):
        creds = AwsCredentials(
            key_id="key",
            secret_key="secret",
            region="eu-west-1",
            endpoint_url=None,
            allow_http=True,
        )
        kwargs = creds.to_pyarrow_kwargs()
        assert kwargs["scheme"] == "http"

    def test_to_duckdb_secret_parts_with_credentials(self):
        creds = AwsCredentials(
            key_id="key",
            secret_key="secret",
            region="eu-west-1",
            endpoint_url=None,
            allow_http=False,
        )
        parts = creds.to_duckdb_secret_parts()
        assert "KEY_ID 'key'" in parts
        assert "SECRET 'secret'" in parts
        assert "REGION 'eu-west-1'" in parts

    def test_to_duckdb_secret_parts_none_credentials_empty_strings(self):
        """When credentials are None, they are included as empty strings.

        Empty KEY_ID/SECRET prevent DuckDB from falling back to environment
        variables that may contain production credentials.
        """
        creds = AwsCredentials(
            key_id=None,
            secret_key=None,
            region="eu-west-1",
            endpoint_url=None,
            allow_http=False,
        )
        parts = creds.to_duckdb_secret_parts()
        assert "KEY_ID ''" in parts
        assert "SECRET ''" in parts
        assert "REGION 'eu-west-1'" in parts

    def test_to_duckdb_secret_parts_only_key_id(self):
        """Only key_id set (no secret_key) -- secret_key is empty string."""
        creds = AwsCredentials(
            key_id="key",
            secret_key=None,
            region="eu-west-1",
            endpoint_url=None,
            allow_http=False,
        )
        parts = creds.to_duckdb_secret_parts()
        assert "KEY_ID 'key'" in parts
        assert "SECRET ''" in parts

    def test_to_duckdb_secret_parts_endpoint_url(self):
        creds = AwsCredentials(
            key_id="key",
            secret_key="secret",
            region="eu-west-1",
            endpoint_url="http://minio:9000",
            allow_http=True,
        )
        parts = creds.to_duckdb_secret_parts()
        assert any("ENDPOINT" in p for p in parts)
        assert any("USE_SSL false" in p for p in parts)
        assert any("URL_STYLE path" in p for p in parts)

    def test_to_duckdb_secret_parts_escapes_quotes(self):
        """Single quotes in credentials are escaped to prevent SQL injection."""
        creds = AwsCredentials(
            key_id="key'with'quotes",
            secret_key="secret'with'quotes",
            region="eu-west-1",
            endpoint_url=None,
            allow_http=False,
        )
        parts = creds.to_duckdb_secret_parts()
        # Single quotes should be doubled
        assert "KEY_ID 'key''with''quotes'" in parts
        assert "SECRET 'secret''with''quotes'" in parts
