"""Secret and config resolution from environment variables and .env files.

Secrets and configuration are never stored in the repository.  They come from
one of two sources:

1. **Environment variables** — set by CI (GitHub Secrets / workflow inputs)
   or manual exports.  Highest priority; always checked first.
2. **``.env`` file** — loaded by ``python-dotenv`` for local development.
   The file lives at the project root and is gitignored.

If a secret is missing from both sources, the pipeline will error when
the secret is actually needed — not at startup.  This allows commands
like ``run-connector`` and ``run-consolidate-analytics`` to run without
any broker API keys.

**Execution modes.**  The ``--mode docker|staging|prod`` CLI flag (see
:func:`set_mode`) determines the execution context:

- **docker** — local development against MinIO.  Broker credentials come
  from ``.env`` or the environment under their base names.
- **staging** — staging (demo) environment.  ECS tasks inject secrets
  under base names from ``/portfolio/demo/`` SSM parameters; local runs
  read them from ``.env`` or the environment.  Storage is the demo S3 bucket.
- **prod** — production environment.  Same base-name resolution; ECS tasks
  use ``/portfolio/prod/`` SSM parameters.

:func:`is_demo` returns ``True`` in staging mode, which drives the
demo S3 bucket selection and the encryption-key file fallback guard.
There is **no cross-mode fallback** — missing credentials are logged
as warnings and :func:`resolve_secret` returns ``None``, allowing callers
to gracefully skip connectors or operations that require the missing secret.

Usage::

    from pipeline.secrets import (
        inject_secrets, get_secret, get_env,
        load_env, parse_bool, is_demo, resolve_secret,
        set_mode, get_mode,
    )

    set_mode("docker")         # called by CLI after parsing --mode
    load_env()                 # silent .env loading at startup (no warnings)
    # ... or ...
    inject_secrets()           # load .env AND validate (logs warnings for missing secrets)
    token = resolve_secret("IBKR_FLEX_TOKEN")  # secret lookup (env var)
    if is_demo():                                # True when --mode staging
        ...
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import overload

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Secrets the pipeline usually needs.  Listed here for startup validation.
# These are set via GitHub Secrets (CI) or .env / manual exports (local dev).
REQUIRED_SECRETS: list[str] = [
    "IBKR_FLEX_TOKEN",
    "IBKR_FLEX_QUERY_ID",
    "T212_API_KEY",
    "T212_API_SECRET",
    "ENCRYPTION_KEY",
]

# S3-specific secrets — only required for staging/prod modes (cloud storage).
REQUIRED_SECRETS_S3: list[str] = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
]

# ---------------------------------------------------------------------------
# Execution mode — single source of truth for --mode flag
# ---------------------------------------------------------------------------

_VALID_MODES = ("docker", "staging", "prod")
_mode: str | None = None


def set_mode(mode: str) -> None:
    """Set the execution mode from the ``--mode`` CLI flag.

    Must be called once at startup (in ``main()``) before any code that
    calls :func:`get_mode`, :func:`is_demo`, or :func:`resolve_storage`.
    Raises :exc:`ValueError` for invalid modes.
    """
    if mode not in _VALID_MODES:
        raise ValueError(f"Invalid mode {mode!r}. Must be one of {_VALID_MODES}.")
    global _mode
    _mode = mode


def get_mode() -> str:
    """Return the current execution mode.

    Raises :exc:`RuntimeError` if :func:`set_mode` has not been called.
    """
    if _mode is None:
        raise RuntimeError("Mode not set. Pass --mode (docker|staging|prod).")
    return _mode


def reset_mode() -> None:
    """Reset the mode to unset.  Used by test fixtures."""
    global _mode
    _mode = None


def load_env() -> None:
    """Load ``.env`` file if it exists (local dev).

    Call this once at startup so that environment variables from ``.env``
    are available to all commands.  Does **not** override variables already
    set in the environment (CI / manual exports).  No warnings or validation
    are performed — use :func:`inject_secrets` to also check that required
    secrets are present.
    """
    env_file = PROJECT_ROOT / ".env"
    if env_file.exists():
        load_dotenv(env_file, override=False)
        logger.info("Loaded environment variables from %s", env_file)


def inject_secrets() -> dict[str, str]:
    """Load ``.env`` and validate available secrets.

    Called by commands that actually need broker or S3 credentials
    (``full``, ``run-connector``, ``upload-xtb``).  Loads
    environment variables from the ``.env`` file (if present), then
    logs warnings for any required secrets that are still missing.

    Secrets are always read under their base names (e.g.
    ``IBKR_FLEX_TOKEN``).  In ECS deployments, the SSM path prefix
    (``/portfolio/demo/`` or ``/portfolio/prod/``) provides environment
    isolation — there is no ``_DEMO`` suffix swap.

    S3-specific secrets (``AWS_ACCESS_KEY_ID``,
    ``AWS_SECRET_ACCESS_KEY``) are validated only for staging and prod
    modes (cloud storage).  They are optional for docker mode (MinIO).

    Returns a dict of all available secrets for caller convenience.
    """
    load_env()

    secrets: dict[str, str] = {}

    for name in REQUIRED_SECRETS:
        value = os.environ.get(name)
        if value:
            secrets[name] = value
        else:
            logger.warning("Required secret %s is not set", name)

    # Validate S3 secrets only for staging/prod (cloud) modes.
    # Docker mode (MinIO) does not require S3 credentials.
    mode = get_mode()
    if mode in ("staging", "prod"):
        for name in REQUIRED_SECRETS_S3:
            value = os.environ.get(name)
            if value:
                secrets[name] = value
            else:
                logger.warning(
                    "S3 secret %s is not set (required for cloud storage)", name
                )

    return secrets


def get_secret(name: str) -> str | None:
    """Get a single secret by environment variable name.

    Returns ``None`` if the secret is not available.  Call
    :func:`inject_secrets` before using this function to ensure
    the ``.env`` file has been loaded.
    """
    return os.environ.get(name)


@overload
def get_env(name: str, default: str) -> str: ...


@overload
def get_env(name: str, default: None = None) -> str | None: ...


def get_env(name: str, default: str | None = None) -> str | None:
    """Get an environment variable, treating empty strings as unset.

    Unlike ``os.environ.get``, this returns *default* when the variable
    is set to an empty string.  CI systems (GitHub Actions) often set
    empty strings for undefined variables, which would silently bypass
    ``os.environ.get`` defaults.

    Returns the env var value if set and non-empty, otherwise *default*.
    """
    value = os.environ.get(name)
    if value:  # non-empty string
        return value
    return default


def parse_bool(name: str, default: bool = False) -> bool:
    """Parse a boolean env var with an explicit default.

    Returns *default* if the env var is not set.  When set, interprets
    ``true``, ``1``, and ``yes`` as ``True`` (case-insensitive) and
    everything else as ``False``.
    """
    value = os.environ.get(name)
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes")


def is_demo() -> bool:
    """Check if the pipeline is running in demo (staging) mode.

    Returns ``True`` when the execution mode is ``staging``.
    In staging mode, storage uses a separate demo bucket/prefix
    and the encryption-key file fallback is disabled.
    """
    return get_mode() == "staging"


def resolve_secret(name: str) -> str | None:
    """Get a secret from the environment by its base name.

    Always reads ``os.environ.get(name)`` directly — there is no
    suffix swap.  Environment isolation is handled by the deployment
    (ECS tasks inject secrets under base names from environment-scoped
    SSM parameters; local runs use ``.env`` or exported env vars).

    When a secret is found, logs the name at info level without logging
    the value.  When a secret is missing, logs a warning and returns
    ``None``, allowing callers to gracefully skip connectors or
    operations that require the missing secret.
    """
    value = os.environ.get(name)
    if value:
        logger.info("Resolved %s", name)
    else:
        logger.warning("Secret %s is not set", name)
    return value


# ---------------------------------------------------------------------------
# AWS credential consolidation
# ---------------------------------------------------------------------------


@dataclass
class AwsCredentials:
    """Resolved AWS credentials for S3 operations.

    Returned by :func:`resolve_aws_credentials`.  Credential fields
    are ``str | None`` — ``None`` means the credential is not available
    for the active mode.  Empty strings are never stored; a credential
    that is present but empty is treated as absent (``None``).

    When a credential is ``None``, helper methods include it as an
    empty string rather than omitting it.  This prevents the SDK from
    silently falling back to environment variables that may contain
    credentials from a different environment.  Callers that
    need IAM role fallback should not call these methods when both
    credentials are ``None``.
    """

    key_id: str | None
    secret_key: str | None
    region: str
    endpoint_url: str | None
    allow_http: bool

    def to_storage_options(self) -> dict[str, str]:
        """Return deltalake/object_store-compatible storage options dict.

        Keys use the lowercase convention required by the
        ``object_store`` Rust crate (e.g. ``aws_access_key_id``).

        When both ``key_id`` and ``secret_key`` are ``None``, credential
        keys are **omitted** entirely, allowing ``object_store`` to fall
        through its default credential chain (including ECS IAM task
        roles).  When either credential is set, both keys are included
        (with the missing one as an empty string) to prevent the SDK
        from silently falling back to environment variables that may
        contain credentials from a different environment.
        """
        opts: dict[str, str] = {
            "aws_region": self.region,
        }
        if self.key_id is not None or self.secret_key is not None:
            opts["aws_access_key_id"] = self.key_id or ""
            opts["aws_secret_access_key"] = self.secret_key or ""
        if self.endpoint_url:
            opts["aws_endpoint_url"] = self.endpoint_url
        if self.allow_http:
            opts["aws_allow_http"] = "true"
        return opts

    def to_pyarrow_kwargs(self) -> dict:
        """Return PyArrow S3FileSystem-compatible keyword arguments.

        Keys use PyArrow convention (``access_key``, ``secret_key``,
        ``region``, ``endpoint_override``, ``scheme``).

        When both ``key_id`` and ``secret_key`` are ``None``, credential
        keys are **omitted** entirely, allowing PyArrow to fall through
        its default credential chain (including ECS IAM task roles).
        When either credential is set, both keys are included (with the
        missing one as an empty string) to prevent PyArrow from silently
        falling back to environment variables that may contain production
        credentials.
        """
        kwargs: dict = {
            "region": self.region,
        }
        if self.key_id is not None or self.secret_key is not None:
            kwargs["access_key"] = self.key_id or ""
            kwargs["secret_key"] = self.secret_key or ""
        if self.endpoint_url:
            from urllib.parse import urlparse

            parsed = urlparse(self.endpoint_url)
            host = parsed.hostname or ""
            port = parsed.port
            endpoint_override = f"{host}:{port}" if port else host
            kwargs["endpoint_override"] = endpoint_override
            if parsed.scheme == "http":
                kwargs["scheme"] = "http"
        if self.allow_http and "scheme" not in kwargs:
            kwargs["scheme"] = "http"
        return kwargs

    def to_duckdb_secret_parts(self) -> list[str]:
        """Return DuckDB CREATE SECRET SQL parts for S3 credentials.

        Returns a list of SQL fragments like ``KEY_ID 'value'``
        suitable for ``CREATE SECRET (TYPE S3, ...)``.  When
        credentials are ``None``, they are included as empty strings
        to prevent DuckDB from falling back to environment variables.
        Returns an empty list only when both credentials are ``None``
        and no endpoint/SSL overrides are needed, signalling that no
        SECRET should be created (allowing IAM role fallback).
        """
        # Escape single quotes to prevent SQL injection via env vars.
        safe_key_id = (self.key_id or "").replace("'", "''")
        safe_secret = (self.secret_key or "").replace("'", "''")
        safe_region = self.region.replace("'", "''")
        parts = [
            f"KEY_ID '{safe_key_id}'",
            f"SECRET '{safe_secret}'",
            f"REGION '{safe_region}'",
        ]
        if self.endpoint_url:
            from urllib.parse import urlparse

            parsed = urlparse(self.endpoint_url)
            host = parsed.hostname or ""
            port = parsed.port
            endpoint_host = f"{host}:{port}" if port else host
            safe_endpoint = endpoint_host.replace("'", "''")
            parts.append(f"ENDPOINT '{safe_endpoint}'")
        if self.allow_http:
            parts.append("USE_SSL false")
            parts.append("URL_STYLE path")
        return parts


def resolve_aws_credentials() -> AwsCredentials:
    """Resolve AWS credentials from environment variables.

    Uses :func:`resolve_secret` for ``AWS_ACCESS_KEY_ID`` and
    ``AWS_SECRET_ACCESS_KEY``.  When a credential is not set or set to an
    empty string, it is normalized to ``None`` (not an empty string),
    which prevents the SDK from falling back to environment variables
    that may contain credentials from a different environment.

    ``AWS_REGION``, ``S3_ENDPOINT_URL``, and ``S3_ALLOW_HTTP`` are
    configuration (not secrets) and are read directly from the
    environment.
    """
    key_id = resolve_secret("AWS_ACCESS_KEY_ID") or None
    secret_key = resolve_secret("AWS_SECRET_ACCESS_KEY") or None
    region = get_env("AWS_REGION", "eu-west-1")
    endpoint_url = get_env("S3_ENDPOINT_URL")
    allow_http = os.environ.get("S3_ALLOW_HTTP", "").lower() in ("1", "true", "yes")

    return AwsCredentials(
        key_id=key_id,
        secret_key=secret_key,
        region=region,
        endpoint_url=endpoint_url,
        allow_http=allow_http,
    )
