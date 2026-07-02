"""Secret and config resolution from environment variables and .env files.

Secrets and configuration are never stored in the repository.  They come from
one of two sources:

1. **Environment variables** — set by CI (GitHub Secrets / workflow inputs)
   or manual exports.  Highest priority; always checked first.
2. **``.env`` file** — loaded by ``python-dotenv`` for local development.
   The file lives at the project root and is gitignored.

If a secret is missing from both sources, the pipeline will error when
the secret is actually needed — not at startup.  This allows commands
like ``transform`` and ``allocate`` to run without any broker API keys.

**Demo mode.**  When the ``DEMO`` environment variable is set to ``true``,
``1``, or ``yes``, the pipeline runs in demo mode.  In demo mode,
:func:`resolve_secret` returns ``_DEMO``-suffixed secrets instead of the
base names, and :func:`inject_secrets` validates the ``_DEMO`` variants.
There is **no cross-mode fallback** — missing credentials for the active
mode are logged as warnings and :func:`resolve_secret` returns ``None``,
allowing callers to gracefully skip connectors or operations that require
the missing secret.

Connector toggles (``IBKR_ENABLED``, ``T212_ENABLED``, ``XTB_ENABLED``)
default to **enabled**.  Set them to ``0``, ``false``, or ``no`` to disable
a connector.

Usage::

    from pipeline.secrets import (
        inject_secrets, get_secret, get_env, is_enabled,
        parse_bool, is_demo, resolve_secret,
    )

    inject_secrets()           # call once at startup (loads .env, validates)
    token = resolve_secret("IBKR_FLEX_TOKEN")  # demo-aware secret lookup
    if is_enabled("IBKR_ENABLED"):              # True unless set to 0/false/no
        ...
    if is_demo():                                # True when DEMO=true
        ...
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

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

# Demo-mode secret mappings: base name → _DEMO variant.
# In demo mode, resolve_secret() uses the _DEMO variant exclusively.
# There is NO fallback to base secrets in demo mode, and NO fallback
# to _DEMO secrets in production mode.
DEMO_SECRET_MAP: dict[str, str] = {
    "IBKR_FLEX_TOKEN": "IBKR_FLEX_TOKEN_DEMO",
    "IBKR_FLEX_QUERY_ID": "IBKR_FLEX_QUERY_ID_DEMO",
    "T212_API_KEY": "T212_API_KEY_DEMO",
    "T212_API_SECRET": "T212_API_SECRET_DEMO",
    "ENCRYPTION_KEY": "ENCRYPTION_KEY_DEMO",
    "AWS_ACCESS_KEY_ID": "AWS_ACCESS_KEY_ID_DEMO",
    "AWS_SECRET_ACCESS_KEY": "AWS_SECRET_ACCESS_KEY_DEMO",
}

# S3-specific secrets — only required when STORAGE_TYPE is cloud.
REQUIRED_SECRETS_S3: list[str] = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
]
REQUIRED_SECRETS_S3_DEMO: list[str] = [
    "AWS_ACCESS_KEY_ID_DEMO",
    "AWS_SECRET_ACCESS_KEY_DEMO",
]

# The _DEMO variants listed for inject_secrets() validation in demo mode.
REQUIRED_SECRETS_DEMO: list[str] = list(DEMO_SECRET_MAP.values())

# Non-AWS demo secrets — validated in the general demo loop of
# inject_secrets().  AWS demo secrets are validated only when
# STORAGE_TYPE is cloud, matching the production path.
REQUIRED_SECRETS_DEMO_NON_AWS: list[str] = [
    name for name in REQUIRED_SECRETS_DEMO if name not in REQUIRED_SECRETS_S3_DEMO
]

# Storage type constants.
STORAGE_TYPE_CLOUD = "cloud"
STORAGE_TYPE_MINIO = "minio"
STORAGE_TYPE_LOCAL = "local"
VALID_STORAGE_TYPES = (STORAGE_TYPE_CLOUD, STORAGE_TYPE_MINIO, STORAGE_TYPE_LOCAL)


def inject_secrets() -> dict[str, str]:
    """Load ``.env`` and validate available secrets.

    Called once at pipeline startup.  Loads environment variables from
    the ``.env`` file (if present), then logs warnings for any required
    secrets that are still missing.

    In demo mode (``DEMO=true``), validates ``_DEMO`` variants instead
    of base secrets.  There is no cross-mode fallback.

    S3-specific secrets (``AWS_ACCESS_KEY_ID``,
    ``AWS_SECRET_ACCESS_KEY``) are validated only when
    ``STORAGE_TYPE`` is ``"cloud"``.  They are optional for
    ``"minio"`` and ``"local"`` storage.

    Returns a dict of all available secrets for caller convenience.
    """
    # Load .env file if it exists (local dev).  Does NOT override
    # variables already set in the environment (CI / manual exports).
    env_file = PROJECT_ROOT / ".env"
    if env_file.exists():
        load_dotenv(env_file, override=False)
        logger.info("Loaded environment variables from %s", env_file)

    secrets: dict[str, str] = {}

    if is_demo():
        for name in REQUIRED_SECRETS_DEMO_NON_AWS:
            value = os.environ.get(name)
            if value:
                secrets[name] = value
            else:
                logger.warning("Demo secret %s is not set (DEMO mode active)", name)
    else:
        for name in REQUIRED_SECRETS:
            value = os.environ.get(name)
            if value:
                secrets[name] = value
            else:
                logger.warning("Required secret %s is not set", name)

    # Validate S3 secrets only for cloud storage.
    storage_type = get_storage_type()
    if storage_type == STORAGE_TYPE_CLOUD:
        s3_secrets = REQUIRED_SECRETS_S3_DEMO if is_demo() else REQUIRED_SECRETS_S3
        label = "Demo S3" if is_demo() else "S3"
        for name in s3_secrets:
            value = os.environ.get(name)
            if value:
                secrets[name] = value
            else:
                logger.warning(
                    "%s secret %s is not set (required for cloud storage)", label, name
                )

    return secrets


def get_secret(name: str) -> str | None:
    """Get a single secret by environment variable name.

    Returns ``None`` if the secret is not available.  Call
    :func:`inject_secrets` before using this function to ensure
    the ``.env`` file has been loaded.
    """
    return os.environ.get(name)


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


def is_enabled(name: str) -> bool:
    """Check if a connector or feature is enabled via an env var.

    Returns ``True`` unless the env var is explicitly set to one of
    ``0``, ``false``, or ``no`` (case-insensitive).  This means
    connectors are **enabled by default**.
    """
    value = os.environ.get(name, "").lower()
    return value not in ("0", "false", "no")


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
    """Check if the pipeline is running in demo mode.

    Returns ``True`` when the ``DEMO`` env var is set to ``true``,
    ``1``, or ``yes`` (case-insensitive).  Defaults to ``False``.

    In demo mode, :func:`resolve_secret` returns ``_DEMO``-suffixed
    secrets and storage uses a separate demo bucket/directory.
    """
    return parse_bool("DEMO", default=False)


def get_storage_type() -> str:
    """Return the storage type from the ``STORAGE_TYPE`` env var.

    Valid values are ``"cloud"`` (S3), ``"minio"`` (S3-compatible),
    and ``"local"`` (filesystem).  If ``STORAGE_TYPE`` is not set,
    defaults to ``"cloud"`` when ``S3_BUCKET`` is set, and ``"local"``
    otherwise.

    Raises :exc:`ValueError` for invalid values.
    """
    explicit = os.environ.get("STORAGE_TYPE", "").lower()
    if explicit:
        if explicit not in VALID_STORAGE_TYPES:
            raise ValueError(
                f"STORAGE_TYPE must be one of {VALID_STORAGE_TYPES}, got {explicit!r}"
            )
        return explicit
    # Backward compatibility: if S3_BUCKET is set and no STORAGE_TYPE,
    # default to cloud.
    if get_env("S3_BUCKET"):
        return STORAGE_TYPE_CLOUD
    return STORAGE_TYPE_LOCAL


def resolve_secret(name: str) -> str | None:
    """Get a secret, using the ``_DEMO`` variant when demo mode is active.

    **Strict isolation — no cross-mode fallback:**

    - When ``DEMO=true`` and *name* is in :data:`DEMO_SECRET_MAP`,
      returns the ``_DEMO`` variant.  If the ``_DEMO`` variant is not
      set, logs a warning and returns ``None`` — **never falls back to
      the base secret**.
    - When ``DEMO=false`` (or unset), returns ``os.environ.get(name)``
      directly — **never reads ``_DEMO`` variants**.
    - For names not in :data:`DEMO_SECRET_MAP` (e.g., config vars
      like ``IBKR_FLEX_BASE_URL``), returns ``os.environ.get(name)``
      regardless of mode.

    When a secret is found, logs the source (base or ``_DEMO`` variant)
    at info level without logging the value.  When a secret is missing,
    logs a warning with the expected variable name.
    """
    if is_demo() and name in DEMO_SECRET_MAP:
        demo_name = DEMO_SECRET_MAP[name]
        value = os.environ.get(demo_name)
        if value:
            logger.info("Resolved %s from %s (demo mode)", name, demo_name)
            return value
        logger.warning(
            "Demo mode is active but %s is not set — "
            "returning None for %s (no fallback to base secret)",
            demo_name,
            name,
        )
        return None
    value = os.environ.get(name)
    if value:
        logger.info("Resolved %s from %s", name, name)
    else:
        logger.debug("Secret %s is not set", name)
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
    production credentials when running in demo mode.  Callers that
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
        Credential keys are always included — when ``None``, they are
        set to empty strings to prevent ``object_store`` from falling
        back to environment variables.
        """
        opts: dict[str, str] = {
            "aws_access_key_id": self.key_id or "",
            "aws_secret_access_key": self.secret_key or "",
            "aws_region": self.region,
        }
        if self.endpoint_url:
            opts["aws_endpoint_url"] = self.endpoint_url
        if self.allow_http:
            opts["aws_allow_http"] = "true"
        return opts

    def to_pyarrow_kwargs(self) -> dict:
        """Return PyArrow S3FileSystem-compatible keyword arguments.

        Keys use PyArrow convention (``access_key``, ``secret_key``,
        ``region``, ``endpoint_override``, ``scheme``).
        Credential keys are always included — when ``None``, they are
        set to empty strings to prevent PyArrow from falling back to
        environment variables.
        """
        kwargs: dict = {
            "region": self.region,
            "access_key": self.key_id or "",
            "secret_key": self.secret_key or "",
        }
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
    ``AWS_SECRET_ACCESS_KEY`` so that demo mode uses ``_DEMO``
    variants exclusively.  When a credential is not set or set to an
    empty string, it is normalized to ``None`` (not an empty string),
    which prevents the SDK from falling back to environment variables
    that may contain production credentials.

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
