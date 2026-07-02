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
        inject_secrets, get_secret, get_config, is_enabled,
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

# The _DEMO variants listed for inject_secrets() validation in demo mode.
REQUIRED_SECRETS_DEMO: list[str] = list(DEMO_SECRET_MAP.values())

# S3-specific secrets — only required when STORAGE_TYPE is cloud.
REQUIRED_SECRETS_S3: list[str] = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
]
REQUIRED_SECRETS_S3_DEMO: list[str] = [
    "AWS_ACCESS_KEY_ID_DEMO",
    "AWS_SECRET_ACCESS_KEY_DEMO",
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
        for name in REQUIRED_SECRETS_DEMO:
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


def get_config(name: str, default: str | None = None) -> str | None:
    """Get a config value from environment variables with an optional default.

    Returns the env var value if set, otherwise *default*.
    """
    return os.environ.get(name, default)


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
    if os.environ.get("S3_BUCKET"):
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
