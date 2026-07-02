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
mode are a hard error.

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
}

# The _DEMO variants listed for inject_secrets() validation in demo mode.
REQUIRED_SECRETS_DEMO: list[str] = list(DEMO_SECRET_MAP.values())


def inject_secrets() -> dict[str, str]:
    """Load ``.env`` and validate available secrets.

    Called once at pipeline startup.  Loads environment variables from
    the ``.env`` file (if present), then logs warnings for any required
    secrets that are still missing.

    In demo mode (``DEMO=true``), validates ``_DEMO`` variants instead
    of base secrets.  There is no cross-mode fallback.

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


def resolve_secret(name: str) -> str | None:
    """Get a secret, using the ``_DEMO`` variant when demo mode is active.

    **Strict isolation — no cross-mode fallback:**

    - When ``DEMO=true`` and *name* is in :data:`DEMO_SECRET_MAP`,
      returns the ``_DEMO`` variant.  Raises :exc:`EnvironmentError`
      if the ``_DEMO`` variant is not set — **never falls back to the
      base secret**.
    - When ``DEMO=false`` (or unset), returns ``os.environ.get(name)``
      directly — **never reads ``_DEMO`` variants**.
    - For names not in :data:`DEMO_SECRET_MAP` (e.g., config vars
      like ``IBKR_FLEX_BASE_URL``), returns ``os.environ.get(name)``
      regardless of mode.
    """
    if is_demo() and name in DEMO_SECRET_MAP:
        demo_name = DEMO_SECRET_MAP[name]
        value = os.environ.get(demo_name)
        if value:
            return value
        raise EnvironmentError(
            f"Demo mode is active but {demo_name} is not set. "
            f"Set {demo_name} to provide demo credentials for {name}."
        )
    return os.environ.get(name)
