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

Connector toggles (``IBKR_ENABLED``, ``T212_ENABLED``, ``XTB_ENABLED``)
default to **enabled**.  Set them to ``0``, ``false``, or ``no`` to disable
a connector.

Usage::

    from pipeline.secrets import inject_secrets, get_secret, get_config, is_enabled

    inject_secrets()           # call once at startup (loads .env, validates)
    token = get_secret("IBKR_FLEX_TOKEN")  # returns str | None
    base_url = get_config("IBKR_BASE_URL")  # returns str | None
    if is_enabled("IBKR_ENABLED"):          # True unless set to 0/false/no
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
    "T212_API_KEY",
    "T212_API_SECRET",
    "PORTFOLIO_ENCRYPTION_KEY",
]


def inject_secrets() -> dict[str, str]:
    """Load ``.env`` and validate available secrets.

    Called once at pipeline startup.  Loads environment variables from
    the ``.env`` file (if present), then logs warnings for any required
    secrets that are still missing.

    Returns a dict of all available secrets for caller convenience.
    """
    # Load .env file if it exists (local dev).  Does NOT override
    # variables already set in the environment (CI / manual exports).
    env_file = PROJECT_ROOT / ".env"
    if env_file.exists():
        load_dotenv(env_file, override=False)
        logger.info("Loaded environment variables from %s", env_file)

    secrets: dict[str, str] = {}
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