"""Secret resolution from environment variables and Bitwarden Vault CLI.

Secrets are never stored on disk.  They come from one of two sources:

1. **Already-set environment variables** — CI (GitHub Secrets) or manual
   exports.  If the variable is present, the Bitwarden lookup is skipped.
2. **Bitwarden Vault CLI (``bw``)** — if ``BW_SESSION`` is set (vault is
   unlocked), each secret is fetched by its vault item name.

If a secret is missing from both sources, the pipeline will error when
the secret is actually needed — not at startup.  This allows commands
like ``transform`` and ``allocate`` to run without any broker API keys.

Usage::

    from pipeline.secrets import inject_secrets, get_secret

    inject_secrets()           # call once at startup
    token = get_secret("IBKR_FLEX_TOKEN")  # returns str | None
"""

from __future__ import annotations

import os
import subprocess

# Maps environment variable name → Bitwarden vault item name.
# Only secrets that the pipeline actually needs are listed here.
_BW_SECRET_MAP: dict[str, str] = {
    "IBKR_FLEX_TOKEN": "IBKR Flex Token",
    "T212_API_KEY": "Trading 212 API Key",
    "T212_API_SECRET": "Trading 212 API Secret",
    "PIPELINE_DATA_DIR": "Pipeline Data Dir",
    "PORTFOLIO_ENCRYPTION_KEY": "Portfolio Encryption Key",
}


def resolve_secrets() -> dict[str, str]:
    """Fetch secrets from the environment and Bitwarden.

    Priority:

    1. Already-set environment variables (skip Bitwarden lookup).
    2. ``bw get password <item> --session $BW_SESSION`` if
       ``BW_SESSION`` is set.
    3. Missing secrets are simply not returned — the pipeline will
       error when a required secret is accessed.
    """
    secrets: dict[str, str] = {}
    bw_session = os.environ.get("BW_SESSION")

    for env_name, bw_name in _BW_SECRET_MAP.items():
        # Already set in environment (CI or manual) — use it.
        if env_name in os.environ:
            secrets[env_name] = os.environ[env_name]
            continue

        # Try Bitwarden Vault CLI.
        if bw_session:
            try:
                result = subprocess.run(
                    ["bw", "get", "password", bw_name, "--session", bw_session],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                secrets[env_name] = result.stdout.strip()
            except subprocess.CalledProcessError:
                pass  # Secret not found in vault — skip.

    return secrets


def inject_secrets() -> dict[str, str]:
    """Resolve secrets and inject them into ``os.environ``.

    Called once at pipeline startup.  Returns the secrets dict so
    callers that need direct access can use it.
    """
    secrets = resolve_secrets()
    os.environ.update(secrets)
    return secrets


def get_secret(name: str) -> str | None:
    """Get a single secret by environment variable name.

    Returns ``None`` if the secret is not available.  Call
    :func:`inject_secrets` before using this function.
    """
    return os.environ.get(name)