"""Fernet encrypt/decrypt helpers for financial data columns.

This is the only module that imports from ``cryptography``.
"""

from __future__ import annotations

import base64
import os
from pathlib import Path

from cryptography.fernet import Fernet


def generate_key() -> bytes:
    """Generate a new Fernet encryption key."""
    return Fernet.generate_key()


def load_key(path: Path | None = None) -> bytes:
    """Load a Fernet key from disk or the ``PORTFOLIO_ENCRYPTION_KEY`` env var.

    Parameters
    ----------
    path:
        Explicit path to the key file.  When *None*, falls back to
        ``PORTFOLIO_ENCRYPTION_KEY`` env var, then to
        ``.secrets/encryption.key`` relative to the project root.
    """
    env_key = os.environ.get("PORTFOLIO_ENCRYPTION_KEY")
    if env_key:
        return env_key.encode("utf-8") if isinstance(env_key, str) else env_key

    if path is None:
        from pipeline.storage import get_storage

        path = Path(get_storage().encryption_key_file)

    if not path.exists():
        raise FileNotFoundError(
            f"Encryption key not found at {path}. "
            "Run 'python -m pipeline.keygen' to create one, "
            "or set the PORTFOLIO_ENCRYPTION_KEY environment variable."
        )
    return path.read_bytes().strip()


def encrypt(value: bytes, key: bytes) -> bytes:
    """Encrypt *value* with Fernet and return the token as raw bytes."""
    return Fernet(key).encrypt(value)


def decrypt(token: bytes, key: bytes) -> bytes:
    """Decrypt a Fernet *token* back to the original bytes."""
    return Fernet(key).decrypt(token)


def encrypt_float(value: float, key: bytes) -> bytes:
    """Encrypt a float by encoding it as a string first."""
    return encrypt(str(value).encode("utf-8"), key)


def decrypt_float(token: bytes, key: bytes) -> float:
    """Decrypt a Fernet token back to a float."""
    return float(decrypt(token, key).decode("utf-8"))


def encrypt_string(value: str, key: bytes) -> bytes:
    """Encrypt a string value."""
    return encrypt(value.encode("utf-8"), key)


def decrypt_string(token: bytes, key: bytes) -> str:
    """Decrypt a Fernet token back to a string."""
    return decrypt(token, key).decode("utf-8")