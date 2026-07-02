"""Fernet encrypt/decrypt helpers for financial data columns.

This is the only module that imports from ``cryptography``.
"""

from __future__ import annotations

from pathlib import Path

from cryptography.fernet import Fernet


def generate_key() -> bytes:
    """Generate a new Fernet encryption key."""
    return Fernet.generate_key()


def load_key(path: Path | None = None) -> bytes:
    """Load a Fernet key from the ``ENCRYPTION_KEY`` env var or a key file.

    In demo mode, checks ``ENCRYPTION_KEY_DEMO`` via
    :func:`pipeline.secrets.resolve_secret`.  There is no cross-mode
    fallback — if the key for the active mode is missing, a hard error
    is raised.  In demo mode, the file-based fallback is **disabled**
    because ``.secrets/encryption.key`` is shared between modes and
    would contain the production key.

    Parameters
    ----------
    path:
        Explicit path to the key file.  When *None* and the env var
        is not set, falls back to ``.secrets/encryption.key`` relative
        to the project root (production mode only; raises in demo mode).

    Raises
    ------
    EnvironmentError
        If demo mode is active and ``ENCRYPTION_KEY_DEMO`` is not set.
    FileNotFoundError
        If the key file does not exist at the resolved path.
    """
    from pipeline.secrets import is_demo, resolve_secret

    env_key = resolve_secret("ENCRYPTION_KEY")
    if env_key:
        return env_key.encode("utf-8") if isinstance(env_key, str) else env_key

    # resolve_secret returned None — in demo mode, this means
    # ENCRYPTION_KEY_DEMO was not set.  Falling through to the
    # file-based key would use the production key, violating isolation.
    if is_demo():
        raise EnvironmentError(
            "ENCRYPTION_KEY_DEMO is not set.  In demo mode, the encryption "
            "key must be provided via the ENCRYPTION_KEY_DEMO environment "
            "variable — there is no fallback to the file-based key."
        )

    if path is None:
        from pipeline.storage import get_storage

        path = Path(get_storage().encryption_key_file)

    if not path.exists():
        raise FileNotFoundError(
            f"Encryption key not found at {path}. "
            "Run 'python -m pipeline.keygen' to create one, "
            "or set the ENCRYPTION_KEY environment variable."
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
