"""CLI: generate a Fernet encryption key for pipeline data.

Usage::

    python -m pipeline.keygen
"""

from __future__ import annotations

from pipeline.crypto import generate_key
from pipeline.paths import ENCRYPTION_KEY_FILE, SECRETS_DIR


def main() -> None:
    SECRETS_DIR.mkdir(parents=True, exist_ok=True)
    if ENCRYPTION_KEY_FILE.exists():
        print(f"Encryption key already exists at {ENCRYPTION_KEY_FILE}")
        return
    key = generate_key()
    ENCRYPTION_KEY_FILE.write_bytes(key)
    print(f"Encryption key written to {ENCRYPTION_KEY_FILE}")


if __name__ == "__main__":
    main()