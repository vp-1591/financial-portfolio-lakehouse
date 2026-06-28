"""CLI: generate a Fernet encryption key for pipeline data.

Usage::

    python -m pipeline.keygen
"""

from __future__ import annotations

from pipeline.crypto import generate_key
from pipeline.storage import get_storage


def main() -> None:
    config = get_storage()
    config.secrets_dir.mkdir(parents=True, exist_ok=True)
    if config.encryption_key_file.exists():
        print(f"Encryption key already exists at {config.encryption_key_file}")
        return
    key = generate_key()
    config.encryption_key_file.write_bytes(key)
    print(f"Encryption key written to {config.encryption_key_file}")


if __name__ == "__main__":
    main()