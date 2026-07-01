"""CLI: generate a Fernet encryption key for pipeline data.

Usage::

    python -m pipeline.keygen
"""

from __future__ import annotations

from pathlib import Path

from pipeline.crypto import generate_key
from pipeline.storage import S3Backend, get_storage


def main() -> None:
    config = get_storage()
    if isinstance(config.backend, S3Backend):
        print("In S3 mode, set the ENCRYPTION_KEY environment variable.")
        print(
            "Example: export ENCRYPTION_KEY=$(python -c "
            "'from pipeline.crypto import generate_key; print(generate_key().decode())')"
        )
        return
    Path(config.secrets_dir).mkdir(parents=True, exist_ok=True)
    if Path(config.encryption_key_file).exists():
        print(f"Encryption key already exists at {config.encryption_key_file}")
        return
    key = generate_key()
    Path(config.encryption_key_file).write_bytes(key)
    print(f"Encryption key written to {config.encryption_key_file}")


if __name__ == "__main__":
    main()
