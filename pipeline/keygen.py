"""CLI: print instructions for generating a Fernet encryption key.

Usage::

    python -m pipeline.keygen

In every execution mode (``docker``, ``staging``, ``prod``) the storage
backend is S3-backed, so the encryption key must be supplied via the
``ENCRYPTION_KEY`` environment variable rather than written to a local
file. This command prints how to generate one.
"""

from __future__ import annotations

from pipeline.crypto import generate_key


def main() -> None:
    print("Set the ENCRYPTION_KEY environment variable with a Fernet key.")
    print(
        "Example: export ENCRYPTION_KEY=$(python -c "
        "'from pipeline.crypto import generate_key; print(generate_key().decode())')"
    )
    # Generate once here so the command is not a no-op and surfaces any
    # cryptography install issues immediately.
    generate_key()


if __name__ == "__main__":
    main()
