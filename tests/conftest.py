"""Shared test fixtures: Fernet key, temp directories, sample data."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from pipeline.crypto import generate_key
from pipeline.storage import LocalBackend, StorageConfig, use_storage


@pytest.fixture()
def fernet_key() -> bytes:
    """Return a freshly generated Fernet key for test use."""
    return generate_key()


@pytest.fixture()
def tmp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary data/ directory structure for pipeline tests.

    Also injects a ``StorageConfig`` that points at the temp directory
    so that all code under test resolves paths to ``tmp_path`` instead
    of the project's real ``data/`` directory.
    """
    data = tmp_path / "data"
    for subdir in [
        "raw/ibkr_snapshot", "raw/ibkr_cdc",
        "raw/trading212_snapshot", "raw/trading212_cdc",
        "raw/xtb_snapshot", "raw/xtb_cdc",
        "normalized/ibkr_snapshot", "normalized/ibkr_cdc",
        "normalized/trading212_snapshot", "normalized/trading212_cdc",
        "normalized/xtb_snapshot", "normalized/xtb_cdc",
        "normalized/consolidated_holdings",
        "analytics/portfolio_allocation",
    ]:
        (data / subdir).mkdir(parents=True, exist_ok=True)

    config = StorageConfig(
        env="test",
        data_dir=data,
        raw_dir=data / "raw",
        normalized_dir=data / "normalized",
        analytics_dir=data / "analytics",
        secrets_dir=tmp_path / ".secrets",
        encryption_key_file=tmp_path / ".secrets" / "encryption.key",
        backend=LocalBackend(data),
    )
    use_storage(config)
    return data


@pytest.fixture()
def secrets_dir(tmp_path: Path) -> Path:
    """Create a temporary .secrets/ directory with a test encryption key."""
    secrets = tmp_path / ".secrets"
    secrets.mkdir()
    key = generate_key()
    (secrets / "encryption.key").write_bytes(key)
    return secrets


@pytest.fixture()
def env_key(fernet_key: bytes):
    """Set PORTFOLIO_ENCRYPTION_KEY env var for the duration of a test."""
    os.environ["PORTFOLIO_ENCRYPTION_KEY"] = fernet_key.decode("utf-8")
    try:
        yield
    finally:
        os.environ.pop("PORTFOLIO_ENCRYPTION_KEY", None)