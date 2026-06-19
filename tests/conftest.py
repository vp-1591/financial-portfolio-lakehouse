"""Shared test fixtures: Fernet key, temp directories, sample data."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from pipeline.crypto import generate_key


@pytest.fixture()
def fernet_key() -> bytes:
    """Return a freshly generated Fernet key for test use."""
    return generate_key()


@pytest.fixture()
def tmp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary data/ directory structure for pipeline tests."""
    data = tmp_path / "data"
    (data / "raw" / "ibkr_snapshot").mkdir(parents=True)
    (data / "raw" / "ibkr_cdc").mkdir(parents=True)
    (data / "raw" / "trading212_snapshot").mkdir(parents=True)
    (data / "raw" / "trading212_cdc").mkdir(parents=True)
    (data / "raw" / "xtb_snapshot").mkdir(parents=True)
    (data / "raw" / "xtb_cdc").mkdir(parents=True)
    (data / "normalized" / "ibkr_snapshot").mkdir(parents=True)
    (data / "normalized" / "ibkr_cdc").mkdir(parents=True)
    (data / "normalized" / "trading212_snapshot").mkdir(parents=True)
    (data / "normalized" / "trading212_cdc").mkdir(parents=True)
    (data / "normalized" / "xtb_snapshot").mkdir(parents=True)
    (data / "normalized" / "xtb_cdc").mkdir(parents=True)
    (data / "normalized" / "consolidated_holdings").mkdir(parents=True)
    (data / "analytics" / "portfolio_allocation").mkdir(parents=True)
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
def env_key(fernet_key: bytes) -> None:
    """Set PORTFOLIO_ENCRYPTION_KEY env var for the duration of a test."""
    os.environ["PORTFOLIO_ENCRYPTION_KEY"] = fernet_key.decode("utf-8")
    try:
        yield
    finally:
        os.environ.pop("PORTFOLIO_ENCRYPTION_KEY", None)