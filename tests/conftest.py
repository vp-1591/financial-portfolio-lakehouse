"""Shared test fixtures: Fernet key, temp directories, sample data."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from pipeline.crypto import generate_key
from pipeline.storage import LocalBackend, StorageConfig, use_storage

# All pipeline-related environment variables that tests must isolate from.
# Cleared before each test so local .env files and shell env vars don't leak.
_PIPELINE_ENV_VARS = [
    "DEMO",
    "STORAGE_TYPE",
    "S3_BUCKET",
    "S3_BUCKET_DEMO",
    "S3_PREFIX",
    "S3_PREFIX_DEMO",
    "PIPELINE_DATA_DIR",
    "PIPELINE_DATA_DIR_DEMO",
    "S3_ENDPOINT_URL",
    "S3_ALLOW_HTTP",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "AWS_ACCESS_KEY_ID_DEMO",
    "AWS_SECRET_ACCESS_KEY_DEMO",
    "IBKR_FLEX_TOKEN",
    "IBKR_FLEX_QUERY_ID",
    "IBKR_FLEX_CDC_QUERY_ID",
    "IBKR_FLEX_TOKEN_DEMO",
    "IBKR_FLEX_QUERY_ID_DEMO",
    "IBKR_FLEX_CDC_QUERY_ID_DEMO",
    "T212_API_KEY",
    "T212_API_SECRET",
    "T212_API_KEY_DEMO",
    "T212_API_SECRET_DEMO",
    "ENCRYPTION_KEY",
    "ENCRYPTION_KEY_DEMO",
    "IBKR_ENABLED",
    "T212_ENABLED",
    "XTB_ENABLED",
    "IBKR_FLEX_BASE_URL",
    "IBKR_FLEX_BASE_URL_DEMO",
    "T212_BASE_URL",
    "T212_BASE_URL_DEMO",
    "XTB_REPORT_PATH",
]


@pytest.fixture(autouse=True)
def _isolate_pipeline_env(monkeypatch, tmp_path):
    """Clear all pipeline env vars and reset singletons for test isolation.

    Ensures tests are isolated from local .env files and shell environment.
    Individual tests can set env vars via ``monkeypatch.setenv`` as needed;
    ``monkeypatch`` restores the original values after each test.

    ``PROJECT_ROOT`` is redirected to an empty temp directory so that
    ``load_dotenv`` finds no ``.env`` file by default.  Tests that
    intentionally exercise ``.env`` loading override this by setting
    ``PROJECT_ROOT`` to their own ``tmp_path`` with a crafted ``.env``.
    """
    for var in _PIPELINE_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    # Redirect PROJECT_ROOT to an empty temp dir so load_dotenv finds no .env.
    # Tests that need .env loading set PROJECT_ROOT to their own tmp_path.
    monkeypatch.setattr("pipeline.secrets.PROJECT_ROOT", tmp_path)
    # Reset storage singleton so resolve_storage() re-reads env vars
    import pipeline.storage

    pipeline.storage._config = None
    yield
    pipeline.storage._config = None


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
        "raw/ibkr_snapshot",
        "raw/ibkr_cdc",
        "raw/trading212_snapshot",
        "raw/trading212_cdc",
        "raw/xtb_snapshot",
        "raw/xtb_cdc",
        "normalized/ibkr_snapshot",
        "normalized/ibkr_cdc",
        "normalized/trading212_snapshot",
        "normalized/trading212_cdc",
        "normalized/xtb_snapshot",
        "normalized/xtb_cdc",
        "normalized/consolidated_holdings",
        "analytics/portfolio_allocation",
    ]:
        (data / subdir).mkdir(parents=True, exist_ok=True)

    config = StorageConfig(
        data_dir=str(data),
        raw_dir=str(data / "raw"),
        normalized_dir=str(data / "normalized"),
        analytics_dir=str(data / "analytics"),
        secrets_dir=str(tmp_path / ".secrets"),
        encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
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
    """Set ENCRYPTION_KEY env var for the duration of a test."""
    os.environ["ENCRYPTION_KEY"] = fernet_key.decode("utf-8")
    try:
        yield
    finally:
        os.environ.pop("ENCRYPTION_KEY", None)
