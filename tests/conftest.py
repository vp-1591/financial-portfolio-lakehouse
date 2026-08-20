"""Shared test fixtures: Fernet key, temp directories, sample data."""

from __future__ import annotations

from pathlib import Path

import pytest

import pipeline.secrets
from pipeline.crypto import generate_key
from pipeline.storage import StorageConfig, use_storage
from tests.local_backend import LocalBackend

# All pipeline-related environment variables that tests must isolate from.
# Cleared before each test so local .env files and shell env vars don't leak.
# Note: STAGING and STORAGE_TYPE are removed (replaced by --mode flag).
_PIPELINE_ENV_VARS = [
    "S3_BUCKET",
    "PIPELINE_DATA_DIR",
    "S3_ENDPOINT_URL",
    "S3_ALLOW_HTTP",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION",
    "IBKR_FLEX_TOKEN",
    "IBKR_FLEX_QUERY_ID",
    "IBKR_FLEX_EVENTS_QUERY_ID",
    "T212_API_KEY",
    "T212_API_SECRET",
    "ENCRYPTION_KEY",
    "IBKR_FLEX_BASE_URL",
    "T212_BASE_URL",
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
    # Reset storage singleton so resolve_storage() re-reads env vars.
    # This autouse teardown is what guarantees no ``_setup_storage`` fixture
    # (in test_consolidate_pipeline.py, test_transform_pipeline.py,
    # test_report.py, test_quality.py, test_events_analytics.py,
    # test_portfolio_holdings.py) leaves ``set_mode("docker")`` / ``_config``
    # set after its test: ``_config = None`` and ``reset_mode()`` run before
    # AND after every test, so a test running after a docker-mode test always
    # observes the default (None) mode (A2 mechanics 1-2, A3 F4, A7 F36).
    pipeline.storage._config = None
    pipeline.secrets.reset_mode()
    yield
    pipeline.storage._config = None
    pipeline.secrets.reset_mode()


@pytest.fixture()
def fernet_key() -> bytes:
    """Return a freshly generated Fernet key for test use."""
    return generate_key()


@pytest.fixture()
def docker_mode():
    """Set execution mode to 'docker' for the duration of a test.

    Most tests run in docker mode (MinIO/local S3).  The autouse
    _isolate_pipeline_env fixture resets the mode to None before and
    after each test, so this fixture is only needed for tests that
    exercise code that calls get_mode() / is_staging() / resolve_storage().
    """
    pipeline.secrets.set_mode("docker")
    yield
    pipeline.secrets.reset_mode()


@pytest.fixture()
def tmp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary data/ directory structure for pipeline tests.

    Also injects a ``StorageConfig`` that points at the temp directory
    so that all code under test resolves paths to ``tmp_path`` instead
    of the project's real ``data/`` directory.
    """
    data = tmp_path / "data"
    for subdir in [
        "raw/ibkr",
        "raw/trading212",
        "raw/xtb",
        "normalized/ibkr_snapshot",
        "normalized/ibkr_events",
        "normalized/trading212_snapshot",
        "normalized/trading212_events",
        "normalized/xtb_snapshot",
        "normalized/xtb_events",
        "normalized/consolidated_holdings",
        "analytics/portfolio_holdings",
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
def env_key(fernet_key: bytes, monkeypatch: pytest.MonkeyPatch):
    """Set ENCRYPTION_KEY env var for the duration of a test.

    Uses ``monkeypatch.setenv`` so the var is restored automatically at
    teardown (no manual ``os.environ`` mutation; A4/A7 F35). The autouse
    ``_isolate_pipeline_env`` fixture also deletes ``ENCRYPTION_KEY`` before
    each test, so the two-layer isolation is robust even if a test fails
    before teardown.
    """
    monkeypatch.setenv("ENCRYPTION_KEY", fernet_key.decode("utf-8"))
    yield
