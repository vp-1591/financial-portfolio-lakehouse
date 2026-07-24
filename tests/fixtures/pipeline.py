"""End-to-end pipeline fixtures: raw -> normalized -> consolidated -> holdings.

Provides helpers that set up a complete pipeline environment with fixture
data, using a temporary directory and a test ``StorageConfig``.
"""

from __future__ import annotations

from pathlib import Path

from deltalake import write_deltalake

from pipeline.crypto import generate_key
from tests.local_backend import LocalBackend
from pipeline.storage import StorageConfig, use_storage


def setup_pipeline_env(
    tmp_path: Path,
    *,
    include_ibkr: bool = True,
    include_t212: bool = True,
    include_xtb: bool = True,
) -> tuple[StorageConfig, bytes]:
    """Set up a complete pipeline fixture environment.

    Creates a ``StorageConfig`` pointed at *tmp_path*, writes the Fernet
    encryption key, and writes normalized Delta tables for each enabled
    broker.  Returns ``(config, fernet_key)``.

    The caller is responsible for calling ``use_storage()`` before running
    pipeline code — this function does that automatically.
    """
    from tests.fixtures.ibkr import ibkr_normalized_snapshot
    from tests.fixtures.trading212 import t212_normalized_snapshot
    from tests.fixtures.xtb import xtb_normalized_snapshot

    fernet_key = generate_key()
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
        "analytics/portfolio_holdings",
    ]:
        (data / subdir).mkdir(parents=True, exist_ok=True)

    secrets = tmp_path / ".secrets"
    secrets.mkdir(parents=True, exist_ok=True)
    (secrets / "encryption.key").write_bytes(fernet_key)

    config = StorageConfig(
        data_dir=str(data),
        raw_dir=str(data / "raw"),
        normalized_dir=str(data / "normalized"),
        analytics_dir=str(data / "analytics"),
        secrets_dir=str(secrets),
        encryption_key_file=str(secrets / "encryption.key"),
        backend=LocalBackend(data),
    )
    use_storage(config)

    if include_ibkr:
        table = ibkr_normalized_snapshot(fernet_key=fernet_key)
        path = config.normalized_path("ibkr_snapshot")
        write_deltalake(path, table, mode="overwrite")

    if include_t212:
        table = t212_normalized_snapshot(fernet_key=fernet_key)
        path = config.normalized_path("trading212_snapshot")
        write_deltalake(path, table, mode="overwrite")

    if include_xtb:
        table = xtb_normalized_snapshot(fernet_key=fernet_key)
        path = config.normalized_path("xtb_snapshot")
        write_deltalake(path, table, mode="overwrite")

    return config, fernet_key


def write_normalized_table(
    data_dir: Path,
    broker: str,
    table,
) -> str:
    """Write a normalized Delta table for a given broker.

    Returns the path to the written table.
    """
    path = str(data_dir / "normalized" / f"{broker}_snapshot")
    write_deltalake(path, table, mode="overwrite")
    return path
