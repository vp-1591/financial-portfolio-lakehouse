"""End-to-end tests for the transform pipeline using fixture data.

Tests that raw Delta tables can be transformed into normalized Delta tables
for each broker connector.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from deltalake import write_deltalake

from pipeline.connectors.registry import get
from pipeline.crypto import generate_key
from pipeline.storage import LocalBackend, StorageConfig, use_storage
from tests.fixtures.ibkr import ibkr_raw_positions, ibkr_normalized_snapshot
from tests.fixtures.trading212 import t212_raw_snapshot, t212_normalized_snapshot
from tests.fixtures.xtb import xtb_raw_snapshot, xtb_normalized_snapshot


@pytest.fixture(autouse=True)
def _setup_storage(tmp_path: Path) -> None:
    """Inject a tmp_path-based StorageConfig for all transform tests."""
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


class TestIBKRTransform:
    """Test IBKR raw -> normalized transform with fixture data."""

    def test_transform_snapshot_produces_rows(self):
        fernet_key = generate_key()
        raw_table = ibkr_raw_positions(fernet_key=fernet_key)
        connector = get("ibkr")
        result = connector.transform_snapshot(raw_table, fernet_key)
        assert result.num_rows >= 2  # at least 1 equity + 1 cash

    def test_transform_snapshot_has_correct_schema(self):
        fernet_key = generate_key()
        raw_table = ibkr_raw_positions(fernet_key=fernet_key)
        connector = get("ibkr")
        result = connector.transform_snapshot(raw_table, fernet_key)
        from pipeline.normalized.models import ibkr_snapshot_normalized_schema
        assert result.schema.equals(ibkr_snapshot_normalized_schema)

    def test_transform_snapshot_contains_equity_and_cash(self):
        fernet_key = generate_key()
        raw_table = ibkr_raw_positions(fernet_key=fernet_key)
        connector = get("ibkr")
        result = connector.transform_snapshot(raw_table, fernet_key)
        types = result.column("position_type").to_pylist()
        assert "EQUITY" in types
        assert "CASH" in types


class TestT212Transform:
    """Test Trading 212 raw -> normalized transform with fixture data."""

    def test_transform_snapshot_produces_rows(self):
        fernet_key = generate_key()
        raw_table = t212_raw_snapshot(fernet_key=fernet_key)
        connector = get("trading212")
        result = connector.transform_snapshot(raw_table, fernet_key)
        assert result.num_rows >= 2

    def test_transform_snapshot_has_correct_schema(self):
        fernet_key = generate_key()
        raw_table = t212_raw_snapshot(fernet_key=fernet_key)
        connector = get("trading212")
        result = connector.transform_snapshot(raw_table, fernet_key)
        from pipeline.normalized.models import trading212_snapshot_normalized_schema
        assert result.schema.equals(trading212_snapshot_normalized_schema)


class TestXTBTransform:
    """Test XTB raw -> normalized transform with fixture data."""

    def test_transform_snapshot_produces_rows(self):
        fernet_key = generate_key()
        raw_table = xtb_raw_snapshot(fernet_key=fernet_key)
        connector = get("xtb")
        result = connector.transform_snapshot(raw_table, fernet_key)
        assert result.num_rows >= 2

    def test_transform_snapshot_has_correct_schema(self):
        fernet_key = generate_key()
        raw_table = xtb_raw_snapshot(fernet_key=fernet_key)
        connector = get("xtb")
        result = connector.transform_snapshot(raw_table, fernet_key)
        from pipeline.normalized.models import xtb_snapshot_normalized_schema
        assert result.schema.equals(xtb_snapshot_normalized_schema)


class TestNormalizedFixtureWrite:
    """Test that normalized fixture data can be written and read back."""

    @pytest.mark.parametrize("broker,snapshot_factory", [
        ("ibkr", ibkr_normalized_snapshot),
        ("trading212", t212_normalized_snapshot),
        ("xtb", xtb_normalized_snapshot),
    ])
    def test_write_and_read_normalized_snapshot(self, broker, snapshot_factory, tmp_path: Path):
        fernet_key = generate_key()
        table = snapshot_factory(fernet_key=fernet_key)
        path = str((tmp_path / "data" / "normalized" / f"{broker}_snapshot"))
        write_deltalake(path, table, mode="overwrite")

        from deltalake import DeltaTable
        dt = DeltaTable(path)
        read_back = dt.to_pyarrow_table()
        assert read_back.num_rows == table.num_rows