"""End-to-end tests for the consolidate pipeline using fixture data.

Tests that normalized snapshots from multiple brokers can be extracted,
consolidated, and written to the consolidated_holdings Delta table.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from deltalake import DeltaTable, write_deltalake

from pipeline.crypto import decrypt_float, generate_key
from pipeline.normalized.consolidate import (
    CurrencyConverter,
    Holding,
    consolidate_holdings,
)
from pipeline.normalized.extract import extract_holdings
from pipeline.storage import LocalBackend, StorageConfig, get_storage, use_storage
from tests.fixtures.ibkr import ibkr_normalized_snapshot
from tests.fixtures.trading212 import t212_normalized_snapshot
from tests.fixtures.xtb import xtb_normalized_snapshot


@pytest.fixture(autouse=True)
def _setup_storage(tmp_path: Path) -> None:
    """Inject a tmp_path-based StorageConfig for all consolidate tests."""
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
        data_dir=data,
        raw_dir=data / "raw",
        normalized_dir=data / "normalized",
        analytics_dir=data / "analytics",
        secrets_dir=tmp_path / ".secrets",
        encryption_key_file=tmp_path / ".secrets" / "encryption.key",
        backend=LocalBackend(data),
    )
    use_storage(config)


class TestExtractHoldings:
    """Test extracting holdings from normalized snapshots."""

    def test_extract_ibkr_holdings(self, tmp_path: Path):
        fernet_key = generate_key()
        table = ibkr_normalized_snapshot(fernet_key=fernet_key)
        path = str((tmp_path / "data" / "normalized" / "ibkr_snapshot"))
        write_deltalake(path, table, mode="overwrite")

        holdings = extract_holdings("ibkr", path, fernet_key)
        assert len(holdings) >= 2
        assert all(isinstance(h, Holding) for h in holdings)
        assert any(h.ticker == "VWCE" for h in holdings)

    def test_extract_trading212_holdings(self, tmp_path: Path):
        fernet_key = generate_key()
        table = t212_normalized_snapshot(fernet_key=fernet_key)
        path = str((tmp_path / "data" / "normalized" / "trading212_snapshot"))
        write_deltalake(path, table, mode="overwrite")

        holdings = extract_holdings("trading212", path, fernet_key)
        assert len(holdings) >= 2

    def test_extract_xtb_holdings(self, tmp_path: Path):
        fernet_key = generate_key()
        table = xtb_normalized_snapshot(fernet_key=fernet_key)
        path = str((tmp_path / "data" / "normalized" / "xtb_snapshot"))
        write_deltalake(path, table, mode="overwrite")

        holdings = extract_holdings("xtb", path, fernet_key)
        assert len(holdings) >= 2


class TestConsolidateMultiBroker:
    """Test consolidation across multiple brokers."""

    def test_consolidate_multi_broker_holdings(self, tmp_path: Path):
        """Consolidate IBKR + T212 + XTB holdings into one table."""
        fernet_key = generate_key()
        config = get_storage()

        # Write normalized fixtures for each broker
        for broker, factory in [
            ("ibkr", ibkr_normalized_snapshot),
            ("trading212", t212_normalized_snapshot),
            ("xtb", xtb_normalized_snapshot),
        ]:
            table = factory(fernet_key=fernet_key)
            path = str(config.normalized_dir / f"{broker}_snapshot")
            write_deltalake(path, table, mode="overwrite")

        # Extract holdings from each broker
        all_holdings: list[Holding] = []
        for broker_name in ("ibkr", "trading212", "xtb"):
            snapshot_path = str(config.normalized_dir / f"{broker_name}_snapshot")
            holdings = extract_holdings(broker_name, snapshot_path, fernet_key)
            all_holdings.extend(holdings)

        assert len(all_holdings) >= 6  # at least 2 per broker

        # Consolidate with manual FX rates
        converter = CurrencyConverter(
            target_currency="EUR",
            manual_rates={"USD": 0.9, "GBP": 1.15, "PLN": 0.25},
        )
        result = consolidate_holdings(
            all_holdings,
            fernet_key,
            converter,
            table_path=str(config.normalized_dir / "consolidated_holdings"),
        )

        assert result.num_rows >= 6
        # Verify the output has the right schema
        from pipeline.normalized.models import consolidated_holdings_schema
        assert result.schema.equals(consolidated_holdings_schema)

    def test_consolidate_values_are_encrypted(self, tmp_path: Path):
        """Verify that consolidated values are Fernet-encrypted."""
        fernet_key = generate_key()
        config = get_storage()

        # Write a single broker's normalized data
        table = ibkr_normalized_snapshot(fernet_key=fernet_key)
        path = str(config.normalized_dir / "ibkr_snapshot")
        write_deltalake(path, table, mode="overwrite")

        holdings = extract_holdings("ibkr", path, fernet_key)
        converter = CurrencyConverter(target_currency="EUR", manual_rates={"USD": 0.9})

        result = consolidate_holdings(
            holdings,
            fernet_key,
            converter,
            table_path=str(config.normalized_dir / "consolidated_holdings"),
        )

        # Values should be binary (encrypted)
        values = result.column("value").to_pylist()
        assert all(isinstance(v, bytes) for v in values)

        # Values should be decryptable
        decrypted = [decrypt_float(v, fernet_key) for v in values]
        assert all(v > 0 for v in decrypted)