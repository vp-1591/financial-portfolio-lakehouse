"""End-to-end tests for the allocate pipeline using fixture data.

Tests that consolidated holdings can be allocated into portfolio
percentages and written to the analytics Delta table.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from deltalake import write_deltalake

from pipeline.analytics.allocation import allocate_percentages
from pipeline.crypto import decrypt_float, generate_key
from pipeline.normalized.consolidate import CurrencyConverter, Holding, consolidate_holdings
from pipeline.normalized.models import consolidated_holdings_schema
from pipeline.storage import LocalBackend, StorageConfig, get_storage, use_storage
from tests.fixtures.ibkr import ibkr_normalized_snapshot
from tests.fixtures.trading212 import t212_normalized_snapshot
from tests.fixtures.xtb import xtb_normalized_snapshot


def _write_consolidated_holdings(
    tmp_path: Path,
    fernet_key: bytes,
) -> str:
    """Write a consolidated holdings Delta table from all broker fixtures.

    Returns the path to the written table.
    """
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

    # Extract and consolidate holdings
    from pipeline.normalized.extract import extract_holdings
    all_holdings: list[Holding] = []
    for broker_name in ("ibkr", "trading212", "xtb"):
        snapshot_path = str(config.normalized_dir / f"{broker_name}_snapshot")
        holdings = extract_holdings(broker_name, snapshot_path, fernet_key)
        all_holdings.extend(holdings)

    converter = CurrencyConverter(
        target_currency="EUR",
        manual_rates={"USD": 0.9, "GBP": 1.15, "PLN": 0.25},
    )
    consolidate_holdings(
        all_holdings,
        fernet_key,
        converter,
        table_path=str(config.normalized_dir / "consolidated_holdings"),
    )
    return str(config.normalized_dir / "consolidated_holdings")


@pytest.fixture(autouse=True)
def _setup_storage(tmp_path: Path) -> None:
    """Inject a tmp_path-based StorageConfig for all allocate tests."""
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


class TestAllocatePipeline:
    """Test the full allocate pipeline from consolidated holdings."""

    def test_allocate_produces_allocation_table(self, tmp_path: Path):
        """allocate_percentages returns a table with correct schema."""
        fernet_key = generate_key()
        table_path = _write_consolidated_holdings(tmp_path, fernet_key)
        config = get_storage()

        result = allocate_percentages(
            table_path=table_path,
            fernet_key=fernet_key,
            analytics_path=str(config.analytics_dir / "portfolio_allocation"),
        )

        from pipeline.analytics.models import portfolio_allocation_schema
        assert result.schema.equals(portfolio_allocation_schema)
        assert result.num_rows >= 3  # at least one row per broker

    def test_allocate_percentages_sum_to_100(self, tmp_path: Path):
        """Percentages should sum to approximately 100%."""
        fernet_key = generate_key()
        table_path = _write_consolidated_holdings(tmp_path, fernet_key)
        config = get_storage()

        result = allocate_percentages(
            table_path=table_path,
            fernet_key=fernet_key,
            analytics_path=str(config.analytics_dir / "portfolio_allocation"),
        )

        percentages = result.column("percentage").to_pylist()
        total = sum(percentages)
        assert abs(total - 100.0) < 0.5, f"Percentages sum to {total}, expected ~100"

    def test_allocate_tickers_are_present(self, tmp_path: Path):
        """All tickers from the fixture data should appear in the output."""
        fernet_key = generate_key()
        table_path = _write_consolidated_holdings(tmp_path, fernet_key)
        config = get_storage()

        result = allocate_percentages(
            table_path=table_path,
            fernet_key=fernet_key,
            analytics_path=str(config.analytics_dir / "portfolio_allocation"),
        )

        tickers = result.column("ticker").to_pylist()
        # IBKR fixture has VWCE and AAPL
        assert "VWCE" in tickers
        # XTB fixture has VWCE.DE (may be normalized)
        # At minimum, we expect multiple distinct tickers
        assert len(set(tickers)) >= 3

    def test_allocate_writes_delta_table(self, tmp_path: Path):
        """The allocation result should be written to a Delta table."""
        fernet_key = generate_key()
        table_path = _write_consolidated_holdings(tmp_path, fernet_key)
        config = get_storage()
        analytics_path = str(config.analytics_dir / "portfolio_allocation")

        allocate_percentages(
            table_path=table_path,
            fernet_key=fernet_key,
            analytics_path=analytics_path,
        )

        from deltalake import DeltaTable
        dt = DeltaTable(analytics_path)
        read_back = dt.to_pyarrow_table()
        assert read_back.num_rows >= 3