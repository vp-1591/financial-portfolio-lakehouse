"""Tests for the portfolio_holdings analytics gold table.

Verifies that build_portfolio_holdings correctly joins consolidated holdings
with per-broker snapshots to produce native-currency values, base-currency
values, and position types.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from pipeline.analytics.holdings import build_portfolio_holdings
from pipeline.analytics.models import portfolio_holdings_schema
from pipeline.crypto import generate_key
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
    """Inject a tmp_path-based StorageConfig for all holdings tests."""
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


def _build_consolidated_holdings(fernet_key: bytes) -> pa.Table:
    """Write broker snapshots and build consolidated_holdings from them.

    Returns the Delta table path for consolidated_holdings.
    """
    config = get_storage()

    # Write normalized fixtures for each broker
    for broker, factory in [
        ("ibkr", ibkr_normalized_snapshot),
        ("trading212", t212_normalized_snapshot),
        ("xtb", xtb_normalized_snapshot),
    ]:
        table = factory(fernet_key=fernet_key)
        path = config.normalized_path(f"{broker}_snapshot")
        write_deltalake(path, table, mode="overwrite")

    # Extract and consolidate
    all_holdings: list[Holding] = []
    for broker_name in ("ibkr", "trading212", "xtb"):
        snapshot_path = config.normalized_path(f"{broker_name}_snapshot")
        holdings = extract_holdings(broker_name, snapshot_path, fernet_key)
        all_holdings.extend(holdings)

    converter = CurrencyConverter(
        target_currency="EUR",
        manual_rates={"USD": 0.9, "GBP": 1.15, "PLN": 0.25},
    )
    result = consolidate_holdings(
        all_holdings,
        fernet_key,
        converter,
        table_path=config.normalized_path("consolidated_holdings"),
    )
    return result


class TestBuildPortfolioHoldings:
    """Tests for build_portfolio_holdings."""

    def test_schema_matches(self, tmp_path: Path):
        """Result table matches the portfolio_holdings_schema."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        assert result.schema.equals(portfolio_holdings_schema)

    def test_row_count_matches_consolidated(self, tmp_path: Path):
        """One row per consolidated holding."""
        fernet_key = generate_key()
        consolidated = _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        assert result.num_rows == consolidated.num_rows

    def test_value_base_is_decrypted_float(self, tmp_path: Path):
        """value_base column contains decrypted float values, not bytes."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        values = result.column("value_base").to_pylist()
        assert all(isinstance(v, float) for v in values)
        assert all(v > 0 for v in values)

    def test_position_type_populated(self, tmp_path: Path):
        """position_type has EQUITY and CASH values."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        position_types = set(result.column("position_type").to_pylist())
        assert "EQUITY" in position_types
        assert "CASH" in position_types

    def test_base_currency_matches_consolidated(self, tmp_path: Path):
        """base_currency matches the consolidated holdings currency (target)."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        base_currencies = set(result.column("base_currency").to_pylist())
        # With manual_rates targeting EUR, all base currencies should be EUR
        assert base_currencies == {"EUR"}

    def test_writes_delta_table(self, tmp_path: Path):
        """Portfolio holdings table is written to the analytics layer."""
        from deltalake import DeltaTable

        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        config = get_storage()
        dt = DeltaTable(config.analytics_path("portfolio_holdings"))
        stored = dt.to_pyarrow_table()
        assert stored.num_rows == result.num_rows

    def test_native_value_for_eur_positions(self, tmp_path: Path):
        """EUR-denominated positions should have native value equal to value_base."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        # Find EUR-currency rows where base is also EUR
        import polars as pl

        df = pl.from_arrow(result)
        eur_native = df.filter(
            (pl.col("value_currency") == "EUR") & (pl.col("base_currency") == "EUR")
        )
        # For EUR positions, native value should equal base value
        assert len(eur_native) > 0
        for row in eur_native.iter_rows(named=True):
            assert abs(row["value"] - row["value_base"]) < 0.01, (
                f"EUR position {row['ticker']}: native {row['value']} != base {row['value_base']}"
            )

    def test_missing_consolidated_raises(self, tmp_path: Path):
        """FileNotFoundError when consolidated_holdings table is missing."""
        fernet_key = generate_key()
        with pytest.raises(FileNotFoundError, match="Consolidated holdings"):
            build_portfolio_holdings(fernet_key=fernet_key)
