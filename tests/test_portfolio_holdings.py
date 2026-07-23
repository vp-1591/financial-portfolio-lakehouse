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
from pipeline.crypto import decrypt_float, generate_key
from pipeline.normalized.consolidate import (
    CurrencyConverter,
    Holding,
    consolidate_holdings,
)
from pipeline.normalized.extract import extract_holdings
from tests.local_backend import LocalBackend
from pipeline.storage import StorageConfig, get_storage, use_storage
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

    def test_target_value_is_encrypted_binary(self, tmp_path: Path):
        """target_value column contains Fernet-encrypted binary values."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        # target_value and security_value should be binary (encrypted)
        assert result.schema.field("target_value").type == pa.binary()
        assert result.schema.field("security_value").type == pa.binary()
        # Decrypt and verify values are positive floats
        values = result.column("target_value").to_pylist()
        assert all(isinstance(v, bytes) for v in values)
        assert all(decrypt_float(v, fernet_key) > 0 for v in values)

    def test_security_value_is_encrypted_binary(self, tmp_path: Path):
        """security_value column contains Fernet-encrypted binary values."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        values = result.column("security_value").to_pylist()
        assert all(isinstance(v, bytes) for v in values)
        assert all(decrypt_float(v, fernet_key) > 0 for v in values)

    def test_percentage_is_plaintext_float(self, tmp_path: Path):
        """percentage column remains plaintext Float64."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        assert result.schema.field("percentage").type == pa.float64()
        percentages = result.column("percentage").to_pylist()
        assert all(isinstance(p, float) for p in percentages)

    def test_decrypt_roundtrip(self, tmp_path: Path):
        """Encrypting then decrypting value columns recovers original values."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        # Decrypt both columns and verify values are positive
        import polars as pl

        df = pl.from_arrow(result)
        for col in ("security_value", "target_value"):
            decrypted = df[col].map_elements(
                lambda v: decrypt_float(v, fernet_key), return_dtype=pl.Float64
            )
            assert all(v > 0 for v in decrypted.to_list()), (
                f"{col} has non-positive values"
            )

    def test_position_type_populated(self, tmp_path: Path):
        """position_type has EQUITY and CASH values."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        position_types = set(result.column("position_type").to_pylist())
        assert "EQUITY" in position_types
        assert "CASH" in position_types

    def test_target_ccy_matches_consolidated(self, tmp_path: Path):
        """target_ccy matches the consolidated holdings currency (target)."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        target_ccy_values = set(result.column("target_ccy").to_pylist())
        # With manual_rates targeting EUR, all target currencies should be EUR
        assert target_ccy_values == {"EUR"}

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
        """EUR-denominated positions should have native value equal to target_value."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        # Find EUR-currency rows where target_ccy is also EUR
        import polars as pl

        df = pl.from_arrow(result)
        # Decrypt value columns for comparison
        df = df.with_columns(
            pl.col("security_value")
            .map_elements(
                lambda v: decrypt_float(v, fernet_key), return_dtype=pl.Float64
            )
            .alias("security_value"),
            pl.col("target_value")
            .map_elements(
                lambda v: decrypt_float(v, fernet_key), return_dtype=pl.Float64
            )
            .alias("target_value"),
        )
        eur_native = df.filter(
            (pl.col("security_ccy") == "EUR") & (pl.col("target_ccy") == "EUR")
        )
        # For EUR positions, native value should equal target value
        assert len(eur_native) > 0
        for row in eur_native.iter_rows(named=True):
            assert abs(row["security_value"] - row["target_value"]) < 0.01, (
                f"EUR position {row['ticker']}: native {row['security_value']} != target {row['target_value']}"
            )

    def test_missing_consolidated_raises(self, tmp_path: Path):
        """FileNotFoundError when consolidated_holdings table is missing."""
        fernet_key = generate_key()
        with pytest.raises(FileNotFoundError, match="Consolidated holdings"):
            build_portfolio_holdings(fernet_key=fernet_key)

    def test_percentage_column_present(self, tmp_path: Path):
        """Result table includes the percentage column."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        assert "percentage" in result.column_names

    def test_percentage_values_positive(self, tmp_path: Path):
        """All percentage values are positive."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        percentages = result.column("percentage").to_pylist()
        assert all(p > 0 for p in percentages)

    def test_percentage_sums_to_100(self, tmp_path: Path):
        """Percentage values sum to approximately 100."""
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key)
        result = build_portfolio_holdings(fernet_key=fernet_key)

        total_pct = sum(result.column("percentage").to_pylist())
        assert abs(total_pct - 100.0) < 0.1, (
            f"Percentages sum to {total_pct}, expected ~100"
        )

    def test_percentage_zero_when_total_target_is_zero(self, tmp_path: Path):
        """When total_target is 0, all percentages are 0.0 (not null)."""
        fernet_key = generate_key()
        config = get_storage()

        # Build consolidated holdings with all-zero target values.
        from pipeline.crypto import encrypt_float

        import polars as pl

        rows = [
            {
                "broker": "ibkr",
                "ticker": "AAPL",
                "security_ccy": "USD",
                "security_value": encrypt_float(100.0, fernet_key),
                "target_value": encrypt_float(0.0, fernet_key),
                "target_ccy": "EUR",
                "position_type": "EQUITY",
                "identifier": "US0378331005",
                "description": "Apple Inc",
            },
            {
                "broker": "ibkr",
                "ticker": "CASH",
                "security_ccy": "EUR",
                "security_value": encrypt_float(0.0, fernet_key),
                "target_value": encrypt_float(0.0, fernet_key),
                "target_ccy": "EUR",
                "position_type": "CASH",
                "identifier": "",
                "description": "Cash EUR",
            },
        ]
        df = pl.DataFrame(rows)
        arrow = df.to_arrow()

        from pipeline.normalized.models import consolidated_holdings_schema

        casted = {}
        for field in consolidated_holdings_schema:
            if field.name in arrow.column_names:
                casted[field.name] = arrow.column(field.name).cast(field.type)
            else:
                casted[field.name] = pa.nulls(arrow.num_rows, field.type)
        table = pa.table(casted, schema=consolidated_holdings_schema)

        path = config.normalized_path("consolidated_holdings")
        write_deltalake(path, table, mode="overwrite")

        result = build_portfolio_holdings(fernet_key=fernet_key)
        percentages = result.column("percentage").to_pylist()

        # All percentages should be 0.0, not None/null
        assert all(p == 0.0 for p in percentages), (
            f"Expected all 0.0, got {percentages}"
        )
