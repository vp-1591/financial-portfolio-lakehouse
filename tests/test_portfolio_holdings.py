"""Tests for the portfolio_holdings analytics gold table.

Verifies that build_portfolio_holdings correctly joins consolidated holdings
with per-broker snapshots to produce native-currency values, base-currency
values, and position types.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from pipeline.analytics.holdings import build_portfolio_holdings
from pipeline.analytics.models import portfolio_holdings_schema
from pipeline.crypto import decrypt_float, encrypt_float, generate_key
from pipeline.normalized.consolidate import (
    CurrencyConverter,
    Holding,
    consolidate_holdings,
)
from pipeline.normalized.extract import extract_holdings
from pipeline.normalized.models import consolidated_holdings_schema
from pipeline.storage import StorageConfig, get_storage, use_storage
from tests.fixtures.ibkr import ibkr_normalized_snapshot
from tests.fixtures.trading212 import t212_normalized_snapshot
from tests.fixtures.xtb import xtb_normalized_snapshot
from tests.local_backend import LocalBackend


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


def _build_consolidated_holdings(
    fernet_key: bytes,
    brokers: tuple[str, ...] = ("ibkr", "trading212", "xtb"),
) -> pa.Table:
    """Write broker snapshots and build consolidated_holdings from them.

    Returns the Delta table path for consolidated_holdings.

    The ``brokers`` parameter lets value-assertion tests exclude XTB (F10
    scope is IBKR + T212 only — XTB is deferred per F3).
    """
    config = get_storage()

    broker_factories = {
        "ibkr": ibkr_normalized_snapshot,
        "trading212": t212_normalized_snapshot,
        "xtb": xtb_normalized_snapshot,
    }

    # Write normalized fixtures for each requested broker
    for broker_name in brokers:
        table = broker_factories[broker_name](fernet_key=fernet_key)
        path = config.normalized_path(f"{broker_name}_snapshot")
        write_deltalake(path, table, mode="overwrite")

    # Extract and consolidate
    all_holdings: list[Holding] = []
    for broker_name in brokers:
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
        """EUR-denominated positions have known expected plaintext amounts.

        W3: assert against hand-verified expected amounts (from the IBKR
        fixture: VWCE=5000, BOND01=2500, CASH EUR=2000; and the T212 fixture:
        VWCEl_EQ=2500 once the snapshot uses the instrument currency EUR), not
        just cross-column equality of two decrypted columns (which passes even
        if ``decrypt_float`` returns a constant — the round1-monetary
        structural hole at line 207).
        """
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key, brokers=("ibkr", "trading212"))
        result = build_portfolio_holdings(fernet_key=fernet_key)

        import polars as pl

        df = pl.from_arrow(result)
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
        assert len(eur_native) > 0

        # Hand-verified expected plaintext amounts. IBKR rows (VWCE, BOND01,
        # CASH EUR) plus the T212 VWCEl_EQ equity whose snapshot now uses the
        # instrument trading currency (EUR), so security_value (native EUR) ==
        # target_value (EUR) and both must match the original fixture value —
        # not just each other.
        expected_eur = {
            "VWCE": 5000.0,
            "BOND01": 2500.0,
            "CASH EUR": 2000.0,
            "VWCEl_EQ": 2500.0,
        }
        for row in eur_native.iter_rows(named=True):
            ticker = row["ticker"]
            expected = expected_eur[ticker]
            assert row["security_value"] == pytest.approx(expected, rel=1e-6), (
                f"EUR position {ticker}: security_value {row['security_value']} != {expected}"
            )
            assert row["target_value"] == pytest.approx(expected, rel=1e-6), (
                f"EUR position {ticker}: target_value {row['target_value']} != {expected}"
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
        """Percentage values match hand-verified expected numbers.

        W3: assert ACTUAL percentage values (known expected numbers from
        the IBKR + T212 fixtures), not just ``> 0``.  Removing the
        percentage division in the portfolio math must fail this test.
        """
        fernet_key = generate_key()
        _build_consolidated_holdings(fernet_key, brokers=("ibkr", "trading212"))
        result = build_portfolio_holdings(fernet_key=fernet_key)

        # Build a {ticker: percentage} dict from the result
        tickers = result.column("ticker").to_pylist()
        percentages = result.column("percentage").to_pylist()
        actual = dict(zip(tickers, percentages, strict=True))

        # Hand-verified: total_target = 17145.0
        # (5000+2700+450+2500+2000 + 2500+1620+375) where the T212 equities now
        # use their instrument currency: VWCEl_EQ=2500 EUR (1:1) and
        # AAPLu_EQ=1800 USD -> 1620 EUR (x0.9). CASH PLN=1500 PLN -> 375 EUR.
        # percentage = round(target_value / total_target * 100, 4)
        expected_pct = {
            "VWCE": 29.1630,  # 5000/17145*100
            "AAPL": 15.7480,  # 2700/17145*100
            "SPY 20251219 C400": 2.6247,  # 450/17145*100
            "BOND01": 14.5815,  # 2500/17145*100
            "CASH EUR": 11.6652,  # 2000/17145*100
            "VWCEl_EQ": 14.5815,  # 2500/17145*100
            "AAPLu_EQ": 9.4488,  # 1620/17145*100
            "CASH PLN": 2.1872,  # 375/17145*100
        }
        for ticker, expected in expected_pct.items():
            assert ticker in actual, f"Missing ticker {ticker} in result"
            assert actual[ticker] == pytest.approx(expected, rel=1e-4), (
                f"{ticker}: percentage {actual[ticker]} != {expected}"
            )

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
        import polars as pl

        from pipeline.crypto import encrypt_float

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


# ---------------------------------------------------------------------------
# Golden safety net at the build_portfolio_holdings boundary
# ---------------------------------------------------------------------------


def _assert_golden_equal(
    result_rows: list[dict],
    expected_rows: list[dict],
    float_cols: list[str],
    sort_keys: list[str],
) -> None:
    """Compare decrypted result rows against hand-verified expected rows.

    Golden-test conventions:
    - Sort both sides by ``sort_keys`` before comparison.
    - Schema check: every expected column must be present in result.
    - Per-column: ``pytest.approx(rel=1e-6)`` for floats, exact for strings.
    - Encrypted columns are never compared here — caller passes decrypted
      plaintext only.

    Small duplication from sibling fixers F4/F5 is plan-compliant.
    """
    assert len(result_rows) == len(expected_rows), (
        f"Row count mismatch: result={len(result_rows)}, expected={len(expected_rows)}"
    )

    result_sorted = sorted(result_rows, key=lambda r: tuple(r[k] for k in sort_keys))
    expected_sorted = sorted(
        expected_rows, key=lambda r: tuple(r[k] for k in sort_keys)
    )

    for i, (result_row, expected_row) in enumerate(
        zip(result_sorted, expected_sorted, strict=True)
    ):
        # Schema check: every expected key must be present in result
        for col in expected_row:
            assert col in result_row, f"Row {i}: column {col!r} missing from result"

        for col, expected_val in expected_row.items():
            actual_val = result_row[col]
            if col in float_cols:
                assert actual_val == pytest.approx(expected_val, rel=1e-6), (
                    f"Row {i} column {col!r}: {actual_val} != {expected_val}"
                )
            else:
                assert actual_val == expected_val, (
                    f"Row {i} column {col!r}: {actual_val!r} != {expected_val!r}"
                )


class TestGoldenPortfolioHoldings:
    """Golden safety net at the ``build_portfolio_holdings`` boundary.

    Catches drift the per-cell tripwires never name (an un-asserted column,
    a ``decrypt_float → 1.0`` mutation that makes cross-column-only asserts
    silently pass).  Expected values are hand-verified plaintext constants
    sourced from the IBKR + T212 fixture values and FX rates — NOT from
    running the SUT.
    """

    def test_golden_build_portfolio_holdings(self, tmp_path: Path):
        """3-row golden comparison at the analytics boundary.

        Consolidated input (hand-crafted, 3 rows):
          - AAPL/IBKR:  security=3000 USD, target=2700 EUR (3000×0.9)
          - VWCE/IBKR:  security=5000 EUR, target=5000 EUR (1:1)
          - CASH PLN/T212: security=1500 PLN, target=375 EUR (1500×0.25)

        Expected output (decrypted plaintext, hand-verified math):
          total_target = 8075.0
          percentage = round(target / total_target * 100, 4)
        """
        fernet_key = generate_key()
        config = get_storage()
        now = datetime.now(UTC)

        # Write a 3-row consolidated_holdings table with encrypted values
        consolidated_rows = [
            {
                "fetched_at": now,
                "broker": "IBKR",
                "ticker": "AAPL",
                "security_value": encrypt_float(3000.0, fernet_key),
                "security_ccy": "USD",
                "target_value": encrypt_float(2700.0, fernet_key),
                "target_ccy": "EUR",
                "identifier": "ISIN:US0378331005",
                "description": "Apple Inc",
                "position_type": "EQUITY",
            },
            {
                "fetched_at": now,
                "broker": "IBKR",
                "ticker": "VWCE",
                "security_value": encrypt_float(5000.0, fernet_key),
                "security_ccy": "EUR",
                "target_value": encrypt_float(5000.0, fernet_key),
                "target_ccy": "EUR",
                "identifier": "ISIN:IE00BK5BQT80",
                "description": "Vanguard FTSE All-World UCITS ETF",
                "position_type": "EQUITY",
            },
            {
                "fetched_at": now,
                "broker": "Trading 212",
                "ticker": "CASH PLN",
                "security_value": encrypt_float(1500.0, fernet_key),
                "security_ccy": "PLN",
                "target_value": encrypt_float(375.0, fernet_key),
                "target_ccy": "EUR",
                "identifier": "-",
                "description": "Cash PLN",
                "position_type": "CASH",
            },
        ]

        from tests.test_quality import _rows_to_table

        cons_table = _rows_to_table(consolidated_rows, consolidated_holdings_schema)
        write_deltalake(
            config.normalized_path("consolidated_holdings"),
            cons_table,
            mode="overwrite",
        )

        result = build_portfolio_holdings(fernet_key=fernet_key)

        # Schema check
        assert result.schema.equals(portfolio_holdings_schema)

        # Decrypt value columns and build result rows for comparison
        import polars as pl

        df = pl.from_arrow(result)
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

        # Columns to compare (exclude calculated_at — varies per run)
        compare_cols = [
            "broker",
            "ticker",
            "security_ccy",
            "security_value",
            "target_value",
            "target_ccy",
            "percentage",
            "position_type",
            "identifier",
            "description",
        ]
        result_rows = df.select(compare_cols).to_dicts()

        # Hand-verified expected plaintext values.
        # total_target = 2700 + 5000 + 375 = 8075
        # AAPL: 2700/8075*100 = 33.4365
        # VWCE: 5000/8075*100 = 61.9195
        # CASH PLN: 375/8075*100 = 4.644
        expected_rows = [
            {
                "broker": "IBKR",
                "ticker": "AAPL",
                "security_ccy": "USD",
                "security_value": 3000.0,
                "target_value": 2700.0,
                "target_ccy": "EUR",
                "percentage": 33.4365,
                "position_type": "EQUITY",
                "identifier": "ISIN:US0378331005",
                "description": "Apple Inc",
            },
            {
                "broker": "IBKR",
                "ticker": "VWCE",
                "security_ccy": "EUR",
                "security_value": 5000.0,
                "target_value": 5000.0,
                "target_ccy": "EUR",
                "percentage": 61.9195,
                "position_type": "EQUITY",
                "identifier": "ISIN:IE00BK5BQT80",
                "description": "Vanguard FTSE All-World UCITS ETF",
            },
            {
                "broker": "Trading 212",
                "ticker": "CASH PLN",
                "security_ccy": "PLN",
                "security_value": 1500.0,
                "target_value": 375.0,
                "target_ccy": "EUR",
                "percentage": 4.644,
                "position_type": "CASH",
                "identifier": "-",
                "description": "Cash PLN",
            },
        ]

        _assert_golden_equal(
            result_rows,
            expected_rows,
            float_cols=["security_value", "target_value", "percentage"],
            sort_keys=["ticker", "broker"],
        )
