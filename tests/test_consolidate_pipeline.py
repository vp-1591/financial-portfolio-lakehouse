"""End-to-end tests for the consolidate pipeline using fixture data.

Tests that normalized snapshots from multiple brokers can be extracted,
consolidated, and written to the consolidated_holdings Delta table.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from deltalake import write_deltalake

from pipeline.crypto import decrypt_float, generate_key
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
    """Inject a tmp_path-based StorageConfig for all consolidate tests."""
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


def assert_golden_equal(
    result,
    expected_rows: list[dict],
    fernet_key: bytes,
    float_cols: list[str],
    sort_keys: list[str],
) -> None:
    """Compare a ``consolidate_holdings`` result against hand-verified rows.

    Verifies the schema explicitly against ``consolidated_holdings_schema``,
    decrypts the encrypted monetary columns into plaintext floats, sorts
    both *result* and *expected* by *sort_keys* for order-independent
    comparison, then compares per-column: ``pytest.approx`` for monetary/FX
    floats and exact equality for strings/enums. The ``fetched_at`` timestamp
    is runtime-generated, so it is checked for type/consistency rather than
    matched against a literal value.
    """
    assert result.schema.equals(consolidated_holdings_schema)
    assert result.num_rows == len(expected_rows)

    # Decrypt encrypted monetary columns into plaintext floats.
    decrypted: dict[str, list[float]] = {}
    for col in float_cols:
        decrypted[col] = [
            decrypt_float(v, fernet_key) for v in result.column(col).to_pylist()
        ]

    column_names = result.schema.names
    result_rows: list[dict] = []
    for i in range(result.num_rows):
        row: dict = {}
        for col in column_names:
            if col in decrypted:
                row[col] = decrypted[col][i]
            else:
                row[col] = result.column(col)[i].as_py()
        result_rows.append(row)

    def _sort_key(row: dict) -> tuple:
        return tuple(row[k] for k in sort_keys)

    result_rows.sort(key=_sort_key)
    expected_rows.sort(key=_sort_key)

    # Schema check: every business column (excluding the runtime fetched_at)
    # must be present in both result and expected.
    business_cols = [c for c in column_names if c != "fetched_at"]
    assert set(business_cols) == set(expected_rows[0].keys()), (
        f"Column mismatch: result={set(business_cols)} "
        f"expected={set(expected_rows[0].keys())}"
    )

    for col in expected_rows[0]:
        for i in range(len(result_rows)):
            got = result_rows[i][col]
            exp = expected_rows[i][col]
            if col in float_cols:
                assert got == pytest.approx(exp, rel=1e-6), (
                    f"row {i} column {col}: got {got} expected {exp}"
                )
            else:
                assert got == exp, f"row {i} column {col}: got {got!r} expected {exp!r}"

    # fetched_at is runtime-generated; verify it is a datetime and consistent
    # across all rows (the writer stamps every row with the same `now`).
    fetched_ats = [result_rows[i]["fetched_at"] for i in range(result.num_rows)]
    assert all(isinstance(v, datetime) for v in fetched_ats), (
        f"fetched_at should be datetime, got {[type(v).__name__ for v in fetched_ats]}"
    )
    assert len(set(fetched_ats)) == 1, "fetched_at should be identical across rows"


class TestExtractHoldings:
    """Test extracting holdings from normalized snapshots."""

    def test_extract_ibkr_holdings(self, tmp_path: Path):
        fernet_key = generate_key()
        table = ibkr_normalized_snapshot(fernet_key=fernet_key)
        path = str(tmp_path / "data" / "normalized" / "ibkr_snapshot")
        write_deltalake(path, table, mode="overwrite")

        holdings = extract_holdings("ibkr", path, fernet_key)
        assert len(holdings) >= 2
        assert all(isinstance(h, Holding) for h in holdings)
        assert any(h.ticker == "VWCE" for h in holdings)

        # H2: decrypted security_value must match the pre-encryption input
        # values from the IBKR fixture (not just be positive). A decrypt
        # corruption (decrypt_float -> 1.0) makes these assertions fail.
        expected_values = {
            "VWCE": pytest.approx(5000.0),
            "AAPL": pytest.approx(3000.0),  # USD native (3000, not 2700)
            "SPY 20251219 C400": pytest.approx(500.0),
            "BOND01": pytest.approx(2500.0),
            "CASH EUR": pytest.approx(2000.0),
        }
        by_ticker = {h.ticker: h.value for h in holdings}
        for ticker, expected in expected_values.items():
            assert ticker in by_ticker, f"missing ticker {ticker!r} in {by_ticker}"
            assert by_ticker[ticker] == expected, (
                f"{ticker}: decrypted value {by_ticker[ticker]} != {expected}"
            )

    def test_extract_trading212_holdings(self, tmp_path: Path):
        fernet_key = generate_key()
        table = t212_normalized_snapshot(fernet_key=fernet_key)
        path = str(tmp_path / "data" / "normalized" / "trading212_snapshot")
        write_deltalake(path, table, mode="overwrite")

        holdings = extract_holdings("trading212", path, fernet_key)
        assert len(holdings) >= 2

        # H2: decrypted security_value must match the pre-encryption T212
        # fixture values (instrument-native EUR/USD for equities, wallet PLN
        # for CASH). A decrypt corruption (decrypt_float -> 1.0) makes these
        # assertions fail.
        expected_values = {
            "VWCEl_EQ": pytest.approx(2500.0),
            "AAPLu_EQ": pytest.approx(1800.0),
            "CASH PLN": pytest.approx(1500.0),
        }
        by_ticker = {h.ticker: h.value for h in holdings}
        for ticker, expected in expected_values.items():
            assert ticker in by_ticker, f"missing ticker {ticker!r} in {by_ticker}"
            assert by_ticker[ticker] == expected, (
                f"{ticker}: decrypted value {by_ticker[ticker]} != {expected}"
            )

    def test_extract_xtb_holdings(self, tmp_path: Path):
        fernet_key = generate_key()
        table = xtb_normalized_snapshot(fernet_key=fernet_key)
        path = str(tmp_path / "data" / "normalized" / "xtb_snapshot")
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
            path = config.normalized_path(f"{broker}_snapshot")
            write_deltalake(path, table, mode="overwrite")

        # Extract holdings from each broker
        all_holdings: list[Holding] = []
        for broker_name in ("ibkr", "trading212", "xtb"):
            snapshot_path = config.normalized_path(f"{broker_name}_snapshot")
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
            table_path=config.normalized_path("consolidated_holdings"),
        )

        assert result.num_rows >= 6
        # Verify the output has the right schema
        assert result.schema.equals(consolidated_holdings_schema)

        # The target_ccy column must match the converter's target currency,
        # since all values have been converted to that currency.
        currency_col = result.column("target_ccy").to_pylist()
        assert all(c == "EUR" for c in currency_col), (
            f"target_ccy column should all be EUR (target), got: {currency_col}"
        )

        # H1: verify FX-converted target_value against hand-computed expected
        # amounts for a multi-currency subset (IBKR EUR/USD + T212
        # instrument-native EUR/USD and wallet PLN for CASH). A skip-FX
        # mutation (converted_value = holding.value) makes these fail.
        tickers = result.column("ticker").to_pylist()
        brokers = result.column("broker").to_pylist()
        target_values = [
            decrypt_float(v, fernet_key)
            for v in result.column("target_value").to_pylist()
        ]
        security_values = [
            decrypt_float(v, fernet_key)
            for v in result.column("security_value").to_pylist()
        ]
        by_key = {
            (tickers[i], brokers[i]): (
                security_values[i],
                target_values[i],
            )
            for i in range(result.num_rows)
        }
        # (security_value native, target_value FX-converted to EUR)
        expected_converted = {
            # IBKR native EUR -> EUR (rate 1.0)
            ("VWCE", "IBKR"): (pytest.approx(5000.0), pytest.approx(5000.0)),
            # IBKR native USD -> EUR at 0.9
            ("AAPL", "IBKR"): (pytest.approx(3000.0), pytest.approx(2700.0)),
            ("SPY 20251219 C400", "IBKR"): (pytest.approx(500.0), pytest.approx(450.0)),
            ("BOND01", "IBKR"): (pytest.approx(2500.0), pytest.approx(2500.0)),
            ("CASH EUR", "IBKR"): (pytest.approx(2000.0), pytest.approx(2000.0)),
            # T212 instrument-native: VWCEl_EQ EUR->EUR (rate 1.0)
            ("VWCEl_EQ", "Trading 212"): (
                pytest.approx(2500.0),
                pytest.approx(2500.0),
            ),
            # T212 AAPLu_EQ USD->EUR at 0.9
            ("AAPLu_EQ", "Trading 212"): (
                pytest.approx(1800.0),
                pytest.approx(1620.0),
            ),
            # T212 CASH PLN->EUR at 0.25 (wallet currency, unchanged)
            ("CASH PLN", "Trading 212"): (
                pytest.approx(1500.0),
                pytest.approx(375.0),
            ),
        }
        for key, (exp_sec, exp_target) in expected_converted.items():
            assert key in by_key, f"missing {key!r} in consolidated output"
            sec, target = by_key[key]
            assert sec == exp_sec, f"{key}: security_value {sec} != {exp_sec}"
            assert target == exp_target, f"{key}: target_value {target} != {exp_target}"

    def test_consolidate_values_are_encrypted(self, tmp_path: Path):
        """Verify that consolidated values are Fernet-encrypted and that the
        FX-converted amounts match hand-computed expected numbers.
        """
        fernet_key = generate_key()
        config = get_storage()

        # Write a single broker's normalized data
        table = ibkr_normalized_snapshot(fernet_key=fernet_key)
        path = config.normalized_path("ibkr_snapshot")
        write_deltalake(path, table, mode="overwrite")

        holdings = extract_holdings("ibkr", path, fernet_key)
        converter = CurrencyConverter(target_currency="EUR", manual_rates={"USD": 0.9})

        result = consolidate_holdings(
            holdings,
            fernet_key,
            converter,
            table_path=config.normalized_path("consolidated_holdings"),
        )

        # Values should be binary (encrypted)
        values = result.column("target_value").to_pylist()
        assert all(isinstance(v, bytes) for v in values)

        # target_ccy must be the converter's target currency for every row.
        target_ccys = result.column("target_ccy").to_pylist()
        assert all(c == "EUR" for c in target_ccys), (
            f"target_ccy should all be EUR, got: {target_ccys}"
        )

        # H1 + H2: decrypt security_value and target_value and assert the
        # exact FX-converted amounts (not just positivity). security_value
        # must match the pre-encryption native input; target_value must match
        # native * FX rate. A skip-FX mutation or decrypt_float -> 1.0 makes
        # these assertions fail.
        tickers = result.column("ticker").to_pylist()
        security_values = [
            decrypt_float(v, fernet_key)
            for v in result.column("security_value").to_pylist()
        ]
        target_values = [
            decrypt_float(v, fernet_key)
            for v in result.column("target_value").to_pylist()
        ]
        by_ticker = {
            tickers[i]: (security_values[i], target_values[i])
            for i in range(result.num_rows)
        }
        # (native security_value, target_value in EUR)
        expected = {
            "VWCE": (pytest.approx(5000.0), pytest.approx(5000.0)),  # EUR->EUR
            "AAPL": (pytest.approx(3000.0), pytest.approx(2700.0)),  # USD * 0.9
            "SPY 20251219 C400": (pytest.approx(500.0), pytest.approx(450.0)),
            "BOND01": (pytest.approx(2500.0), pytest.approx(2500.0)),  # EUR->EUR
            "CASH EUR": (pytest.approx(2000.0), pytest.approx(2000.0)),  # EUR->EUR
        }
        for ticker, (exp_sec, exp_target) in expected.items():
            assert ticker in by_ticker, f"missing {ticker!r} in {by_ticker}"
            sec, target = by_ticker[ticker]
            assert sec == exp_sec, f"{ticker}: security_value {sec} != {exp_sec}"
            assert target == exp_target, (
                f"{ticker}: target_value {target} != {exp_target}"
            )


class TestConsolidateHoldingsGolden:
    """Golden safety-net test at the ``consolidate_holdings`` boundary.

    The targeted asserts above pin the specific mutations the auditor tried.
    This golden test is a full-output comparison that catches drift the
    targeted asserts never name (an un-asserted column, a mutation nobody
    thought to try). Expected values are hand-verified math (NOT a snapshot
    of ``consolidate_holdings`` output), derived from the F1/F2 fixture
    values and known manual FX rates.
    """

    def test_consolidate_holdings_golden(self, tmp_path: Path):
        fernet_key = generate_key()
        config = get_storage()

        # Construct Holdings directly (the input boundary to
        # consolidate_holdings) spanning EUR (target), USD, and PLN so FX
        # conversion is exercised in the net, not just targeted asserts.
        holdings = [
            Holding(
                broker="IBKR",
                ticker="AAPL",
                currency="USD",
                value=3000.0,
                identifier="ISIN:US0378331005",
                security_currency="USD",
                description="Apple Inc",
                position_type="EQUITY",
            ),
            Holding(
                broker="IBKR",
                ticker="CASH EUR",
                currency="EUR",
                value=2000.0,
                security_currency="EUR",
                description="Cash EUR",
                position_type="CASH",
            ),
            Holding(
                broker="Trading 212",
                ticker="CASH PLN",
                currency="PLN",
                value=1500.0,
                security_currency="PLN",
                description="Cash PLN",
                position_type="CASH",
            ),
            Holding(
                broker="Trading 212",
                ticker="VWCEl_EQ",
                currency="EUR",
                value=2500.0,
                identifier="ISIN:IE00BK5BQT80",
                security_currency="EUR",
                description="Vanguard FTSE All-World UCITS ETF",
                position_type="EQUITY",
            ),
        ]
        converter = CurrencyConverter(
            target_currency="EUR",
            manual_rates={"USD": 0.9, "PLN": 0.25},
        )
        result = consolidate_holdings(
            holdings,
            fernet_key,
            converter,
            table_path=config.normalized_path("consolidated_holdings"),
        )

        # Hand-verified expected rows: security_value is the native input,
        # target_value is native * FX rate (USD*0.9, PLN*0.25, EUR*1.0).
        expected = [
            {
                "broker": "IBKR",
                "ticker": "AAPL",
                "security_value": 3000.0,
                "security_ccy": "USD",
                "target_value": 2700.0,
                "target_ccy": "EUR",
                "identifier": "ISIN:US0378331005",
                "description": "Apple Inc",
                "position_type": "EQUITY",
            },
            {
                "broker": "IBKR",
                "ticker": "CASH EUR",
                "security_value": 2000.0,
                "security_ccy": "EUR",
                "target_value": 2000.0,
                "target_ccy": "EUR",
                "identifier": "-",
                "description": "Cash EUR",
                "position_type": "CASH",
            },
            {
                "broker": "Trading 212",
                "ticker": "CASH PLN",
                "security_value": 1500.0,
                "security_ccy": "PLN",
                "target_value": 375.0,
                "target_ccy": "EUR",
                "identifier": "-",
                "description": "Cash PLN",
                "position_type": "CASH",
            },
            {
                "broker": "Trading 212",
                "ticker": "VWCEl_EQ",
                "security_value": 2500.0,
                "security_ccy": "EUR",
                "target_value": 2500.0,
                "target_ccy": "EUR",
                "identifier": "ISIN:IE00BK5BQT80",
                "description": "Vanguard FTSE All-World UCITS ETF",
                "position_type": "EQUITY",
            },
        ]

        assert_golden_equal(
            result,
            expected,
            fernet_key=fernet_key,
            float_cols=["security_value", "target_value"],
            sort_keys=["ticker", "broker"],
        )
