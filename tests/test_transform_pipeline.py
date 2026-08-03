"""End-to-end tests for the transform pipeline using fixture data.

Tests that raw Delta tables can be transformed into normalized Delta tables
for each broker connector.
"""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from pipeline.connectors.registry import get
from pipeline.crypto import decrypt_float, generate_key
from pipeline.storage import StorageConfig, use_storage
from tests.fixtures.ibkr import ibkr_normalized_snapshot, ibkr_raw_positions
from tests.fixtures.trading212 import t212_normalized_snapshot, t212_raw_snapshot
from tests.fixtures.xtb import xtb_normalized_snapshot, xtb_raw_snapshot
from tests.local_backend import LocalBackend


@pytest.fixture(autouse=True)
def _setup_storage(tmp_path: Path) -> None:
    """Inject a tmp_path-based StorageConfig for all transform tests."""
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

    def test_transform_snapshot_security_values_match_known_amounts(self):
        """Decrypt security_value and assert known expected amounts (A2 H4/W6).

        A ``security_value = 1.0`` mutation in ``ibkr/transform.py:144`` or a
        ``decrypt_float → 1.0`` global mutation must fail these assertions.
        """
        fernet_key = generate_key()
        raw_table = ibkr_raw_positions(fernet_key=fernet_key)
        connector = get("ibkr")
        result = connector.transform_snapshot(raw_table, fernet_key)

        labels = result.column("label").to_pylist()
        values = [
            decrypt_float(v, fernet_key)
            for v in result.column("security_value").to_pylist()
        ]
        by_label = dict(zip(labels, values))

        # Values are in native (security) currency — no FX pre-conversion.
        assert by_label["VWCE"] == pytest.approx(5000.0)
        assert by_label["AAPL"] == pytest.approx(3000.0)
        assert by_label["SPY 20251219 C400"] == pytest.approx(500.0)
        # BOND01: positionValue=0 but position=100 * markPrice=25 → 2500.0
        # (the zero-positionValue fallback reads ``position``, the real Flex key).
        assert by_label["BOND01"] == pytest.approx(2500.0)
        assert by_label["CASH EUR"] == pytest.approx(2000.0)


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

    def test_transform_snapshot_security_values_match_known_amounts(self):
        """Decrypt security_value and assert known expected amounts (A2 H4/W6).

        A ``security_value = 1.0`` mutation or a ``decrypt_float → 1.0`` global
        mutation must fail these assertions.
        """
        fernet_key = generate_key()
        raw_table = t212_raw_snapshot(fernet_key=fernet_key)
        connector = get("trading212")
        result = connector.transform_snapshot(raw_table, fernet_key)

        labels = result.column("label").to_pylist()
        values = [
            decrypt_float(v, fernet_key)
            for v in result.column("security_value").to_pylist()
        ]
        by_label = dict(zip(labels, values))

        # walletImpact.currentValue for equities; cash.availableToTrade for CASH.
        assert by_label["VWCEl_EQ"] == pytest.approx(2500.0)
        assert by_label["AAPLu_EQ"] == pytest.approx(1800.0)
        assert by_label["CASH PLN"] == pytest.approx(1500.0)


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

    @pytest.mark.parametrize(
        "broker,snapshot_factory",
        [
            ("ibkr", ibkr_normalized_snapshot),
            ("trading212", t212_normalized_snapshot),
            ("xtb", xtb_normalized_snapshot),
        ],
    )
    def test_write_and_read_normalized_snapshot(
        self, broker, snapshot_factory, tmp_path: Path
    ):
        fernet_key = generate_key()
        table = snapshot_factory(fernet_key=fernet_key)
        path = str(tmp_path / "data" / "normalized" / f"{broker}_snapshot")
        write_deltalake(path, table, mode="overwrite")

        from deltalake import DeltaTable

        dt = DeltaTable(path)
        read_back = dt.to_pyarrow_table()
        assert read_back.num_rows == table.num_rows


# ---------------------------------------------------------------------------
# Golden safety net — transform_snapshot boundary (F4)
# ---------------------------------------------------------------------------


def assert_golden_equal(
    result: pa.Table,
    expected: pa.Table,
    fernet_key: bytes,
    float_expected: dict[str, dict[tuple[str, str], float]],
    float_cols: list[str],
    str_cols: list[str],
) -> None:
    """Full-output comparison at the transform_snapshot boundary.

    Compares decrypted plaintext (never Fernet tokens), sorted by a stable key
    (account_id + label) before ``to_dict``, with per-column equality:
    ``pytest.approx(rel_tol=1e-6)`` for monetary floats, exact for strings.
    Schema is compared explicitly so a column addition reports cleanly.

    Expected float values are passed as **hand-verified plaintext constants**
    (``float_expected``, keyed by ``(account_id, label)``), NOT decrypted from
    the ``expected`` table. This is the critical anti-rubber-stamp property: a
    ``decrypt_float → 1.0`` global mutation makes the result side decrypt to
    ``1.0`` while ``float_expected`` stays at the real amounts, so the golden
    test fails. If ``expected`` floats were decrypted with the same broken
    helper, both sides would become ``1.0`` and the bug would pass silently.

    The ``expected`` table is still used for schema + string-column comparison
    (string columns are not Fernet-encrypted, so a broken ``decrypt_float``
    cannot mask string drift). It is sourced from the F1/F2 round-trip-verified
    fixtures (diffed against real demo bronze), NOT from re-running the SUT — so
    a transform bug cannot be baked into the golden values.
    """
    assert result.schema.equals(expected.schema), (
        f"Schema mismatch:\nresult={result.schema}\nexpected={expected.schema}"
    )
    assert result.num_rows == expected.num_rows

    # Sort both by a stable key (account_id + label) before comparison.
    sort_keys = [("account_id", "ascending"), ("label", "ascending")]
    result_sorted = result.sort_by(sort_keys)
    expected_sorted = expected.sort_by(sort_keys)

    # String columns: exact match between result and expected (both plaintext).
    for col in str_cols:
        actual_vals = result_sorted.column(col).to_pylist()
        expected_vals = expected_sorted.column(col).to_pylist()
        assert actual_vals == expected_vals, (
            f"Column {col} mismatch: {actual_vals} != {expected_vals}"
        )

    # Float columns: decrypt result, compare against KNOWN PLAINTEXT constants.
    # A ``decrypt_float → 1.0`` mutation breaks the result side only → fail.
    result_keys = list(
        zip(
            result_sorted.column("account_id").to_pylist(),
            result_sorted.column("label").to_pylist(),
        )
    )
    for col in float_cols:
        actual_vals = [
            decrypt_float(v, fernet_key) for v in result_sorted.column(col).to_pylist()
        ]
        for (acct_id, label), actual in zip(result_keys, actual_vals):
            exp = float_expected[col][(acct_id, label)]
            assert actual == pytest.approx(exp, rel=1e-6), (
                f"Column {col} mismatch for ({acct_id!r}, {label!r}): {actual} != {exp}"
            )


class TestTransformSnapshotGolden:
    """Golden safety net at the transform_snapshot boundary (IBKR + T212).

    The targeted ``pytest.approx`` asserts in TestIBKRTransform/TestT212Transform
    are tripwires pinning specific mutations. This golden test is a complementary
    full-output comparison that catches drift the targeted asserts never name
    (an un-asserted column, a mutation nobody thought to try). Expected values
    come from the F1/F2 round-trip-verified fixtures (diffed against real demo
    bronze), NOT from snapshotting ``transform_snapshot`` output.
    """

    _IBKR_STR_COLS: ClassVar[list[str]] = [
        "account_id",
        "position_type",
        "label",
        "asset_class",
        "security_ccy",
        "isin",
        "description",
    ]
    _T212_STR_COLS: ClassVar[list[str]] = [
        "account_id",
        "position_type",
        "label",
        "name",
        "asset_class",
        "security_ccy",
        "isin",
    ]
    _FLOAT_COLS: ClassVar[list[str]] = ["security_value"]

    # Hand-verified plaintext expected values, keyed by (account_id, label).
    # Sourced from the F1/F2 fixtures (round-trip-verified against real demo
    # bronze). NOT from running transform_snapshot, so a transform bug cannot
    # be baked in. NB: these are the VALUES the transform must produce, not a
    # decryption of the expected table — so a broken decrypt_float cannot make
    # both sides agree on the wrong number.
    _IBKR_FLOAT_EXPECTED: ClassVar[dict[str, dict[tuple[str, str], float]]] = {
        "security_value": {
            ("U123456", "AAPL"): 3000.0,
            ("U123456", "BOND01"): 2500.0,  # position * markPrice = 100 * 25
            ("U123456", "CASH EUR"): 2000.0,
            ("U123456", "SPY 20251219 C400"): 500.0,
            ("U123456", "VWCE"): 5000.0,
        }
    }
    _T212_FLOAT_EXPECTED: ClassVar[dict[str, dict[tuple[str, str], float]]] = {
        "security_value": {
            ("", "AAPLu_EQ"): 1800.0,  # walletImpact.currentValue
            ("", "CASH PLN"): 1500.0,  # cash.availableToTrade
            ("", "VWCEl_EQ"): 2500.0,  # walletImpact.currentValue
        }
    }

    def test_ibkr_transform_snapshot_golden(self) -> None:
        fernet_key = generate_key()
        raw = ibkr_raw_positions(fernet_key=fernet_key)
        result = get("ibkr").transform_snapshot(raw, fernet_key)
        expected = ibkr_normalized_snapshot(fernet_key=fernet_key)

        assert_golden_equal(
            result,
            expected,
            fernet_key,
            float_expected=self._IBKR_FLOAT_EXPECTED,
            float_cols=self._FLOAT_COLS,
            str_cols=self._IBKR_STR_COLS,
        )

    def test_t212_transform_snapshot_golden(self) -> None:
        fernet_key = generate_key()
        raw = t212_raw_snapshot(fernet_key=fernet_key)
        result = get("trading212").transform_snapshot(raw, fernet_key)
        expected = t212_normalized_snapshot(fernet_key=fernet_key)

        assert_golden_equal(
            result,
            expected,
            fernet_key,
            float_expected=self._T212_FLOAT_EXPECTED,
            float_cols=self._FLOAT_COLS,
            str_cols=self._T212_STR_COLS,
        )
