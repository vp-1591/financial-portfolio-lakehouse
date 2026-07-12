"""Tests for the data quality validation framework."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from pipeline.analytics.models import (
    data_quality_schema,
    portfolio_allocation_schema,
)
from pipeline.analytics.quality import (
    FAIL,
    PASS,
    WARN,
    check_freshness,
    check_reconciliation,
    check_required_nulls,
    check_row_count_stability,
    check_schema,
    run_validation,
)
from pipeline.crypto import encrypt_float, generate_key
from pipeline.normalized.models import (
    cdc_events_normalized_schema,
    consolidated_holdings_schema,
)
from pipeline.storage import LocalBackend, StorageConfig, get_storage, use_storage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _rows_to_table(
    rows: list[dict],
    schema: pa.Schema,
) -> pa.Table:
    """Convert a list of row dicts to a PyArrow Table with the given schema.

    PyArrow's ``pa.table`` does not accept row-oriented dicts with a schema
    directly — it needs column-oriented data.  This helper transposes rows
    into columns and applies the schema.
    """
    if not rows:
        return pa.table({f.name: [] for f in schema}, schema=schema)

    column_names = [f.name for f in schema]
    columns: dict[str, list] = {name: [] for name in column_names}
    for row in rows:
        for name in column_names:
            columns[name].append(row.get(name))

    return pa.table(columns, schema=schema)


def _make_holdings_table(
    fernet_key: bytes,
    rows: list[dict] | None = None,
) -> pa.Table:
    """Build a minimal consolidated_holdings table."""
    now = datetime.now(timezone.utc)
    if rows is None:
        rows = [
            {
                "fetched_at": now,
                "broker": "IBKR",
                "ticker": "VWCE",
                "currency": "EUR",
                "value": encrypt_float(5000.0, fernet_key),
                "identifier": "IE00BK5BQT80",
                "security_currency": "EUR",
                "description": "Vanguard FTSE All-World",
            }
        ]
    return _rows_to_table(rows, consolidated_holdings_schema)


def _make_cdc_table(
    fernet_key: bytes,
    rows: list[dict] | None = None,
) -> pa.Table:
    """Build a minimal cdc_events table."""
    now = datetime.now(timezone.utc)
    if rows is None:
        rows = [
            {
                "fetched_at": now,
                "broker": "IBKR",
                "account_id": "U123456",
                "event_id": "evt-1",
                "source": "flex",
                "event_type": "DIVIDEND",
                "raw_event_type": "Dividends",
                "event_datetime": "2026-03-01 00:00:00",
                "currency": "EUR",
                "cash_amount": encrypt_float(42.5, fernet_key),
                "settle_date": None,
                "ticker": "VWCE",
                "isin": "IE00BK5BQT80",
                "description": "Vanguard dividend",
                "quantity": None,
                "price": None,
                "side": None,
                "gross_amount": None,
                "fee_amount": None,
                "tax_amount": None,
                "net_amount": None,
                "base_currency": "EUR",
                "fx_rate_to_base": None,
                "amount_base": encrypt_float(42.5, fernet_key),
            }
        ]
    return _rows_to_table(rows, cdc_events_normalized_schema)


def _make_allocation_table() -> pa.Table:
    """Build a minimal portfolio_allocation table."""
    now = datetime.now(timezone.utc)
    return pa.table(
        {
            "calculated_at": [now],
            "ticker": ["VWCE"],
            "percentage": [100.0],
            "broker": ["IBKR"],
            "identifier": ["IE00BK5BQT80"],
            "security_currency": ["EUR"],
            "description": ["Vanguard FTSE All-World"],
        },
        schema=portfolio_allocation_schema,
    )


@pytest.fixture(autouse=True)
def _setup_storage(tmp_path: Path) -> None:
    """Inject a tmp_path-based StorageConfig for all quality tests."""
    data = tmp_path / "data"
    for subdir in [
        "normalized/consolidated_holdings",
        "normalized/cdc_events",
        "analytics/portfolio_allocation",
        "analytics/data_quality",
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


# ---------------------------------------------------------------------------
# Schema check tests
# ---------------------------------------------------------------------------


class TestCheckSchema:
    """Tests for check_schema."""

    def test_pass_on_valid_table(self) -> None:
        """Schema check passes when the table matches expected schema."""
        table = _make_allocation_table()
        result = check_schema(
            "portfolio_allocation", table, portfolio_allocation_schema
        )
        assert result.status == PASS

    def test_fail_on_missing_column(self) -> None:
        """Schema check fails when a required column is missing."""
        table = _make_allocation_table()
        # Drop the 'ticker' column
        table = table.drop_columns(["ticker"])
        result = check_schema(
            "portfolio_allocation", table, portfolio_allocation_schema
        )
        assert result.status == FAIL
        assert "ticker" in result.details

    def test_fail_on_extra_column(self) -> None:
        """Schema check fails when an extra column is present."""
        table = _make_allocation_table()
        # Add an extra column
        table = table.append_column(
            "extra", pa.array(["oops"] * table.num_rows, type=pa.string())
        )
        result = check_schema(
            "portfolio_allocation", table, portfolio_allocation_schema
        )
        assert result.status == FAIL
        assert "extra" in result.details

    def test_fail_on_type_mismatch(self) -> None:
        """Schema check fails when a column has a different type."""
        now = datetime.now(timezone.utc)
        # Use string instead of float for percentage
        wrong_table = pa.table(
            {
                "calculated_at": [now],
                "ticker": ["VWCE"],
                "percentage": ["not_a_float"],  # wrong type
                "broker": ["IBKR"],
                "identifier": ["IE00BK5BQT80"],
                "security_currency": ["EUR"],
                "description": ["Vanguard FTSE All-World"],
            },
            schema=pa.schema(
                [
                    pa.field("calculated_at", pa.timestamp("us", tz="UTC")),
                    pa.field("ticker", pa.string()),
                    pa.field("percentage", pa.string()),  # mismatch
                    pa.field("broker", pa.string()),
                    pa.field("identifier", pa.string()),
                    pa.field("security_currency", pa.string()),
                    pa.field("description", pa.string()),
                ]
            ),
        )
        result = check_schema(
            "portfolio_allocation", wrong_table, portfolio_allocation_schema
        )
        assert result.status == FAIL
        assert "percentage" in result.details


# ---------------------------------------------------------------------------
# Required nulls check tests
# ---------------------------------------------------------------------------


class TestCheckRequiredNulls:
    """Tests for check_required_nulls."""

    def test_pass_on_no_nulls(self) -> None:
        """Required nulls check passes when all required fields are non-null."""
        table = _make_allocation_table()
        result = check_required_nulls(
            "portfolio_allocation", table, portfolio_allocation_schema
        )
        assert result.status == PASS

    def test_fail_on_null_value(self) -> None:
        """Required nulls check fails when a required field has nulls."""
        fernet_key = generate_key()
        now = datetime.now(timezone.utc)
        # Create holdings with null in a required field (broker)
        # broker is in REQUIRED_FIELDS for consolidated_holdings
        rows = [
            {
                "fetched_at": now,
                "broker": None,  # null in required field
                "ticker": "VWCE",
                "currency": "EUR",
                "value": encrypt_float(5000.0, fernet_key),
                "identifier": "IE00BK5BQT80",
                "security_currency": "EUR",
                "description": "Vanguard FTSE All-World",
            }
        ]
        table = _rows_to_table(rows, consolidated_holdings_schema)
        result = check_required_nulls(
            "consolidated_holdings", table, consolidated_holdings_schema
        )
        assert result.status == FAIL
        assert "broker" in result.details


# ---------------------------------------------------------------------------
# Row count stability check tests
# ---------------------------------------------------------------------------


class TestCheckRowCountStability:
    """Tests for check_row_count_stability."""

    def test_first_run_passes(self) -> None:
        """First run (no previous count) passes."""
        table = _make_allocation_table()
        result = check_row_count_stability("portfolio_allocation", table, None)
        assert result.status == PASS
        assert "First run" in result.details

    def test_stable_count_passes(self) -> None:
        """Stable row count (no >50% drop) passes."""
        table = _make_allocation_table()  # 1 row
        result = check_row_count_stability("portfolio_allocation", table, 1)
        assert result.status == PASS

    def test_large_drop_warns(self) -> None:
        """Row count dropping >50% compared to previous triggers WARN."""
        table = _make_allocation_table()  # 1 row
        result = check_row_count_stability("portfolio_allocation", table, 100)
        assert result.status == WARN
        assert "dropped" in result.details

    def test_moderate_change_passes(self) -> None:
        """Row count changing but not >50% drop passes."""
        table = _make_allocation_table()  # 1 row
        result = check_row_count_stability("portfolio_allocation", table, 1)
        assert result.status == PASS


# ---------------------------------------------------------------------------
# Freshness check tests
# ---------------------------------------------------------------------------


class TestCheckFreshness:
    """Tests for check_freshness."""

    def test_recent_data_passes(self) -> None:
        """Fresh data (within threshold) passes."""
        table = _make_allocation_table()
        result = check_freshness(
            "portfolio_allocation", table, "calculated_at", freshness_days=7
        )
        assert result.status == PASS

    def test_stale_data_warns(self) -> None:
        """Data older than the freshness threshold triggers WARN."""
        # Build a table with old timestamps
        old_ts = datetime.now(timezone.utc) - timedelta(days=30)
        table = pa.table(
            {
                "calculated_at": [old_ts],
                "ticker": ["VWCE"],
                "percentage": [100.0],
                "broker": ["IBKR"],
                "identifier": ["IE00BK5BQT80"],
                "security_currency": ["EUR"],
                "description": ["Vanguard FTSE All-World"],
            },
            schema=portfolio_allocation_schema,
        )
        result = check_freshness(
            "portfolio_allocation", table, "calculated_at", freshness_days=7
        )
        assert result.status == WARN

    def test_missing_freshness_column_warns(self) -> None:
        """Missing freshness column triggers WARN."""
        table = pa.table(
            {"col_a": [1]}, schema=pa.schema([pa.field("col_a", pa.int64())])
        )
        result = check_freshness("some_table", table, "fetched_at", freshness_days=7)
        assert result.status == WARN


# ---------------------------------------------------------------------------
# Reconciliation check tests
# ---------------------------------------------------------------------------


class TestCheckReconciliation:
    """Tests for check_reconciliation."""

    def test_pass_when_brokers_match(self) -> None:
        """Reconciliation passes when all holdings brokers exist in CDC."""
        fernet_key = generate_key()
        holdings = _make_holdings_table(fernet_key)
        cdc = _make_cdc_table(fernet_key)
        result = check_reconciliation("consolidated_holdings", holdings, cdc)
        assert result.status == PASS

    def test_warn_on_missing_broker_in_cdc(self) -> None:
        """Reconciliation warns when a holdings broker is missing from CDC."""
        fernet_key = generate_key()
        now = datetime.now(timezone.utc)
        # Holdings with broker "XTB"
        holdings_rows = [
            {
                "fetched_at": now,
                "broker": "XTB",
                "ticker": "VWCE",
                "currency": "EUR",
                "value": encrypt_float(5000.0, fernet_key),
                "identifier": "IE00BK5BQT80",
                "security_currency": "EUR",
                "description": "Vanguard FTSE All-World",
            }
        ]
        holdings = _rows_to_table(holdings_rows, consolidated_holdings_schema)
        # CDC only has IBKR
        cdc = _make_cdc_table(fernet_key)
        result = check_reconciliation("consolidated_holdings", holdings, cdc)
        assert result.status == WARN
        assert "XTB" in result.details

    def test_skip_for_non_holdings_table(self) -> None:
        """Reconciliation is not applicable for tables other than consolidated_holdings."""
        table = _make_allocation_table()
        result = check_reconciliation("portfolio_allocation", table, None)
        assert result.status == PASS
        assert "not applicable" in result.details

    def test_warn_when_cdc_unavailable(self) -> None:
        """Reconciliation warns when CDC events table is not available."""
        fernet_key = generate_key()
        holdings = _make_holdings_table(fernet_key)
        result = check_reconciliation("consolidated_holdings", holdings, None)
        assert result.status == WARN
        assert "not available" in result.details


# ---------------------------------------------------------------------------
# run_validation integration tests
# ---------------------------------------------------------------------------


class TestRunValidation:
    """Integration tests for run_validation."""

    def test_returns_zero_on_all_pass(self, tmp_path: Path) -> None:
        """run_validation returns 0 when all checks pass."""
        fernet_key = generate_key()
        storage = get_storage()

        # Write test tables
        holdings = _make_holdings_table(fernet_key)
        cdc = _make_cdc_table(fernet_key)
        allocation = _make_allocation_table()

        write_deltalake(
            storage.normalized_path("consolidated_holdings"),
            holdings,
            mode="overwrite",
        )
        write_deltalake(storage.normalized_path("cdc_events"), cdc, mode="overwrite")
        write_deltalake(
            storage.analytics_path("portfolio_allocation"),
            allocation,
            mode="overwrite",
        )

        exit_code = run_validation(fernet_key=fernet_key, freshness_days=30)
        assert exit_code == 0

    def test_returns_one_on_fail(self, tmp_path: Path) -> None:
        """run_validation returns 1 when a FAIL-level check fails."""
        fernet_key = generate_key()
        storage = get_storage()

        # Write a valid holdings table
        holdings = _make_holdings_table(fernet_key)
        write_deltalake(
            storage.normalized_path("consolidated_holdings"),
            holdings,
            mode="overwrite",
        )

        # Write a CDC table with wrong schema (missing columns)
        # to trigger a schema FAIL
        now = datetime.now(timezone.utc)
        bad_cdc = pa.table(
            {
                "fetched_at": [now],
                "broker": ["IBKR"],
            },
            schema=pa.schema(
                [
                    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
                    pa.field("broker", pa.string()),
                ]
            ),
        )
        write_deltalake(
            storage.normalized_path("cdc_events"), bad_cdc, mode="overwrite"
        )

        # Also need allocation table to not trigger "table not found" WARNs
        allocation = _make_allocation_table()
        write_deltalake(
            storage.analytics_path("portfolio_allocation"),
            allocation,
            mode="overwrite",
        )

        exit_code = run_validation(fernet_key=fernet_key, freshness_days=30)
        assert exit_code == 1

    def test_fail_on_warn_flag(self, tmp_path: Path) -> None:
        """run_validation returns 1 with --fail-on-warn when WARN exists."""
        fernet_key = generate_key()
        storage = get_storage()

        # Write tables with stale data to trigger freshness WARN
        old_ts = datetime.now(timezone.utc) - timedelta(days=30)
        holdings_rows = [
            {
                "fetched_at": old_ts,
                "broker": "IBKR",
                "ticker": "VWCE",
                "currency": "EUR",
                "value": encrypt_float(5000.0, fernet_key),
                "identifier": "IE00BK5BQT80",
                "security_currency": "EUR",
                "description": "Vanguard FTSE All-World",
            }
        ]
        holdings = _rows_to_table(holdings_rows, consolidated_holdings_schema)
        cdc = _make_cdc_table(fernet_key)
        allocation = _make_allocation_table()

        write_deltalake(
            storage.normalized_path("consolidated_holdings"),
            holdings,
            mode="overwrite",
        )
        write_deltalake(storage.normalized_path("cdc_events"), cdc, mode="overwrite")
        write_deltalake(
            storage.analytics_path("portfolio_allocation"),
            allocation,
            mode="overwrite",
        )

        # With --fail-on-warn, stale data should return 1
        exit_code = run_validation(
            fernet_key=fernet_key, freshness_days=7, fail_on_warn=True
        )
        assert exit_code == 1


# ---------------------------------------------------------------------------
# data_quality table round-trip test
# ---------------------------------------------------------------------------


class TestDataQualityRoundTrip:
    """Test that data_quality results can be written and read back."""

    def test_write_and_read_results(self, tmp_path: Path) -> None:
        """Results written to data_quality Delta table match the declared schema."""
        fernet_key = generate_key()
        storage = get_storage()

        # Write tables so validation has something to check
        holdings = _make_holdings_table(fernet_key)
        cdc = _make_cdc_table(fernet_key)
        allocation = _make_allocation_table()

        write_deltalake(
            storage.normalized_path("consolidated_holdings"),
            holdings,
            mode="overwrite",
        )
        write_deltalake(storage.normalized_path("cdc_events"), cdc, mode="overwrite")
        write_deltalake(
            storage.analytics_path("portfolio_allocation"),
            allocation,
            mode="overwrite",
        )

        # Run validation to produce data_quality table
        exit_code = run_validation(fernet_key=fernet_key, freshness_days=30)
        assert exit_code == 0

        # Read back and verify schema
        from deltalake import DeltaTable

        dq_path = storage.analytics_path("data_quality")
        dq_dt = DeltaTable(dq_path)
        result = dq_dt.to_pyarrow_table()

        assert result.schema.equals(data_quality_schema, check_metadata=False)
        assert result.num_rows > 0
        # Should have at least schema + required_nulls + row_count_stability
        # + freshness for each of the 3 validated tables
        check_names = set(result.column("check_name").to_pylist())
        assert "schema" in check_names
        assert "required_nulls" in check_names
        assert "row_count_stability" in check_names
        assert "freshness" in check_names
