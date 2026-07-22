"""Tests for the data quality validation framework."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from pipeline.analytics.models import (
    data_quality_schema,
    portfolio_holdings_schema,
)
from pipeline.analytics.quality import (
    FAIL,
    NON_EMPTY_REQUIRED,
    PASS,
    WARN,
    check_freshness,
    check_non_empty,
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
    ibkr_snapshot_normalized_schema,
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
                "target_ccy": "EUR",
                "target_value": encrypt_float(5000.0, fernet_key),
                "identifier": "IE00BK5BQT80",
                "security_ccy": "EUR",
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
                "security_ccy": "EUR",
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
                "target_fx_rate": None,
                "target_value": encrypt_float(42.5, fernet_key),
                "target_ccy": "EUR",
            }
        ]
    return _rows_to_table(rows, cdc_events_normalized_schema)


def _make_portfolio_holdings_table(fernet_key: bytes) -> pa.Table:
    """Build a minimal portfolio_holdings table with encrypted value columns."""
    from pipeline.crypto import encrypt_float

    now = datetime.now(timezone.utc)
    return pa.table(
        {
            "calculated_at": [now],
            "broker": ["IBKR"],
            "ticker": ["VWCE"],
            "security_ccy": ["EUR"],
            "security_value": [encrypt_float(5000.0, fernet_key)],
            "target_value": [encrypt_float(5000.0, fernet_key)],
            "target_ccy": ["EUR"],
            "percentage": [100.0],
            "position_type": ["EQUITY"],
            "identifier": ["IE00BK5BQT80"],
            "description": ["Vanguard FTSE All-World"],
        },
        schema=portfolio_holdings_schema,
    )


@pytest.fixture(autouse=True)
def _setup_storage(tmp_path: Path) -> None:
    """Inject a tmp_path-based StorageConfig for all quality tests."""
    data = tmp_path / "data"
    for subdir in [
        "normalized/consolidated_holdings",
        "normalized/cdc_events",
        "normalized/ibkr_snapshot",
        "normalized/ibkr_cdc",
        "normalized/trading212_snapshot",
        "normalized/trading212_cdc",
        "normalized/xtb_snapshot",
        "normalized/xtb_cdc",
        "analytics/portfolio_holdings",
        "analytics/data_quality",
        "analytics/dividend_income",
        "analytics/interest_income",
        "analytics/cash_flow_summary",
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
        fernet_key = generate_key()
        table = _make_portfolio_holdings_table(fernet_key)
        result = check_schema("portfolio_holdings", table, portfolio_holdings_schema)
        assert result.status == PASS

    def test_fail_on_missing_column(self) -> None:
        """Schema check fails when a required column is missing."""
        fernet_key = generate_key()
        table = _make_portfolio_holdings_table(fernet_key)
        # Drop the 'ticker' column
        table = table.drop_columns(["ticker"])
        result = check_schema("portfolio_holdings", table, portfolio_holdings_schema)
        assert result.status == FAIL
        assert "ticker" in result.details

    def test_fail_on_extra_column(self) -> None:
        """Schema check fails when an extra column is present."""
        fernet_key = generate_key()
        table = _make_portfolio_holdings_table(fernet_key)
        # Add an extra column
        table = table.append_column(
            "extra", pa.array(["oops"] * table.num_rows, type=pa.string())
        )
        result = check_schema("portfolio_holdings", table, portfolio_holdings_schema)
        assert result.status == FAIL
        assert "extra" in result.details

    def test_fail_on_type_mismatch(self) -> None:
        """Schema check fails when a column has a different type."""
        fernet_key = generate_key()
        now = datetime.now(timezone.utc)
        # Use float64 instead of binary for security_value (wrong type)
        wrong_table = pa.table(
            {
                "calculated_at": [now],
                "broker": ["IBKR"],
                "ticker": ["VWCE"],
                "security_ccy": ["EUR"],
                "security_value": [5000.0],  # wrong type: float64 instead of binary
                "target_value": [encrypt_float(5000.0, fernet_key)],  # correct binary
                "target_ccy": ["EUR"],
                "percentage": [100.0],
                "position_type": ["EQUITY"],
                "identifier": ["IE00BK5BQT80"],
                "description": ["Vanguard FTSE All-World"],
            },
            schema=pa.schema(
                [
                    pa.field("calculated_at", pa.timestamp("us", tz="UTC")),
                    pa.field("broker", pa.string()),
                    pa.field("ticker", pa.string()),
                    pa.field("security_ccy", pa.string()),
                    pa.field(
                        "security_value", pa.float64()
                    ),  # mismatch: should be binary
                    pa.field("target_value", pa.binary()),
                    pa.field("target_ccy", pa.string()),
                    pa.field("percentage", pa.float64()),
                    pa.field("position_type", pa.string()),
                    pa.field("identifier", pa.string()),
                    pa.field("description", pa.string()),
                ]
            ),
        )
        result = check_schema(
            "portfolio_holdings", wrong_table, portfolio_holdings_schema
        )
        assert result.status == FAIL
        assert "security_value" in result.details


# ---------------------------------------------------------------------------
# Required nulls check tests
# ---------------------------------------------------------------------------


class TestCheckRequiredNulls:
    """Tests for check_required_nulls."""

    def test_pass_on_no_nulls(self) -> None:
        """Required nulls check passes when all required fields are non-null."""
        fernet_key = generate_key()
        table = _make_portfolio_holdings_table(fernet_key)
        result = check_required_nulls(
            "portfolio_holdings", table, portfolio_holdings_schema
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
                "target_ccy": "EUR",
                "target_value": encrypt_float(5000.0, fernet_key),
                "identifier": "IE00BK5BQT80",
                "security_ccy": "EUR",
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
        fernet_key = generate_key()
        table = _make_portfolio_holdings_table(fernet_key)
        result = check_row_count_stability("portfolio_holdings", table, None)
        assert result.status == PASS
        assert "First run" in result.details

    def test_stable_count_passes(self) -> None:
        """Stable row count (no >50% drop) passes."""
        fernet_key = generate_key()
        table = _make_portfolio_holdings_table(fernet_key)  # 1 row
        result = check_row_count_stability("portfolio_holdings", table, 1)
        assert result.status == PASS

    def test_large_drop_warns(self) -> None:
        """Row count dropping >50% compared to previous triggers WARN."""
        fernet_key = generate_key()
        table = _make_portfolio_holdings_table(fernet_key)  # 1 row
        result = check_row_count_stability("portfolio_holdings", table, 100)
        assert result.status == WARN
        assert "dropped" in result.details

    def test_moderate_change_passes(self) -> None:
        """Row count changing but not >50% drop passes."""
        fernet_key = generate_key()
        table = _make_portfolio_holdings_table(fernet_key)  # 1 row
        result = check_row_count_stability("portfolio_holdings", table, 1)
        assert result.status == PASS


# ---------------------------------------------------------------------------
# Freshness check tests
# ---------------------------------------------------------------------------


class TestCheckFreshness:
    """Tests for check_freshness."""

    def test_recent_data_passes(self) -> None:
        """Fresh data (within threshold) passes."""
        fernet_key = generate_key()
        table = _make_portfolio_holdings_table(fernet_key)
        result = check_freshness(
            "portfolio_holdings", table, "calculated_at", freshness_days=7
        )
        assert result.status == PASS

    def test_stale_data_warns(self) -> None:
        """Data older than the freshness threshold triggers WARN."""
        # Build a table with old timestamps
        fernet_key = generate_key()
        old_ts = datetime.now(timezone.utc) - timedelta(days=30)
        table = pa.table(
            {
                "calculated_at": [old_ts],
                "broker": ["IBKR"],
                "ticker": ["VWCE"],
                "security_ccy": ["EUR"],
                "security_value": [encrypt_float(5000.0, fernet_key)],
                "target_value": [encrypt_float(5000.0, fernet_key)],
                "target_ccy": ["EUR"],
                "percentage": [100.0],
                "position_type": ["EQUITY"],
                "identifier": ["IE00BK5BQT80"],
                "description": ["Vanguard FTSE All-World"],
            },
            schema=portfolio_holdings_schema,
        )
        result = check_freshness(
            "portfolio_holdings", table, "calculated_at", freshness_days=7
        )
        assert result.status == WARN

    def test_missing_freshness_column_warns(self) -> None:
        """Missing freshness column triggers WARN."""
        table = pa.table(
            {"col_a": [1]}, schema=pa.schema([pa.field("col_a", pa.int64())])
        )
        result = check_freshness("some_table", table, "fetched_at", freshness_days=7)
        assert result.status == WARN

    def test_empty_table_passes_freshness(self) -> None:
        """An empty table passes freshness — empty data cannot be stale."""
        # Build an empty table with the correct schema but zero rows
        table = pa.table(
            {
                "calculated_at": pa.array([], type=pa.timestamp("us", tz="UTC")),
                "broker": pa.array([], type=pa.string()),
                "ticker": pa.array([], type=pa.string()),
                "security_ccy": pa.array([], type=pa.string()),
                "security_value": pa.array([], type=pa.binary()),
                "target_value": pa.array([], type=pa.binary()),
                "target_ccy": pa.array([], type=pa.string()),
                "percentage": pa.array([], type=pa.float64()),
                "position_type": pa.array([], type=pa.string()),
                "identifier": pa.array([], type=pa.string()),
                "description": pa.array([], type=pa.string()),
            },
            schema=portfolio_holdings_schema,
        )
        result = check_freshness(
            "portfolio_holdings", table, "calculated_at", freshness_days=7
        )
        assert result.status == PASS
        assert "empty" in result.details.lower()


# ---------------------------------------------------------------------------
# Non-empty check tests
# ---------------------------------------------------------------------------


class TestCheckNonEmpty:
    """Tests for check_non_empty — CDC tables must not be empty."""

    def test_pass_on_rows(self) -> None:
        """A table with rows passes the non-empty check."""
        table = pa.table({"x": [1, 2, 3]})
        result = check_non_empty("ibkr_cdc", table)
        assert result.status == PASS
        assert "3" in result.details

    def test_fail_on_zero_rows(self) -> None:
        """An empty table fails the non-empty check."""
        # Decision: docs/adr/0087-make-cdc-mandatory-and-fail-on-empty-silver-cdc.md
        table = pa.table({"x": pa.array([], type=pa.int64())})
        result = check_non_empty("ibkr_cdc", table)
        assert result.status == FAIL
        assert "0 rows" in result.details

    def test_non_empty_required_registry(self) -> None:
        """NON_EMPTY_REQUIRED includes cdc_events, ibkr_cdc, trading212_cdc but not xtb_cdc."""
        assert "cdc_events" in NON_EMPTY_REQUIRED
        assert "ibkr_cdc" in NON_EMPTY_REQUIRED
        assert "trading212_cdc" in NON_EMPTY_REQUIRED
        assert "xtb_cdc" not in NON_EMPTY_REQUIRED


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
                "target_ccy": "EUR",
                "target_value": encrypt_float(5000.0, fernet_key),
                "identifier": "IE00BK5BQT80",
                "security_ccy": "EUR",
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
        fernet_key = generate_key()
        table = _make_portfolio_holdings_table(fernet_key)
        result = check_reconciliation("portfolio_holdings", table, None)
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

        # Write test tables — including NON_EMPTY_REQUIRED CDC tables
        holdings = _make_holdings_table(fernet_key)
        cdc = _make_cdc_table(fernet_key)
        portfolio_holdings = _make_portfolio_holdings_table(fernet_key)

        write_deltalake(
            storage.normalized_path("consolidated_holdings"),
            holdings,
            mode="overwrite",
        )
        write_deltalake(storage.normalized_path("cdc_events"), cdc, mode="overwrite")
        # Write required broker CDC tables (NON_EMPTY_REQUIRED)
        write_deltalake(storage.normalized_path("ibkr_cdc"), cdc, mode="overwrite")
        write_deltalake(
            storage.normalized_path("trading212_cdc"), cdc, mode="overwrite"
        )
        write_deltalake(
            storage.analytics_path("portfolio_holdings"),
            portfolio_holdings,
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

        # Also need portfolio_holdings table to not trigger "table not found" WARNs
        portfolio_holdings = _make_portfolio_holdings_table(fernet_key)
        write_deltalake(
            storage.analytics_path("portfolio_holdings"),
            portfolio_holdings,
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
                "target_ccy": "EUR",
                "target_value": encrypt_float(5000.0, fernet_key),
                "identifier": "IE00BK5BQT80",
                "security_ccy": "EUR",
                "description": "Vanguard FTSE All-World",
            }
        ]
        holdings = _rows_to_table(holdings_rows, consolidated_holdings_schema)
        cdc = _make_cdc_table(fernet_key)
        portfolio_holdings = _make_portfolio_holdings_table(fernet_key)

        write_deltalake(
            storage.normalized_path("consolidated_holdings"),
            holdings,
            mode="overwrite",
        )
        write_deltalake(storage.normalized_path("cdc_events"), cdc, mode="overwrite")
        write_deltalake(
            storage.analytics_path("portfolio_holdings"),
            portfolio_holdings,
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
        portfolio_holdings = _make_portfolio_holdings_table(fernet_key)

        write_deltalake(
            storage.normalized_path("consolidated_holdings"),
            holdings,
            mode="overwrite",
        )
        write_deltalake(storage.normalized_path("cdc_events"), cdc, mode="overwrite")
        # Write required broker CDC tables (NON_EMPTY_REQUIRED)
        write_deltalake(storage.normalized_path("ibkr_cdc"), cdc, mode="overwrite")
        write_deltalake(
            storage.normalized_path("trading212_cdc"), cdc, mode="overwrite"
        )
        write_deltalake(
            storage.analytics_path("portfolio_holdings"),
            portfolio_holdings,
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
        # + freshness for each validated table
        check_names = set(result.column("check_name").to_pylist())
        assert "schema" in check_names
        assert "required_nulls" in check_names
        assert "row_count_stability" in check_names
        assert "freshness" in check_names


# ---------------------------------------------------------------------------
# Scoped validation tests
# ---------------------------------------------------------------------------


def _make_ibkr_snapshot_table(fernet_key: bytes) -> pa.Table:
    """Build a minimal ibkr_snapshot table."""
    now = datetime.now(timezone.utc)
    return pa.table(
        {
            "fetched_at": [now],
            "account_id": ["U123456"],
            "position_type": ["EQUITY"],
            "label": ["VWCE"],
            "asset_class": ["STK"],
            "security_value": [encrypt_float(5000.0, fernet_key)],
            "security_ccy": ["EUR"],
            "isin": ["IE00BK5BQT80"],
            "description": ["Vanguard FTSE All-World"],
        },
        schema=ibkr_snapshot_normalized_schema,
    )


class TestScopedValidation:
    """Tests for run_validation with the tables parameter."""

    def test_scoped_validates_only_specified_tables(self, tmp_path: Path) -> None:
        """When tables is specified, only those tables are validated."""
        fernet_key = generate_key()
        storage = get_storage()

        # Write only ibkr_snapshot — other tables are missing
        snapshot = _make_ibkr_snapshot_table(fernet_key)
        write_deltalake(
            storage.normalized_path("ibkr_snapshot"), snapshot, mode="overwrite"
        )

        exit_code = run_validation(
            fernet_key=fernet_key,
            freshness_days=30,
            tables=["ibkr_snapshot"],
        )
        assert exit_code == 0

        # Verify only ibkr_snapshot checks were written to data_quality
        from deltalake import DeltaTable

        dq_dt = DeltaTable(storage.analytics_path("data_quality"))
        result = dq_dt.to_pyarrow_table()
        table_names = set(result.column("table_name").to_pylist())
        assert table_names == {"ibkr_snapshot"}

    def test_unknown_table_name_warns(self, tmp_path: Path) -> None:
        """An unknown table name in tables list produces a WARN result."""
        fernet_key = generate_key()
        exit_code = run_validation(
            fernet_key=fernet_key,
            freshness_days=30,
            tables=["nonexistent_table"],
        )
        # WARN without fail_on_warn → exit 0
        assert exit_code == 0

        from deltalake import DeltaTable

        storage = get_storage()
        dq_dt = DeltaTable(storage.analytics_path("data_quality"))
        result = dq_dt.to_pyarrow_table()
        statuses = result.column("status").to_pylist()
        assert "WARN" in statuses
        details = result.column("details").to_pylist()
        assert any("Unknown table name" in d for d in details)

    def test_per_connector_schema_pass(self, tmp_path: Path) -> None:
        """Per-connector snapshot schema check passes on valid data."""
        fernet_key = generate_key()
        storage = get_storage()

        snapshot = _make_ibkr_snapshot_table(fernet_key)
        write_deltalake(
            storage.normalized_path("ibkr_snapshot"), snapshot, mode="overwrite"
        )

        exit_code = run_validation(
            fernet_key=fernet_key,
            freshness_days=30,
            tables=["ibkr_snapshot"],
        )
        assert exit_code == 0

    def test_per_connector_schema_fail_on_dropped_column(self, tmp_path: Path) -> None:
        """Per-connector schema check fails when a column is missing."""
        fernet_key = generate_key()
        storage = get_storage()

        # Write snapshot with a missing column
        snapshot = _make_ibkr_snapshot_table(fernet_key)
        bad_snapshot = snapshot.drop_columns(["security_ccy"])
        write_deltalake(
            storage.normalized_path("ibkr_snapshot"), bad_snapshot, mode="overwrite"
        )

        exit_code = run_validation(
            fernet_key=fernet_key,
            freshness_days=30,
            tables=["ibkr_snapshot"],
        )
        assert exit_code == 1
