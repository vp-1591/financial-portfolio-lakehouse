"""Tests for CDC analytics table builders."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from pipeline.analytics.cdc_tables import (
    build_cash_flow_summary,
    build_dividend_income,
    build_interest_income,
)
from pipeline.analytics.models import (
    cash_flow_summary_schema,
    dividend_income_schema,
    interest_income_schema,
)
from pipeline.connectors.transform_utils import build_normalized_table
from pipeline.crypto import generate_key
from pipeline.normalized.models import cdc_events_normalized_schema
from pipeline.storage import LocalBackend, StorageConfig, use_storage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cdc_table(
    fernet_key: bytes,
    rows: list[dict],
) -> pa.Table:
    """Build a cdc_events table from row dicts, encrypting binary columns.

    Each row dict should have all required CDC fields.  Encrypted columns
    (cash_amount, amount_base, fx_rate_to_base, etc.) should contain plain
    floats — the helper encrypts them automatically.
    """
    now = datetime.now(timezone.utc)
    encrypt_columns = [
        "cash_amount",
        "amount_base",
        "fx_rate_to_base",
        "gross_amount",
        "fee_amount",
        "tax_amount",
        "net_amount",
        "quantity",
        "price",
    ]

    prepared_rows = []
    for row in rows:
        record = {
            "fetched_at": row.get("fetched_at", now),
            "broker": row.get("broker", "IBKR"),
            "account_id": row.get("account_id", "U123456"),
            "event_id": row.get("event_id", "evt-0"),
            "source": row.get("source", "flex"),
            "event_type": row.get("event_type", "DIVIDEND"),
            "raw_event_type": row.get("raw_event_type", ""),
            "event_datetime": row.get("event_datetime", "2026-01-15"),
            "value_currency": row.get("value_currency", "EUR"),
        }
        # Add encrypted columns as plain floats — build_normalized_table
        # will encrypt them.
        for col in encrypt_columns:
            val = row.get(col)
            record[col] = val  # Will be encrypted by build_normalized_table

        # Add nullable columns that are not encrypted
        record["settle_date"] = row.get("settle_date")
        record["ticker"] = row.get("ticker")
        record["isin"] = row.get("isin")
        record["description"] = row.get("description")
        record["side"] = row.get("side")
        record["base_currency"] = row.get("base_currency")

        prepared_rows.append(record)

    return build_normalized_table(
        prepared_rows,
        cdc_events_normalized_schema,
        fernet_key,
        encrypt_columns=encrypt_columns,
    )


def _write_cdc_to_delta(
    table: pa.Table,
    tmp_path: Path,
    storage: StorageConfig,
) -> str:
    """Write a CDC events table to Delta and return its path."""
    path = storage.normalized_path("cdc_events")
    storage.backend.ensure_parent(path)
    write_deltalake(
        path, table, mode="overwrite", storage_options=storage.storage_options
    )
    return path


@pytest.fixture(autouse=True)
def _setup_storage(tmp_path: Path) -> None:
    """Inject a tmp_path-based StorageConfig for all CDC analytics tests."""
    data = tmp_path / "data"
    for subdir in [
        "normalized/cdc_events",
        "normalized/consolidated_holdings",
        "analytics/portfolio_allocation",
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


@pytest.fixture()
def fernet_key() -> bytes:
    return generate_key()


# ---------------------------------------------------------------------------
# TestBuildDividendIncome
# ---------------------------------------------------------------------------


class TestBuildDividendIncome:
    """Tests for build_dividend_income."""

    def test_produces_correct_schema(self, fernet_key: bytes, tmp_path: Path) -> None:
        """Result schema matches dividend_income_schema."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "DIVIDEND",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 42.5,
                    "ticker": "VWCE",
                    "isin": "IE00BK5BQT80",
                    "description": "Vanguard dividend",
                    "base_currency": "EUR",
                    "amount_base": 42.5,
                }
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_dividend_income(fernet_key=fernet_key)
        assert result.schema.equals(dividend_income_schema, check_metadata=False)

    def test_filters_only_dividends(self, fernet_key: bytes, tmp_path: Path) -> None:
        """Only DIVIDEND events appear in the result."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "DIVIDEND",
                    "event_id": "div-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 42.5,
                    "ticker": "VWCE",
                    "base_currency": "EUR",
                    "amount_base": 42.5,
                },
                {
                    "event_type": "INTEREST",
                    "event_id": "int-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 10.0,
                    "base_currency": "EUR",
                    "amount_base": 10.0,
                },
                {
                    "event_type": "DEPOSIT",
                    "event_id": "dep-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 5000.0,
                    "base_currency": "EUR",
                    "amount_base": 5000.0,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_dividend_income(fernet_key=fernet_key)
        assert result.num_rows == 1
        # The single row should be the dividend
        assert result.column("broker")[0].as_py() == "IBKR"
        assert result.column("ticker")[0].as_py() == "VWCE"

    def test_groups_by_period_broker_security(
        self, fernet_key: bytes, tmp_path: Path
    ) -> None:
        """Events are grouped by period, broker, security, currency."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "DIVIDEND",
                    "event_id": "div-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 42.5,
                    "ticker": "VWCE",
                    "isin": "IE00BK5BQT80",
                    "description": "Vanguard dividend",
                    "base_currency": "EUR",
                    "amount_base": 42.5,
                },
                {
                    "event_type": "DIVIDEND",
                    "event_id": "div-2",
                    "event_datetime": "2026-04-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 30.0,
                    "ticker": "VWCE",
                    "isin": "IE00BK5BQT80",
                    "description": "Vanguard dividend",
                    "base_currency": "EUR",
                    "amount_base": 30.0,
                },
                {
                    "event_type": "DIVIDEND",
                    "event_id": "div-3",
                    "event_datetime": "2026-03-15",
                    "broker": "T212",
                    "value_currency": "USD",
                    "cash_amount": 100.0,
                    "ticker": "AAPL",
                    "isin": "US0378331005",
                    "description": "Apple dividend",
                    "base_currency": "EUR",
                    "amount_base": 90.0,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_dividend_income(fernet_key=fernet_key)
        assert result.num_rows == 3  # Two different months for IBKR, one for T212

        # Check period format
        months = set(result.column("period_month").to_pylist())
        assert "2026-03" in months
        assert "2026-04" in months

        quarters = set(result.column("period_quarter").to_pylist())
        assert "2026-Q1" in quarters

    def test_sums_cash_amount_and_amount_base(
        self, fernet_key: bytes, tmp_path: Path
    ) -> None:
        """Two dividends in the same group are summed."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "DIVIDEND",
                    "event_id": "div-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 42.5,
                    "ticker": "VWCE",
                    "base_currency": "EUR",
                    "amount_base": 42.5,
                },
                {
                    "event_type": "DIVIDEND",
                    "event_id": "div-2",
                    "event_datetime": "2026-03-15",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 10.0,
                    "ticker": "VWCE",
                    "base_currency": "EUR",
                    "amount_base": 10.0,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_dividend_income(fernet_key=fernet_key)
        assert result.num_rows == 1
        assert abs(result.column("cash_amount")[0].as_py() - 52.5) < 0.01
        assert abs(result.column("amount_base")[0].as_py() - 52.5) < 0.01
        assert result.column("event_count")[0].as_py() == 2

    def test_handles_null_amount_base_with_fx_rate_fallback(
        self, fernet_key: bytes, tmp_path: Path
    ) -> None:
        """When amount_base is null but fx_rate_to_base exists, use cash_amount * fx_rate_to_base."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "DIVIDEND",
                    "event_id": "div-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "USD",
                    "cash_amount": 100.0,
                    "ticker": "AAPL",
                    "base_currency": "EUR",
                    "amount_base": None,  # Null: should fall back to 100 * 0.9 = 90
                    "fx_rate_to_base": 0.9,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_dividend_income(fernet_key=fernet_key)
        assert result.num_rows == 1
        assert abs(result.column("cash_amount")[0].as_py() - 100.0) < 0.01
        # amount_base should be 100 * 0.9 = 90.0
        assert abs(result.column("amount_base")[0].as_py() - 90.0) < 0.01

    def test_handles_completely_null_amount_base(
        self, fernet_key: bytes, tmp_path: Path
    ) -> None:
        """When both amount_base and fx_rate_to_base are null, amount_base stays null."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "DIVIDEND",
                    "event_id": "div-1",
                    "event_datetime": "2026-03-01",
                    "broker": "XTB",
                    "value_currency": "PLN",
                    "cash_amount": 50.0,
                    "ticker": "CD Projekt",
                    "base_currency": None,
                    "amount_base": None,
                    "fx_rate_to_base": None,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_dividend_income(fernet_key=fernet_key)
        assert result.num_rows == 1
        assert abs(result.column("cash_amount")[0].as_py() - 50.0) < 0.01
        assert result.column("amount_base")[0].as_py() is None

    def test_writes_delta_table(self, fernet_key: bytes, tmp_path: Path) -> None:
        """The result is written to the analytics Delta table and can be read back."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "DIVIDEND",
                    "event_id": "div-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 42.5,
                    "ticker": "VWCE",
                    "base_currency": "EUR",
                    "amount_base": 42.5,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_dividend_income(fernet_key=fernet_key)
        assert result.num_rows == 1

        # Read back from Delta
        from deltalake import DeltaTable

        dt = DeltaTable(
            storage.analytics_path("dividend_income"),
            storage_options=storage.storage_options,
        )
        readback = dt.to_pyarrow_table()
        assert readback.num_rows == 1

    def test_raises_on_missing_cdc_table(
        self, fernet_key: bytes, tmp_path: Path
    ) -> None:
        """FileNotFoundError when no cdc_events table exists."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        with pytest.raises(FileNotFoundError, match="CDC events table not found"):
            build_dividend_income(fernet_key=fernet_key)


# ---------------------------------------------------------------------------
# TestBuildInterestIncome
# ---------------------------------------------------------------------------


class TestBuildInterestIncome:
    """Tests for build_interest_income."""

    def test_produces_correct_schema(self, fernet_key: bytes, tmp_path: Path) -> None:
        """Result schema matches interest_income_schema."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "INTEREST",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 35.0,
                    "base_currency": "EUR",
                    "amount_base": 35.0,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_interest_income(fernet_key=fernet_key)
        assert result.schema.equals(interest_income_schema, check_metadata=False)

    def test_filters_only_interest(self, fernet_key: bytes, tmp_path: Path) -> None:
        """Only INTEREST events appear in the result."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "INTEREST",
                    "event_id": "int-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 35.0,
                    "base_currency": "EUR",
                    "amount_base": 35.0,
                },
                {
                    "event_type": "DIVIDEND",
                    "event_id": "div-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 42.5,
                    "ticker": "VWCE",
                    "base_currency": "EUR",
                    "amount_base": 42.5,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_interest_income(fernet_key=fernet_key)
        assert result.num_rows == 1
        assert result.column("broker")[0].as_py() == "IBKR"

    def test_groups_by_period_broker_currency(
        self, fernet_key: bytes, tmp_path: Path
    ) -> None:
        """Events are grouped by period, broker, and currency."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "INTEREST",
                    "event_id": "int-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 35.0,
                    "base_currency": "EUR",
                    "amount_base": 35.0,
                },
                {
                    "event_type": "INTEREST",
                    "event_id": "int-2",
                    "event_datetime": "2026-04-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 20.0,
                    "base_currency": "EUR",
                    "amount_base": 20.0,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_interest_income(fernet_key=fernet_key)
        assert result.num_rows == 2  # Two different months

    def test_sums_amounts(self, fernet_key: bytes, tmp_path: Path) -> None:
        """Two interest events in the same group are summed."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "INTEREST",
                    "event_id": "int-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 35.0,
                    "base_currency": "EUR",
                    "amount_base": 35.0,
                },
                {
                    "event_type": "INTEREST",
                    "event_id": "int-2",
                    "event_datetime": "2026-03-15",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 15.0,
                    "base_currency": "EUR",
                    "amount_base": 15.0,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_interest_income(fernet_key=fernet_key)
        assert result.num_rows == 1
        assert abs(result.column("cash_amount")[0].as_py() - 50.0) < 0.01
        assert abs(result.column("amount_base")[0].as_py() - 50.0) < 0.01
        assert result.column("event_count")[0].as_py() == 2


# ---------------------------------------------------------------------------
# TestBuildCashFlowSummary
# ---------------------------------------------------------------------------


class TestBuildCashFlowSummary:
    """Tests for build_cash_flow_summary."""

    def test_produces_correct_schema(self, fernet_key: bytes, tmp_path: Path) -> None:
        """Result schema matches cash_flow_summary_schema."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "DEPOSIT",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 5000.0,
                    "base_currency": "EUR",
                    "amount_base": 5000.0,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_cash_flow_summary(fernet_key=fernet_key)
        assert result.schema.equals(cash_flow_summary_schema, check_metadata=False)

    def test_includes_all_event_types(self, fernet_key: bytes, tmp_path: Path) -> None:
        """All CDC event types appear in the summary."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        event_types = ["DIVIDEND", "INTEREST", "DEPOSIT", "WITHDRAWAL", "FEE", "TRADE"]
        rows = [
            {
                "event_type": etype,
                "event_id": f"{etype.lower()}-1",
                "event_datetime": "2026-03-01",
                "broker": "IBKR",
                "value_currency": "EUR",
                "cash_amount": 100.0,
                "base_currency": "EUR",
                "amount_base": 100.0,
            }
            for etype in event_types
        ]
        cdc = _make_cdc_table(fernet_key, rows)
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_cash_flow_summary(fernet_key=fernet_key)
        result_types = set(result.column("event_type").to_pylist())
        assert result_types == set(event_types)

    def test_groups_by_period_broker_type_currency(
        self, fernet_key: bytes, tmp_path: Path
    ) -> None:
        """Events are grouped by period, broker, type, and currency."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "DEPOSIT",
                    "event_id": "dep-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 5000.0,
                    "base_currency": "EUR",
                    "amount_base": 5000.0,
                },
                {
                    "event_type": "DEPOSIT",
                    "event_id": "dep-2",
                    "event_datetime": "2026-04-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 3000.0,
                    "base_currency": "EUR",
                    "amount_base": 3000.0,
                },
                {
                    "event_type": "DIVIDEND",
                    "event_id": "div-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 42.5,
                    "ticker": "VWCE",
                    "base_currency": "EUR",
                    "amount_base": 42.5,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_cash_flow_summary(fernet_key=fernet_key)
        assert result.num_rows == 3  # March deposit, April deposit, March dividend

    def test_sums_amounts(self, fernet_key: bytes, tmp_path: Path) -> None:
        """Two deposits in the same group are summed."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "DEPOSIT",
                    "event_id": "dep-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 5000.0,
                    "base_currency": "EUR",
                    "amount_base": 5000.0,
                },
                {
                    "event_type": "DEPOSIT",
                    "event_id": "dep-2",
                    "event_datetime": "2026-03-15",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 2000.0,
                    "base_currency": "EUR",
                    "amount_base": 2000.0,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_cash_flow_summary(fernet_key=fernet_key)
        assert result.num_rows == 1
        assert abs(result.column("cash_amount")[0].as_py() - 7000.0) < 0.01
        assert abs(result.column("amount_base")[0].as_py() - 7000.0) < 0.01

    def test_event_count_matches_source(
        self, fernet_key: bytes, tmp_path: Path
    ) -> None:
        """event_count matches the number of source events per group."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "DEPOSIT",
                    "event_id": "dep-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 5000.0,
                    "base_currency": "EUR",
                    "amount_base": 5000.0,
                },
                {
                    "event_type": "DEPOSIT",
                    "event_id": "dep-2",
                    "event_datetime": "2026-03-15",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 2000.0,
                    "base_currency": "EUR",
                    "amount_base": 2000.0,
                },
                {
                    "event_type": "DIVIDEND",
                    "event_id": "div-1",
                    "event_datetime": "2026-03-01",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 42.5,
                    "ticker": "VWCE",
                    "base_currency": "EUR",
                    "amount_base": 42.5,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_cash_flow_summary(fernet_key=fernet_key)
        assert result.num_rows == 2  # Two groups: DEPOSIT and DIVIDEND

        # Find the DEPOSIT row
        event_types = result.column("event_type").to_pylist()
        deposit_idx = event_types.index("DEPOSIT")
        assert result.column("event_count")[deposit_idx].as_py() == 2

        dividend_idx = event_types.index("DIVIDEND")
        assert result.column("event_count")[dividend_idx].as_py() == 1


# ---------------------------------------------------------------------------
# TestDateParsing
# ---------------------------------------------------------------------------


class TestDateParsing:
    """Tests for event_datetime parsing across broker formats."""

    def test_ibkr_datetime_format(self, fernet_key: bytes, tmp_path: Path) -> None:
        """IBKR format '2026-03-01 00:00:00' is parsed to period_month '2026-03'."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "DIVIDEND",
                    "event_id": "div-1",
                    "event_datetime": "2026-03-01 00:00:00",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 42.5,
                    "ticker": "VWCE",
                    "base_currency": "EUR",
                    "amount_base": 42.5,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_dividend_income(fernet_key=fernet_key)
        assert result.num_rows == 1
        assert result.column("period_month")[0].as_py() == "2026-03"
        assert result.column("period_quarter")[0].as_py() == "2026-Q1"

    def test_date_only_format(self, fernet_key: bytes, tmp_path: Path) -> None:
        """Date-only format '2024-01-15' is parsed to period_month '2024-01'."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "DEPOSIT",
                    "event_id": "dep-1",
                    "event_datetime": "2024-01-15",
                    "broker": "XTB",
                    "value_currency": "PLN",
                    "cash_amount": 1000.0,
                    "base_currency": "EUR",
                    "amount_base": 230.0,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_cash_flow_summary(fernet_key=fernet_key)
        assert result.num_rows == 1
        assert result.column("period_month")[0].as_py() == "2024-01"

    def test_iso_format(self, fernet_key: bytes, tmp_path: Path) -> None:
        """ISO format '2024-01-15T10:30:00Z' is parsed to period_month '2024-01'."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "TRADE",
                    "event_id": "trade-1",
                    "event_datetime": "2024-01-15T10:30:00Z",
                    "broker": "T212",
                    "value_currency": "USD",
                    "cash_amount": 1500.0,
                    "base_currency": "EUR",
                    "amount_base": 1350.0,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_cash_flow_summary(fernet_key=fernet_key)
        assert result.num_rows == 1
        assert result.column("period_month")[0].as_py() == "2024-01"

    def test_ibkr_compact_date_format(self, fernet_key: bytes, tmp_path: Path) -> None:
        """IBKR compact date '20260204' is parsed to period_month '2026-02'."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "INTEREST",
                    "event_id": "int-1",
                    "event_datetime": "20260204",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 12.50,
                    "base_currency": "EUR",
                    "amount_base": 12.50,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_interest_income(fernet_key=fernet_key)
        assert result.num_rows == 1
        assert result.column("period_month")[0].as_py() == "2026-02"

    def test_ibkr_compact_datetime_format(
        self, fernet_key: bytes, tmp_path: Path
    ) -> None:
        """IBKR compact datetime '20260702;022904' is parsed to period_month '2026-07'."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "TRADE",
                    "event_id": "trade-1",
                    "event_datetime": "20260702;022904",
                    "broker": "IBKR",
                    "value_currency": "USD",
                    "cash_amount": -1501.0,
                    "base_currency": "EUR",
                    "amount_base": -1350.9,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_cash_flow_summary(fernet_key=fernet_key)
        assert result.num_rows == 1
        assert result.column("period_month")[0].as_py() == "2026-07"

    def test_ibkr_normalised_iso_parsed(
        self, fernet_key: bytes, tmp_path: Path
    ) -> None:
        """IBKR datetime normalised to ISO '2026-02-04T00:00:00Z' is parsed correctly."""
        storage = StorageConfig(
            data_dir=str(tmp_path / "data"),
            raw_dir=str(tmp_path / "data" / "raw"),
            normalized_dir=str(tmp_path / "data" / "normalized"),
            analytics_dir=str(tmp_path / "data" / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(tmp_path / "data"),
        )
        use_storage(storage)

        cdc = _make_cdc_table(
            fernet_key,
            [
                {
                    "event_type": "INTEREST",
                    "event_id": "int-1",
                    "event_datetime": "2026-02-04T00:00:00Z",
                    "broker": "IBKR",
                    "value_currency": "EUR",
                    "cash_amount": 12.50,
                    "base_currency": "EUR",
                    "amount_base": 12.50,
                },
            ],
        )
        _write_cdc_to_delta(cdc, tmp_path, storage)

        result = build_interest_income(fernet_key=fernet_key)
        assert result.num_rows == 1
        assert result.column("period_month")[0].as_py() == "2026-02"
