"""Tests for the normalize_currency pipeline step."""

from __future__ import annotations


import pyarrow as pa
import pytest

from pipeline.connectors.transform_utils import build_normalized_table
from pipeline.crypto import decrypt_float, generate_key
from pipeline.normalized.models import cdc_events_normalized_schema
from pipeline.normalized.normalize import normalize_currency


def _make_cdc_table(
    events: list[dict],
    fernet_key: bytes,
) -> pa.Table:
    """Build a CDC events table from event dicts."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    records = []
    for event in events:
        record = {
            "fetched_at": now,
            "broker": event.get("broker", "IBKR"),
            "account_id": event.get("account_id", "U123"),
            "event_id": event.get("event_id", "evt-1"),
            "source": event.get("source", "CashTransaction"),
            "event_type": event.get("event_type", "DIVIDEND"),
            "raw_event_type": event.get("raw_event_type", "Dividends"),
            "event_datetime": event.get("event_datetime", "2024-01-15"),
            "security_ccy": event.get("security_ccy", "USD"),
            "cash_amount": event.get("cash_amount", 100.0),
            "settle_date": event.get("settle_date", "2024-01-18"),
            "ticker": event.get("ticker", "AAPL"),
            "isin": event.get("isin", ""),
            "description": event.get("description", ""),
            "quantity": event.get("quantity"),
            "price": event.get("price"),
            "side": event.get("side"),
            "gross_amount": event.get("gross_amount"),
            "fee_amount": event.get("fee_amount"),
            "tax_amount": event.get("tax_amount"),
            "target_fx_rate": event.get("target_fx_rate"),
            "target_value": event.get("target_value"),
            "target_ccy": event.get("target_ccy"),
        }
        records.append(record)

    encrypt_cols = ["cash_amount"]
    # Add target columns to encryption if present
    for col in ("target_fx_rate", "target_value"):
        if any(r.get(col) is not None for r in records):
            encrypt_cols.append(col)

    return build_normalized_table(
        records,
        cdc_events_normalized_schema,
        fernet_key,
        encrypt_columns=encrypt_cols,
    )


class TestNormalizeCurrency:
    """Tests for normalize_currency filling target_fx_rate and target_value."""

    @pytest.fixture(autouse=True)
    def _setup_storage(self, tmp_data_dir):
        """Set up storage config and mode for all normalize_currency tests."""
        from pipeline.secrets import set_mode

        set_mode("docker")

    def test_same_currency_gets_rate_1(self, tmp_path) -> None:
        """When security_ccy == target_ccy, target_fx_rate = 1.0."""
        fernet_key = generate_key()
        table = _make_cdc_table(
            [{"security_ccy": "EUR", "cash_amount": 42.5}],
            fernet_key,
        )

        # Write the Delta table
        from deltalake import write_deltalake

        table_path = str(tmp_path / "cdc_events")
        write_deltalake(table_path, table, mode="overwrite")

        from pipeline.normalized.consolidate import CurrencyConverter

        converter = CurrencyConverter("EUR", manual_rates={"USD": 0.9})

        result = normalize_currency(
            table_path=table_path,
            fernet_key=fernet_key,
            converter=converter,
        )

        assert result.num_rows == 1
        # target_ccy should be EUR
        assert result.column("target_ccy")[0].as_py() == "EUR"
        # target_fx_rate should be 1.0 for same currency
        rate = decrypt_float(result.column("target_fx_rate")[0].as_py(), fernet_key)
        assert rate == pytest.approx(1.0)
        # target_value should equal cash_amount
        value = decrypt_float(result.column("target_value")[0].as_py(), fernet_key)
        assert value == pytest.approx(42.5)

    def test_ibkr_with_broker_rate(self, tmp_path) -> None:
        """IBKR events with target_fx_rate already set use the broker rate."""
        fernet_key = generate_key()
        table = _make_cdc_table(
            [
                {
                    "broker": "IBKR",
                    "security_ccy": "USD",
                    "cash_amount": 1000.0,
                    "target_fx_rate": 0.92,  # IBKR fxRateToBase
                }
            ],
            fernet_key,
        )

        from deltalake import write_deltalake

        table_path = str(tmp_path / "cdc_events")
        write_deltalake(table_path, table, mode="overwrite")

        from pipeline.normalized.consolidate import CurrencyConverter

        converter = CurrencyConverter("EUR", manual_rates={"USD": 0.9})

        result = normalize_currency(
            table_path=table_path,
            fernet_key=fernet_key,
            converter=converter,
        )

        assert result.num_rows == 1
        rate = decrypt_float(result.column("target_fx_rate")[0].as_py(), fernet_key)
        assert rate == pytest.approx(0.92)
        value = decrypt_float(result.column("target_value")[0].as_py(), fernet_key)
        assert value == pytest.approx(920.0)  # 1000 * 0.92
        assert result.column("target_ccy")[0].as_py() == "EUR"

    def test_t212_falls_back_to_converter(self, tmp_path) -> None:
        """T212 events without target_fx_rate use CurrencyConverter."""
        fernet_key = generate_key()
        table = _make_cdc_table(
            [
                {
                    "broker": "Trading 212",
                    "security_ccy": "USD",
                    "cash_amount": 500.0,
                    # No target_fx_rate — T212 doesn't provide it
                }
            ],
            fernet_key,
        )

        from deltalake import write_deltalake

        table_path = str(tmp_path / "cdc_events")
        write_deltalake(table_path, table, mode="overwrite")

        from pipeline.normalized.consolidate import CurrencyConverter

        converter = CurrencyConverter("EUR", manual_rates={"USD": 0.85})

        result = normalize_currency(
            table_path=table_path,
            fernet_key=fernet_key,
            converter=converter,
        )

        assert result.num_rows == 1
        rate = decrypt_float(result.column("target_fx_rate")[0].as_py(), fernet_key)
        assert rate == pytest.approx(0.85)
        value = decrypt_float(result.column("target_value")[0].as_py(), fernet_key)
        assert value == pytest.approx(425.0)  # 500 * 0.85
        assert result.column("target_ccy")[0].as_py() == "EUR"

    def test_mixed_brokers_and_currencies(self, tmp_path) -> None:
        """Mixed IBKR (with rate) and T212 (without rate) events."""
        fernet_key = generate_key()
        table = _make_cdc_table(
            [
                {
                    "broker": "IBKR",
                    "event_id": "ibkr-1",
                    "security_ccy": "USD",
                    "cash_amount": 100.0,
                    "target_fx_rate": 0.92,
                },
                {
                    "broker": "Trading 212",
                    "event_id": "t212-1",
                    "security_ccy": "GBP",
                    "cash_amount": 200.0,
                    # No target_fx_rate
                },
                {
                    "broker": "IBKR",
                    "event_id": "ibkr-2",
                    "security_ccy": "EUR",
                    "cash_amount": 50.0,
                    # Same currency → rate = 1.0
                },
            ],
            fernet_key,
        )

        from deltalake import write_deltalake

        table_path = str(tmp_path / "cdc_events")
        write_deltalake(table_path, table, mode="overwrite")

        from pipeline.normalized.consolidate import CurrencyConverter

        converter = CurrencyConverter("EUR", manual_rates={"USD": 0.90, "GBP": 1.15})

        result = normalize_currency(
            table_path=table_path,
            fernet_key=fernet_key,
            converter=converter,
        )

        assert result.num_rows == 3
        # Find rows by event_id
        event_ids = result.column("event_id").to_pylist()

        # IBKR USD: uses broker rate 0.92
        ibkr_usd_idx = event_ids.index("ibkr-1")
        rate = decrypt_float(
            result.column("target_fx_rate")[ibkr_usd_idx].as_py(), fernet_key
        )
        assert rate == pytest.approx(0.92)
        value = decrypt_float(
            result.column("target_value")[ibkr_usd_idx].as_py(), fernet_key
        )
        assert value == pytest.approx(92.0)  # 100 * 0.92

        # T212 GBP: uses converter rate 1.15
        t212_idx = event_ids.index("t212-1")
        rate = decrypt_float(
            result.column("target_fx_rate")[t212_idx].as_py(), fernet_key
        )
        assert rate == pytest.approx(1.15)
        value = decrypt_float(
            result.column("target_value")[t212_idx].as_py(), fernet_key
        )
        assert value == pytest.approx(230.0)  # 200 * 1.15

        # IBKR EUR: same currency → rate = 1.0
        ibkr_eur_idx = event_ids.index("ibkr-2")
        rate = decrypt_float(
            result.column("target_fx_rate")[ibkr_eur_idx].as_py(), fernet_key
        )
        assert rate == pytest.approx(1.0)
        value = decrypt_float(
            result.column("target_value")[ibkr_eur_idx].as_py(), fernet_key
        )
        assert value == pytest.approx(50.0)  # 50 * 1.0

        # All target_ccy values are EUR
        ccys = result.column("target_ccy").to_pylist()
        assert all(c == "EUR" for c in ccys)

    def test_empty_table_returns_early(self, tmp_path) -> None:
        """An empty CDC events table returns early without error."""
        fernet_key = generate_key()

        # Create an empty table matching the schema
        empty_table = pa.table(
            {
                field.name: pa.array([], type=field.type)
                for field in cdc_events_normalized_schema
            },
            schema=cdc_events_normalized_schema,
        )

        from deltalake import write_deltalake

        table_path = str(tmp_path / "cdc_events")
        write_deltalake(table_path, empty_table, mode="overwrite")

        from pipeline.normalized.consolidate import CurrencyConverter

        converter = CurrencyConverter("EUR")

        result = normalize_currency(
            table_path=table_path,
            fernet_key=fernet_key,
            converter=converter,
        )

        assert result.num_rows == 0

    def test_missing_table_raises_file_not_found(self, tmp_path) -> None:
        """When CDC events table doesn't exist, raises FileNotFoundError."""
        fernet_key = generate_key()

        from pipeline.normalized.consolidate import CurrencyConverter

        converter = CurrencyConverter("EUR")

        with pytest.raises(FileNotFoundError, match="CDC events table not found"):
            normalize_currency(
                table_path=str(tmp_path / "nonexistent"),
                fernet_key=fernet_key,
                converter=converter,
            )

    def test_null_cash_amount_handled_gracefully(self, tmp_path) -> None:
        """Rows with null cash_amount get null target_value and target_fx_rate."""
        fernet_key = generate_key()

        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        records = [
            {
                "fetched_at": now,
                "broker": "IBKR",
                "account_id": "U123",
                "event_id": "evt-null",
                "source": "CashTransaction",
                "event_type": "DIVIDEND",
                "raw_event_type": "Dividends",
                "event_datetime": "2024-01-15",
                "security_ccy": "USD",
                "cash_amount": None,  # null cash_amount
                "settle_date": None,
                "ticker": None,
                "isin": None,
                "description": None,
                "quantity": None,
                "price": None,
                "side": None,
                "gross_amount": None,
                "fee_amount": None,
                "tax_amount": None,
                "target_fx_rate": None,
                "target_value": None,
                "target_ccy": None,
            }
        ]

        # Build a table with null cash_amount manually
        from pipeline.connectors.transform_utils import build_normalized_table

        table = build_normalized_table(
            records,
            cdc_events_normalized_schema,
            fernet_key,
            encrypt_columns=["cash_amount"],
        )

        from deltalake import write_deltalake

        table_path = str(tmp_path / "cdc_events")
        write_deltalake(table_path, table, mode="overwrite")

        from pipeline.normalized.consolidate import CurrencyConverter

        converter = CurrencyConverter("EUR", manual_rates={"USD": 0.9})

        result = normalize_currency(
            table_path=table_path,
            fernet_key=fernet_key,
            converter=converter,
        )

        assert result.num_rows == 1
        # null cash_amount → null target_value and target_fx_rate
        assert result.column("target_fx_rate")[0].as_py() is None
        assert result.column("target_value")[0].as_py() is None
