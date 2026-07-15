"""PyArrow schemas for analytics tables.

Phase 2 (Currency Unification) replaces overloaded column names:
- ``value_currency`` → ``security_ccy`` (instrument's trading currency)
- ``value`` / ``value_base`` → ``security_value`` / ``target_value``
- ``base_currency`` → ``target_ccy`` (always EUR)
- ``amount_base`` → ``target_value``
- ``security_currency`` → ``security_ccy``
"""

from __future__ import annotations

import pyarrow as pa

portfolio_allocation_schema = pa.schema(
    [
        pa.field("calculated_at", pa.timestamp("us", tz="UTC")),
        pa.field("ticker", pa.string()),
        pa.field("percentage", pa.float64()),
        pa.field("broker", pa.string()),
        pa.field("identifier", pa.string()),
        pa.field("security_ccy", pa.string()),
        pa.field("description", pa.string()),
    ]
)

portfolio_holdings_schema = pa.schema(
    [
        pa.field("calculated_at", pa.timestamp("us", tz="UTC")),
        pa.field("broker", pa.string()),
        pa.field("ticker", pa.string()),
        pa.field(
            "security_ccy", pa.string()
        ),  # native holding currency (from snapshot)
        pa.field("security_value", pa.float64()),  # decrypted native-currency value
        pa.field(
            "target_value", pa.float64()
        ),  # value in target_ccy (from consolidated_holdings)
        pa.field("target_ccy", pa.string()),  # == consolidated_holdings.target_ccy
        pa.field("position_type", pa.string()),  # EQUITY | CASH
        pa.field("identifier", pa.string()),
        pa.field("description", pa.string()),
    ]
)

data_quality_schema = pa.schema(
    [
        pa.field("checked_at", pa.timestamp("us", tz="UTC")),
        pa.field("table_name", pa.string()),
        pa.field("check_name", pa.string()),
        pa.field("status", pa.string()),  # PASS | FAIL | WARN
        pa.field("details", pa.string()),
        pa.field("threshold", pa.string(), nullable=True),
        pa.field("actual", pa.string(), nullable=True),
    ]
)

# --- CDC analytics tables (gold layer) ---

dividend_income_schema = pa.schema(
    [
        pa.field("calculated_at", pa.timestamp("us", tz="UTC")),
        pa.field("period_month", pa.string()),  # YYYY-MM
        pa.field("period_quarter", pa.string()),  # YYYY-QN
        pa.field("broker", pa.string()),
        pa.field("ticker", pa.string(), nullable=True),
        pa.field("isin", pa.string(), nullable=True),
        pa.field("description", pa.string(), nullable=True),
        pa.field("security_ccy", pa.string()),  # Amount currency
        pa.field(
            "instrument_ccy", pa.string(), nullable=True
        ),  # Instrument's trading currency
        pa.field("cash_amount", pa.float64()),
        pa.field("target_value", pa.float64(), nullable=True),
        pa.field("target_ccy", pa.string(), nullable=True),
        pa.field("event_count", pa.int64()),
    ]
)

interest_income_schema = pa.schema(
    [
        pa.field("calculated_at", pa.timestamp("us", tz="UTC")),
        pa.field("period_month", pa.string()),  # YYYY-MM
        pa.field("period_quarter", pa.string()),  # YYYY-QN
        pa.field("broker", pa.string()),
        pa.field("security_ccy", pa.string()),
        pa.field("cash_amount", pa.float64()),
        pa.field("target_value", pa.float64(), nullable=True),
        pa.field("target_ccy", pa.string(), nullable=True),
        pa.field("event_count", pa.int64()),
    ]
)

cash_flow_summary_schema = pa.schema(
    [
        pa.field("calculated_at", pa.timestamp("us", tz="UTC")),
        pa.field("period_month", pa.string()),  # YYYY-MM
        pa.field("period_quarter", pa.string()),  # YYYY-QN
        pa.field("broker", pa.string()),
        pa.field("event_type", pa.string()),
        pa.field("security_ccy", pa.string()),
        pa.field("cash_amount", pa.float64()),
        pa.field("target_value", pa.float64(), nullable=True),
        pa.field("target_ccy", pa.string(), nullable=True),
        pa.field("event_count", pa.int64()),
    ]
)
