"""PyArrow schemas for analytics tables."""

from __future__ import annotations

import pyarrow as pa

portfolio_allocation_schema = pa.schema(
    [
        pa.field("calculated_at", pa.timestamp("us", tz="UTC")),
        pa.field("ticker", pa.string()),
        pa.field("percentage", pa.float64()),
        pa.field("broker", pa.string()),
        pa.field("identifier", pa.string()),
        pa.field("security_currency", pa.string()),
        pa.field("description", pa.string()),
    ]
)

portfolio_holdings_schema = pa.schema(
    [
        pa.field("calculated_at", pa.timestamp("us", tz="UTC")),
        pa.field("broker", pa.string()),
        pa.field("ticker", pa.string()),
        pa.field(
            "value_currency", pa.string()
        ),  # native holding currency (from snapshot)
        pa.field("value", pa.float64()),  # decrypted native-currency value
        pa.field(
            "value_base", pa.float64()
        ),  # base-currency value (from consolidated_holdings)
        pa.field(
            "base_currency", pa.string()
        ),  # == consolidated_holdings.base_currency
        pa.field("position_type", pa.string()),  # EQUITY | CASH | UNKNOWN
        pa.field("identifier", pa.string()),
        pa.field("security_currency", pa.string()),
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
        pa.field("value_currency", pa.string()),
        pa.field("cash_amount", pa.float64()),
        pa.field("amount_base", pa.float64(), nullable=True),
        pa.field("base_currency", pa.string(), nullable=True),
        pa.field("event_count", pa.int64()),
    ]
)

interest_income_schema = pa.schema(
    [
        pa.field("calculated_at", pa.timestamp("us", tz="UTC")),
        pa.field("period_month", pa.string()),  # YYYY-MM
        pa.field("period_quarter", pa.string()),  # YYYY-QN
        pa.field("broker", pa.string()),
        pa.field("value_currency", pa.string()),
        pa.field("cash_amount", pa.float64()),
        pa.field("amount_base", pa.float64(), nullable=True),
        pa.field("base_currency", pa.string(), nullable=True),
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
        pa.field("value_currency", pa.string()),
        pa.field("cash_amount", pa.float64()),
        pa.field("amount_base", pa.float64(), nullable=True),
        pa.field("base_currency", pa.string(), nullable=True),
        pa.field("event_count", pa.int64()),
    ]
)
