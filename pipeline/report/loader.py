"""Load analytics gold tables into Polars DataFrames via DuckDB.

Each loader function reads a gold Delta table through the query connection
and returns a Polars DataFrame.  Missing tables return an empty DataFrame
with the expected columns so the report can render partial data gracefully.

Gold value columns (``security_value``, ``target_value``, ``cash_amount``)
are stored as Fernet-encrypted ``pa.binary()``.  The loader decrypts them
after reading so that chart and renderer code receives plaintext floats.
"""

from __future__ import annotations

import logging

import polars as pl

logger = logging.getLogger(__name__)

# Expected column schemas for empty fallback DataFrames.
_PORTFOLIO_HOLDINGS_COLUMNS = {
    "calculated_at": pl.Datetime("us", "UTC"),
    "broker": pl.String,
    "ticker": pl.String,
    "security_ccy": pl.String,
    "security_value": pl.Binary,  # Fernet-encrypted
    "target_value": pl.Binary,  # Fernet-encrypted
    "target_ccy": pl.String,
    "percentage": pl.Float64,  # plaintext
    "position_type": pl.String,
    "identifier": pl.String,
    "description": pl.String,
}

_DIVIDEND_INCOME_COLUMNS = {
    "calculated_at": pl.Datetime("us", "UTC"),
    "period_month": pl.String,
    "period_quarter": pl.String,
    "broker": pl.String,
    "ticker": pl.String,
    "isin": pl.String,
    "description": pl.String,
    "security_ccy": pl.String,
    "cash_amount": pl.Binary,  # Fernet-encrypted
    "target_value": pl.Binary,  # Fernet-encrypted
    "target_ccy": pl.String,
    "event_count": pl.Int64,
}

_INTEREST_INCOME_COLUMNS = {
    "calculated_at": pl.Datetime("us", "UTC"),
    "period_month": pl.String,
    "period_quarter": pl.String,
    "broker": pl.String,
    "security_ccy": pl.String,
    "cash_amount": pl.Binary,  # Fernet-encrypted
    "target_value": pl.Binary,  # Fernet-encrypted
    "target_ccy": pl.String,
    "event_count": pl.Int64,
}

_CASH_FLOW_SUMMARY_COLUMNS = {
    "calculated_at": pl.Datetime("us", "UTC"),
    "period_month": pl.String,
    "period_quarter": pl.String,
    "broker": pl.String,
    "event_type": pl.String,
    "security_ccy": pl.String,
    "cash_amount": pl.Binary,  # Fernet-encrypted
    "target_value": pl.Binary,  # Fernet-encrypted
    "target_ccy": pl.String,
    "event_count": pl.Int64,
}

_DATA_QUALITY_COLUMNS = {
    "checked_at": pl.Datetime("us", "UTC"),
    "table_name": pl.String,
    "check_name": pl.String,
    "status": pl.String,
    "details": pl.String,
    "threshold": pl.String,
    "actual": pl.String,
}


def _empty_df(columns: dict[str, pl.DataType]) -> pl.DataFrame:
    """Create an empty DataFrame with the given column names and types."""
    return pl.DataFrame(
        {name: pl.Series([], dtype=dtype) for name, dtype in columns.items()}
    )


def _load_table(view_name: str, columns: dict[str, pl.DataType]) -> pl.DataFrame:
    """Load a Delta table via DuckDB; return empty DataFrame on failure."""
    from pipeline.query import get_connection, refresh

    refresh()
    conn = get_connection()
    try:
        return conn.sql(f"SELECT * FROM {view_name}").pl()
    except Exception:
        logger.warning("%s not available; report section will be empty", view_name)
        return _empty_df(columns)


def load_portfolio_holdings() -> pl.DataFrame:
    """Load the ``portfolio_holdings`` analytics table."""
    return _load_table("portfolio_holdings_analytics", _PORTFOLIO_HOLDINGS_COLUMNS)


def load_dividend_income() -> pl.DataFrame:
    """Load the ``dividend_income`` analytics table."""
    return _load_table("dividend_income_analytics", _DIVIDEND_INCOME_COLUMNS)


def load_interest_income() -> pl.DataFrame:
    """Load the ``interest_income`` analytics table."""
    return _load_table("interest_income_analytics", _INTEREST_INCOME_COLUMNS)


def load_cash_flow_summary() -> pl.DataFrame:
    """Load the ``cash_flow_summary`` analytics table."""
    return _load_table("cash_flow_summary_analytics", _CASH_FLOW_SUMMARY_COLUMNS)


def load_data_quality() -> pl.DataFrame:
    """Load the ``data_quality`` analytics table."""
    return _load_table("data_quality_analytics", _DATA_QUALITY_COLUMNS)


def load_all() -> dict[str, pl.DataFrame]:
    """Load all analytics tables needed by the report.

    Returns a dict keyed by table name; missing tables produce empty DataFrames.
    Gold value columns are decrypted after loading so that chart and renderer
    code receives plaintext floats.
    """
    from pipeline.query import decrypt_df

    tables = {
        "portfolio_holdings": load_portfolio_holdings(),
        "dividend_income": load_dividend_income(),
        "interest_income": load_interest_income(),
        "cash_flow_summary": load_cash_flow_summary(),
        "data_quality": load_data_quality(),
    }
    # Decrypt Fernet-encrypted value columns in gold tables.
    # Decision: docs/adr/0084-encrypt-gold-value-columns.md
    for key in (
        "portfolio_holdings",
        "dividend_income",
        "interest_income",
        "cash_flow_summary",
    ):
        if not tables[key].is_empty():
            tables[key] = decrypt_df(tables[key])

    return tables
