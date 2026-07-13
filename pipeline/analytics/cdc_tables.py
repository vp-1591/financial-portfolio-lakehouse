"""CDC events → gold analytics tables.

Reads the ``cdc_events`` Delta table, decrypts encrypted columns,
and builds three analytics tables:

- ``dividend_income`` — dividends by period, broker, and security
- ``interest_income`` — interest by period and broker
- ``cash_flow_summary`` — all CDC events aggregated by period and type

Each table is written to the analytics layer in overwrite mode.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from pipeline.analytics.models import (
    cash_flow_summary_schema,
    dividend_income_schema,
    interest_income_schema,
)
from pipeline.crypto import decrypt_float

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Binary (Fernet-encrypted) columns in cdc_events that need decryption.
_ENCRYPTED_COLUMNS: list[tuple[str, str]] = [
    ("cash_amount", "cash_amount_decrypted"),
    ("amount_base", "amount_base_decrypted"),
    ("fx_rate_to_base", "fx_rate_to_base_decrypted"),
    ("gross_amount", "gross_amount_decrypted"),
    ("fee_amount", "fee_amount_decrypted"),
    ("tax_amount", "tax_amount_decrypted"),
    ("net_amount", "net_amount_decrypted"),
    ("quantity", "quantity_decrypted"),
    ("price", "price_decrypted"),
]


def _decrypt_column(
    df: pl.DataFrame, col: str, alias: str, fernet_key: bytes
) -> pl.DataFrame:
    """Decrypt a single Fernet-encrypted binary column to Float64."""
    return df.with_columns(
        pl.col(col)
        .map_elements(
            lambda v: decrypt_float(v, fernet_key) if v is not None else None,
            return_dtype=pl.Float64,
        )
        .alias(alias)
    )


def _add_period_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Add ``period_month`` (YYYY-MM) and ``period_quarter`` (YYYY-QN) from ``event_datetime``.

    Handles broker-specific date formats:
    - IBKR: ``2026-03-01 00:00:00``
    - XTB / date-only: ``2024-01-15``
    - T212 ISO: ``2024-01-15T10:30:00Z``
    """
    # Replace trailing 'Z' with '+00:00' so that str.strptime can parse it
    # with the %z directive.  Polars does not recognise bare 'Z' as UTC.
    df = df.with_columns(
        pl.col("event_datetime").str.replace("Z", "+00:00").alias("event_datetime")
    )

    # Parse each format separately to avoid Polars SchemaError when combining
    # timezone-aware and timezone-naive datetimes in a single column.
    # All parsed values are converted to UTC to produce a consistent type.
    parsed_ibkr = pl.col("event_datetime").str.strptime(
        pl.Datetime("us", "UTC"), "%Y-%m-%d %H:%M:%S", strict=False
    )
    parsed_iso = pl.col("event_datetime").str.strptime(
        pl.Datetime("us", "UTC"), "%Y-%m-%dT%H:%M:%S%.f%z", strict=False
    )
    parsed_date = pl.col("event_datetime").str.strptime(
        pl.Datetime("us", "UTC"), "%Y-%m-%d", strict=False
    )

    # Coalesce: try IBKR format first, then ISO, then date-only.
    parsed = pl.coalesce([parsed_ibkr, parsed_iso, parsed_date]).alias("_event_dt")

    df = df.with_columns(parsed)

    # Warn about unparseable dates.
    null_count = df.filter(pl.col("_event_dt").is_null()).height
    if null_count > 0:
        logger.warning(
            "Could not parse event_datetime for %d rows; they will be excluded from aggregation",
            null_count,
        )

    # Drop rows with unparseable dates before aggregation.
    df = df.filter(pl.col("_event_dt").is_not_null())

    df = df.with_columns(
        [
            pl.col("_event_dt").dt.strftime("%Y-%m").alias("period_month"),
            (
                pl.col("_event_dt").dt.year().cast(pl.String)
                + "-Q"
                + pl.col("_event_dt").dt.quarter().cast(pl.String)
            ).alias("period_quarter"),
        ]
    ).drop("_event_dt")

    return df


def _read_cdc_events(
    table_path: str | None = None,
    fernet_key: bytes | None = None,
) -> pl.DataFrame:
    """Read ``cdc_events`` Delta table, decrypt binary columns, add period columns.

    Returns a Polars DataFrame with decrypted float columns and
    ``period_month``/``period_quarter`` columns added.
    """
    if table_path is None:
        from pipeline.storage import get_storage

        table_path = get_storage().normalized_path("cdc_events")

    if fernet_key is None:
        from pipeline.crypto import load_key

        fernet_key = load_key()

    from pipeline.storage import get_storage

    storage_opts = get_storage().storage_options

    try:
        dt = DeltaTable(table_path, storage_options=storage_opts)
    except Exception as exc:
        raise FileNotFoundError(
            f"CDC events table not found at {table_path}. "
            "Run the consolidate-cdc step first to populate the table."
        ) from exc

    arrow_table = dt.to_pyarrow_table()
    df = pl.from_arrow(arrow_table)

    # Decrypt all binary columns.
    for col, alias in _ENCRYPTED_COLUMNS:
        if col in df.columns:
            df = _decrypt_column(df, col, alias, fernet_key)

    # Resolve amount_base: fall back to cash_amount * fx_rate_to_base where null.
    df = df.with_columns(
        pl.when(pl.col("amount_base_decrypted").is_null())
        .then(pl.col("cash_amount_decrypted") * pl.col("fx_rate_to_base_decrypted"))
        .otherwise(pl.col("amount_base_decrypted"))
        .alias("amount_base_resolved")
    )

    # Add period columns.
    df = _add_period_columns(df)

    return df


def _write_analytics_table(
    result: pa.Table,
    schema: pa.Schema,
    analytics_path: str,
) -> pa.Table:
    """Write an analytics table to Delta, casting types to match the schema."""
    from pipeline.storage import get_storage

    storage_opts = get_storage().storage_options
    get_storage().backend.ensure_parent(analytics_path)

    # Cast result columns to match the expected schema types.
    # This handles cases like Int64 → Float64 for aggregated sums.
    casted = {}
    for i, field in enumerate(schema):
        col_name = field.name
        if col_name in result.column_names:
            casted[col_name] = result.column(col_name).cast(field.type)
        else:
            # Shouldn't happen if the table was built correctly.
            raise ValueError(f"Missing column {col_name} in result table")

    result = pa.table(casted, schema=schema)

    write_deltalake(
        analytics_path, result, mode="overwrite", storage_options=storage_opts
    )
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_dividend_income(
    table_path: str | None = None,
    fernet_key: bytes | None = None,
    analytics_path: str | None = None,
) -> pa.Table:
    """Build the ``dividend_income`` analytics table from CDC events.

    Groups DIVIDEND events by period, broker, security, and currency.
    """
    if analytics_path is None:
        from pipeline.storage import get_storage

        analytics_path = get_storage().analytics_path("dividend_income")

    df = _read_cdc_events(table_path=table_path, fernet_key=fernet_key)
    df = df.filter(pl.col("event_type") == "DIVIDEND")

    if df.is_empty():
        logger.warning(
            "No DIVIDEND events found in CDC data; dividend_income table will be empty"
        )

    now = datetime.now(timezone.utc)

    if df.is_empty():
        # Write an empty table with the correct schema.
        result = pa.table(
            {
                "calculated_at": pa.array([], type=pa.timestamp("us", tz="UTC")),
                "period_month": pa.array([], type=pa.string()),
                "period_quarter": pa.array([], type=pa.string()),
                "broker": pa.array([], type=pa.string()),
                "ticker": pa.array([], type=pa.string()),
                "isin": pa.array([], type=pa.string()),
                "description": pa.array([], type=pa.string()),
                "currency": pa.array([], type=pa.string()),
                "cash_amount": pa.array([], type=pa.float64()),
                "amount_base": pa.array([], type=pa.float64()),
                "base_currency": pa.array([], type=pa.string()),
                "event_count": pa.array([], type=pa.int64()),
            },
            schema=dividend_income_schema,
        )
    else:
        agg = (
            df.group_by(
                [
                    "period_month",
                    "period_quarter",
                    "broker",
                    "ticker",
                    "isin",
                    "description",
                    "currency",
                    "base_currency",
                ]
            )
            .agg(
                [
                    pl.col("cash_amount_decrypted").sum().alias("cash_amount"),
                    # Use sum() then replace 0.0 with null when all source values
                    # were null — Polars sum() on all-null Float64 returns 0.0.
                    pl.when(pl.col("amount_base_resolved").null_count() == pl.len())
                    .then(None)
                    .otherwise(pl.col("amount_base_resolved").sum())
                    .alias("amount_base"),
                    pl.col("event_id").count().alias("event_count"),
                ]
            )
            .sort(["period_month", "broker", "ticker"])
        )
        # Cast amount_base to Float64 — Polars sum() on an all-null column
        # produces Null type, which breaks PyArrow schema inference.
        agg = agg.with_columns(pl.col("amount_base").cast(pl.Float64))

        result = pa.table(
            {
                "calculated_at": [now] * len(agg),
                "period_month": agg["period_month"].to_list(),
                "period_quarter": agg["period_quarter"].to_list(),
                "broker": agg["broker"].to_list(),
                "ticker": agg["ticker"].to_list(),
                "isin": agg["isin"].to_list(),
                "description": agg["description"].to_list(),
                "currency": agg["currency"].to_list(),
                "cash_amount": agg["cash_amount"].to_list(),
                "amount_base": agg["amount_base"].to_list(),
                "base_currency": agg["base_currency"].to_list(),
                "event_count": agg["event_count"].to_list(),
            },
            schema=dividend_income_schema,
        )

    return _write_analytics_table(result, dividend_income_schema, analytics_path)


def build_interest_income(
    table_path: str | None = None,
    fernet_key: bytes | None = None,
    analytics_path: str | None = None,
) -> pa.Table:
    """Build the ``interest_income`` analytics table from CDC events.

    Groups INTEREST events by period, broker, and currency.
    """
    if analytics_path is None:
        from pipeline.storage import get_storage

        analytics_path = get_storage().analytics_path("interest_income")

    df = _read_cdc_events(table_path=table_path, fernet_key=fernet_key)
    df = df.filter(pl.col("event_type") == "INTEREST")

    if df.is_empty():
        logger.warning(
            "No INTEREST events found in CDC data; interest_income table will be empty"
        )

    now = datetime.now(timezone.utc)

    if df.is_empty():
        result = pa.table(
            {
                "calculated_at": pa.array([], type=pa.timestamp("us", tz="UTC")),
                "period_month": pa.array([], type=pa.string()),
                "period_quarter": pa.array([], type=pa.string()),
                "broker": pa.array([], type=pa.string()),
                "currency": pa.array([], type=pa.string()),
                "cash_amount": pa.array([], type=pa.float64()),
                "amount_base": pa.array([], type=pa.float64()),
                "base_currency": pa.array([], type=pa.string()),
                "event_count": pa.array([], type=pa.int64()),
            },
            schema=interest_income_schema,
        )
    else:
        agg = (
            df.group_by(
                [
                    "period_month",
                    "period_quarter",
                    "broker",
                    "currency",
                    "base_currency",
                ]
            )
            .agg(
                [
                    pl.col("cash_amount_decrypted").sum().alias("cash_amount"),
                    # Use sum() then replace 0.0 with null when all source values
                    # were null — Polars sum() on all-null Float64 returns 0.0.
                    pl.when(pl.col("amount_base_resolved").null_count() == pl.len())
                    .then(None)
                    .otherwise(pl.col("amount_base_resolved").sum())
                    .alias("amount_base"),
                    pl.col("event_id").count().alias("event_count"),
                ]
            )
            .sort(["period_month", "broker", "currency"])
        )
        # Cast amount_base to Float64 — Polars sum() on an all-null column
        # produces Null type, which breaks PyArrow schema inference.
        agg = agg.with_columns(pl.col("amount_base").cast(pl.Float64))

        result = pa.table(
            {
                "calculated_at": [now] * len(agg),
                "period_month": agg["period_month"].to_list(),
                "period_quarter": agg["period_quarter"].to_list(),
                "broker": agg["broker"].to_list(),
                "currency": agg["currency"].to_list(),
                "cash_amount": agg["cash_amount"].to_list(),
                "amount_base": agg["amount_base"].to_list(),
                "base_currency": agg["base_currency"].to_list(),
                "event_count": agg["event_count"].to_list(),
            },
            schema=interest_income_schema,
        )

    return _write_analytics_table(result, interest_income_schema, analytics_path)


def build_cash_flow_summary(
    table_path: str | None = None,
    fernet_key: bytes | None = None,
    analytics_path: str | None = None,
) -> pa.Table:
    """Build the ``cash_flow_summary`` analytics table from CDC events.

    Groups all events by period, broker, event type, and currency.
    """
    if analytics_path is None:
        from pipeline.storage import get_storage

        analytics_path = get_storage().analytics_path("cash_flow_summary")

    df = _read_cdc_events(table_path=table_path, fernet_key=fernet_key)

    if df.is_empty():
        logger.warning("No CDC events found; cash_flow_summary table will be empty")

    now = datetime.now(timezone.utc)

    if df.is_empty():
        result = pa.table(
            {
                "calculated_at": pa.array([], type=pa.timestamp("us", tz="UTC")),
                "period_month": pa.array([], type=pa.string()),
                "period_quarter": pa.array([], type=pa.string()),
                "broker": pa.array([], type=pa.string()),
                "event_type": pa.array([], type=pa.string()),
                "currency": pa.array([], type=pa.string()),
                "cash_amount": pa.array([], type=pa.float64()),
                "amount_base": pa.array([], type=pa.float64()),
                "base_currency": pa.array([], type=pa.string()),
                "event_count": pa.array([], type=pa.int64()),
            },
            schema=cash_flow_summary_schema,
        )
    else:
        agg = (
            df.group_by(
                [
                    "period_month",
                    "period_quarter",
                    "broker",
                    "event_type",
                    "currency",
                    "base_currency",
                ]
            )
            .agg(
                [
                    pl.col("cash_amount_decrypted").sum().alias("cash_amount"),
                    # Use sum() then replace 0.0 with null when all source values
                    # were null — Polars sum() on all-null Float64 returns 0.0.
                    pl.when(pl.col("amount_base_resolved").null_count() == pl.len())
                    .then(None)
                    .otherwise(pl.col("amount_base_resolved").sum())
                    .alias("amount_base"),
                    pl.col("event_id").count().alias("event_count"),
                ]
            )
            .sort(["period_month", "broker", "event_type"])
        )
        # Cast amount_base to Float64 — Polars sum() on an all-null column
        # produces Null type, which breaks PyArrow schema inference.
        agg = agg.with_columns(pl.col("amount_base").cast(pl.Float64))

        result = pa.table(
            {
                "calculated_at": [now] * len(agg),
                "period_month": agg["period_month"].to_list(),
                "period_quarter": agg["period_quarter"].to_list(),
                "broker": agg["broker"].to_list(),
                "event_type": agg["event_type"].to_list(),
                "currency": agg["currency"].to_list(),
                "cash_amount": agg["cash_amount"].to_list(),
                "amount_base": agg["amount_base"].to_list(),
                "base_currency": agg["base_currency"].to_list(),
                "event_count": agg["event_count"].to_list(),
            },
            schema=cash_flow_summary_schema,
        )

    return _write_analytics_table(result, cash_flow_summary_schema, analytics_path)
