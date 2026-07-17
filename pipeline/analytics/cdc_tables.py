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
from pipeline.crypto import decrypt_float, encrypt_float

logger = logging.getLogger(__name__)


def _resolve_fernet_key(fernet_key: bytes | None) -> bytes:
    """Return *fernet_key* as-is, or load from the default key file."""
    if fernet_key is None:
        from pipeline.crypto import load_key

        return load_key()
    return fernet_key


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Binary (Fernet-encrypted) columns in cdc_events that need decryption.
_ENCRYPTED_COLUMNS: list[tuple[str, str]] = [
    ("cash_amount", "cash_amount_decrypted"),
    ("target_fx_rate", "target_fx_rate_decrypted"),
    ("target_value", "target_value_decrypted"),
    ("gross_amount", "gross_amount_decrypted"),
    ("fee_amount", "fee_amount_decrypted"),
    ("tax_amount", "tax_amount_decrypted"),
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


def _encrypt_gold_values(
    df: pl.DataFrame, columns: list[str], fernet_key: bytes
) -> pl.DataFrame:
    """Encrypt float columns to binary Fernet tokens for gold-layer storage.

    Decision: docs/adr/0084-encrypt-gold-value-columns.md
    """
    for col in columns:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col)
                .map_elements(
                    lambda v, _k=fernet_key: (
                        encrypt_float(v, _k) if v is not None else None
                    ),
                    return_dtype=pl.Binary,
                )
                .alias(col),
            )
    return df


def _add_period_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Add ``period_month`` (YYYY-MM) and ``period_quarter`` (YYYY-QN) from ``event_datetime``.

    Handles broker-specific date formats:
    - IBKR compact datetime: ``20260702;022904``
    - IBKR compact date: ``20260204``
    - IBKR SQL datetime: ``2026-03-01 00:00:00``
    - XTB / date-only: ``2024-01-15``
    - T212 ISO: ``2024-01-15T10:30:00Z``
    """
    # Replace trailing 'Z' with '+00:00' so that str.strptime can parse it
    # with the %z directive.  Polars does not recognise bare 'Z' as UTC.
    df = df.with_columns(
        pl.col("event_datetime").str.replace("Z", "+00:00").alias("event_datetime")
    )

    # Replace semicolons in IBKR compact datetime format (e.g. "20260702;022904")
    # with spaces so that str.strptime can parse it with %Y%m%d %H%M%S.
    # Polars does not support semicolons as literal separators in format strings.
    df = df.with_columns(
        pl.col("event_datetime").str.replace(";", " ").alias("event_datetime")
    )

    # Parse each format separately to avoid Polars SchemaError when combining
    # timezone-aware and timezone-naive datetimes in a single column.
    # All parsed values are converted to UTC to produce a consistent type.
    # Compact formats are tried first since they are more specific.
    parsed_ibkr_compact_dt = pl.col("event_datetime").str.strptime(
        pl.Datetime("us", "UTC"), "%Y%m%d %H%M%S", strict=False
    )
    parsed_ibkr_compact_date = pl.col("event_datetime").str.strptime(
        pl.Datetime("us", "UTC"), "%Y%m%d", strict=False
    )
    parsed_ibkr = pl.col("event_datetime").str.strptime(
        pl.Datetime("us", "UTC"), "%Y-%m-%d %H:%M:%S", strict=False
    )
    parsed_iso = pl.col("event_datetime").str.strptime(
        pl.Datetime("us", "UTC"), "%Y-%m-%dT%H:%M:%S%.f%z", strict=False
    )
    parsed_date = pl.col("event_datetime").str.strptime(
        pl.Datetime("us", "UTC"), "%Y-%m-%d", strict=False
    )

    # Coalesce: try each format, first match wins.
    parsed = pl.coalesce(
        [
            parsed_ibkr_compact_dt,
            parsed_ibkr_compact_date,
            parsed_ibkr,
            parsed_iso,
            parsed_date,
        ]
    ).alias("_event_dt")

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

    fernet_key = _resolve_fernet_key(fernet_key)

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
    # pl.from_arrow returns DataFrame | Series; a PyArrow Table always yields a DataFrame.
    assert isinstance(df, pl.DataFrame)

    # Decrypt all binary columns.
    for col, alias in _ENCRYPTED_COLUMNS:
        if col in df.columns:
            df = _decrypt_column(df, col, alias, fernet_key)

    # Resolve target_value: fall back to cash_amount * target_fx_rate where null.
    # This handles rows where normalize_currency() hasn't been run yet.
    if "target_value_decrypted" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("target_value_decrypted").is_null())
            .then(pl.col("cash_amount_decrypted") * pl.col("target_fx_rate_decrypted"))
            .otherwise(pl.col("target_value_decrypted"))
            .alias("target_value_resolved")
        )
    else:
        # No target columns at all — fall back to cash_amount
        df = df.with_columns(
            pl.col("cash_amount_decrypted").alias("target_value_resolved")
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
    fernet_key = _resolve_fernet_key(fernet_key)
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
                "security_ccy": pa.array([], type=pa.string()),
                "instrument_ccy": pa.array([], type=pa.string()),
                "cash_amount": pa.array([], type=pa.binary()),
                "target_value": pa.array([], type=pa.binary()),
                "target_ccy": pa.array([], type=pa.string()),
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
                    "security_ccy",
                ]
            )
            .agg(
                [
                    pl.col("cash_amount_decrypted").sum().alias("cash_amount"),
                    # Use sum() then replace 0.0 with null when all source values
                    # were null — Polars sum() on all-null Float64 returns 0.0.
                    pl.when(pl.col("target_value_resolved").null_count() == pl.len())
                    .then(None)
                    .otherwise(pl.col("target_value_resolved").sum())
                    .alias("target_value"),
                    # Take the first non-null target_ccy in each group.
                    pl.col("target_ccy")
                    .filter(pl.col("target_ccy").is_not_null())
                    .first()
                    .alias("target_ccy"),
                    # Take the first non-null instrument_ccy in each group (display column).
                    pl.col("instrument_ccy")
                    .filter(pl.col("instrument_ccy").is_not_null())
                    .first()
                    .alias("instrument_ccy"),
                    pl.col("event_id").count().alias("event_count"),
                ]
            )
            .sort(["period_month", "broker", "ticker"])
        )
        # Cast target_value to Float64 — Polars sum() on an all-null column
        # produces Null type, which breaks PyArrow schema inference.
        agg = agg.with_columns(pl.col("target_value").cast(pl.Float64))

        # Fill null target_ccy with the target currency string.
        # This handles groups where all events had null target_ccy (pre-normalize).
        # Get the first non-null target_ccy from the full dataset as a fallback.
        target_ccy_values = df.filter(pl.col("target_ccy").is_not_null())["target_ccy"]
        default_target_ccy = (
            target_ccy_values[0] if len(target_ccy_values) > 0 else "EUR"
        )
        agg = agg.with_columns(
            pl.when(pl.col("target_ccy").is_null())
            .then(pl.lit(default_target_ccy))
            .otherwise(pl.col("target_ccy"))
            .alias("target_ccy")
        )

        # Encrypt gold value columns before writing.
        agg = _encrypt_gold_values(agg, ["cash_amount", "target_value"], fernet_key)

        result = pa.table(
            {
                "calculated_at": [now] * len(agg),
                "period_month": agg["period_month"].to_list(),
                "period_quarter": agg["period_quarter"].to_list(),
                "broker": agg["broker"].to_list(),
                "ticker": agg["ticker"].to_list(),
                "isin": agg["isin"].to_list(),
                "description": agg["description"].to_list(),
                "security_ccy": agg["security_ccy"].to_list(),
                "instrument_ccy": agg["instrument_ccy"].to_list(),
                "cash_amount": agg["cash_amount"].to_list(),
                "target_value": agg["target_value"].to_list(),
                "target_ccy": agg["target_ccy"].to_list(),
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
    fernet_key = _resolve_fernet_key(fernet_key)
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
                "security_ccy": pa.array([], type=pa.string()),
                "cash_amount": pa.array([], type=pa.binary()),
                "target_value": pa.array([], type=pa.binary()),
                "target_ccy": pa.array([], type=pa.string()),
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
                    "security_ccy",
                ]
            )
            .agg(
                [
                    pl.col("cash_amount_decrypted").sum().alias("cash_amount"),
                    pl.when(pl.col("target_value_resolved").null_count() == pl.len())
                    .then(None)
                    .otherwise(pl.col("target_value_resolved").sum())
                    .alias("target_value"),
                    pl.col("target_ccy")
                    .filter(pl.col("target_ccy").is_not_null())
                    .first()
                    .alias("target_ccy"),
                    pl.col("event_id").count().alias("event_count"),
                ]
            )
            .sort(["period_month", "broker", "security_ccy"])
        )
        agg = agg.with_columns(pl.col("target_value").cast(pl.Float64))

        target_ccy_values = df.filter(pl.col("target_ccy").is_not_null())["target_ccy"]
        default_target_ccy = (
            target_ccy_values[0] if len(target_ccy_values) > 0 else "EUR"
        )
        agg = agg.with_columns(
            pl.when(pl.col("target_ccy").is_null())
            .then(pl.lit(default_target_ccy))
            .otherwise(pl.col("target_ccy"))
            .alias("target_ccy")
        )

        # Encrypt gold value columns before writing.
        agg = _encrypt_gold_values(agg, ["cash_amount", "target_value"], fernet_key)

        result = pa.table(
            {
                "calculated_at": [now] * len(agg),
                "period_month": agg["period_month"].to_list(),
                "period_quarter": agg["period_quarter"].to_list(),
                "broker": agg["broker"].to_list(),
                "security_ccy": agg["security_ccy"].to_list(),
                "cash_amount": agg["cash_amount"].to_list(),
                "target_value": agg["target_value"].to_list(),
                "target_ccy": agg["target_ccy"].to_list(),
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
    fernet_key = _resolve_fernet_key(fernet_key)
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
                "security_ccy": pa.array([], type=pa.string()),
                "cash_amount": pa.array([], type=pa.binary()),
                "target_value": pa.array([], type=pa.binary()),
                "target_ccy": pa.array([], type=pa.string()),
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
                    "security_ccy",
                ]
            )
            .agg(
                [
                    pl.col("cash_amount_decrypted").sum().alias("cash_amount"),
                    pl.when(pl.col("target_value_resolved").null_count() == pl.len())
                    .then(None)
                    .otherwise(pl.col("target_value_resolved").sum())
                    .alias("target_value"),
                    pl.col("target_ccy")
                    .filter(pl.col("target_ccy").is_not_null())
                    .first()
                    .alias("target_ccy"),
                    pl.col("event_id").count().alias("event_count"),
                ]
            )
            .sort(["period_month", "broker", "event_type"])
        )
        agg = agg.with_columns(pl.col("target_value").cast(pl.Float64))

        target_ccy_values = df.filter(pl.col("target_ccy").is_not_null())["target_ccy"]
        default_target_ccy = (
            target_ccy_values[0] if len(target_ccy_values) > 0 else "EUR"
        )
        agg = agg.with_columns(
            pl.when(pl.col("target_ccy").is_null())
            .then(pl.lit(default_target_ccy))
            .otherwise(pl.col("target_ccy"))
            .alias("target_ccy")
        )

        # Encrypt gold value columns before writing.
        agg = _encrypt_gold_values(agg, ["cash_amount", "target_value"], fernet_key)

        result = pa.table(
            {
                "calculated_at": [now] * len(agg),
                "period_month": agg["period_month"].to_list(),
                "period_quarter": agg["period_quarter"].to_list(),
                "broker": agg["broker"].to_list(),
                "event_type": agg["event_type"].to_list(),
                "security_ccy": agg["security_ccy"].to_list(),
                "cash_amount": agg["cash_amount"].to_list(),
                "target_value": agg["target_value"].to_list(),
                "target_ccy": agg["target_ccy"].to_list(),
                "event_count": agg["event_count"].to_list(),
            },
            schema=cash_flow_summary_schema,
        )

    return _write_analytics_table(result, cash_flow_summary_schema, analytics_path)
