"""Normalize target currency columns in CDC events.

After connector transforms write CDC events with ``cash_amount`` in
``security_ccy`` (the currency the monetary columns are denominated in)
and (optionally) ``target_fx_rate`` from the broker, this module fills
in ``target_value`` and ``target_ccy`` using a ``CurrencyConverter``.
Rows where ``security_ccy`` already equals ``target_ccy`` get
``target_fx_rate = 1.0`` and ``target_value = cash_amount`` directly,
with no API call needed.
"""

from __future__ import annotations

import logging

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from pipeline.crypto import decrypt_float, encrypt_float
from pipeline.normalized.consolidate import CurrencyConverter
from pipeline.normalized.models import cdc_events_normalized_schema

logger = logging.getLogger(__name__)


def normalize_currency(
    table_path: str | None = None,
    fernet_key: bytes | None = None,
    converter: CurrencyConverter | None = None,
    target_currency: str = "EUR",
    manual_rates: dict[str, float] | None = None,
) -> pa.Table:
    """Fill in ``target_fx_rate``, ``target_value``, and ``target_ccy`` for CDC events.

    Reads the ``cdc_events`` Delta table, decrypts ``cash_amount`` and
    ``target_fx_rate``, computes the missing target-currency columns, and
    overwrites the table.

    Parameters
    ----------
    table_path:
        Path to the ``cdc_events`` Delta table.  Defaults to the
        normalized-layer path from storage config.
    fernet_key:
        Fernet key for encrypting/decrypting value columns.
        When *None*, loaded from the default location.
    converter:
        A ``CurrencyConverter`` instance.  When *None*, one is created
        with *target_currency* and *manual_rates*.
    target_currency:
        The pipeline target currency (default ``"EUR"``).
    manual_rates:
        Manual FX rate overrides, e.g. ``{"USD": 0.92}``.

    Returns
    -------
    pa.Table
        The updated CDC events table with ``target_fx_rate``,
        ``target_value``, and ``target_ccy`` populated.
    """
    from pipeline.storage import get_storage

    storage = get_storage()
    storage_opts = storage.storage_options

    if table_path is None:
        table_path = storage.normalized_path("cdc_events")

    if fernet_key is None:
        from pipeline.crypto import load_key

        fernet_key = load_key()

    if converter is None:
        converter = CurrencyConverter(
            target_currency=target_currency,
            manual_rates=manual_rates,
        )

    target_ccy = converter.target_currency

    # Read the Delta table
    try:
        dt = DeltaTable(str(table_path), storage_options=storage_opts)
    except Exception as exc:
        raise FileNotFoundError(
            f"CDC events table not found at {table_path}. "
            "Run the consolidate-cdc step first to populate the table."
        ) from exc

    arrow_table = dt.to_pyarrow_table()
    if arrow_table.num_rows == 0:
        logger.info("CDC events table is empty; nothing to normalize")
        return arrow_table

    df = pl.from_arrow(arrow_table)

    # Decrypt cash_amount
    df = df.with_columns(
        pl.col("cash_amount")
        .map_elements(
            lambda v: decrypt_float(v, fernet_key) if v is not None else None,
            return_dtype=pl.Float64,
        )
        .alias("cash_amount_decrypted")
    )

    # Decrypt target_fx_rate if present (nullable column)
    if "target_fx_rate" in df.columns:
        df = df.with_columns(
            pl.col("target_fx_rate")
            .map_elements(
                lambda v: decrypt_float(v, fernet_key) if v is not None else None,
                return_dtype=pl.Float64,
            )
            .alias("target_fx_rate_decrypted")
        )
    else:
        df = df.with_columns(
            pl.lit(None).cast(pl.Float64).alias("target_fx_rate_decrypted")
        )

    # Compute target_fx_rate and target_value for each row
    target_fx_rates: list[float | None] = []
    target_values: list[float | None] = []

    for row in df.iter_rows(named=True):
        security_ccy = str(row.get("security_ccy", "") or "").upper()
        cash_amount = row.get("cash_amount_decrypted")
        broker_rate = row.get("target_fx_rate_decrypted")

        # Skip rows with null cash_amount (shouldn't happen but be safe)
        if cash_amount is None:
            target_fx_rates.append(None)
            target_values.append(None)
            continue

        # Same currency: no conversion needed
        if security_ccy == target_ccy:
            target_fx_rates.append(1.0)
            target_values.append(cash_amount)
            continue

        # Broker provided a rate (IBKR with matching account base)
        if broker_rate is not None and broker_rate != 0:
            target_fx_rates.append(broker_rate)
            target_values.append(cash_amount * broker_rate)
            continue

        # Fall back to CurrencyConverter
        try:
            rate = converter.convert(1.0, security_ccy)
            target_fx_rates.append(rate)
            target_values.append(cash_amount * rate)
        except Exception as exc:
            logger.warning(
                "Could not convert %s to %s: %s; leaving target_value null",
                security_ccy,
                target_ccy,
                exc,
            )
            target_fx_rates.append(None)
            target_values.append(None)

    # Encrypt and replace target columns
    df = df.with_columns(
        pl.Series("target_fx_rate_new", target_fx_rates, dtype=pl.Float64),
        pl.Series("target_value_new", target_values, dtype=pl.Float64),
        pl.lit(target_ccy).alias("target_ccy_new"),
    )

    # Encrypt target_fx_rate and target_value
    df = df.with_columns(
        pl.col("target_fx_rate_new")
        .map_elements(
            lambda v: encrypt_float(v, fernet_key) if v is not None else None,
            return_dtype=pl.Binary,
        )
        .alias("target_fx_rate"),
        pl.col("target_value_new")
        .map_elements(
            lambda v: encrypt_float(v, fernet_key) if v is not None else None,
            return_dtype=pl.Binary,
        )
        .alias("target_value"),
        pl.col("target_ccy_new").alias("target_ccy"),
    )

    # Drop temporary columns
    df = df.drop(
        [
            "cash_amount_decrypted",
            "target_fx_rate_decrypted",
            "target_fx_rate_new",
            "target_value_new",
            "target_ccy_new",
        ]
    )

    # Convert back to PyArrow, matching the schema
    result = df.to_arrow()
    result = result.cast(cdc_events_normalized_schema)

    # Overwrite the Delta table
    storage.backend.ensure_parent(str(table_path))
    write_deltalake(
        str(table_path), result, mode="overwrite", storage_options=storage_opts
    )

    logger.info(
        "Normalized %d CDC events to target currency %s",
        result.num_rows,
        target_ccy,
    )
    return result
