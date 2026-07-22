"""Consolidate broker CDC normalized tables into a single cdc_events table.

Reads each broker's CDC normalized Delta table and concatenates all rows
into ``normalized/cdc_events``, producing a unified broker-neutral CDC
table suitable for dashboard queries.

Decision: docs/adr/0087-make-cdc-mandatory-and-fail-on-empty-silver-cdc.md
CDC is mandatory for ibkr and trading212 — a missing or empty required
broker CDC table raises RuntimeError.  XTB is optional (file-based, no
CDC feed).
"""

from __future__ import annotations

import logging

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from pipeline.normalized.models import cdc_events_normalized_schema
from pipeline.storage import get_storage

logger = logging.getLogger(__name__)

# Brokers whose CDC tables are required — must be present and non-empty.
_REQUIRED_CDC_BROKERS = ["ibkr", "trading212"]
# Brokers whose CDC tables are optional (file-based, may legitimately be absent).
_OPTIONAL_CDC_BROKERS = ["xtb"]


def consolidate_cdc_events() -> pa.Table:
    """Merge broker CDC normalized tables into ``normalized/cdc_events``.

    Reads ``normalized/{broker}_cdc`` for each broker, concatenates the
    rows, and writes the result to ``normalized/cdc_events`` using
    overwrite mode.

    Raises :class:`RuntimeError` if a required broker CDC table is missing
    or empty.  Optional brokers are skipped silently when absent and logged
    at DEBUG when empty.

    Returns the concatenated table (guaranteed non-empty because all
    required brokers contributed rows).
    """
    config = get_storage()
    storage_opts = config.storage_options
    tables: list[pa.Table] = []

    # Required brokers: must be present and non-empty.
    for broker in _REQUIRED_CDC_BROKERS:
        cdc_path = config.normalized_path(f"{broker}_cdc")
        try:
            dt = DeltaTable(str(cdc_path), storage_options=storage_opts)
            table = dt.to_pyarrow_table()
        except Exception as exc:
            raise RuntimeError(
                f"Required CDC table {broker}_cdc not found: {exc}"
            ) from exc
        if table.num_rows == 0:
            raise RuntimeError(f"Required CDC table {broker}_cdc is empty (0 rows)")
        tables.append(table)
        logger.info("CDC %s: %d rows", broker, table.num_rows)

    # Optional brokers: skip silently when absent, log at DEBUG when empty.
    for broker in _OPTIONAL_CDC_BROKERS:
        cdc_path = config.normalized_path(f"{broker}_cdc")
        try:
            dt = DeltaTable(str(cdc_path), storage_options=storage_opts)
            table = dt.to_pyarrow_table()
        except Exception:
            logger.debug("CDC %s: no data, skipping (optional)", broker)
            continue
        if table.num_rows > 0:
            tables.append(table)
            logger.info("CDC %s: %d rows", broker, table.num_rows)
        else:
            logger.debug("CDC %s: 0 rows, skipping (optional)", broker)

    # tables is guaranteed non-empty because all required brokers contributed.
    result = pa.concat_tables(tables, schema=cdc_events_normalized_schema)

    output_path = config.normalized_path("cdc_events")
    config.backend.ensure_parent(output_path)
    write_deltalake(
        str(output_path),
        result,
        mode="overwrite",
        storage_options=storage_opts,
    )
    logger.info("Consolidated CDC events: %d rows", result.num_rows)
    return result
