"""Consolidate broker CDC normalized tables into a single cdc_events table.

Reads each broker's CDC normalized Delta table and concatenates all rows
into ``normalized/cdc_events``, producing a unified broker-neutral CDC
table suitable for dashboard queries.
"""

from __future__ import annotations

import logging

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from pipeline.normalized.models import cdc_events_normalized_schema
from pipeline.storage import get_storage

logger = logging.getLogger(__name__)

# Broker names whose CDC tables to consolidate
_CDC_BROKERS = ["ibkr", "trading212", "xtb"]


def consolidate_cdc_events() -> pa.Table | None:
    """Merge all broker CDC normalized tables into ``normalized/cdc_events``.

    Reads ``normalized/{broker}_cdc`` for each broker, concatenates the
    rows, and writes the result to ``normalized/cdc_events`` using
    overwrite mode.

    Returns the concatenated table, or ``None`` if no CDC data exists
    for any broker.
    """
    config = get_storage()
    storage_opts = config.storage_options
    tables: list[pa.Table] = []

    for broker in _CDC_BROKERS:
        cdc_path = config.normalized_path(f"{broker}_cdc")
        try:
            dt = DeltaTable(str(cdc_path), storage_options=storage_opts)
            table = dt.to_pyarrow_table()
            if table.num_rows > 0:
                tables.append(table)
                logger.debug("CDC %s: %d rows", broker, table.num_rows)
        except Exception:
            logger.debug("CDC %s: no data, skipping", broker)
            continue

    if not tables:
        logger.info("No CDC data found for any broker")
        return None

    # Concatenate and cast to unified schema
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
