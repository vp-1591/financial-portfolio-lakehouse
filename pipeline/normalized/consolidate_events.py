"""Consolidate broker events normalized tables into a single events table.

Reads each broker's events normalized Delta table and concatenates all rows
into ``normalized/events``, producing a unified broker-neutral events
table suitable for dashboard queries.

Decision: docs/adr/0110-xtb-file-arrival-only-ingestion.md
Only enabled connectors are read. Missing or empty event tables are skipped
and reported as warnings; the quality validation stage records the warnings.
D15: ``account_id`` is part of the consolidate dedup subset so multi-account
brokers (XTB, D18) do not drop same-ID events across accounts.
"""

from __future__ import annotations

import logging

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from pipeline.connectors.transform_utils import dedup_events
from pipeline.normalized.models import events_normalized_schema
from pipeline.storage import get_storage

logger = logging.getLogger(__name__)


def consolidate_events(connectors: list[str]) -> pa.Table:
    """Merge broker events normalized tables into ``normalized/events``.

    Reads ``normalized/{broker}_events`` for each enabled connector,
    concatenates non-empty tables, and writes ``normalized/events`` using
    overwrite mode.
    """
    config = get_storage()
    storage_opts = config.storage_options

    tables: list[pa.Table] = []

    for broker in connectors:
        events_path = config.normalized_path(f"{broker}_events")
        try:
            dt = DeltaTable(str(events_path), storage_options=storage_opts)
            table = dt.to_pyarrow_table()
        except Exception:
            logger.warning("events %s: table not found, skipping", broker)
            continue
        if table.num_rows == 0:
            logger.warning("events %s: table is empty, skipping", broker)
            continue
        tables.append(table)
        logger.info("events %s: %d rows", broker, table.num_rows)

    result = (
        pa.concat_tables(tables, schema=events_normalized_schema)
        if tables
        else pa.table(
            {
                field.name: pa.array([], type=field.type)
                for field in events_normalized_schema
            },
            schema=events_normalized_schema,
        )
    )

    # Defense-in-depth: dedup across brokers on (broker, event_type, event_id,
    # account_id). D15: ``account_id`` is part of the subset so multi-account
    # brokers (XTB, D18) do not silently drop same-ID events across accounts.
    # Each broker's transform already dedups its own events, but this boundary
    # check guards against future brokers that skip transform-level dedup and
    # against raw-layer replays that bypass the transform contract.  It also
    # catches the T212 full-history re-fetch class of bug regardless of broker.
    # Decision: docs/adr/0105-fix-t212-cdc-dedup-and-concat-type-mismatch.md
    df = pl.from_arrow(result)
    assert isinstance(df, pl.DataFrame)  # pl.from_arrow(pa.Table) -> DataFrame
    df = dedup_events(
        df,
        subset=["broker", "event_type", "event_id", "account_id"],
        label="events consolidate",
    )

    output_path = config.normalized_path("events")
    config.backend.ensure_parent(output_path)
    write_deltalake(
        str(output_path),
        df,
        mode="overwrite",
        storage_options=storage_opts,
    )
    logger.info("Consolidated events: %d rows", df.height)
    # Return an Arrow table to honor the -> pa.Table contract (callers and
    # tests access result.num_rows / result.column(...)); write_deltalake
    # receives the Polars frame directly per the repo rule (no pa.Table write).
    return df.to_arrow()
