"""Consolidate broker events normalized tables into a single events table.

Reads each broker's events normalized Delta table and concatenates all rows
into ``normalized/events``, producing a unified broker-neutral events
table suitable for dashboard queries.

Decision: docs/adr/0110-xtb-file-arrival-only-ingestion.md
Events are mandatory for every registered broker (ibkr, trading212) — a
missing or empty required broker events table raises RuntimeError (the
required-non-empty gate, carried forward from ADR 0087 §Decision).  XTB is
not a scheduled connector: fetch+transform runs only on the EventBridge S3
file-arrival rule, and ``xtb_events`` is consolidated whenever present (a
missing or empty table is skipped, not raised).

D15: the candidate broker set is derived from the connector registry
(``connectors.all()``), not a hardcoded ``_OPTIONAL_EVENTS_BROKERS`` list. Only
``_REQUIRED_EVENTS_BROKERS`` is hardcoded (the ADR 0087 required-non-empty gate).
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

# Brokers whose events tables are required — must be present and non-empty.
_REQUIRED_EVENTS_BROKERS = ["ibkr", "trading212"]


def consolidate_events() -> pa.Table:
    """Merge broker events normalized tables into ``normalized/events``.

    Reads ``normalized/{broker}_events`` for each registered broker, concatenates
    the rows, and writes the result to ``normalized/events`` using
    overwrite mode.

    The candidate broker set is derived from the connector registry
    (``connectors.all()``) — D15. Required brokers
    (:data:`_REQUIRED_EVENTS_BROKERS`) must be present and non-empty (raise on
    missing/empty).

    Raises :class:`RuntimeError` if a required broker events table is missing
    or empty.  Returns the concatenated table (guaranteed non-empty because
    all required brokers contributed rows).
    """
    # Import lazily inside the function to avoid an import cycle:
    # ``connectors.base`` imports ``pipeline.normalized.consolidate`` (for
    # ``Holding``), so ``consolidate_events`` -> ``connectors.registry`` could
    # close a loop if imported at module level.
    from pipeline.connectors.registry import all as all_connectors

    config = get_storage()
    storage_opts = config.storage_options

    # Candidate brokers come from the registry (single source of truth, D15).
    candidate_brokers = [c.name for c in all_connectors()]

    tables: list[pa.Table] = []

    for broker in candidate_brokers:
        events_path = config.normalized_path(f"{broker}_events")
        try:
            dt = DeltaTable(str(events_path), storage_options=storage_opts)
            table = dt.to_pyarrow_table()
        except Exception as exc:
            if broker in _REQUIRED_EVENTS_BROKERS:
                raise RuntimeError(
                    f"Required events table {broker}_events not found at {events_path}"
                ) from exc
            logger.debug("events %s: no data, skipping (optional)", broker)
            continue
        if table.num_rows == 0:
            if broker in _REQUIRED_EVENTS_BROKERS:
                raise RuntimeError(
                    f"Required events table {broker}_events is empty (0 rows)"
                )
            logger.debug("events %s: 0 rows, skipping (optional)", broker)
            continue
        tables.append(table)
        logger.info("events %s: %d rows", broker, table.num_rows)

    # tables is guaranteed non-empty because all required brokers contributed.
    result = pa.concat_tables(tables, schema=events_normalized_schema)

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
