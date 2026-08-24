"""Shared raw ingestion logic: encrypt, in-batch dedup, merge-on-key write."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from pipeline.crypto import encrypt
from pipeline.raw.retention import (
    merge_predicate,
    retention_key,
    retention_value,
)

logger = logging.getLogger(__name__)


def encrypt_raw_payloads(table: pa.Table, fernet_key: bytes) -> pa.Table:
    """Return a new raw table with the ``payload`` column Fernet-encrypted.

    The input table is not mutated; a new table is returned with the
    ``payload`` column replaced by Fernet-encrypted bytes.
    """
    payloads = table.column("payload").to_pylist()
    encrypted = [encrypt(p, fernet_key) for p in payloads]
    idx = table.schema.get_field_index("payload")
    return table.set_column(idx, "payload", pa.array(encrypted, type=pa.binary()))


def _dedup_in_batch(table: pa.Table) -> pa.Table:
    """Remove rows whose ``(source, payload_hash)`` repeats within the batch.

    One batch never inserts two identical rows (AC-2). The cross-run
    accumulated-table scan (``dedup_raw``) is deleted — the bounded merge is
    the replacement (AD-1): it matches on the retention key, so an unchanged
    endpoint is updated in place, not duplicated.
    """
    sources = table.column("source").to_pylist()
    hashes = table.column("payload_hash").to_pylist()
    seen: set[tuple[str, str]] = set()
    mask: list[bool] = []
    for source, payload_hash in zip(sources, hashes):
        key_pair = (source, payload_hash)
        if key_pair in seen:
            mask.append(False)
        else:
            seen.add(key_pair)
            mask.append(True)
    return table.filter(pa.array(mask))


def _dedup_by_retention_key(table: pa.Table, connector_name: str) -> pa.Table:
    """Collapse rows sharing a retention key to one: newest fetch wins (F1.2).

    Two rows with the same retention key in one batch must not let
    merge/loop order decide the winner. Keep the row with the latest
    ``fetched_at``; on a tie (e.g. Trading 212 pages captured at one ``now``),
    keep the LAST row in batch order — the endpoint's final page is what the
    endpoint retains (AC-4). Only non-NULL-keyed rows reach this helper
    (ingest_raw splits them out first); NULL keys are appended (AC-3).
    An empty or single-row input is returned as-is (no collision to resolve).
    """
    if table.num_rows <= 1:
        return table
    sources = table.column("source").to_pylist()
    fetched = table.column("fetched_at").to_pylist()
    best: dict[str, tuple[int, datetime | None]] = {}
    for i, (source, at) in enumerate(zip(sources, fetched)):
        key = retention_value(connector_name, source)
        prev = best.get(key)
        if prev is None or _as_datetime(at) >= _as_datetime(prev[1]):
            best[key] = (i, at)
    keep = sorted(best[key][0] for key in best)
    return table.take(pa.array(keep))


def _as_datetime(value: datetime | None) -> datetime:
    """Normalize a ``fetched_at`` cell for ordering (``None`` sorts first)."""
    if value is None:
        return datetime.min.replace(tzinfo=UTC)
    return value


def _schema_only(table_path: str, template: pa.Table) -> None:
    """Create a schema-only Delta table so transforms always find one."""
    from pipeline.storage import get_storage

    storage_opts = get_storage().storage_options
    try:
        DeltaTable(table_path, storage_options=storage_opts)
    except Exception:
        get_storage().backend.ensure_parent(table_path)
        write_deltalake(
            table_path,
            template.slice(0, 0),
            mode="append",
            storage_options=storage_opts,
        )


def ingest_raw(
    table: pa.Table,
    table_path: str,
    fernet_key: bytes,
    connector_name: str,
) -> None:
    """Encrypt, dedup in-batch, and MERGE a raw batch onto its Delta table.

    The raw write is a ``DeltaTable.merge`` on the broker retention key
    (AD-1, AC-1): XTB ``account_id``, Trading 212/IBKR ``source`` (Trading
    212's key is the pagination-stripped endpoint base, AC-4). Matched keys
    are replaced by the current fetch row (``when_matched_update``), new keys
    inserted (``when_not_matched_insert_all``), absent keys untouched. Rows
    whose retention key is NULL (an unparseable XTB filename) are appended,
    never merged (AC-3). The batch is deduped in-batch on ``(source,
    payload_hash)`` and then on the retention key (latest ``fetched_at`` wins,
    tie -> last in batch order) before the merge (AC-2, F1.2).

    Returns nothing — the transform reads the merged table back (the single
    bronze read, AD-6); the in-memory encrypted-fetch handoff is removed
    (AD-8).
    """
    from pipeline.storage import get_storage

    encrypted = encrypt_raw_payloads(table, fernet_key)
    deduped = _dedup_in_batch(encrypted)
    if deduped.num_rows == 0:
        # Empty batch: create a schema-only table if one does not exist yet,
        # so downstream transforms always find a table to read.
        _schema_only(table_path, encrypted)
        logger.debug(
            "%s: %d rows written (empty batch of %d)",
            table_path,
            0,
            table.num_rows,
        )
        return

    storage_opts = get_storage().storage_options
    get_storage().backend.ensure_parent(table_path)

    key_col = retention_key(connector_name)
    keys = deduped.column(key_col).to_pylist()
    null_key_mask = [value is None for value in keys]
    # AC-3: NULL retention keys are appended, never merged (a MERGE predicate
    # never matches NULL). Distinct (source, payload_hash) null-keyed rows
    # still pass the in-batch dedup above — that is the only dedup they get.
    append_rows = deduped.filter(pa.array(null_key_mask))
    merge_rows = deduped.filter(pa.array([not m for m in null_key_mask]))
    # F1.2: dedup in-batch on the retention key too — one row per key, newest
    # fetched_at wins (tie -> last in batch order).
    merge_rows = _dedup_by_retention_key(merge_rows, connector_name)

    try:
        target = DeltaTable(table_path, storage_options=storage_opts)
    except Exception:
        # First fetch: the merge needs an existing target. Bootstrap the table
        # with the whole deduped batch (the merge's insert path); an empty
        # batch handled above still yields a schema-only table.
        first_rows = pa.concat_tables([merge_rows, append_rows])
        write_deltalake(
            table_path,
            first_rows,
            mode="append",
            storage_options=storage_opts,
        )
        logger.debug("%s: %d rows written (first run)", table_path, first_rows.num_rows)
        return

    if merge_rows.num_rows:
        target.merge(
            source=merge_rows,
            predicate=merge_predicate(connector_name),
            source_alias="s",
            target_alias="t",
        ).when_matched_update(
            updates={column: f"s.{column}" for column in merge_rows.column_names}
        ).when_not_matched_insert_all().execute()
    if append_rows.num_rows:
        write_deltalake(
            table_path,
            append_rows,
            mode="append",
            storage_options=storage_opts,
        )
    logger.debug(
        "%s: %d rows merged / %d appended (from %d)",
        table_path,
        merge_rows.num_rows,
        append_rows.num_rows,
        table.num_rows,
    )
