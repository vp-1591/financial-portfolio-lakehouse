"""Shared raw ingestion logic: encrypt, dedup, write to Delta tables."""

from __future__ import annotations

import logging

import pyarrow as pa
from deltalake import write_deltalake

from pipeline.crypto import encrypt

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


def dedup_raw(table: pa.Table, existing_path: str | None = None) -> pa.Table:
    """Remove rows whose ``(broker, source, payload_hash)`` already exist.

    If *existing_path* is ``None`` or the path does not exist, no
    deduplication is performed and the table is returned as-is.
    """
    if existing_path is None:
        return table

    try:
        from deltalake import DeltaTable

        from pipeline.storage import get_storage

        storage_opts = get_storage().storage_options
        existing_dt = DeltaTable(existing_path, storage_options=storage_opts)
    except Exception:
        return table

    # Project only the dedup-key columns: the accumulated table's payload
    # column (Fernet-encrypted bytes) grows every run, and loading it into
    # memory just to compute a key set is the other peak-memory driver
    # (issue #154). The 3-key scan result is identical to a full read.
    existing = existing_dt.to_pyarrow_table(
        columns=["broker", "source", "payload_hash"]
    )
    if existing.num_rows == 0:
        return table

    existing_keys = set(
        zip(
            existing.column("broker").to_pylist(),
            existing.column("source").to_pylist(),
            existing.column("payload_hash").to_pylist(),
        )
    )

    brokers = table.column("broker").to_pylist()
    sources = table.column("source").to_pylist()
    hashes = table.column("payload_hash").to_pylist()
    mask = [(b, s, h) not in existing_keys for b, s, h in zip(brokers, sources, hashes)]

    if all(mask):
        return table
    if not any(mask):
        return table.slice(0, 0)

    return table.filter(pa.array(mask))


def ingest_raw(
    table: pa.Table,
    table_path: str,
    fernet_key: bytes,
) -> pa.Table:
    """Encrypt, dedup, and write a raw table to a Delta table.

    Returns the Fernet-encrypted pre-dedup table (the current fetch) so the
    caller can hand it to the transform in memory; ``fetch_connector`` uses
    this as the handoff for ``handoff_supported`` connectors. It must be the
    pre-dedup table — an unchanged endpoint deduped out of the write still
    reaches the transform, exactly as it does via the accumulated table.
    When all rows are duplicates (``deduped.num_rows == 0``), creates a
    schema-only Delta table if one does not already exist, so that
    downstream transforms always find a table to read.
    """
    encrypted = encrypt_raw_payloads(table, fernet_key)
    deduped = dedup_raw(encrypted, table_path)
    if deduped.num_rows == 0:
        # Create schema-only table if it doesn't exist yet
        from pipeline.storage import get_storage

        storage_opts = get_storage().storage_options
        try:
            from deltalake import DeltaTable

            DeltaTable(table_path, storage_options=storage_opts)
        except Exception:
            get_storage().backend.ensure_parent(table_path)
            write_deltalake(
                table_path,
                encrypted.slice(0, 0),
                mode="append",
                storage_options=storage_opts,
            )
        logger.debug(
            "%s: %d rows written (deduped from %d)",
            table_path,
            0,
            table.num_rows,
        )
        return encrypted
    from pipeline.storage import get_storage

    storage_opts = get_storage().storage_options
    get_storage().backend.ensure_parent(table_path)
    write_deltalake(table_path, deduped, mode="append", storage_options=storage_opts)
    logger.debug(
        "%s: %d rows written (deduped from %d)",
        table_path,
        deduped.num_rows,
        table.num_rows,
    )
    return encrypted
