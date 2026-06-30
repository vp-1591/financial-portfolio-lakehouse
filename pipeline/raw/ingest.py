"""Shared raw ingestion logic: encrypt, dedup, write to Delta tables."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pyarrow as pa
from deltalake import write_deltalake

from pipeline.crypto import encrypt
from pipeline.raw.models import RAW_SCHEMA


def _compute_payload_hash(payload_bytes: bytes) -> str:
    """Return the SHA-256 hex digest of unencrypted payload bytes."""
    return hashlib.sha256(payload_bytes).hexdigest()


def build_raw_table(
    broker: str,
    source: str,
    payloads: list[tuple[bytes, str]],
    account_id: str = "",
    source_file: str = "",
) -> pa.Table:
    """Build a raw-layer PyArrow table from fetched payload data.

    Parameters
    ----------
    broker:
        Broker display name (``"IBKR"``, ``"Trading 212"``, ``"XTB"``).
    source:
        API endpoint path or sheet name that produced the data.
    payloads:
        List of ``(raw_response_bytes, account_id)`` tuples.
    account_id:
        Default account ID when per-payload account_id is empty.
    source_file:
        XLS filename for XTB reports; empty string for API brokers.

    Returns
    -------
    pa.Table
        A table matching :data:`RAW_SCHEMA` with encrypted payloads.
    """
    fernet_key = b""  # Encryption happens in ingest_raw, not here
    now = datetime.now(timezone.utc)

    fetched_ats: list[datetime] = []
    brokers: list[str] = []
    sources: list[str] = []
    encrypted_payloads: list[bytes] = []
    payload_hashes: list[str] = []
    account_ids: list[str] = []
    source_files: list[str] = []

    for payload_bytes, payload_account_id in payloads:
        fetched_ats.append(now)
        brokers.append(broker)
        sources.append(source)
        payload_hashes.append(_compute_payload_hash(payload_bytes))
        encrypted_payloads.append(payload_bytes)  # Will be encrypted in ingest_raw
        account_ids.append(payload_account_id or account_id)
        source_files.append(source_file)

    return pa.table(
        {
            "fetched_at": fetched_ats,
            "broker": brokers,
            "source": sources,
            "payload": encrypted_payloads,
            "payload_hash": payload_hashes,
            "account_id": account_ids,
            "source_file": source_files,
        },
        schema=RAW_SCHEMA,
    )


def encrypt_raw_payloads(table: pa.Table, fernet_key: bytes) -> pa.Table:
    """Encrypt the payload column of a raw table in-place.

    Returns a new table with the ``payload`` column replaced by
    Fernet-encrypted bytes.
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

    existing = existing_dt.to_pyarrow_table()
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
) -> int:
    """Encrypt, dedup, and write a raw table to a Delta table.

    Returns the number of new rows written.
    """
    encrypted = encrypt_raw_payloads(table, fernet_key)
    deduped = dedup_raw(encrypted, table_path)
    if deduped.num_rows == 0:
        return 0
    from pipeline.storage import get_storage
    storage_opts = get_storage().storage_options
    # DEBUG: verify storage_options is passed correctly for S3
    backend_type = type(get_storage().backend).__name__
    opts_type = type(storage_opts).__name__
    opts_keys = list(storage_opts.keys()) if storage_opts else "None"
    print(f"  DEBUG ingest_raw: backend={backend_type}, storage_opts={opts_type}({opts_keys}), path={table_path[:30]}...")
    get_storage().backend.ensure_parent(table_path)
    write_deltalake(table_path, deduped, mode="append", storage_options=storage_opts)
    return deduped.num_rows