"""Shared utilities for bronze → silver (raw → normalized) transforms.

Provides helpers to decrypt, parse, and iterate raw Delta table rows,
and to build normalized PyArrow tables from row dicts using Polars for
column encryption and schema casting.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterator

import polars as pl

if TYPE_CHECKING:
    import pyarrow as pa

from pipeline.crypto import decrypt, encrypt_float


@dataclass
class DecodedRow:
    """A decoded raw-layer row with decrypted payload and parsed content."""

    fetched_at: datetime
    account_id: str
    source: str
    source_file: str
    payload_parsed: Any  # Parsed JSON (dict or list), or None
    payload_raw: bytes  # Decrypted bytes (for XML payloads)


def decode_payload(payload: bytes | memoryview, fernet_key: bytes) -> bytes | None:
    """Decrypt a raw payload. Returns None on decryption failure."""
    if isinstance(payload, memoryview):
        payload = bytes(payload)
    try:
        return decrypt(payload, fernet_key)
    except Exception:
        return None


def parse_json(data: bytes) -> Any | None:
    """Parse JSON bytes. Returns None on parse failure."""
    try:
        return json.loads(data)
    except (json.JSONDecodeError, TypeError):
        return None


def coerce_fetched_at(value: Any) -> datetime:
    """Convert a fetched_at value to a timezone-aware datetime.

    Handles: datetime objects, ISO-format strings, and Arrow/Pandas Timestamp objects.
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    # pandas Timestamp or other datetime-like
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    return value


def iter_raw_payloads(
    raw: pa.Table,
    fernet_key: bytes,
    *,
    require_json: bool = True,
) -> Iterator[DecodedRow]:
    """Iterate over rows of a raw table, decrypting and parsing payloads.

    Yields :class:`DecodedRow` for rows where decryption (and optionally
    JSON parsing) succeeds.  Skips rows that fail.

    When *require_json* is ``False``, rows with non-JSON payloads (e.g.\
    XML) are still yielded with ``payload_parsed=None`` and
    ``payload_raw`` set to the decrypted bytes.

    Parameters
    ----------
    raw:
        PyArrow table matching :data:`RAW_SCHEMA`.
    fernet_key:
        Fernet key for decrypting the ``payload`` column.
    require_json:
        If True (default), skip rows whose payloads cannot be parsed as
        JSON.  Set to False for sources that produce XML or other formats.
    """

    fetched_ats = raw.column("fetched_at").to_pylist()
    account_ids = raw.column("account_id").to_pylist()
    sources = raw.column("source").to_pylist()
    payloads = raw.column("payload").to_pylist()
    source_files = raw.column("source_file").to_pylist()

    for i in range(len(fetched_ats)):
        fetched_at = coerce_fetched_at(fetched_ats[i])
        account_id = str(account_ids[i] or "")
        source = str(sources[i] or "")
        source_file = str(source_files[i] or "")
        payload_bytes = payloads[i]

        decrypted = decode_payload(payload_bytes, fernet_key)
        if decrypted is None:
            continue

        parsed = parse_json(decrypted)
        if require_json and parsed is None:
            continue

        yield DecodedRow(
            fetched_at=fetched_at,
            account_id=account_id,
            source=source,
            source_file=source_file,
            payload_parsed=parsed,
            payload_raw=decrypted,
        )


def build_normalized_table(
    records: list[dict[str, Any]],
    schema: "pa.Schema",
    fernet_key: bytes,
    encrypt_columns: list[str] | None = None,
) -> "pa.Table":
    """Build a normalized PyArrow table from row dicts, encrypting specified columns.

    Replaces the manual list-append pattern (initialize N empty lists, loop
    and append to each, encrypt inline, assemble ``pa.table()``) with a single
    Polars DataFrame construction followed by batch column encryption.

    Parameters
    ----------
    records:
        List of dicts, one per output row.  Keys must match schema field names.
        Values for columns listed in *encrypt_columns* must be plain floats
        (not yet encrypted) — the function applies Fernet encryption via
        ``encrypt_float``.
    schema:
        Target PyArrow schema.  Encrypted columns must be ``pa.binary()``
        in the schema but ``float`` in the input dicts.
    fernet_key:
        Fernet key for encrypting float columns.
    encrypt_columns:
        Column names whose float values should be Fernet-encrypted to binary.
        Defaults to an empty list (no encryption).
    """
    import pyarrow as pa

    if encrypt_columns is None:
        encrypt_columns = []

    # Empty result set: return a correctly-typed empty table.
    if not records:
        return pa.table(
            {field.name: pa.array([], type=field.type) for field in schema},
            schema=schema,
        )

    df = pl.DataFrame(records)

    # Encrypt specified float columns to binary Fernet tokens.
    for col_name in encrypt_columns:
        if col_name in df.columns:
            df = df.with_columns(
                pl.col(col_name)
                .map_elements(
                    lambda v, _key=fernet_key: (
                        encrypt_float(v, _key) if v is not None else None
                    ),
                    return_dtype=pl.Binary,
                )
                .alias(col_name),
            )

    # Ensure all schema columns are present; fill missing with null.
    for field in schema:
        if field.name not in df.columns:
            df = df.with_columns(pl.lit(None).alias(field.name))

    # Reorder columns to match schema order.
    df = df.select([field.name for field in schema])

    # Convert to PyArrow and cast to target schema.
    arrow_table = df.to_arrow()
    return arrow_table.cast(schema)
