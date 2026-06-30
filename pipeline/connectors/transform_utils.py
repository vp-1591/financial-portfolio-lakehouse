"""Shared utilities for bronze → silver (raw → normalized) transforms.

Provides Polars-based helpers to decrypt, parse, and iterate raw Delta
table rows, replacing the former pandas ``iterrows`` + manual list-append
pattern.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    import pyarrow as pa


from pipeline.crypto import decrypt


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
