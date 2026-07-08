"""XTB connector: fetch raw snapshot and CDC data from XLS files."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

import pyarrow as pa

from pipeline.raw.models import RAW_SCHEMA


def _read_file_bytes(file_path: str | Path) -> tuple[bytes, str]:
    """Read file bytes from a local path or S3 URI.

    Parameters
    ----------
    file_path:
        Absolute local path or ``s3://`` URI.

    Returns
    -------
    tuple[bytes, str]
        ``(content, filename)`` where *filename* is the basename of the
        file (e.g. ``report.xlsx``).

    Notes
    -----
    EventBridge object keys arrive percent-encoded (e.g. spaces become
    ``%20``).  This function decodes the S3 key once via
    :func:`urllib.parse.unquote` so that ``read_s3_bytes`` receives the
    human-readable key.  ``parse_s3_uri`` is shared with
    ``upload_to_staging`` / ``read_s3_bytes`` which handle locally-typed
    keys that are already decoded, so decoding is done **here** at the
    XTB boundary rather than in the shared helper — naive unquote there
    would risk double-decoding literal ``%`` sequences.

    **Caveat:** XTB report filenames should not contain literal ``%``
    characters, as they would be misinterpreted as percent-encoding
    markers.
    """
    file_path = str(file_path)

    if file_path.startswith("s3://"):
        from pipeline.s3 import parse_s3_uri, read_s3_bytes

        bucket, key = parse_s3_uri(file_path)
        decoded_key = unquote(key)
        decoded_uri = f"s3://{bucket}/{decoded_key}"
        content, filename = read_s3_bytes(decoded_uri)
        return content, filename

    path = Path(file_path).resolve()
    return path.read_bytes(), path.name


def fetch_snapshot(file_path: str | Path) -> pa.Table:
    """Fetch XTB positions and cash from an Excel report.

    Stores the raw .xlsx file bytes as the payload, leaving parsing
    to the transform layer.

    Parameters
    ----------
    file_path:
        Absolute path to the XTB .xlsx report, or an ``s3://`` URI.
    """
    payload, filename = _read_file_bytes(file_path)
    now = datetime.now(timezone.utc)

    return pa.table(
        {
            "fetched_at": [now],
            "broker": ["XTB"],
            "source": ["OPEN POSITION"],
            "payload": [payload],
            "payload_hash": [hashlib.sha256(payload).hexdigest()],
            "source_file": [filename],
        },
        schema=RAW_SCHEMA,
    )


def fetch_cdc(file_path: str | Path) -> pa.Table:
    """Fetch XTB cash operations (CDC) from an Excel report.

    Stores the raw .xlsx file bytes as the payload, leaving parsing
    to the transform layer.

    Parameters
    ----------
    file_path:
        Absolute path to the XTB .xlsx report, or an ``s3://`` URI.
    """
    payload, filename = _read_file_bytes(file_path)
    now = datetime.now(timezone.utc)

    return pa.table(
        {
            "fetched_at": [now],
            "broker": ["XTB"],
            "source": ["CASH OPERATION"],
            "payload": [payload],
            "payload_hash": [hashlib.sha256(payload).hexdigest()],
            "source_file": [filename],
        },
        schema=RAW_SCHEMA,
    )
