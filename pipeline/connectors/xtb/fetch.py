"""XTB connector: fetch raw snapshot and CDC data from XLS files."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

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
    """
    file_path = str(file_path)

    if file_path.startswith("s3://"):
        from pipeline.s3 import read_s3_bytes

        return read_s3_bytes(file_path)

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
