"""XTB connector: fetch raw snapshot and CDC data from XLS files."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa

from pipeline.raw.models import RAW_SCHEMA


def fetch_snapshot(file_path: str | Path) -> pa.Table:
    """Fetch XTB positions and cash from an Excel report.

    Stores the raw .xlsx file bytes as the payload, leaving parsing
    to the transform layer.

    Parameters
    ----------
    file_path:
        Absolute path to the XTB .xlsx report.
    """
    report_path = Path(file_path)
    payload = report_path.read_bytes()
    now = datetime.now(timezone.utc)

    return pa.table(
        {
            "fetched_at": [now],
            "broker": ["XTB"],
            "source": ["OPEN POSITION"],
            "payload": [payload],
            "payload_hash": [hashlib.sha256(payload).hexdigest()],
            "source_file": [report_path.name],
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
        Absolute path to the XTB .xlsx report.
    """
    report_path = Path(file_path)
    payload = report_path.read_bytes()
    now = datetime.now(timezone.utc)

    return pa.table(
        {
            "fetched_at": [now],
            "broker": ["XTB"],
            "source": ["CASH OPERATION"],
            "payload": [payload],
            "payload_hash": [hashlib.sha256(payload).hexdigest()],
            "source_file": [report_path.name],
        },
        schema=RAW_SCHEMA,
    )
