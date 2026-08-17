"""XTB connector: fetch raw report data from XLS files.

Stage 3 (D17 shared bronze): ``fetch_snapshot`` writes a single raw row with
``source == "XTB_REPORT"`` carrying the full 3-sheet workbook. The separate
``fetch_cdc`` path is removed — both snapshot and CDC silvers derive from the
same ``xtb_snapshot`` raw table. ``fetch_kwargs``/``args.xtb_file`` loop
support lives in :func:`pipeline.run.fetch_connector`, not here.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
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
    """Fetch an XTB report (full 3-sheet workbook) and return a raw-layer table.

    Stores the raw .xlsx file bytes as the payload with
    ``source == "XTB_REPORT"`` (D17 shared bronze). Both snapshot and CDC
    silvers derive from this single raw row; parsing is left to the
    transform layer.

    Parameters
    ----------
    file_path:
        Absolute path to the XTB .xlsx report, or an ``s3://`` URI.
    """
    payload, filename = _read_file_bytes(file_path)
    now = datetime.now(UTC)

    return pa.table(
        {
            "fetched_at": [now],
            "broker": ["XTB"],
            "source": ["XTB_REPORT"],
            "payload": [payload],
            "payload_hash": [hashlib.sha256(payload).hexdigest()],
            "source_file": [filename],
        },
        schema=RAW_SCHEMA,
    )
