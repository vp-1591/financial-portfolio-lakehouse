"""S3 helpers for staging file uploads and downloads.

Uses PyArrow's ``S3FileSystem`` (already a dependency) for S3 operations.
Phase 1 only needs upload, download, and delete — no need for boto3 yet.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pyarrow.fs as pafs

from pipeline.secrets import resolve_aws_credentials

logger = logging.getLogger(__name__)


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Parse an ``s3://`` URI into ``(bucket, key)``.

    Parameters
    ----------
    uri:
        An S3 URI like ``s3://my-bucket/pipeline/staging/xtb/report.xlsx``.

    Returns
    -------
    tuple[str, str]
        ``(bucket, key)`` where *key* is everything after the bucket name.

    Raises
    ------
    ValueError
        If *uri* does not start with ``s3://``.
    """
    if not uri.startswith("s3://"):
        raise ValueError(f"Not an S3 URI: {uri!r}")
    path = uri[5:]  # strip "s3://"
    slash = path.find("/")
    if slash == -1:
        # s3://bucket — no key
        return path, ""
    bucket = path[:slash]
    key = path[slash + 1 :]
    return bucket, key


def _make_s3fs() -> pafs.S3FileSystem:
    """Build an ``S3FileSystem`` from resolved AWS credentials.

    Uses :func:`pipeline.secrets.resolve_aws_credentials` which provides
    environment-scoped credentials (no cross-mode fallback).
    """
    creds = resolve_aws_credentials()
    kwargs = creds.to_pyarrow_kwargs()
    return pafs.S3FileSystem(**kwargs)


def upload_to_staging(local_path: str | Path, s3_uri: str) -> str:
    """Upload a local file to S3 staging.

    Parameters
    ----------
    local_path:
        Absolute path to the local file to upload.
    s3_uri:
        Destination S3 URI (e.g. ``s3://bucket/pipeline/staging/xtb/report.xlsx``).

    Returns
    -------
    str
        The S3 URI of the uploaded file.

    Raises
    ------
    FileNotFoundError
        If *local_path* does not exist.
    """
    local_path = Path(local_path)
    if not local_path.exists():
        raise FileNotFoundError(f"File not found: {local_path}")

    bucket, key = parse_s3_uri(s3_uri)
    s3fs = _make_s3fs()

    logger.info("Uploading %s → %s", local_path, s3_uri)
    s3fs.upload_file(str(local_path), f"{bucket}/{key}")
    logger.info("Upload complete: %s", s3_uri)
    return s3_uri


def read_s3_bytes(s3_uri: str) -> tuple[bytes, str]:
    """Read bytes from an S3 object and return the content with its filename.

    Parameters
    ----------
    s3_uri:
        S3 URI of the object to read (e.g.
        ``s3://bucket/pipeline/staging/xtb/report.xlsx``).

    Returns
    -------
    tuple[bytes, str]
        ``(content, filename)`` where *filename* is the last component of
        the S3 key (e.g. ``report.xlsx``).
    """
    bucket, key = parse_s3_uri(s3_uri)
    s3fs = _make_s3fs()

    logger.info("Reading %s", s3_uri)
    stream = s3fs.open_input_stream(f"{bucket}/{key}")
    try:
        content = stream.read()
    finally:
        stream.close()

    filename = key.rsplit("/", 1)[-1] if "/" in key else key
    return content, filename


def delete_from_staging(s3_uri: str) -> None:
    """Delete a staging file from S3.

    Logs a warning on failure but does **not** raise — staging cleanup is
    best-effort and should not block the pipeline.
    """
    try:
        bucket, key = parse_s3_uri(s3_uri)
        s3fs = _make_s3fs()
        s3fs.delete(f"{bucket}/{key}")
        logger.info("Deleted staging file: %s", s3_uri)
    except Exception:
        logger.warning("Failed to delete staging file: %s", s3_uri, exc_info=True)
