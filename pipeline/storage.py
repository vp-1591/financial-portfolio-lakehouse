"""Swappable storage configuration for data paths.

Supports local filesystem paths and S3 cloud storage.  The active
configuration is a module-level singleton resolved from environment
variables:

- ``S3_BUCKET``: if set, uses :class:`S3Backend` with ``s3://bucket/prefix/...``
- ``PIPELINE_DATA_DIR``: local data directory (default: ``PROJECT_ROOT / "data"``)

Local development can also use a ``.env`` file (loaded by
:mod:`pipeline.secrets`) to set these variables.

Usage::

    from pipeline.storage import get_storage

    config = get_storage()
    raw_path = config.raw_path("ibkr_snapshot")
    # e.g. "/abs/path/to/data/raw/ibkr_snapshot"  (LocalBackend)
    # or  "s3://my-bucket/pipeline/raw/ibkr_snapshot"  (S3Backend)
"""

from __future__ import annotations

import logging
import os
import shutil
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable

from pipeline.secrets import (
    STORAGE_TYPE_CLOUD,
    STORAGE_TYPE_MINIO,
    get_storage_type,
    is_demo,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Default S3 prefix within the bucket.
S3_DEFAULT_PREFIX = "pipeline"


# ---------------------------------------------------------------------------
# Storage backend protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class StorageBackend(Protocol):
    """Protocol for storage backends.

    :class:`LocalBackend` returns local filesystem paths.
    :class:`S3Backend` returns ``s3://`` URIs.
    """

    def table_path(self, layer: str, table_name: str) -> str: ...
    def ensure_parent(self, table_path: str) -> None:
        """Create parent dirs and clean up orphaned files from failed writes."""
        ...

    @property
    def storage_options(self) -> dict[str, str] | None: ...


class LocalBackend:
    """Local filesystem storage backend.

    ``table_path()`` returns absolute filesystem paths.
    ``ensure_parent()`` creates parent directories as needed.
    ``storage_options`` returns ``{"allow_unsafe_rename": "true"}`` because
    Docker volume mounts on Windows (NTFS) and some network filesystems do not
    support the atomic renames that Delta Lake's commit protocol requires.
    This is safe for single-writer usage (the pipeline runs sequentially).
    """

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir.resolve()

    def table_path(self, layer: str, table_name: str) -> str:
        return str(self.data_dir / layer / table_name)

    def ensure_parent(self, table_path: str) -> None:
        """Create parent directory and rescue orphaned files from failed writes.

        If the table directory exists but contains parquet files without a
        ``_delta_log/`` sub-directory, the table is in a corrupted state from
        a previous failed write (e.g. Docker volume mount rename failure).
        Move the orphaned directory to ``.rescue/<table_name>_<timestamp>/``
        under the data directory so ``write_deltalake`` can start fresh
        and the data remains recoverable.
        """
        table_dir = Path(table_path)
        table_dir.parent.mkdir(parents=True, exist_ok=True)

        if table_dir.exists() and not (table_dir / "_delta_log").exists():
            rescue_dir = (
                self.data_dir
                / ".rescue"
                / f"{table_dir.name}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
            )
            rescue_dir.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(table_dir), str(rescue_dir))
            logger.warning(
                "Rescued orphaned table %s → %s",
                table_dir,
                rescue_dir,
            )

    @property
    def storage_options(self) -> dict[str, str]:
        return {"allow_unsafe_rename": "true"}


class S3Backend:
    """S3 storage backend using deltalake's native object_store support.

    ``table_path()`` returns ``s3://bucket/prefix/layer/table`` URIs.
    ``ensure_parent()`` is a no-op — S3 does not require parent
    directories to exist before writing.
    ``storage_options()`` returns a dict of AWS credentials for
    ``deltalake`` operations.

    AWS credentials are read from standard environment variables:
    ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, ``AWS_REGION``.
    The ``deltalake`` library handles S3 connectivity via its Rust
    ``object_store`` crate — no ``boto3`` dependency required.

    For S3-compatible stores (MinIO, etc.), set ``S3_ENDPOINT_URL``
    to the server URL (e.g. ``http://minio:9000``) and
    ``S3_ALLOW_HTTP=true`` to allow non-HTTPS connections.
    """

    def __init__(self, bucket: str, prefix: str = S3_DEFAULT_PREFIX) -> None:
        # Strip s3:// prefix if present — the bucket name should be just the
        # bucket, not a full URI.
        if bucket.startswith("s3://"):
            bucket = bucket[5:]
        elif bucket.startswith("s3a://"):
            bucket = bucket[6:]
        # Strip leading slashes — a bare bucket name should not start with /.
        bucket = bucket.lstrip("/")
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")

    def table_path(self, layer: str, table_name: str) -> str:
        if self.prefix:
            return f"s3://{self.bucket}/{self.prefix}/{layer}/{table_name}"
        return f"s3://{self.bucket}/{layer}/{table_name}"

    def ensure_parent(self, table_path: str) -> None:
        # S3 does not require parent directories to exist.
        pass

    @property
    def storage_options(self) -> dict[str, str]:
        """AWS credentials for deltalake S3 operations.

        Uses :func:`pipeline.secrets.resolve_aws_credentials` for AWS
        credentials so that demo mode uses ``_DEMO`` variants
        exclusively — no fallback to base credentials.  In production
        mode, uses base credentials only — no fallback to demo
        credentials.

        Keys use lowercase convention required by the ``object_store``
        Rust crate (e.g. ``aws_access_key_id``).  Uppercase keys like
        ``AWS_ACCESS_KEY_ID`` are silently ignored by ``object_store``,
        causing S3 authentication to fall back to EC2 instance metadata
        and fail on non-EC2 machines.

        When credentials are ``None`` (missing for the active mode),
        they are set to empty strings explicitly rather than omitted,
        preventing ``object_store`` from falling back to environment
        variables that may contain production credentials.

        For S3-compatible stores (MinIO), set ``S3_ENDPOINT_URL`` and
        ``S3_ALLOW_HTTP`` environment variables.
        """
        from pipeline.secrets import resolve_aws_credentials

        creds = resolve_aws_credentials()
        return creds.to_storage_options()


# ---------------------------------------------------------------------------
# Storage configuration
# ---------------------------------------------------------------------------


@dataclass
class StorageConfig:
    """Resolved storage configuration.

    Created by :func:`resolve_storage` based on environment variables,
    or injected explicitly by tests via :func:`use_storage`.

    All path fields are ``str`` (not ``Path``) so that S3 URIs like
    ``s3://bucket/prefix`` are represented correctly.  Use the backend
    convenience methods (:meth:`raw_path`, :meth:`normalized_path`,
    :meth:`analytics_path`) instead of constructing paths manually.
    """

    data_dir: str  # local path or s3:// URI prefix
    raw_dir: str  # local path or s3:// URI
    normalized_dir: str
    analytics_dir: str
    secrets_dir: str  # always local (only used by LocalBackend)
    encryption_key_file: str  # always local (or env-var sourced)
    backend: StorageBackend = field(default=None)  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.backend is None:
            self.backend = LocalBackend(Path(self.data_dir))

    # Convenience methods that delegate to the backend -----------------------

    def raw_path(self, table_name: str) -> str:
        """Return the full path for a raw-layer table."""
        return self.backend.table_path("raw", table_name)

    def normalized_path(self, table_name: str) -> str:
        """Return the full path for a normalized-layer table."""
        return self.backend.table_path("normalized", table_name)

    def analytics_path(self, table_name: str) -> str:
        """Return the full path for an analytics-layer table."""
        return self.backend.table_path("analytics", table_name)

    @property
    def storage_options(self) -> dict[str, str] | None:
        """Return storage options for ``deltalake`` operations.

        Returns ``None`` for local storage (no cloud config needed).
        Returns a dict of AWS credentials for S3 storage.
        """
        return self.backend.storage_options


# ---------------------------------------------------------------------------
# Module-level singleton management
# ---------------------------------------------------------------------------

_config: StorageConfig | None = None


def resolve_storage() -> StorageConfig:
    """Resolve and activate a :class:`StorageConfig`.

    Backend selection is controlled by the ``STORAGE_TYPE`` env var:

    - ``cloud`` (default when ``S3_BUCKET`` is set): use
      :class:`S3Backend` with AWS S3.
    - ``minio``: use :class:`S3Backend` with a MinIO-compatible
      endpoint (requires ``S3_ENDPOINT_URL``).
    - ``local`` (default when ``S3_BUCKET`` is not set): use
      :class:`LocalBackend` with the local filesystem.

    In demo mode (``DEMO=true``), storage paths are isolated:

    - **S3 mode**: uses ``S3_BUCKET_DEMO`` (or ``{S3_BUCKET}-demo``)
      and ``S3_PREFIX_DEMO`` (or ``"pipeline_demo"``).
    - **Local mode**: uses ``PIPELINE_DATA_DIR_DEMO`` (or
      ``{data_dir}_demo``).

    Tests must call :func:`use_storage` with a ``tmp_path``-based
    config to prevent accidental writes to the project's ``data/``
    directory.
    """
    global _config

    storage_type = get_storage_type()
    demo = is_demo()

    if storage_type in (STORAGE_TYPE_CLOUD, STORAGE_TYPE_MINIO):
        s3_bucket = os.environ.get("S3_BUCKET")
        if not s3_bucket:
            raise ValueError(
                f"STORAGE_TYPE is '{storage_type}' but S3_BUCKET is not set"
            )

        if storage_type == STORAGE_TYPE_MINIO:
            endpoint_url = os.environ.get("S3_ENDPOINT_URL")
            if not endpoint_url:
                logger.warning(
                    "STORAGE_TYPE is 'minio' but S3_ENDPOINT_URL is not set; "
                    "MinIO typically requires an endpoint URL"
                )

        if demo:
            bucket = os.environ.get("S3_BUCKET_DEMO") or f"{s3_bucket}-demo"
            prefix = os.environ.get("S3_PREFIX_DEMO", "pipeline_demo")
        else:
            bucket = s3_bucket
            prefix = os.environ.get("S3_PREFIX", S3_DEFAULT_PREFIX)
        backend = S3Backend(bucket=bucket, prefix=prefix)
        base = f"s3://{backend.bucket}/{prefix}"
        config = StorageConfig(
            data_dir=base,
            raw_dir=f"{base}/raw",
            normalized_dir=f"{base}/normalized",
            analytics_dir=f"{base}/analytics",
            secrets_dir=str(PROJECT_ROOT / ".secrets"),
            encryption_key_file=str(PROJECT_ROOT / ".secrets" / "encryption.key"),
            backend=backend,
        )
    else:
        data_dir_str = os.environ.get("PIPELINE_DATA_DIR")
        if data_dir_str:
            data_dir = Path(data_dir_str)
        else:
            data_dir = PROJECT_ROOT / "data"

        if demo:
            demo_dir_str = os.environ.get("PIPELINE_DATA_DIR_DEMO")
            if demo_dir_str:
                data_dir = Path(demo_dir_str)
            else:
                data_dir = Path(f"{data_dir}_demo")

        config = StorageConfig(
            data_dir=str(data_dir),
            raw_dir=str(data_dir / "raw"),
            normalized_dir=str(data_dir / "normalized"),
            analytics_dir=str(data_dir / "analytics"),
            secrets_dir=str(PROJECT_ROOT / ".secrets"),
            encryption_key_file=str(PROJECT_ROOT / ".secrets" / "encryption.key"),
            backend=LocalBackend(data_dir),
        )

    _config = config
    return config


def use_storage(config: StorageConfig) -> StorageConfig:
    """Set the global storage configuration explicitly.

    Used by tests to inject a ``tmp_path``-based config, and by the
    CLI to set the storage before any path references are resolved.
    """
    global _config
    _config = config
    return config


def get_storage() -> StorageConfig:
    """Return the current storage configuration.

    Lazily initialises with defaults if not yet configured.
    """
    global _config
    if _config is None:
        _config = resolve_storage()
    return _config
