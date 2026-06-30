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

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable

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
    def ensure_parent(self, table_path: str) -> None: ...
    @property
    def storage_options(self) -> dict[str, str] | None: ...


class LocalBackend:
    """Local filesystem storage backend.

    ``table_path()`` returns absolute filesystem paths.
    ``ensure_parent()`` creates parent directories as needed.
    ``storage_options`` returns ``None`` — no cloud config needed.
    """

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir.resolve()

    def table_path(self, layer: str, table_name: str) -> str:
        return str(self.data_dir / layer / table_name)

    def ensure_parent(self, table_path: str) -> None:
        Path(table_path).parent.mkdir(parents=True, exist_ok=True)

    @property
    def storage_options(self) -> dict[str, str] | None:
        return None


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

        Keys use lowercase convention required by the ``object_store``
        Rust crate (e.g. ``aws_access_key_id``).  Uppercase keys like
        ``AWS_ACCESS_KEY_ID`` are silently ignored by ``object_store``,
        causing S3 authentication to fall back to EC2 instance metadata
        and fail on non-EC2 machines.

        Empty credentials are omitted so that ``object_store`` can fall
        back to its own credential chain (environment variables, IAM
        role, etc.) rather than overriding with an empty string.
        """
        opts: dict[str, str] = {}
        key_id = os.environ.get("AWS_ACCESS_KEY_ID", "")
        secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        region = os.environ.get("AWS_REGION", "eu-west-1")
        if key_id:
            opts["aws_access_key_id"] = key_id
        if secret:
            opts["aws_secret_access_key"] = secret
        opts["aws_region"] = region
        return opts


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

    Backend selection priority:

    1. If ``S3_BUCKET`` env var is set, use :class:`S3Backend` with
       optional ``S3_PREFIX`` (default ``"pipeline"``).
    2. Otherwise, use :class:`LocalBackend` with ``PIPELINE_DATA_DIR``
       or the project default ``PROJECT_ROOT / "data"``.

    Tests must call :func:`use_storage` with a ``tmp_path``-based
    config to prevent accidental writes to the project's ``data/``
    directory.
    """
    global _config

    s3_bucket = os.environ.get("S3_BUCKET")

    if s3_bucket:
        prefix = os.environ.get("S3_PREFIX", S3_DEFAULT_PREFIX)
        backend = S3Backend(bucket=s3_bucket, prefix=prefix)
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
