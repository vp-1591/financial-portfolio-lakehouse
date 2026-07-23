"""Swappable storage configuration for data paths.

Storage backend selection is driven by the ``--mode`` CLI flag (set via
:func:`pipeline.secrets.set_mode` before calling :func:`resolve_storage`):

- **docker** — :class:`S3Backend` with MinIO endpoint (``S3_ENDPOINT_URL``).
- **staging** — :class:`S3Backend` with the demo S3 bucket
  (``S3_BUCKET_DEMO`` or ``{S3_BUCKET}-demo``, prefix ``pipeline_demo``).
- **prod** — :class:`S3Backend` with the production S3 bucket
  (``S3_BUCKET``, prefix ``pipeline``).

Local development can use a ``.env`` file (loaded by
:mod:`pipeline.secrets`) to set S3 credentials and bucket names.

Tests must call :func:`use_storage` with a ``tmp_path``-based
config to prevent accidental writes to the project's ``data/``
directory.

Usage::

    from pipeline.secrets import set_mode
    from pipeline.storage import get_storage

    set_mode("docker")
    config = get_storage()
    raw_path = config.raw_path("ibkr_snapshot")
    # e.g. "s3://my-bucket/pipeline/raw/ibkr_snapshot"  (S3Backend)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

from pipeline.secrets import (
    get_env,
    get_mode,
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

    :class:`S3Backend` returns ``s3://`` URIs. Tests may also use the
    local-filesystem backend in :mod:`tests.local_backend`.
    """

    def table_path(self, layer: str, table_name: str) -> str: ...
    def staging_path(
        self, staging_prefix: str, connector_name: str, filename: str
    ) -> str: ...
    def ensure_parent(self, table_path: str) -> None:
        """Create parent dirs and clean up orphaned files from failed writes."""
        ...

    @property
    def storage_options(self) -> dict[str, str] | None: ...


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

    def staging_path(
        self, staging_prefix: str, connector_name: str, filename: str
    ) -> str:
        if self.prefix:
            return f"s3://{self.bucket}/{self.prefix}/{staging_prefix}/{connector_name}/{filename}"
        return f"s3://{self.bucket}/{staging_prefix}/{connector_name}/{filename}"

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
    secrets_dir: str  # always local (filesystem path for secrets/key file)
    encryption_key_file: str  # always local (or env-var sourced)
    backend: StorageBackend  # S3Backend in all modes; tests inject a backend

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

    def staging_path(self, connector_name: str, filename: str) -> str:
        """Return the full path for a staging file.

        Uses ``staging`` prefix in production, ``staging_demo`` in demo
        mode — matching the existing ``pipeline``/``pipeline_demo`` pattern.
        """

        prefix = "staging_demo" if is_demo() else "staging"
        return self.backend.staging_path(prefix, connector_name, filename)

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

    Backend selection is driven by the execution mode (set via
    :func:`pipeline.secrets.set_mode`):

    - **docker** — :class:`S3Backend` with MinIO endpoint.  Requires
      ``S3_BUCKET``; warns if ``S3_ENDPOINT_URL`` is not set.
    - **staging** — :class:`S3Backend` with the demo S3 bucket
      (``S3_BUCKET_DEMO`` or ``{S3_BUCKET}-demo``, prefix ``pipeline_demo``).
    - **prod** — :class:`S3Backend` with the production S3 bucket
      (``S3_BUCKET``, prefix ``pipeline``).

    Tests must call :func:`use_storage` with a ``tmp_path``-based
    config to prevent accidental writes to the project's ``data/``
    directory.
    """
    global _config

    mode = get_mode()

    s3_bucket = get_env("S3_BUCKET")

    if mode == "docker":
        # MinIO — local S3-compatible storage.
        if not s3_bucket:
            raise ValueError(
                "S3_BUCKET is required in docker mode (MinIO). "
                "Set S3_BUCKET in .env or environment."
            )
        endpoint_url = get_env("S3_ENDPOINT_URL")
        if not endpoint_url:
            logger.warning(
                "S3_ENDPOINT_URL is not set; "
                "docker mode (MinIO) typically requires an endpoint URL"
            )
        prefix = get_env("S3_PREFIX", S3_DEFAULT_PREFIX)
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
    elif mode == "staging":
        # Demo S3 bucket.
        bucket = get_env("S3_BUCKET_DEMO") or (
            f"{s3_bucket}-demo" if s3_bucket else None
        )
        if not bucket:
            raise ValueError(
                "Staging mode requires S3_BUCKET_DEMO or S3_BUCKET "
                "to determine the S3 bucket"
            )
        prefix = get_env("S3_PREFIX_DEMO", "pipeline_demo")
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
    elif mode == "prod":
        # Production S3 bucket.
        if not s3_bucket:
            raise ValueError(
                "S3_BUCKET is required in prod mode. "
                "Set S3_BUCKET in .env or environment."
            )
        prefix = get_env("S3_PREFIX", S3_DEFAULT_PREFIX)
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
        raise ValueError(f"Unsupported mode: {mode!r}")

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
