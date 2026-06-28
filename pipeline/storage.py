"""Swappable storage configuration for data paths.

Supports local filesystem paths and is designed for future extension
to S3/GCS cloud storage backends.  The active configuration is a
module-level singleton resolved from the ``PIPELINE_DATA_DIR``
environment variable or the project default.

Usage::

    from pipeline.storage import get_storage

    config = get_storage()
    raw_path = config.raw_path("ibkr_snapshot")
    # e.g. "/abs/path/to/data/raw/ibkr_snapshot"
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable

PROJECT_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Storage backend protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class StorageBackend(Protocol):
    """Protocol for storage backends.

    Initially only :class:`LocalBackend` exists.  Future implementations
    (``S3Backend``, ``GCSBackend``) will implement this protocol so that
    ``table_path()`` returns the appropriate URI scheme.
    """

    def table_path(self, layer: str, table_name: str) -> str: ...
    def ensure_parent(self, table_path: str) -> None: ...


class LocalBackend:
    """Local filesystem storage backend.

    ``table_path()`` returns absolute filesystem paths.
    ``ensure_parent()`` creates parent directories as needed.
    """

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir.resolve()

    def table_path(self, layer: str, table_name: str) -> str:
        return str(self.data_dir / layer / table_name)

    def ensure_parent(self, table_path: str) -> None:
        Path(table_path).parent.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Storage configuration
# ---------------------------------------------------------------------------


@dataclass
class StorageConfig:
    """Resolved storage configuration.

    Created by :func:`resolve_storage` based on the ``PIPELINE_DATA_DIR``
    environment variable, or injected explicitly by tests via
    :func:`use_storage`.
    """

    data_dir: Path
    raw_dir: Path
    normalized_dir: Path
    analytics_dir: Path
    secrets_dir: Path
    encryption_key_file: Path
    backend: StorageBackend = field(default=None)  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.backend is None:
            self.backend = LocalBackend(self.data_dir)

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


# ---------------------------------------------------------------------------
# Module-level singleton management
# ---------------------------------------------------------------------------

_config: StorageConfig | None = None


def resolve_storage() -> StorageConfig:
    """Resolve and activate a :class:`StorageConfig`.

    Data directory priority:

    1. ``PIPELINE_DATA_DIR`` environment variable (set by Bitwarden or
       manually).
    2. ``PROJECT_ROOT / "data"`` (default, for local dev without
       secrets).

    Tests must call :func:`use_storage` with a ``tmp_path``-based
    config to prevent accidental writes to the project's ``data/``
    directory.
    """
    global _config

    data_dir_str = os.environ.get("PIPELINE_DATA_DIR")
    if data_dir_str:
        data_dir = Path(data_dir_str)
    else:
        data_dir = PROJECT_ROOT / "data"

    config = StorageConfig(
        data_dir=data_dir,
        raw_dir=data_dir / "raw",
        normalized_dir=data_dir / "normalized",
        analytics_dir=data_dir / "analytics",
        secrets_dir=PROJECT_ROOT / ".secrets",
        encryption_key_file=PROJECT_ROOT / ".secrets" / "encryption.key",
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