"""Swappable storage configuration for data paths.

Supports local filesystem paths and is designed for future extension
to S3/GCS cloud storage backends.  The active configuration is a
module-level singleton resolved from the ``--env`` CLI flag or the
``PIPELINE_ENV`` environment variable.

Usage::

    from pipeline.storage import get_storage

    config = get_storage()
    raw_path = config.raw_path("ibkr_snapshot")
    # For prod: "/abs/path/to/data/raw/ibkr_snapshot"
    # For dev:  "/abs/path/to/data-dev/raw/ibkr_snapshot"
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable

PROJECT_ROOT = Path(__file__).resolve().parents[1]

_VALID_ENVS = frozenset({"prod", "dev"})


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

    Created by :func:`resolve_storage` based on the active environment,
    or injected explicitly by tests via :func:`use_storage`.
    """

    env: str
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


def resolve_storage(env: str | None = None) -> StorageConfig:
    """Resolve and activate a :class:`StorageConfig` for *env*.

    Priority::

      1. Explicit *env* parameter (from CLI ``--env``)
      2. ``PIPELINE_ENV`` environment variable
      3. Default ``"prod"``

    Only ``"prod"`` and ``"dev"`` are accepted.  ``"test"`` must be
    configured explicitly via :func:`use_storage` to prevent accidental
    writes to the project's ``data/`` directory during tests.
    """
    global _config

    if env is None:
        env = os.environ.get("PIPELINE_ENV", "prod")

    env = env.lower()

    if env not in _VALID_ENVS:
        raise ValueError(
            f"Unknown environment {env!r}. "
            f"Accepted values: {', '.join(sorted(_VALID_ENVS))}. "
            "Tests must use use_storage() to inject a tmp_path config."
        )

    if env == "dev":
        data_dir = PROJECT_ROOT / "data-dev"
    else:
        data_dir = PROJECT_ROOT / "data"

    config = StorageConfig(
        env=env,
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
    CLI to set the env before any path references are resolved.
    """
    global _config
    _config = config
    return config


def get_storage() -> StorageConfig:
    """Return the current storage configuration.

    Lazily initialises with ``"prod"`` defaults if not yet configured.
    """
    global _config
    if _config is None:
        _config = resolve_storage("prod")
    return _config