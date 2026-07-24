"""Local-filesystem storage backend for tests.

``LocalBackend`` reads and writes Delta tables to the local filesystem
(absolute paths) instead of S3. It is used by tests for fast, isolated
runs over a ``tmp_path`` directory so they never touch S3/MinIO or the
project's ``data/`` directory.

This class used to live in :mod:`pipeline.storage`, but after the
``--mode`` flag (ADR 0090) :func:`pipeline.storage.resolve_storage` only
ever returns :class:`pipeline.storage.S3Backend` — there is no
``--mode local``. ``LocalBackend`` is therefore test-only and lives here.
"""

from __future__ import annotations

import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


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

    def staging_path(
        self, staging_prefix: str, connector_name: str, filename: str
    ) -> str:
        return str(self.data_dir / staging_prefix / connector_name / filename)

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
                "Rescued orphaned table %s -> %s",
                table_dir,
                rescue_dir,
            )

    @property
    def storage_options(self) -> dict[str, str]:
        return {"allow_unsafe_rename": "true"}
