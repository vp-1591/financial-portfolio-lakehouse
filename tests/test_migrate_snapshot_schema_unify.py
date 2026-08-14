"""Tests for the snapshot-schema-unify migration (rename ``name`` -> ``description``).

Covers the unit-testable ``rename_name_to_description()`` entry point against
local temp Delta tables (``storage_opts={}``; deltalake accepts local paths
with empty storage options, mirroring the ``write_deltalake`` local-temp
pattern in ``tests/test_report.py``). The order-sensitive schema assertion would
have caught the column-reorder bug fixed in commit 3d2271d.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from pipeline.migrations.migrate_snapshot_schema_unify import (
    rename_name_to_description,
)
from pipeline.normalized.models import snapshot_normalized_schema

_TS = pa.timestamp("us", tz="UTC")


def _old_snapshot_table() -> pa.Table:
    """Build an old-schema snapshot table with ``name`` placed mid-schema.

    ``name`` sits where the old T212/XTB schema had it (NOT last), so a plain
    rename would leave ``description`` mid-schema and fail the order-sensitive
    ``schema.equals`` check. The column set matches the target schema's set
    (``name`` standing in for ``description``).
    """
    cols = {
        "fetched_at": pa.array([datetime(2024, 1, 1, tzinfo=UTC)], type=_TS),
        "account_id": pa.array(["acct-1"], type=pa.string()),
        "position_type": pa.array(["EQUITY"], type=pa.string()),
        "label": pa.array(["VWCE"], type=pa.string()),
        "asset_class": pa.array(["EQUITY"], type=pa.string()),
        "name": pa.array(["Vanguard FTSE All-World"], type=pa.string()),
        "security_value": pa.array([b"\x01\x02\x03"], type=pa.binary()),
        "security_ccy": pa.array(["EUR"], type=pa.string()),
        "isin": pa.array(["IE00BK5BQT80"], type=pa.string()),
    }
    return pa.table(cols)


def _write(path: Path, table: pa.Table) -> None:
    write_deltalake(str(path), table, mode="overwrite", schema_mode="overwrite")


def _read(path: Path) -> pa.Table:
    return DeltaTable(str(path)).to_pyarrow_table()


def test_rename_migrates_name_to_description(tmp_path: Path) -> None:
    table_path = tmp_path / "trading212_snapshot"
    _write(table_path, _old_snapshot_table())

    migrated = rename_name_to_description(
        "trading212_snapshot", str(table_path), {}, snapshot_normalized_schema
    )

    assert migrated is True
    result = _read(table_path)
    assert "description" in result.column_names
    assert "name" not in result.column_names
    # Order-sensitive equality: catches the column-reorder bug (description must
    # be last, not where `name` sat mid-schema). quality.check_schema relies on
    # the same order-sensitive schema.equals.
    assert result.schema == snapshot_normalized_schema
    # `description` carries the old `name` data.
    assert result.column("description").to_pylist() == ["Vanguard FTSE All-World"]


def test_rename_is_idempotent_when_already_migrated(tmp_path: Path) -> None:
    table_path = tmp_path / "trading212_snapshot"
    # Already in the target schema (has description, no name).
    already = _old_snapshot_table().rename_columns({"name": "description"})
    already = already.select(list(snapshot_normalized_schema.names))
    _write(table_path, already)

    migrated = rename_name_to_description(
        "trading212_snapshot", str(table_path), {}, snapshot_normalized_schema
    )

    assert migrated is False
    assert _read(table_path).column_names == list(snapshot_normalized_schema.names)


def test_rename_returns_false_for_missing_table(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist"

    migrated = rename_name_to_description(
        "trading212_snapshot", str(missing), {}, snapshot_normalized_schema
    )

    assert migrated is False


def test_dry_run_does_not_write(tmp_path: Path) -> None:
    table_path = tmp_path / "xtb_snapshot"
    _write(table_path, _old_snapshot_table())

    migrated = rename_name_to_description(
        "xtb_snapshot", str(table_path), {}, snapshot_normalized_schema, dry_run=True
    )

    assert migrated is True
    # Unchanged: still has `name`, no `description`.
    result = _read(table_path)
    assert "name" in result.column_names
    assert "description" not in result.column_names


def test_rename_raises_on_unexpected_columns(tmp_path: Path) -> None:
    """A table with `name` but an unexpected column set raises instead of
    silently returning False, so the migration exits non-zero on a real schema
    problem rather than reporting 'nothing to migrate'."""
    table_path = tmp_path / "trading212_snapshot"
    bad = _old_snapshot_table().append_column(
        "unexpected", pa.array(["x"], type=pa.string())
    )
    _write(table_path, bad)

    with pytest.raises(RuntimeError, match="Column set mismatch"):
        rename_name_to_description(
            "trading212_snapshot", str(table_path), {}, snapshot_normalized_schema
        )


def test_rename_propagates_non_notfound_errors(monkeypatch, tmp_path: Path) -> None:
    """An auth/region/permission error opening an existing table is NOT
    swallowed as 'absent' (only TableNotFoundError is) -- it propagates so
    main() exits non-zero, the signal a pre-deploy gate needs."""
    import pipeline.migrations.migrate_snapshot_schema_unify as mod

    def _boom(*args: object, **kwargs: object) -> object:
        raise OSError("simulated S3 auth/region error")

    monkeypatch.setattr(mod, "DeltaTable", _boom)

    with pytest.raises(OSError, match="simulated S3 auth/region error"):
        rename_name_to_description(
            "trading212_snapshot",
            str(tmp_path / "trading212_snapshot"),
            {},
            snapshot_normalized_schema,
        )
