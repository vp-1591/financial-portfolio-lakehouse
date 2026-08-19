"""Tests for the drop-gross_amount migration (drop the ``gross_amount`` column).

Covers the unit-testable ``drop_gross_amount()`` entry point against local
temp Delta tables (``storage_opts={}``; deltalake accepts local paths with
empty storage options, mirroring the ``write_deltalake`` local-temp pattern in
``tests/test_report.py``). The order-sensitive schema assertion mirrors
``quality.check_schema``'s order-sensitive ``schema.equals``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

import pipeline.migrations.migrate_cdc_events_drop_gross_amount as mod
from pipeline.migrations.migrate_cdc_events_drop_gross_amount import (
    drop_gross_amount,
)
from pipeline.normalized.models import cdc_events_normalized_schema

_TS = pa.timestamp("us", tz="UTC")


def _old_cdc_table() -> pa.Table:
    """Build an old-schema CDC table with ``gross_amount`` after ``side``.

    Every column matches ``cdc_events_normalized_schema`` (same fields, same
    order) except that ``gross_amount`` (binary) is inserted right after
    ``side`` — its historical position. The order-sensitive schema assertion
    in ``drop_gross_amount`` depends on this exact ordering.
    """
    timestamp = datetime(2024, 1, 1, tzinfo=UTC)
    arrays: dict[str, pa.Array] = {}
    for field in cdc_events_normalized_schema:
        if pa.types.is_timestamp(field.type):
            values = [timestamp]
        elif pa.types.is_binary(field.type):
            values = [b"\x01"]
        else:
            values = [""]
        arrays[field.name] = pa.array(values, type=field.type)

    names = list(cdc_events_normalized_schema.names)
    side_index = names.index("side")
    names.insert(side_index + 1, "gross_amount")
    arrays["gross_amount"] = pa.array([b"\x01"], type=pa.binary())

    return pa.table(arrays).select(names)


def _write(path: Path, table: pa.Table) -> None:
    write_deltalake(str(path), table, mode="overwrite", schema_mode="overwrite")


def _read(path: Path) -> pa.Table:
    return DeltaTable(str(path)).to_pyarrow_table()


def test_drop_migrates_table(tmp_path: Path) -> None:
    table_path = tmp_path / "cdc_events"
    _write(table_path, _old_cdc_table())

    migrated = drop_gross_amount(str(table_path), {})

    assert migrated is True
    result = _read(table_path)
    assert "gross_amount" not in result.column_names
    # Order-sensitive equality: quality.check_schema relies on the same
    # order-sensitive schema.equals.
    assert result.schema == cdc_events_normalized_schema


def test_drop_is_idempotent(tmp_path: Path) -> None:
    table_path = tmp_path / "cdc_events"
    # Already in the target schema (no gross_amount).
    already = _old_cdc_table().select(list(cdc_events_normalized_schema.names))
    _write(table_path, already)

    migrated = drop_gross_amount(str(table_path), {})

    assert migrated is False
    assert _read(table_path).column_names == list(cdc_events_normalized_schema.names)


def test_drop_returns_false_for_missing_table(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist"

    migrated = drop_gross_amount(str(missing), {})

    assert migrated is False


def test_dry_run_does_not_write(tmp_path: Path) -> None:
    table_path = tmp_path / "cdc_events"
    _write(table_path, _old_cdc_table())

    migrated = drop_gross_amount(str(table_path), {}, dry_run=True)

    assert migrated is True
    # Unchanged: still has gross_amount.
    result = _read(table_path)
    assert "gross_amount" in result.column_names


def test_drop_raises_on_unexpected_columns(tmp_path: Path) -> None:
    """A table with gross_amount but an unexpected column set raises instead of
    silently returning False, so the migration exits non-zero on a real schema
    problem rather than reporting 'nothing to migrate'."""
    table_path = tmp_path / "cdc_events"
    bad = _old_cdc_table().append_column(
        "unexpected", pa.array(["x"], type=pa.string())
    )
    _write(table_path, bad)

    with pytest.raises(RuntimeError, match="Schema mismatch"):
        drop_gross_amount(str(table_path), {})


def test_drop_propagates_non_notfound_errors(monkeypatch, tmp_path: Path) -> None:
    """An auth/region/permission error opening an existing table is NOT
    swallowed as 'absent' (only TableNotFoundError is) -- it propagates so
    main() exits non-zero, the signal a pre-deploy gate needs."""

    def _boom(*args: object, **kwargs: object) -> object:
        raise OSError("simulated S3 auth/region error")

    monkeypatch.setattr(mod, "DeltaTable", _boom)

    with pytest.raises(OSError, match="simulated S3 auth/region error"):
        drop_gross_amount(str(tmp_path / "cdc_events"), {})
