"""Story 5-4: per-account staleness flag + account purge escape hatch.

Exercises the per-account freshness check (AC-1..AC-3) and the purge command
(AC-4..AC-5) against REAL local Delta tables in ``tmp_path`` — deltalake and
polars are never mocked. Groups: T3.1 stale+fresh accounts, T3.2 all-fresh /
empty / unregistered, T3.3 byte-identical re-fetch regression (issue #157),
T3.4 purge scope, T3.5 purge dry-run.
"""

from __future__ import annotations

import argparse
import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from pipeline.analytics.quality import (
    ACCOUNT_STALENESS_KEYS,
    PASS,
    WARN,
    check_account_freshness,
    check_freshness,
    run_validation,
)
from pipeline.crypto import generate_key
from pipeline.normalized.models import (
    events_normalized_schema,
    snapshot_normalized_schema,
)
from pipeline.raw.models import RAW_SCHEMA
from pipeline.run import cmd_purge_account
from tests.fixtures.xtb import build_new_format_xlsx_bytes

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _snapshot(rows: list[dict]) -> pa.Table:
    """Build a snapshot table from row dicts (fetched_at, account_id, ...)."""
    columns = {f.name: [] for f in snapshot_normalized_schema}
    for row in rows:
        for name, values in columns.items():
            values.append(row.get(name))
    return pa.table(columns, schema=snapshot_normalized_schema)


def _snapshot_row(account_id: str, fetched_at: datetime) -> dict:
    """One minimal snapshot row (only freshness-relevant fields vary)."""
    return {
        "fetched_at": fetched_at,
        "account_id": account_id,
        "position_type": "EQUITY",
        "label": "SXR8.DE",
        "asset_class": "ETF",
        "security_value": b"x",
        "security_ccy": "EUR",
        "isin": "",
        "description": "Core S&P 500",
    }


def _events(rows: list[dict]) -> pa.Table:
    """Build an events table from row dicts (fetched_at, account_id, ...)."""
    columns = {f.name: [] for f in events_normalized_schema}
    for row in rows:
        for name, values in columns.items():
            values.append(row.get(name))
    return pa.table(columns, schema=events_normalized_schema)


def _event_row(account_id: str, fetched_at: datetime, event_id: str) -> dict:
    """One minimal events row (only freshness-relevant fields vary)."""
    return {
        "fetched_at": fetched_at,
        "broker": "XTB",
        "account_id": account_id,
        "event_id": event_id,
        "source": "XTB_REPORT",
        "event_type": "DIVIDEND",
        "raw_event_type": "Dividend",
        "event_datetime": "2026-01-01 00:00:00",
        "security_ccy": "EUR",
        "instrument_ccy": None,
        "cash_amount": b"x",
        "settle_date": None,
        "ticker": "",
        "isin": "",
        "description": "",
        "quantity": None,
        "price": None,
        "side": None,
        "fee_amount": None,
        "tax_amount": None,
        "target_fx_rate": None,
        "target_value": None,
        "target_ccy": None,
    }


def _raw_table(rows: list[dict]) -> pa.Table:
    """Build a raw table from row dicts (RAW_SCHEMA)."""
    columns = {f.name: [] for f in RAW_SCHEMA}
    for row in rows:
        for name, values in columns.items():
            values.append(row.get(name))
    return pa.table(columns, schema=RAW_SCHEMA)


def _raw_row(
    source: str,
    payload: bytes,
    *,
    account_id: str | None,
    fetched_at: datetime | None = None,
    broker: str = "XTB",
) -> dict:
    """One RAW_SCHEMA row spec; ``payload_hash`` is the plaintext digest."""
    return {
        "fetched_at": fetched_at or datetime.now(UTC),
        "broker": broker,
        "source": source,
        "payload": payload,
        "payload_hash": hashlib.sha256(payload).hexdigest(),
        "account_id": account_id,
    }


def _read(path: str) -> pa.Table:
    return DeltaTable(path).to_pyarrow_table()


# ---------------------------------------------------------------------------
# T3.1 / T3.2 — per-account staleness check
# ---------------------------------------------------------------------------


class TestAccountFreshness:
    """AC-1/AC-2: stale accounts flagged per-account; fresh accounts are not."""

    def test_stale_and_fresh_account_warns_stale_only(self) -> None:
        """A stale account is named; a fresh account in the same table is not."""
        now = datetime.now(UTC)
        stale_ts = now - timedelta(days=30)
        table = _snapshot(
            [
                _snapshot_row("111", stale_ts),
                _snapshot_row("222", now),
            ]
        )

        result = check_account_freshness("xtb_snapshot", table, "account_id", 7)
        assert result.status == WARN
        assert "111" in result.details
        assert "222" not in result.details

        # The table-level freshness check still PASSes — the fresh account's
        # rows mask the stale account's old max (the stale-hiding path).
        result = check_freshness("xtb_snapshot", table, "fetched_at", 7)
        assert result.status == PASS

    def test_all_accounts_fresh_passes(self) -> None:
        now = datetime.now(UTC)
        table = _snapshot(
            [
                _snapshot_row("111", now),
                _snapshot_row("222", now),
            ]
        )
        result = check_account_freshness("xtb_snapshot", table, "account_id", 7)
        assert result.status == PASS

    def test_empty_table_passes(self) -> None:
        """An empty table is PASS — freshness is not applicable (ADR 0072)."""
        table = _snapshot([])
        result = check_account_freshness("xtb_snapshot", table, "account_id", 7)
        assert result.status == PASS

    def test_missing_key_column_passes(self) -> None:
        """A table without the registered key column is PASS (not applicable)."""
        table = pa.table({"fetched_at": [datetime.now(UTC)]})
        result = check_account_freshness("xtb_snapshot", table, "account_id", 7)
        assert result.status == PASS

    def test_unregistered_snapshots_not_checked(self, tmp_data_dir: Path) -> None:
        """Trading 212 / IBKR snapshots are not in the registry and get no
        ``account_freshness`` check from run_validation."""
        assert "trading212_snapshot" not in ACCOUNT_STALENESS_KEYS
        assert "ibkr_snapshot" not in ACCOUNT_STALENESS_KEYS

        now = datetime.now(UTC)
        write_deltalake(
            str(tmp_data_dir / "normalized" / "trading212_snapshot"),
            _snapshot([_snapshot_row("", now)]),
            mode="overwrite",
        )
        rc = run_validation(
            fernet_key=generate_key(),
            freshness_days=7,
            tables=["trading212_snapshot"],
        )
        assert rc == 0

        dq = _read(str(tmp_data_dir / "analytics" / "data_quality"))
        check_names = set(dq.column("check_name").to_pylist())
        assert "account_freshness" not in check_names

    def test_run_validation_emits_account_freshness_for_xtb(
        self, tmp_data_dir: Path
    ) -> None:
        """run_validation wires the per-account check for registered tables."""
        now = datetime.now(UTC)
        stale_ts = now - timedelta(days=30)
        write_deltalake(
            str(tmp_data_dir / "normalized" / "xtb_snapshot"),
            _snapshot(
                [
                    _snapshot_row("111", stale_ts),
                    _snapshot_row("222", now),
                ]
            ),
            mode="overwrite",
        )
        rc = run_validation(
            fernet_key=generate_key(),
            freshness_days=7,
            tables=["xtb_snapshot"],
        )
        assert rc == 0  # WARN without fail_on_warn

        dq = _read(str(tmp_data_dir / "analytics" / "data_quality"))
        rows = dq.to_pylist()
        acct = [r for r in rows if r["check_name"] == "account_freshness"]
        assert len(acct) == 1
        assert acct[0]["status"] == WARN
        assert "111" in acct[0]["details"]

        fresh = [r for r in rows if r["check_name"] == "freshness"]
        assert fresh[0]["status"] == PASS


# ---------------------------------------------------------------------------
# T3.3 — byte-identical re-fetch regression (issue #157)
# ---------------------------------------------------------------------------


class TestByteIdenticalRefetch:
    """AC-3: the 5-2 merge write bumps ``fetched_at`` on a byte-identical
    re-fetch, so no stale warning is emitted."""

    def test_merge_write_bumps_fetched_at_no_stale_warning(
        self, tmp_data_dir: Path, fernet_key: bytes
    ) -> None:
        from pipeline.connectors.xtb.transform import transform_snapshot
        from pipeline.raw.ingest import ingest_raw

        raw_path = str(tmp_data_dir / "raw" / "xtb")
        old_ts = datetime.now(UTC) - timedelta(days=30)
        payload = build_new_format_xlsx_bytes(account_id="111")

        ingest_raw(
            _raw_table(
                [_raw_row("XTB_REPORT", payload, account_id="111", fetched_at=old_ts)]
            ),
            raw_path,
            fernet_key,
            "xtb",
        )
        now_ts = datetime.now(UTC)
        ingest_raw(
            _raw_table(
                [_raw_row("XTB_REPORT", payload, account_id="111", fetched_at=now_ts)]
            ),
            raw_path,
            fernet_key,
            "xtb",
        )

        # The merge write replaced the matched row in place — no duplication,
        # and fetched_at reflects the current fetch (issue #157).
        raw = _read(raw_path)
        assert raw.num_rows == 1
        assert raw.column("fetched_at")[0].as_py() == now_ts

        # The transform propagates raw's fetched_at to silver.
        snapshot = transform_snapshot(raw, fernet_key)
        assert snapshot.num_rows == 3
        assert all(ts.as_py() == now_ts for ts in snapshot.column("fetched_at"))

        assert (
            check_account_freshness("xtb_snapshot", snapshot, "account_id", 7).status
            == PASS
        )
        assert check_freshness("xtb_snapshot", snapshot, "fetched_at", 7).status == PASS


# ---------------------------------------------------------------------------
# T3.4 / T3.5 — purge escape hatch
# ---------------------------------------------------------------------------


class TestPurgeAccount:
    """AC-4/AC-5: purge removes one account's records everywhere; dry-run
    without ``--yes`` deletes nothing."""

    def _write_tables(self, tmp_data_dir: Path) -> None:
        now = datetime.now(UTC)
        write_deltalake(
            str(tmp_data_dir / "raw" / "xtb"),
            _raw_table(
                [
                    _raw_row("XTB_REPORT", b"r1", account_id="111"),
                    _raw_row("XTB_REPORT", b"r2", account_id="222"),
                    _raw_row("XTB_REPORT", b"r3", account_id=None),
                ]
            ),
            mode="overwrite",
        )
        write_deltalake(
            str(tmp_data_dir / "normalized" / "xtb_snapshot"),
            _snapshot(
                [
                    _snapshot_row("111", now),
                    _snapshot_row("222", now),
                ]
            ),
            mode="overwrite",
        )
        write_deltalake(
            str(tmp_data_dir / "normalized" / "xtb_events"),
            _events(
                [
                    _event_row("111", now, "e1"),
                    _event_row("222", now, "e2"),
                ]
            ),
            mode="overwrite",
        )
        # A different broker's raw table must be untouched by the purge.
        write_deltalake(
            str(tmp_data_dir / "raw" / "trading212"),
            _raw_table(
                [
                    _raw_row(
                        "/equity/positions",
                        b"p1",
                        account_id=None,
                        broker="Trading 212",
                    )
                ]
            ),
            mode="overwrite",
        )

    def test_purge_removes_target_account_only(
        self, tmp_data_dir: Path, capsys: pytest.CaptureFixture
    ) -> None:
        self._write_tables(tmp_data_dir)
        rc = cmd_purge_account(
            argparse.Namespace(broker="xtb", account_id="111", yes=True)
        )
        assert rc == 0

        raw = _read(str(tmp_data_dir / "raw" / "xtb"))
        raw_ids = raw.column("account_id").to_pylist()
        assert "111" not in raw_ids
        assert "222" in raw_ids
        assert None in raw_ids  # NULL-keyed raw row untouched (AC-5 residue)

        snap = _read(str(tmp_data_dir / "normalized" / "xtb_snapshot"))
        assert snap.column("account_id").to_pylist() == ["222"]

        events = _read(str(tmp_data_dir / "normalized" / "xtb_events"))
        assert events.column("account_id").to_pylist() == ["222"]

        # Other brokers untouched.
        t212 = _read(str(tmp_data_dir / "raw" / "trading212"))
        assert t212.num_rows == 1

        out = capsys.readouterr().out
        assert "deleted" in out

    def test_purge_trading212_raises(self) -> None:
        """Trading 212's snapshot account_id is '' and its raw retention key is
        'source' — no per-account identity to purge (AC-5)."""
        with pytest.raises(RuntimeError):
            cmd_purge_account(
                argparse.Namespace(broker="trading212", account_id="x", yes=True)
            )

    def test_purge_without_yes_deletes_nothing(
        self, tmp_data_dir: Path, capsys: pytest.CaptureFixture
    ) -> None:
        self._write_tables(tmp_data_dir)
        rc = cmd_purge_account(
            argparse.Namespace(broker="xtb", account_id="111", yes=False)
        )
        assert rc == 0

        out = capsys.readouterr().out
        assert "Would delete" in out
        assert "WHERE account_id = '111'" in out

        assert _read(str(tmp_data_dir / "raw" / "xtb")).num_rows == 3
        assert _read(str(tmp_data_dir / "normalized" / "xtb_snapshot")).num_rows == 2
        assert _read(str(tmp_data_dir / "normalized" / "xtb_events")).num_rows == 2
