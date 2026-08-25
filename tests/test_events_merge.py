"""Story 5.3 tests: append-preserving events MERGE + single bronze read (AD-4, AD-6).

The events write in ``transform_connector`` is a ``DeltaTable.merge`` keyed on
the broker's full event identity (AC-1: never ``event_id`` alone), with
``when_matched_update`` firing only when the incoming ``fetched_at`` is
strictly newer (``>``, pinned) and ``when_not_matched_insert_all`` otherwise;
nothing is ever deleted (AC-2), so an event absent from the current broker
response survives. ``raw/{broker}`` is read ONCE per broker run and the same
table feeds both the snapshot and the events transforms (AC-4).

Tests write REAL local Delta tables in ``tmp_path`` (deltalake/polars are
never mocked) and exercise the merge end-to-end through ``transform_connector``
for IBKR — its transforms read only ``fetched_at``/``source``/``payload``, so
raw tables are built inline with the NEW raw schema story 5-1 ships
(``fetched_at, broker, source, payload, payload_hash, account_id``) rather
than ``tests/fixtures/*.py`` (which 5-1 rewrites).
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from unittest.mock import patch

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from pipeline import run as run_module
from pipeline.connectors.registry import get
from pipeline.crypto import decrypt_float, encrypt
from pipeline.storage import get_storage

# Raw-table schema story 5-1 ships (nullable account_id, source_file dropped).
NEW_RAW_SCHEMA = pa.schema(
    [
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("broker", pa.string()),
        pa.field("source", pa.string()),
        pa.field("payload", pa.binary()),
        pa.field("payload_hash", pa.string()),
        pa.field("account_id", pa.string()),
    ]
)

T1 = datetime(2026, 8, 1, 6, 0, tzinfo=UTC)
T2 = datetime(2026, 8, 2, 6, 0, tzinfo=UTC)


def _txn(event_id: str, type_: str, amount: float, date: str = "20260115") -> dict:
    """One minimal IBKR CashTransaction for a synthetic Flex events payload."""
    return {
        "account": "U123",
        "date": date,
        "settle": date,
        "amount": amount,
        "type": type_,
        "event_id": event_id,
    }


def _flex_events_xml(transactions: list[dict]) -> bytes:
    """Build a minimal IBKR Flex events XML carrying the given CashTransactions."""
    txs = "".join(
        f'<CashTransaction accountId="{t["account"]}" currency="EUR" '
        f'fxRateToBase="1.0" dateTime="{t["date"]}" settleDate="{t["settle"]}" '
        f'amount="{t["amount"]}" type="{t["type"]}" transactionID="{t["event_id"]}"/>'
        for t in transactions
    )
    return (
        '<FlexQueryResponse queryName="events" type="AF">'
        '<FlexStatements count="1">'
        '<FlexStatement accountId="U123" fromDate="20260101" toDate="20260331">'
        f"<CashTransactions>{txs}</CashTransactions>"
        "</FlexStatement>"
        "</FlexStatements>"
        "</FlexQueryResponse>"
    ).encode()


def _ibkr_raw_events(
    payloads: list[tuple[datetime, list[dict]]], fernet_key: bytes
) -> pa.Table:
    """Build a raw ``{fetched_at, transactions}`` table on the NEW raw schema."""
    fetched_ats: list[datetime] = []
    brokers: list[str] = []
    sources: list[str] = []
    payload_col: list[bytes] = []
    hashes: list[str] = []
    account_ids: list[str | None] = []
    for fetched_at, transactions in payloads:
        raw_bytes = _flex_events_xml(transactions)
        fetched_ats.append(fetched_at)
        brokers.append("IBKR")
        sources.append("flex_events")
        payload_col.append(encrypt(raw_bytes, fernet_key))
        hashes.append(hashlib.sha256(raw_bytes).hexdigest())
        account_ids.append(None)
    return pa.table(
        {
            "fetched_at": fetched_ats,
            "broker": brokers,
            "source": sources,
            "payload": payload_col,
            "payload_hash": hashes,
            "account_id": account_ids,
        },
        schema=NEW_RAW_SCHEMA,
    )


def _write_raw_ibkr(raw: pa.Table) -> None:
    """Overwrite ``raw/ibkr`` with *raw* — simulating the bounded bronze after fetch."""
    raw_path = run_module.get_raw_path("ibkr")
    get_storage().backend.ensure_parent(raw_path)
    write_deltalake(
        raw_path, raw, mode="overwrite", storage_options=get_storage().storage_options
    )


def _read_events() -> pa.Table:
    norm_path = get_storage().normalized_path("ibkr_events")
    return DeltaTable(
        norm_path, storage_options=get_storage().storage_options
    ).to_pyarrow_table()


def _events_by_id(events: pa.Table) -> dict[str, dict]:
    return {row["event_id"]: row for row in events.to_pylist()}


@pytest.mark.usefixtures("docker_mode")
class TestEventsMergeAppendPreserving:
    """AC-2/AC-5: events absent from the current response survive the merge."""

    def test_event_absent_from_later_response_stays(
        self, tmp_data_dir, fernet_key: bytes
    ) -> None:
        """T3.1 CAP-2 success: an IBKR event missing from a later Flex response
        remains in normalized storage (the overwrite mode would drop it)."""
        _write_raw_ibkr(
            _ibkr_raw_events(
                [
                    (
                        T1,
                        [
                            _txn("CT001", "Dividends", 42.5),
                            _txn("CT002", "Dividends", 35.0),
                        ],
                    )
                ],
                fernet_key,
            )
        )
        assert run_module.transform_connector(get("ibkr"), fernet_key) == 0

        # Run 2's Flex response no longer includes CT002 (query window moved).
        _write_raw_ibkr(
            _ibkr_raw_events([(T2, [_txn("CT001", "Dividends", 42.5)])], fernet_key)
        )
        assert run_module.transform_connector(get("ibkr"), fernet_key) == 0

        events = _events_by_id(_read_events())
        assert set(events) == {"CT001", "CT002"}
        assert events["CT001"]["fetched_at"] == T2  # re-fetched -> updated
        assert events["CT002"]["fetched_at"] == T1  # absent -> untouched survivor

    def test_moving_flex_query_window_does_not_remove_existing_events(
        self, tmp_data_dir, fernet_key: bytes
    ) -> None:
        """T3.3: a forward window move adds the new events and never deletes the old."""
        _write_raw_ibkr(
            _ibkr_raw_events(
                [
                    (
                        T1,
                        [
                            _txn("CT001", "Dividends", 10.0),
                            _txn("CT002", "Dividends", 20.0),
                            _txn("CT003", "Deposits/Withdrawals", 100.0),
                        ],
                    )
                ],
                fernet_key,
            )
        )
        assert run_module.transform_connector(get("ibkr"), fernet_key) == 0

        # Window slid forward: CT001 falls out of the query, CT004 appears.
        _write_raw_ibkr(
            _ibkr_raw_events(
                [
                    (
                        T2,
                        [
                            _txn("CT002", "Dividends", 22.0),
                            _txn("CT003", "Deposits/Withdrawals", 100.0),
                            _txn("CT004", "Dividends", 15.0, date="20260401"),
                        ],
                    )
                ],
                fernet_key,
            )
        )
        assert run_module.transform_connector(get("ibkr"), fernet_key) == 0

        events = _events_by_id(_read_events())
        assert set(events) == {"CT001", "CT002", "CT003", "CT004"}
        assert events["CT001"]["fetched_at"] == T1  # window-loss survivor
        assert events["CT002"]["fetched_at"] == T2  # re-fetched, updated
        assert events["CT004"]["fetched_at"] == T2

    def test_merging_the_same_batch_twice_is_a_no_op(
        self, tmp_data_dir, fernet_key: bytes
    ) -> None:
        """T3.4: a repeated identical fetch converges — equal fetched_at never
        updates (``>`` pin) and matching identities never re-insert."""
        raw = _ibkr_raw_events(
            [
                (
                    T1,
                    [
                        _txn("CT001", "Dividends", 60.0),
                        _txn("CT002", "Dividends", 20.0),
                    ],
                )
            ],
            fernet_key,
        )
        _write_raw_ibkr(raw)
        assert run_module.transform_connector(get("ibkr"), fernet_key) == 0
        _write_raw_ibkr(raw)  # identical batch, identical fetched_at
        assert run_module.transform_connector(get("ibkr"), fernet_key) == 0

        events = _read_events()
        assert events.num_rows == 2
        rows = _events_by_id(events)
        assert rows["CT001"]["fetched_at"] == T1
        assert rows["CT002"]["fetched_at"] == T1
        assert set(events.column("event_id").to_pylist()) == {"CT001", "CT002"}


@pytest.mark.usefixtures("docker_mode")
class TestEventIdentityUpdateNewer:
    """AC-2 pin: ``when_matched_update`` fires only on a strictly-newer fetched_at."""

    def test_repeated_event_id_resolves_to_latest_fetched_at(
        self, tmp_data_dir, fernet_key: bytes
    ) -> None:
        """T3.2: the newest fetched_at wins, and older/equal re-fetches do not regress."""
        _write_raw_ibkr(
            _ibkr_raw_events([(T1, [_txn("CT001", "Dividends", 42.5)])], fernet_key)
        )
        assert run_module.transform_connector(get("ibkr"), fernet_key) == 0

        _write_raw_ibkr(
            _ibkr_raw_events([(T2, [_txn("CT001", "Dividends", 60.0)])], fernet_key)
        )
        assert run_module.transform_connector(get("ibkr"), fernet_key) == 0

        # OLDER re-fetch: the strictly-newer (``>``) pin must reject it.
        _write_raw_ibkr(
            _ibkr_raw_events([(T1, [_txn("CT001", "Dividends", 10.0)])], fernet_key)
        )
        assert run_module.transform_connector(get("ibkr"), fernet_key) == 0
        row = _events_by_id(_read_events())["CT001"]
        assert row["fetched_at"] == T2
        assert decrypt_float(row["cash_amount"], fernet_key) == 60.0

        # EQUAL fetched_at is also rejected — pins ``>`` over ``>=``.
        _write_raw_ibkr(
            _ibkr_raw_events([(T2, [_txn("CT001", "Dividends", 99.0)])], fernet_key)
        )
        assert run_module.transform_connector(get("ibkr"), fernet_key) == 0
        row = _events_by_id(_read_events())["CT001"]
        assert row["fetched_at"] == T2
        assert decrypt_float(row["cash_amount"], fernet_key) == 60.0


class TestXtbEventIdentityFullSubset:
    """AC-1/adversarial F8: XTB's merge key is the FULL (event_type, event_id,
    account_id) subset — same-ID events across accounts stay distinct."""

    def test_identity_subsets_match_acceptance_criteria(self) -> None:
        assert run_module.EVENT_IDENTITY_SUBSETS["ibkr"] == ("event_id",)
        assert run_module.EVENT_IDENTITY_SUBSETS["trading212"] == (
            "event_type",
            "event_id",
        )
        assert run_module.EVENT_IDENTITY_SUBSETS["xtb"] == (
            "event_type",
            "event_id",
            "account_id",
        )

    def test_same_event_id_across_two_accounts_stays_distinct(
        self, tmp_data_dir, fernet_key: bytes
    ) -> None:
        """T3.5: two DEPOSITs sharing event_id D1 on different accounts survive
        the merge — proof the predicate keys on more than ``event_id``."""
        schema = pa.schema(
            [
                pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
                pa.field("event_type", pa.string()),
                pa.field("event_id", pa.string()),
                pa.field("account_id", pa.string()),
                pa.field("amount", pa.float64()),
            ]
        )

        def _row(account_id: str, amount: float) -> pa.Table:
            return pa.table(
                {
                    "fetched_at": [T1],
                    "event_type": ["DEPOSIT"],
                    "event_id": ["D1"],
                    "account_id": [account_id],
                    "amount": [amount],
                },
                schema=schema,
            )

        norm_path = get_storage().normalized_path("xtb_events")
        get_storage().backend.ensure_parent(norm_path)
        for account_id, amount in (("11111111", 100.0), ("22222222", 200.0)):
            run_module._merge_events(
                norm_path,
                _row(account_id, amount),
                ("event_type", "event_id", "account_id"),
                get_storage().storage_options,
            )
        table = DeltaTable(
            norm_path, storage_options=get_storage().storage_options
        ).to_pyarrow_table()
        rows = table.to_pylist()
        assert len(rows) == 2
        assert {r["account_id"] for r in rows} == {"11111111", "22222222"}

        # Re-merging one account's batch is a no-op (both rows survive).
        run_module._merge_events(
            norm_path,
            _row("11111111", 100.0),
            ("event_type", "event_id", "account_id"),
            get_storage().storage_options,
        )
        table = DeltaTable(
            norm_path, storage_options=get_storage().storage_options
        ).to_pyarrow_table()
        assert table.num_rows == 2


@pytest.mark.usefixtures("docker_mode")
class TestSingleBronzeRead:
    """AC-4/T3.6: the raw/{broker} Delta table is opened once per broker run."""

    def test_bronze_table_is_read_once_per_broker_run(
        self, tmp_data_dir, fernet_key: bytes
    ) -> None:
        """T3.6: counting DeltaTable constructions shows one raw-table open for
        both layers and one events-target open for the MERGE."""
        from deltalake import DeltaTable as RealDeltaTable

        _write_raw_ibkr(
            _ibkr_raw_events([(T1, [_txn("CT001", "Dividends", 60.0)])], fernet_key)
        )
        # Seed the events target so the counted run exercises the MERGE path.
        assert run_module.transform_connector(get("ibkr"), fernet_key) == 0

        raw_path = run_module.get_raw_path("ibkr")
        events_path = get_storage().normalized_path("ibkr_events")
        opened: list[str] = []

        def _counting(path: str, **kwargs):
            opened.append(path)
            return RealDeltaTable(path, **kwargs)

        with patch("deltalake.DeltaTable", side_effect=_counting):
            assert run_module.transform_connector(get("ibkr"), fernet_key) == 0
        assert opened.count(raw_path) == 1  # single bronze read
        assert opened.count(events_path) == 1  # events MERGE target
