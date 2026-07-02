"""Tests for pipeline.connectors.transform_utils shared utilities."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from pipeline.connectors.transform_utils import (
    build_normalized_table,
    coerce_fetched_at,
    decode_payload,
    iter_raw_payloads,
    parse_json,
)
from pipeline.crypto import decrypt_float, encrypt, generate_key


class TestDecodePayload:
    """Tests for decode_payload."""

    @pytest.fixture
    def fernet_key(self) -> bytes:
        return generate_key()

    def test_decrypts_valid_payload(self, fernet_key: bytes) -> None:
        plaintext = b'{"positions": []}'
        encrypted = encrypt(plaintext, fernet_key)
        result = decode_payload(encrypted, fernet_key)
        assert result == plaintext

    def test_handles_memoryview(self, fernet_key: bytes) -> None:
        plaintext = b'{"data": 1}'
        encrypted = encrypt(plaintext, fernet_key)
        mv = memoryview(encrypted)
        result = decode_payload(mv, fernet_key)
        assert result == plaintext

    def test_returns_none_on_decryption_failure(self, fernet_key: bytes) -> None:
        wrong_key = generate_key()
        plaintext = b"secret"
        encrypted = encrypt(plaintext, fernet_key)
        result = decode_payload(encrypted, wrong_key)
        assert result is None

    def test_returns_none_on_garbage_input(self, fernet_key: bytes) -> None:
        result = decode_payload(b"not-encrypted", fernet_key)
        assert result is None


class TestParseJson:
    """Tests for parse_json."""

    def test_parses_valid_json(self) -> None:
        data = b'{"key": "value"}'
        result = parse_json(data)
        assert result == {"key": "value"}

    def test_parses_json_list(self) -> None:
        data = b"[1, 2, 3]"
        result = parse_json(data)
        assert result == [1, 2, 3]

    def test_returns_none_on_invalid_json(self) -> None:
        result = parse_json(b"not json")
        assert result is None

    def test_returns_none_on_none_input(self) -> None:
        result = parse_json(None)  # type: ignore[arg-type]
        assert result is None


class TestCoerceFetchedAt:
    """Tests for coerce_fetched_at."""

    def test_passes_through_datetime(self) -> None:
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert coerce_fetched_at(dt) is dt

    def test_parses_iso_string(self) -> None:
        result = coerce_fetched_at("2024-01-15T12:00:00+00:00")
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1

    def test_handles_naive_datetime_string(self) -> None:
        result = coerce_fetched_at("2024-06-01T00:00:00")
        assert isinstance(result, datetime)


class TestIterRawPayloads:
    """Tests for iter_raw_payloads."""

    @pytest.fixture
    def fernet_key(self) -> bytes:
        return generate_key()

    def _make_raw_table(
        self,
        rows: list[tuple[datetime, str, bytes, str, str]],
        fernet_key: bytes,
    ) -> pa.Table:
        """Build a raw table with encrypted payloads."""
        from pipeline.raw.models import RAW_SCHEMA

        fetched_ats = []
        brokers = []
        sources = []
        payloads = []
        payload_hashes = []
        account_ids = []
        source_files = []

        for fetched_at, source, payload, account_id, source_file in rows:
            fetched_ats.append(fetched_at)
            brokers.append("test_broker")
            sources.append(source)
            payloads.append(encrypt(payload, fernet_key))
            payload_hashes.append("hash_" + source)
            account_ids.append(account_id)
            source_files.append(source_file)

        return pa.table(
            {
                "fetched_at": fetched_ats,
                "broker": brokers,
                "source": sources,
                "payload": payloads,
                "payload_hash": payload_hashes,
                "account_id": account_ids,
                "source_file": source_files,
            },
            schema=RAW_SCHEMA,
        )

    def test_iterates_valid_rows(self, fernet_key: bytes) -> None:

        payload = json.dumps({"ticker": "AAPL"}).encode()
        table = self._make_raw_table(
            [
                (
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    "/positions",
                    payload,
                    "acct1",
                    "",
                )
            ],
            fernet_key,
        )

        rows = list(iter_raw_payloads(table, fernet_key))
        assert len(rows) == 1
        assert rows[0].account_id == "acct1"
        assert rows[0].source == "/positions"
        assert rows[0].payload_parsed == {"ticker": "AAPL"}

    def test_skips_rows_with_bad_decryption(self, fernet_key: bytes) -> None:

        payload = json.dumps({"ticker": "AAPL"}).encode()
        table = self._make_raw_table(
            [
                (
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    "/positions",
                    payload,
                    "acct1",
                    "",
                )
            ],
            fernet_key,
        )

        # Use a different key to simulate decryption failure
        wrong_key = generate_key()
        rows = list(iter_raw_payloads(table, wrong_key))
        assert len(rows) == 0

    def test_skips_rows_with_invalid_json(self, fernet_key: bytes) -> None:
        table = self._make_raw_table(
            [
                (
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    "/positions",
                    b"not-json",
                    "acct1",
                    "",
                )
            ],
            fernet_key,
        )

        rows = list(iter_raw_payloads(table, fernet_key))
        assert len(rows) == 0

    def test_require_json_false_yields_raw_bytes(self, fernet_key: bytes) -> None:
        table = self._make_raw_table(
            [
                (
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    "/positions",
                    b"<xml>data</xml>",
                    "acct1",
                    "",
                )
            ],
            fernet_key,
        )

        rows = list(iter_raw_payloads(table, fernet_key, require_json=False))
        assert len(rows) == 1
        assert rows[0].payload_parsed is None
        assert rows[0].payload_raw == b"<xml>data</xml>"

    def test_multiple_rows(self, fernet_key: bytes) -> None:
        payload1 = json.dumps({"ticker": "AAPL"}).encode()
        payload2 = json.dumps({"ticker": "MSFT"}).encode()

        table = self._make_raw_table(
            [
                (
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    "/positions",
                    payload1,
                    "acct1",
                    "",
                ),
                (
                    datetime(2024, 1, 2, tzinfo=timezone.utc),
                    "/positions",
                    payload2,
                    "acct2",
                    "",
                ),
            ],
            fernet_key,
        )

        rows = list(iter_raw_payloads(table, fernet_key))
        assert len(rows) == 2
        assert rows[0].payload_parsed == {"ticker": "AAPL"}
        assert rows[1].payload_parsed == {"ticker": "MSFT"}

    def test_empty_table(self, fernet_key: bytes) -> None:
        from pipeline.raw.models import RAW_SCHEMA

        table = pa.table(
            {
                "fetched_at": [],
                "broker": [],
                "source": [],
                "payload": [],
                "payload_hash": [],
                "account_id": [],
                "source_file": [],
            },
            schema=RAW_SCHEMA,
        )

        rows = list(iter_raw_payloads(table, fernet_key))
        assert len(rows) == 0


class TestBuildNormalizedTable:
    """Tests for build_normalized_table."""

    @pytest.fixture
    def fernet_key(self) -> bytes:
        return generate_key()

    def test_empty_records_returns_empty_table_with_correct_schema(
        self, fernet_key: bytes
    ) -> None:
        from pipeline.normalized.models import xtb_snapshot_normalized_schema

        result = build_normalized_table(
            [], xtb_snapshot_normalized_schema, fernet_key, encrypt_columns=["value"]
        )
        assert result.num_rows == 0
        assert result.schema.equals(xtb_snapshot_normalized_schema)

    def test_single_record_with_encrypted_column(self, fernet_key: bytes) -> None:
        from pipeline.normalized.models import xtb_snapshot_normalized_schema

        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        records = [
            {
                "fetched_at": now,
                "account_id": "XTB-123",
                "position_type": "EQUITY",
                "label": "VWCE.DE",
                "name": "Vanguard FTSE All-World",
                "asset_class": "EQUITY",
                "currency": "EUR",
                "value": 1000.0,
                "value_currency": "EUR",
                "isin": "IE00BK5BQT80",
            }
        ]
        result = build_normalized_table(
            records,
            xtb_snapshot_normalized_schema,
            fernet_key,
            encrypt_columns=["value"],
        )
        assert result.num_rows == 1
        assert result.column("label")[0].as_py() == "VWCE.DE"
        assert result.column("isin")[0].as_py() == "IE00BK5BQT80"
        # Verify encryption round-trip
        encrypted_value = result.column("value")[0].as_py()
        assert decrypt_float(encrypted_value, fernet_key) == 1000.0

    def test_multiple_encrypt_columns(self, fernet_key: bytes) -> None:
        from pipeline.normalized.models import trading212_cdc_normalized_schema

        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        records = [
            {
                "fetched_at": now,
                "account_id": "T212-ABC",
                "event_type": "ORDER",
                "event_id": "1",
                "ticker": "AAPL",
                "isin": "US0378331005",
                "currency": "USD",
                "value": 1800.0,
                "quantity": 10.0,
                "event_date": "2024-01-01",
            }
        ]
        result = build_normalized_table(
            records,
            trading212_cdc_normalized_schema,
            fernet_key,
            encrypt_columns=["value", "quantity"],
        )
        assert result.num_rows == 1
        assert decrypt_float(result.column("value")[0].as_py(), fernet_key) == 1800.0
        assert decrypt_float(result.column("quantity")[0].as_py(), fernet_key) == 10.0

    def test_no_encrypt_columns(self, fernet_key: bytes) -> None:
        schema = pa.schema(
            [
                pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
                pa.field("account_id", pa.string()),
                pa.field("label", pa.string()),
            ]
        )
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        records = [{"fetched_at": now, "account_id": "A1", "label": "X"}]
        result = build_normalized_table(records, schema, fernet_key)
        assert result.num_rows == 1
        assert result.column("account_id")[0].as_py() == "A1"
        assert result.column("label")[0].as_py() == "X"

    def test_schema_column_ordering_preserved(self, fernet_key: bytes) -> None:
        from pipeline.normalized.models import trading212_snapshot_normalized_schema

        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        records = [
            {
                "fetched_at": now,
                "account_id": "T212-1",
                "position_type": "EQUITY",
                "label": "AAPL",
                "name": "Apple Inc",
                "asset_class": "EQUITY",
                "currency": "USD",
                "value": 500.0,
                "value_currency": "USD",
                "isin": "US0378331005",
                "security_currency": "USD",
            }
        ]
        result = build_normalized_table(
            records,
            trading212_snapshot_normalized_schema,
            fernet_key,
            encrypt_columns=["value"],
        )
        assert list(result.schema.names) == list(
            trading212_snapshot_normalized_schema.names
        )
