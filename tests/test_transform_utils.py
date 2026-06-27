"""Tests for pipeline.connectors.transform_utils shared utilities."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from pipeline.connectors.transform_utils import (
    DecodedRow,
    coerce_fetched_at,
    decode_payload,
    iter_raw_payloads,
    parse_json,
)
from pipeline.crypto import decrypt, encrypt, generate_key


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
        data = b'[1, 2, 3]'
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
        from pipeline.raw.models import RAW_SCHEMA

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
        from pipeline.raw.models import RAW_SCHEMA

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