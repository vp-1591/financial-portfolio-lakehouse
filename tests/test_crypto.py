"""Tests for pipeline.crypto module."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.crypto import (
    decrypt,
    decrypt_float,
    decrypt_string,
    encrypt,
    encrypt_float,
    encrypt_string,
    generate_key,
    load_key,
)


class TestGenerateKey:
    def test_returns_bytes(self) -> None:
        key = generate_key()
        assert isinstance(key, bytes)
        assert len(key) > 0

    def test_generates_unique_keys(self) -> None:
        assert generate_key() != generate_key()


class TestEncryptDecrypt:
    def test_roundtrip_bytes(self) -> None:
        key = generate_key()
        plaintext = b"secret financial data"
        encrypted = encrypt(plaintext, key)
        assert encrypted != plaintext
        assert decrypt(encrypted, key) == plaintext

    def test_roundtrip_float(self) -> None:
        key = generate_key()
        value = 1234.5678
        encrypted = encrypt_float(value, key)
        assert decrypt_float(encrypted, key) == pytest.approx(value)

    def test_roundtrip_string(self) -> None:
        key = generate_key()
        text = "IE00BK5BQT80"
        encrypted = encrypt_string(text, key)
        assert decrypt_string(encrypted, key) == text

    def test_wrong_key_raises_error(self) -> None:
        key1 = generate_key()
        key2 = generate_key()
        encrypted = encrypt(b"test", key1)
        with pytest.raises(Exception):
            decrypt(encrypted, key2)

    def test_tampered_ciphertext_raises_error(self) -> None:
        key = generate_key()
        encrypted = encrypt(b"test", key)
        tampered = encrypted[:-5] + b"\x00\x00\x00\x00\x00"
        with pytest.raises(Exception):
            decrypt(tampered, key)

    def test_empty_plaintext_roundtrip(self) -> None:
        key = generate_key()
        encrypted = encrypt(b"", key)
        assert decrypt(encrypted, key) == b""

    def test_negative_float_roundtrip(self) -> None:
        key = generate_key()
        value = -999.99
        encrypted = encrypt_float(value, key)
        assert decrypt_float(encrypted, key) == pytest.approx(value)

    def test_zero_float_roundtrip(self) -> None:
        key = generate_key()
        encrypted = encrypt_float(0.0, key)
        assert decrypt_float(encrypted, key) == pytest.approx(0.0)


class TestLoadKey:
    def test_loads_from_env_var(self, fernet_key: bytes, env_key: None) -> None:
        loaded = load_key()
        assert loaded == fernet_key

    def test_loads_from_file(self, tmp_path: Path) -> None:
        key = generate_key()
        key_file = tmp_path / "test.key"
        key_file.write_bytes(key)
        loaded = load_key(key_file)
        assert loaded == key

    def test_raises_when_missing(self, tmp_path: Path) -> None:
        # Clear any env var to force file lookup
        import os

        os.environ.pop("PORTFOLIO_ENCRYPTION_KEY", None)
        with pytest.raises(FileNotFoundError):
            load_key(tmp_path / "nonexistent.key")

    def test_env_var_takes_precedence_over_file(
        self, tmp_path: Path, fernet_key: bytes
    ) -> None:
        import os

        os.environ["PORTFOLIO_ENCRYPTION_KEY"] = fernet_key.decode("utf-8")
        # Even though file doesn't exist, env var should work
        loaded = load_key(tmp_path / "nonexistent.key")
        assert loaded == fernet_key
        os.environ.pop("PORTFOLIO_ENCRYPTION_KEY", None)
