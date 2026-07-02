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

        os.environ.pop("ENCRYPTION_KEY", None)
        with pytest.raises(FileNotFoundError):
            load_key(tmp_path / "nonexistent.key")

    def test_env_var_takes_precedence_over_file(
        self, tmp_path: Path, fernet_key: bytes
    ) -> None:
        import os

        os.environ["ENCRYPTION_KEY"] = fernet_key.decode("utf-8")
        # Even though file doesn't exist, env var should work
        loaded = load_key(tmp_path / "nonexistent.key")
        assert loaded == fernet_key
        os.environ.pop("ENCRYPTION_KEY", None)

    def test_demo_mode_raises_when_demo_key_missing(self, monkeypatch, tmp_path):
        """In demo mode, missing ENCRYPTION_KEY_DEMO raises EnvironmentError.

        load_key() must NOT fall through to the file-based key because
        .secrets/encryption.key contains the production key, which would
        violate demo/production isolation.
        """
        monkeypatch.setenv("DEMO", "true")
        monkeypatch.delenv("ENCRYPTION_KEY_DEMO", raising=False)
        monkeypatch.delenv("ENCRYPTION_KEY", raising=False)

        with pytest.raises(EnvironmentError, match="ENCRYPTION_KEY_DEMO"):
            load_key()

    def test_demo_mode_uses_demo_key_when_set(self, monkeypatch, tmp_path):
        """In demo mode, ENCRYPTION_KEY_DEMO is used exclusively."""
        from pipeline.crypto import generate_key

        demo_key = generate_key()
        monkeypatch.setenv("DEMO", "true")
        monkeypatch.setenv("ENCRYPTION_KEY_DEMO", demo_key.decode("utf-8"))
        # Production key is set but must NOT be used
        monkeypatch.setenv("ENCRYPTION_KEY", "prod-key-value")

        result = load_key()
        assert result == demo_key

    def test_production_mode_falls_back_to_file(self, monkeypatch, tmp_path):
        """In production mode, load_key() still falls back to file-based key."""
        monkeypatch.delenv("DEMO", raising=False)
        monkeypatch.delenv("ENCRYPTION_KEY", raising=False)

        key = generate_key()
        key_file = tmp_path / "encryption.key"
        key_file.write_bytes(key)

        result = load_key(key_file)
        assert result == key
