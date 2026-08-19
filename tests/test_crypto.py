"""Tests for pipeline.crypto module."""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline import storage
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
from pipeline.keygen import main
from pipeline.secrets import reset_mode, set_mode


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
        set_mode("docker")
        loaded = load_key()
        assert loaded == fernet_key
        reset_mode()

    def test_loads_from_file(self, tmp_path: Path) -> None:
        key = generate_key()
        key_file = tmp_path / "test.key"
        key_file.write_bytes(key)
        set_mode("docker")
        loaded = load_key(key_file)
        assert loaded == key
        reset_mode()

    def test_raises_when_missing(self, tmp_path: Path) -> None:
        # Clear any env var to force file lookup
        os.environ.pop("ENCRYPTION_KEY", None)
        set_mode("docker")
        with pytest.raises(
            FileNotFoundError,
            match="Set the ENCRYPTION_KEY environment variable",
        ):
            load_key(tmp_path / "nonexistent.key")
        reset_mode()

    def test_env_var_takes_precedence_over_file(
        self, tmp_path: Path, fernet_key: bytes
    ) -> None:
        os.environ["ENCRYPTION_KEY"] = fernet_key.decode("utf-8")
        # Even though file doesn't exist, env var should work
        set_mode("docker")
        loaded = load_key(tmp_path / "nonexistent.key")
        assert loaded == fernet_key
        os.environ.pop("ENCRYPTION_KEY", None)
        reset_mode()

    def test_staging_mode_raises_when_key_missing(self, monkeypatch, tmp_path):
        """In staging mode, missing ENCRYPTION_KEY raises EnvironmentError.

        load_key() must NOT fall through to the file-based key because
        .secrets/encryption.key contains the production key, which would
        violate staging/production isolation.
        """
        monkeypatch.delenv("ENCRYPTION_KEY", raising=False)
        set_mode("staging")

        with pytest.raises(EnvironmentError, match="ENCRYPTION_KEY"):
            load_key()
        reset_mode()

    def test_staging_mode_uses_key_when_set(self, monkeypatch, tmp_path):
        """In staging mode, ENCRYPTION_KEY is used."""
        key = generate_key()
        monkeypatch.setenv("ENCRYPTION_KEY", key.decode("utf-8"))
        set_mode("staging")

        result = load_key()
        assert result == key
        reset_mode()

    def test_production_mode_falls_back_to_file(self, monkeypatch, tmp_path):
        """In production mode, load_key() still falls back to file-based key."""
        monkeypatch.delenv("ENCRYPTION_KEY", raising=False)
        set_mode("prod")

        key = generate_key()
        key_file = tmp_path / "encryption.key"
        key_file.write_bytes(key)

        result = load_key(key_file)
        assert result == key
        reset_mode()

    def test_storage_fallback_resolves_key_file(self, monkeypatch, tmp_path):
        """load_key(path=None) in prod mode resolves via get_storage().encryption_key_file.

        Exercises crypto.py:59-61 — the storage-based key-file fallback that
        ``test_production_mode_falls_back_to_file`` skips by passing an explicit
        ``key_file``. A bug in this resolution path would go undetected otherwise.
        """
        monkeypatch.delenv("ENCRYPTION_KEY", raising=False)
        set_mode("prod")

        key = generate_key()
        key_file = tmp_path / "encryption.key"
        key_file.write_bytes(key)

        monkeypatch.setattr(
            storage,
            "get_storage",
            lambda: SimpleNamespace(encryption_key_file=str(key_file)),
        )

        assert load_key() == key
        reset_mode()

    def test_storage_fallback_raises_with_guidance_when_file_missing(
        self, monkeypatch, tmp_path
    ):
        """load_key(path=None) via storage fallback raises FileNotFoundError with the
        ENCRYPTION_KEY guidance message when the resolved key file does not exist."""
        monkeypatch.delenv("ENCRYPTION_KEY", raising=False)
        set_mode("prod")

        missing = tmp_path / "missing.key"
        monkeypatch.setattr(
            storage,
            "get_storage",
            lambda: SimpleNamespace(encryption_key_file=str(missing)),
        )

        with pytest.raises(
            FileNotFoundError,
            match="Set the ENCRYPTION_KEY environment variable",
        ):
            load_key()
        reset_mode()


class TestKeygenMain:
    """Cover pipeline.keygen.main() — previously 0% coverage."""

    def test_main_prints_guidance_and_generates_key(self, capsys):
        main()
        captured = capsys.readouterr()
        assert "ENCRYPTION_KEY" in captured.out
        assert "generate_key" in captured.out
