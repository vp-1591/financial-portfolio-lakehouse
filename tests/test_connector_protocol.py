"""Tests for the connector self-description protocol.

Verifies that each connector correctly implements:
- ``fetch_kwargs()`` — builds connector-specific snapshot kwargs
- ``fetch_cdc_kwargs()`` — returns CDC kwargs (same as snapshot for T212, empty otherwise)
- ``required_secrets()`` — lists expected secret env-var names
- ``enabled_env_var`` — declares the correct *_ENABLED variable name
- ``extract_holdings()`` — extracts Holding objects from a normalized DataFrame
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from pipeline.connectors.registry import get
from pipeline.crypto import encrypt_float, generate_key
from pipeline.normalized.consolidate import Holding


# ---------------------------------------------------------------------------
# fetch_kwargs
# ---------------------------------------------------------------------------


class TestIbkrFetchKwargs:
    """IBKR fetch_kwargs resolves Flex token, query ID, and base URL."""

    def test_returns_kwargs_when_secrets_set(self, monkeypatch) -> None:
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "test-token")
        monkeypatch.setenv("IBKR_FLEX_QUERY_ID", "42")
        connector = get("ibkr")
        args = argparse.Namespace()
        kwargs = connector.fetch_kwargs(args)
        assert kwargs == {
            "flex_token": "test-token",
            "flex_query_id": "42",
            "flex_base_url": "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService",
        }

    def test_returns_empty_dict_when_token_missing(self, monkeypatch) -> None:
        monkeypatch.delenv("IBKR_FLEX_TOKEN", raising=False)
        monkeypatch.setenv("IBKR_FLEX_QUERY_ID", "42")
        connector = get("ibkr")
        args = argparse.Namespace()
        assert connector.fetch_kwargs(args) == {}

    def test_returns_empty_dict_when_query_id_missing(self, monkeypatch) -> None:
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "test-token")
        monkeypatch.delenv("IBKR_FLEX_QUERY_ID", raising=False)
        connector = get("ibkr")
        args = argparse.Namespace()
        assert connector.fetch_kwargs(args) == {}

    def test_custom_base_url(self, monkeypatch) -> None:
        monkeypatch.setenv("IBKR_FLEX_TOKEN", "test-token")
        monkeypatch.setenv("IBKR_FLEX_QUERY_ID", "42")
        monkeypatch.setenv("IBKR_FLEX_BASE_URL", "https://custom.example.com")
        connector = get("ibkr")
        args = argparse.Namespace()
        kwargs = connector.fetch_kwargs(args)
        assert kwargs["flex_base_url"] == "https://custom.example.com"

    def test_demo_mode_uses_demo_variants(self, monkeypatch) -> None:
        monkeypatch.setenv("DEMO", "true")
        monkeypatch.setenv("IBKR_FLEX_TOKEN_DEMO", "demo-token")
        monkeypatch.setenv("IBKR_FLEX_QUERY_ID_DEMO", "99")
        monkeypatch.delenv("IBKR_FLEX_TOKEN", raising=False)
        monkeypatch.delenv("IBKR_FLEX_QUERY_ID", raising=False)
        connector = get("ibkr")
        args = argparse.Namespace()
        kwargs = connector.fetch_kwargs(args)
        assert kwargs["flex_token"] == "demo-token"
        assert kwargs["flex_query_id"] == "99"


class TestTrading212FetchKwargs:
    """Trading 212 fetch_kwargs resolves API key, secret, and base URL."""

    def test_returns_kwargs_when_secrets_set(self, monkeypatch) -> None:
        monkeypatch.setenv("T212_API_KEY", "test-key")
        monkeypatch.setenv("T212_API_SECRET", "test-secret")
        monkeypatch.delenv("T212_BASE_URL", raising=False)
        monkeypatch.delenv("DEMO", raising=False)
        connector = get("trading212")
        args = argparse.Namespace()
        kwargs = connector.fetch_kwargs(args)
        assert kwargs == {
            "api_key": "test-key",
            "api_secret": "test-secret",
            "base_url": "https://live.trading212.com/api/v0",
        }

    def test_returns_empty_dict_when_api_key_missing(self, monkeypatch) -> None:
        monkeypatch.delenv("T212_API_KEY", raising=False)
        connector = get("trading212")
        args = argparse.Namespace()
        assert connector.fetch_kwargs(args) == {}

    def test_demo_mode_selects_demo_base_url(self, monkeypatch) -> None:
        monkeypatch.setenv("DEMO", "true")
        monkeypatch.setenv("T212_API_KEY_DEMO", "demo-key")
        monkeypatch.setenv("T212_API_SECRET_DEMO", "demo-secret")
        monkeypatch.delenv("T212_API_KEY", raising=False)
        monkeypatch.delenv("T212_API_SECRET", raising=False)
        monkeypatch.delenv("T212_BASE_URL", raising=False)
        connector = get("trading212")
        args = argparse.Namespace()
        kwargs = connector.fetch_kwargs(args)
        assert kwargs["base_url"] == "https://demo.trading212.com/api/v0"

    def test_custom_base_url_overrides_default(self, monkeypatch) -> None:
        monkeypatch.setenv("T212_API_KEY", "test-key")
        monkeypatch.setenv("T212_API_SECRET", "test-secret")
        monkeypatch.setenv("T212_BASE_URL", "https://custom.api.com")
        connector = get("trading212")
        args = argparse.Namespace()
        kwargs = connector.fetch_kwargs(args)
        assert kwargs["base_url"] == "https://custom.api.com"


class TestXtbFetchKwargs:
    """XTB fetch_kwargs reads from args.xtb_file."""

    def test_returns_kwargs_with_single_file(self) -> None:
        connector = get("xtb")
        args = argparse.Namespace(xtb_file=["/path/to/report.xlsx"])
        kwargs = connector.fetch_kwargs(args)
        assert kwargs == {"file_path": "/path/to/report.xlsx"}

    def test_returns_kwargs_with_string_file(self) -> None:
        connector = get("xtb")
        args = argparse.Namespace(xtb_file="/path/to/report.xlsx")
        kwargs = connector.fetch_kwargs(args)
        assert kwargs == {"file_path": "/path/to/report.xlsx"}

    def test_returns_empty_dict_when_no_file(self) -> None:
        connector = get("xtb")
        args = argparse.Namespace(xtb_file=None)
        assert connector.fetch_kwargs(args) == {}

    def test_returns_empty_dict_when_no_attribute(self) -> None:
        connector = get("xtb")
        args = argparse.Namespace()
        assert connector.fetch_kwargs(args) == {}


# ---------------------------------------------------------------------------
# fetch_cdc_kwargs
# ---------------------------------------------------------------------------


class TestFetchCdcKwargs:
    """CDC kwargs: T212 returns snapshot kwargs, IBKR/XTB return {}."""

    def test_ibkr_returns_empty(self) -> None:
        assert get("ibkr").fetch_cdc_kwargs() == {}

    def test_xtb_returns_empty(self) -> None:
        assert get("xtb").fetch_cdc_kwargs() == {}

    def test_t212_returns_snapshot_kwargs(self, monkeypatch) -> None:
        monkeypatch.setenv("T212_API_KEY", "test-key")
        monkeypatch.setenv("T212_API_SECRET", "test-secret")
        monkeypatch.delenv("T212_BASE_URL", raising=False)
        monkeypatch.delenv("DEMO", raising=False)
        connector = get("trading212")
        cdc_kwargs = connector.fetch_cdc_kwargs()
        snapshot_kwargs = connector.fetch_kwargs(argparse.Namespace())
        assert cdc_kwargs == snapshot_kwargs


# ---------------------------------------------------------------------------
# required_secrets
# ---------------------------------------------------------------------------


class TestRequiredSecrets:
    """Each connector lists the base secret env-var names it needs."""

    def test_ibkr_required_secrets(self) -> None:
        assert get("ibkr").required_secrets() == [
            "IBKR_FLEX_TOKEN",
            "IBKR_FLEX_QUERY_ID",
        ]

    def test_t212_required_secrets(self) -> None:
        assert get("trading212").required_secrets() == [
            "T212_API_KEY",
            "T212_API_SECRET",
        ]

    def test_xtb_required_secrets(self) -> None:
        # XTB reads from uploaded files, not API secrets
        assert get("xtb").required_secrets() == []


# ---------------------------------------------------------------------------
# enabled_env_var
# ---------------------------------------------------------------------------


class TestEnabledEnvVar:
    """Each connector declares the correct *_ENABLED env var name."""

    def test_ibkr_enabled_env_var(self) -> None:
        assert get("ibkr").enabled_env_var == "IBKR_ENABLED"

    def test_t212_enabled_env_var(self) -> None:
        assert get("trading212").enabled_env_var == "T212_ENABLED"

    def test_xtb_enabled_env_var(self) -> None:
        assert get("xtb").enabled_env_var == "XTB_ENABLED"


# ---------------------------------------------------------------------------
# extract_holdings
# ---------------------------------------------------------------------------


def _ibkr_normalized_df(fernet_key: bytes) -> pl.DataFrame:
    """Build a normalized IBKR DataFrame matching the fixture schema."""
    return pl.DataFrame(
        {
            "label": ["VWCE", "AAPL"],
            "security_ccy": ["EUR", "USD"],
            "security_value": [
                encrypt_float(5000.0, fernet_key),
                encrypt_float(2700.0, fernet_key),
            ],
            "isin": ["IE00BK5BQT80", "US0378331005"],
            "description": ["Vanguard FTSE All-World", "Apple Inc"],
        }
    )


def _t212_normalized_df(fernet_key: bytes) -> pl.DataFrame:
    """Build a normalized Trading 212 DataFrame matching the fixture schema."""
    return pl.DataFrame(
        {
            "label": ["VWCE_DE_EQ", "AAPL_US_EQ"],
            "name": ["Vanguard FTSE All-World UCITS ETF", "Apple Inc"],
            "security_ccy": ["EUR", "USD"],
            "security_value": [
                encrypt_float(2500.0, fernet_key),
                encrypt_float(1800.0, fernet_key),
            ],
            "isin": ["IE00BK5BQT80", "US0378331005"],
        }
    )


def _xtb_normalized_df(fernet_key: bytes) -> pl.DataFrame:
    """Build a normalized XTB DataFrame matching the fixture schema."""
    return pl.DataFrame(
        {
            "label": ["VWCE.DE", "CDR.PL"],
            "name": ["Vanguard FTSE All-World UCITS ETF", "CD Projekt"],
            "security_ccy": ["EUR", "PLN"],
            "security_value": [
                encrypt_float(1000.0, fernet_key),
                encrypt_float(2500.0, fernet_key),
            ],
            "isin": ["IE00BK5BQT80", "PL9999900006"],
        }
    )


def _decrypt_df(df: pl.DataFrame, fernet_key: bytes) -> pl.DataFrame:
    """Add a security_value_decrypted column to a DataFrame."""
    from pipeline.crypto import decrypt_float

    return df.with_columns(
        pl.col("security_value")
        .map_elements(
            lambda v: decrypt_float(v, fernet_key),
            return_dtype=pl.Float64,
        )
        .alias("security_value_decrypted")
    )


class TestIbkrExtractHoldings:
    def test_extracts_ibkr_holdings(self) -> None:
        fernet_key = generate_key()
        df = _decrypt_df(_ibkr_normalized_df(fernet_key), fernet_key)
        connector = get("ibkr")
        holdings = connector.extract_holdings(df, fernet_key)

        assert len(holdings) == 2
        assert holdings[0].broker == "IBKR"
        assert holdings[0].ticker == "VWCE"
        assert holdings[0].currency == "EUR"
        assert holdings[0].identifier == "ISIN:IE00BK5BQT80"
        assert holdings[0].security_currency == "EUR"
        assert holdings[0].description == "Vanguard FTSE All-World"

    def test_isin_formatting(self) -> None:
        fernet_key = generate_key()
        df = _decrypt_df(_ibkr_normalized_df(fernet_key), fernet_key)
        connector = get("ibkr")
        holdings = connector.extract_holdings(df, fernet_key)

        assert holdings[0].identifier == "ISIN:IE00BK5BQT80"
        assert holdings[1].identifier == "ISIN:US0378331005"

    def test_empty_isin_produces_empty_identifier(self) -> None:
        fernet_key = generate_key()
        df = pl.DataFrame(
            {
                "label": ["CASH EUR"],
                "security_ccy": ["EUR"],
                "security_value": [encrypt_float(2000.0, fernet_key)],
                "isin": [""],
                "description": ["Cash EUR"],
            }
        )
        df = _decrypt_df(df, fernet_key)
        connector = get("ibkr")
        holdings = connector.extract_holdings(df, fernet_key)

        assert holdings[0].identifier == ""


class TestTrading212ExtractHoldings:
    def test_extracts_t212_holdings(self) -> None:
        fernet_key = generate_key()
        df = _decrypt_df(_t212_normalized_df(fernet_key), fernet_key)
        connector = get("trading212")
        holdings = connector.extract_holdings(df, fernet_key)

        assert len(holdings) == 2
        assert holdings[0].broker == "Trading 212"
        assert holdings[0].ticker == "VWCE_DE_EQ"
        assert holdings[0].description == "Vanguard FTSE All-World UCITS ETF"

    def test_t212_uses_name_column_for_description(self) -> None:
        fernet_key = generate_key()
        df = _decrypt_df(_t212_normalized_df(fernet_key), fernet_key)
        connector = get("trading212")
        holdings = connector.extract_holdings(df, fernet_key)

        # T212 uses the "name" column for description, not "description"
        assert holdings[0].description == "Vanguard FTSE All-World UCITS ETF"


class TestXtbExtractHoldings:
    def test_extracts_xtb_holdings(self) -> None:
        fernet_key = generate_key()
        df = _decrypt_df(_xtb_normalized_df(fernet_key), fernet_key)
        connector = get("xtb")
        holdings = connector.extract_holdings(df, fernet_key)

        assert len(holdings) == 2
        assert holdings[0].broker == "XTB"
        assert holdings[0].ticker == "VWCE.DE"

    def test_xtb_security_currency_from_security_ccy(self) -> None:
        """XTB derives security_currency from security_ccy column."""
        fernet_key = generate_key()
        df = _decrypt_df(_xtb_normalized_df(fernet_key), fernet_key)
        connector = get("xtb")
        holdings = connector.extract_holdings(df, fernet_key)

        # XTB schema has security_ccy column;
        # security_currency should match security_ccy
        assert holdings[0].security_currency == "EUR"
        assert holdings[1].security_currency == "PLN"

    def test_xtb_uses_name_column_for_description(self) -> None:
        fernet_key = generate_key()
        df = _decrypt_df(_xtb_normalized_df(fernet_key), fernet_key)
        connector = get("xtb")
        holdings = connector.extract_holdings(df, fernet_key)

        assert holdings[0].description == "Vanguard FTSE All-World UCITS ETF"


# ---------------------------------------------------------------------------
# Regression: extract_holdings shim still works end-to-end
# ---------------------------------------------------------------------------


class TestExtractHoldingsShim:
    """Verify that pipeline.normalized.extract.extract_holdings still works
    after delegating to connector.extract_holdings."""

    def test_ibkr_shim(self, tmp_path: Path) -> None:
        from deltalake import write_deltalake

        from pipeline.normalized.extract import extract_holdings
        from pipeline.storage import LocalBackend, StorageConfig, use_storage
        from tests.fixtures.ibkr import ibkr_normalized_snapshot

        fernet_key = generate_key()
        data = tmp_path / "data"
        data.mkdir()
        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(data),
        )
        use_storage(config)

        table = ibkr_normalized_snapshot(fernet_key=fernet_key)
        path = str(data / "normalized" / "ibkr_snapshot")
        write_deltalake(path, table, mode="overwrite")

        holdings = extract_holdings("ibkr", path, fernet_key)
        assert len(holdings) >= 2
        assert all(isinstance(h, Holding) for h in holdings)
        assert any(h.ticker == "VWCE" for h in holdings)

    def test_t212_shim(self, tmp_path: Path) -> None:
        from deltalake import write_deltalake

        from pipeline.normalized.extract import extract_holdings
        from pipeline.storage import LocalBackend, StorageConfig, use_storage
        from tests.fixtures.trading212 import t212_normalized_snapshot

        fernet_key = generate_key()
        data = tmp_path / "data"
        data.mkdir()
        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(data),
        )
        use_storage(config)

        table = t212_normalized_snapshot(fernet_key=fernet_key)
        path = str(data / "normalized" / "trading212_snapshot")
        write_deltalake(path, table, mode="overwrite")

        holdings = extract_holdings("trading212", path, fernet_key)
        assert len(holdings) >= 2
        assert all(isinstance(h, Holding) for h in holdings)

    def test_xtb_shim(self, tmp_path: Path) -> None:
        from deltalake import write_deltalake

        from pipeline.normalized.extract import extract_holdings
        from pipeline.storage import LocalBackend, StorageConfig, use_storage
        from tests.fixtures.xtb import xtb_normalized_snapshot

        fernet_key = generate_key()
        data = tmp_path / "data"
        data.mkdir()
        config = StorageConfig(
            data_dir=str(data),
            raw_dir=str(data / "raw"),
            normalized_dir=str(data / "normalized"),
            analytics_dir=str(data / "analytics"),
            secrets_dir=str(tmp_path / ".secrets"),
            encryption_key_file=str(tmp_path / ".secrets" / "encryption.key"),
            backend=LocalBackend(data),
        )
        use_storage(config)

        table = xtb_normalized_snapshot(fernet_key=fernet_key)
        path = str(data / "normalized" / "xtb_snapshot")
        write_deltalake(path, table, mode="overwrite")

        holdings = extract_holdings("xtb", path, fernet_key)
        assert len(holdings) >= 2
        assert all(isinstance(h, Holding) for h in holdings)
        # XTB derives security_currency from security_ccy
        assert all(h.security_currency != "" for h in holdings)
