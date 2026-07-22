"""Tests for run-connector and run-consolidate-analytics subcommands.

Verifies the extracted ``fetch_connector``/``transform_connector`` helpers,
the ``cmd_run_connector``/``cmd_run_consolidate_analytics`` commands, and
regression of existing ``cmd_fetch``/``cmd_transform``/``cmd_full`` paths.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch


from pipeline.connectors.registry import get
from pipeline.crypto import generate_key
from pipeline.run import (
    FetchResult,
    cmd_fetch,
    cmd_full,
    cmd_run_consolidate_analytics,
    cmd_run_connector,
    cmd_transform,
    fetch_connector,
    transform_connector,
)


# ---------------------------------------------------------------------------
# Argparse dispatch
# ---------------------------------------------------------------------------


class TestArgparseDispatch:
    """run-connector and run-consolidate-analytics are present in the commands dict."""

    def test_run_connector_in_commands_dict(self) -> None:

        # Parse with the real parser to verify subcommand is registered.
        # We only check the commands dict here, since running main() would
        # require storage setup.
        from pipeline import run as run_module

        assert "run-connector" in run_module.main.__code__.co_consts
        # Verify cmd_run_connector is callable
        assert callable(cmd_run_connector)

    def test_run_consolidate_analytics_in_commands_dict(self) -> None:
        assert callable(cmd_run_consolidate_analytics)

    def test_run_connector_ibkr_resolves(self) -> None:
        """run-connector ibkr resolves via get("ibkr")."""
        connector = get("ibkr")
        assert connector.name == "ibkr"

    def test_run_connector_trading212_resolves(self) -> None:
        connector = get("trading212")
        assert connector.name == "trading212"

    def test_run_connector_xtb_resolves(self) -> None:
        connector = get("xtb")
        assert connector.name == "xtb"

    def test_cdc_supported_ibkr(self) -> None:
        """IBKR supports CDC."""
        assert get("ibkr").cdc_supported is True

    def test_cdc_supported_trading212(self) -> None:
        """Trading 212 supports CDC."""
        assert get("trading212").cdc_supported is True

    def test_cdc_supported_xtb(self) -> None:
        """XTB does not support CDC."""
        assert get("xtb").cdc_supported is False


# ---------------------------------------------------------------------------
# fetch_connector / transform_connector isolation
# ---------------------------------------------------------------------------


class TestFetchConnectorIsolation:
    """fetch_connector uses connector.fetch_kwargs (no if/elif)."""

    @patch("pipeline.raw.ingest.ingest_raw", return_value=1)
    def test_uses_fetch_kwargs(
        self, mock_ingest: MagicMock, tmp_data_dir: Path
    ) -> None:
        """fetch_connector calls connector.fetch_kwargs(args) and passes result to fetch_snapshot."""
        connector = get("ibkr")
        args = argparse.Namespace()

        with (
            patch.object(
                connector,
                "fetch_kwargs",
                return_value={
                    "flex_token": "t",
                    "flex_query_id": "q",
                    "flex_base_url": "u",
                },
            ) as mock_kwargs,
            patch.object(
                connector, "fetch_snapshot", return_value=MagicMock(num_rows=1)
            ) as mock_snapshot,
            patch.object(connector, "fetch_cdc_kwargs", return_value={}),
        ):
            fernet_key = generate_key()
            rc = fetch_connector(connector, args, fernet_key)
            assert rc == FetchResult.SUCCESS
            mock_kwargs.assert_called_once_with(args)
            mock_snapshot.assert_called_once()

    @patch("pipeline.raw.ingest.ingest_raw", return_value=1)
    def test_skips_connector_when_kwargs_empty(
        self, mock_ingest: MagicMock, tmp_data_dir: Path
    ) -> None:
        """fetch_connector returns SKIPPED and skips when fetch_kwargs returns {}."""
        connector = get("ibkr")
        args = argparse.Namespace()

        with (
            patch.object(connector, "fetch_kwargs", return_value={}),
            patch.object(connector, "fetch_snapshot") as mock_snapshot,
        ):
            fernet_key = generate_key()
            rc = fetch_connector(connector, args, fernet_key)
            assert rc == FetchResult.SKIPPED
            mock_snapshot.assert_not_called()

    @patch("pipeline.raw.ingest.ingest_raw", return_value=1)
    def test_returns_nonzero_on_snapshot_error(
        self, mock_ingest: MagicMock, tmp_data_dir: Path
    ) -> None:
        """fetch_connector returns ERROR when snapshot fetch raises an exception."""
        connector = get("ibkr")
        args = argparse.Namespace()

        with (
            patch.object(
                connector,
                "fetch_kwargs",
                return_value={
                    "flex_token": "t",
                    "flex_query_id": "q",
                    "flex_base_url": "u",
                },
            ),
            patch.object(
                connector,
                "fetch_snapshot",
                side_effect=RuntimeError("API timeout"),
            ),
            patch.object(connector, "fetch_cdc_kwargs", return_value={}),
        ):
            fernet_key = generate_key()
            rc = fetch_connector(connector, args, fernet_key)
            assert rc == FetchResult.ERROR


class TestTransformConnectorIsolation:
    """transform_connector delegates to connector transform methods."""

    def test_transform_connector_returns_zero(self, tmp_data_dir: Path) -> None:
        """transform_connector returns 0 even when no raw data exists."""
        connector = get("ibkr")
        fernet_key = generate_key()
        rc = transform_connector(connector, fernet_key)
        # No raw data → DeltaTable fails → continue → return 0
        assert rc == 0


# ---------------------------------------------------------------------------
# cmd_run_connector
# ---------------------------------------------------------------------------


class TestCmdRunConnector:
    """cmd_run_connector dispatches to fetch_connector+transform_connector."""

    def test_disabled_connector_returns_zero(self, monkeypatch) -> None:
        """Disabled connector logs and returns 0 (runtime gate)."""
        monkeypatch.setenv("IBKR_ENABLED", "0")
        args = argparse.Namespace(connector="ibkr")
        rc = cmd_run_connector(args)
        assert rc == 0

    @patch("pipeline.run.run_validation", return_value=0)
    @patch("pipeline.run.transform_connector", return_value=0)
    @patch("pipeline.run.fetch_connector", return_value=FetchResult.SUCCESS)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    @patch("pipeline.run.is_enabled", return_value=True)
    def test_enabled_connector_calls_fetch_then_transform(
        self,
        mock_enabled: MagicMock,
        mock_key: MagicMock,
        mock_fetch: MagicMock,
        mock_transform: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        args = argparse.Namespace(connector="ibkr")
        rc = cmd_run_connector(args)
        assert rc == 0
        mock_fetch.assert_called_once()
        mock_transform.assert_called_once()
        mock_validate.assert_called_once_with(
            fernet_key=b"test-key",
            tables=["ibkr_snapshot", "ibkr_cdc"],
        )

    @patch("pipeline.run.run_validation", return_value=0)
    @patch("pipeline.run.transform_connector", return_value=0)
    @patch("pipeline.run.fetch_connector", return_value=FetchResult.ERROR)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    @patch("pipeline.run.is_enabled", return_value=True)
    def test_fetch_failure_skips_transform(
        self,
        mock_enabled: MagicMock,
        mock_key: MagicMock,
        mock_fetch: MagicMock,
        mock_transform: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        """If fetch_connector returns ERROR, transform and validate are skipped."""
        args = argparse.Namespace(connector="ibkr")
        rc = cmd_run_connector(args)
        assert rc == 1
        mock_transform.assert_not_called()
        mock_validate.assert_not_called()

    @patch("pipeline.run.run_validation")
    @patch("pipeline.run.transform_connector")
    @patch("pipeline.run.fetch_connector", return_value=FetchResult.SKIPPED)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    @patch("pipeline.run.is_enabled", return_value=True)
    def test_skipped_connector_returns_zero(
        self,
        mock_enabled: MagicMock,
        mock_key: MagicMock,
        mock_fetch: MagicMock,
        mock_transform: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        """If fetch_connector returns SKIPPED, cmd_run_connector returns 0 without
        calling transform or validation — there's no data to process."""
        args = argparse.Namespace(connector="ibkr")
        rc = cmd_run_connector(args)
        assert rc == 0
        mock_transform.assert_not_called()
        mock_validate.assert_not_called()

    @patch("pipeline.run.run_validation", return_value=1)
    @patch("pipeline.run.transform_connector", return_value=0)
    @patch("pipeline.run.fetch_connector", return_value=0)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    @patch("pipeline.run.is_enabled", return_value=True)
    def test_validation_failure_returns_nonzero(
        self,
        mock_enabled: MagicMock,
        mock_key: MagicMock,
        mock_fetch: MagicMock,
        mock_transform: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        """If run_validation returns non-zero, cmd_run_connector returns it."""
        args = argparse.Namespace(connector="ibkr")
        rc = cmd_run_connector(args)
        assert rc == 1

    def test_xtb_without_file_returns_1(self, monkeypatch) -> None:
        """XTB without --xtb-file in dedicated subcommand returns 1."""
        monkeypatch.setenv("XTB_ENABLED", "1")
        args = argparse.Namespace(connector="xtb", xtb_file=None)
        rc = cmd_run_connector(args)
        assert rc == 1

    @patch("pipeline.run.run_validation", return_value=0)
    @patch("pipeline.run.transform_connector", return_value=0)
    @patch("pipeline.run.fetch_connector", return_value=0)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    @patch("pipeline.run.is_enabled", return_value=True)
    def test_xtb_with_file_calls_fetch(
        self,
        mock_enabled: MagicMock,
        mock_key: MagicMock,
        mock_fetch: MagicMock,
        mock_transform: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        args = argparse.Namespace(connector="xtb", xtb_file=["report.xlsx"])
        rc = cmd_run_connector(args)
        assert rc == 0
        mock_validate.assert_called_once_with(
            fernet_key=b"test-key",
            tables=["xtb_snapshot"],
        )
        mock_fetch.assert_called_once()
        mock_transform.assert_called_once()


# ---------------------------------------------------------------------------
# cmd_fetch — no-credentials error path
# ---------------------------------------------------------------------------


class TestCmdFetchNoCredentials:
    """cmd_fetch fails loudly when all connectors lack credentials."""

    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_all_skipped_returns_nonzero(self, mock_key: MagicMock) -> None:
        """All enabled connectors skipped → exit 1 with error message."""
        real_connectors = [get("ibkr"), get("trading212")]
        args = argparse.Namespace(xtb_file=None)
        with (
            patch("pipeline.run.is_enabled", return_value=True),
            patch("pipeline.run.all_connectors", return_value=real_connectors),
            patch("pipeline.run.inject_secrets"),
            patch("pipeline.run.fetch_connector", return_value=FetchResult.SKIPPED),
        ):
            rc = cmd_fetch(args)
        assert rc == 1

    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_all_skipped_error_message(self, mock_key: MagicMock, capsys) -> None:
        """All skipped connectors print a helpful error message to stderr."""
        real_connectors = [get("ibkr")]
        args = argparse.Namespace(xtb_file=None)
        with (
            patch("pipeline.run.is_enabled", return_value=True),
            patch("pipeline.run.all_connectors", return_value=real_connectors),
            patch("pipeline.run.inject_secrets"),
            patch("pipeline.run.fetch_connector", return_value=FetchResult.SKIPPED),
        ):
            rc = cmd_fetch(args)
        assert rc == 1
        stderr = capsys.readouterr().err
        assert "No broker credentials found" in stderr
        assert "IBKR_FLEX_TOKEN" in stderr
        assert "T212_API_KEY" in stderr
        assert "--mode staging" in stderr

    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_one_success_overrides_all_skipped(self, mock_key: MagicMock) -> None:
        """If at least one connector succeeds, cmd_fetch returns 0."""
        real_connectors = [get("ibkr"), get("trading212")]
        args = argparse.Namespace(xtb_file=None)
        with (
            patch("pipeline.run.is_enabled", return_value=True),
            patch("pipeline.run.all_connectors", return_value=real_connectors),
            patch("pipeline.run.inject_secrets"),
            patch(
                "pipeline.run.fetch_connector",
                side_effect=[FetchResult.SUCCESS, FetchResult.SKIPPED],
            ),
        ):
            rc = cmd_fetch(args)
        assert rc == 0

    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_one_error_returns_nonzero(self, mock_key: MagicMock, capsys) -> None:
        """If any connector errors, cmd_fetch returns 1."""
        real_connectors = [get("ibkr"), get("trading212")]
        args = argparse.Namespace(xtb_file=None)
        with (
            patch("pipeline.run.is_enabled", return_value=True),
            patch("pipeline.run.all_connectors", return_value=real_connectors),
            patch("pipeline.run.inject_secrets"),
            patch(
                "pipeline.run.fetch_connector",
                side_effect=[FetchResult.SUCCESS, FetchResult.ERROR],
            ),
        ):
            rc = cmd_fetch(args)
        assert rc == 1
        stderr = capsys.readouterr().err
        assert "connector(s) succeeded" in stderr
        assert "failed" in stderr

    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_all_disabled_returns_zero(self, mock_key: MagicMock) -> None:
        """All connectors disabled → cmd_fetch returns 0, no error message."""
        real_connectors = [get("ibkr"), get("trading212")]
        args = argparse.Namespace(xtb_file=None)
        with (
            patch("pipeline.run.is_enabled", return_value=False),
            patch("pipeline.run.all_connectors", return_value=real_connectors),
            patch("pipeline.run.inject_secrets"),
            patch("pipeline.run.fetch_connector") as mock_fetch,
        ):
            rc = cmd_fetch(args)
        assert rc == 0
        mock_fetch.assert_not_called()

    def test_fetch_result_enum_values(self) -> None:
        """FetchResult enum values match expected int values."""
        assert FetchResult.SUCCESS == 0
        assert FetchResult.ERROR == 1
        assert FetchResult.SKIPPED == 2


class TestFetchConnectorXtbSkip:
    """fetch_connector returns SKIPPED for XTB without --xtb-file."""

    def test_xtb_returns_skipped_when_no_file(self, tmp_data_dir: Path) -> None:
        """XTB connector returns FetchResult.SKIPPED when no --xtb-file is provided."""
        connector = get("xtb")
        args = argparse.Namespace(xtb_file=None)
        fernet_key = generate_key()
        rc = fetch_connector(connector, args, fernet_key)
        assert rc == FetchResult.SKIPPED


# ---------------------------------------------------------------------------
# cmd_run_consolidate_analytics
# ---------------------------------------------------------------------------


class TestCmdRunConsolidateAnalytics:
    """cmd_run_consolidate_analytics runs consolidate, validates silver, then analytics."""

    @patch("pipeline.run._normalize_cdc", return_value=0)
    @patch("pipeline.run._consolidate_cdc", return_value=0)
    @patch("pipeline.run.run_validation", return_value=0)
    @patch("pipeline.run.cmd_analytics", return_value=0)
    @patch("pipeline.run.cmd_consolidate", return_value=0)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_calls_consolidate_validate_silver_then_analytics(
        self,
        mock_key: MagicMock,
        mock_consolidate: MagicMock,
        mock_analytics: MagicMock,
        mock_validate: MagicMock,
        mock_consolidate_cdc: MagicMock,
        mock_normalize_cdc: MagicMock,
    ) -> None:
        args = argparse.Namespace(
            target_currency="EUR",
            fx_rate=[],
            isin=[],
            isin_map_file=[],
        )
        rc = cmd_run_consolidate_analytics(args)
        assert rc == 0
        mock_consolidate.assert_called_once_with(args)
        mock_analytics.assert_called_once_with(args)
        # run_validation called twice: silver then gold
        assert mock_validate.call_count == 2
        mock_validate.assert_any_call(
            fernet_key=b"test-key",
            tables=["consolidated_holdings", "cdc_events"],
        )
        mock_validate.assert_any_call(
            fernet_key=b"test-key",
            tables=[
                "portfolio_holdings",
                "dividend_income",
                "interest_income",
                "cash_flow_summary",
            ],
        )

    @patch("pipeline.run._normalize_cdc", return_value=0)
    @patch("pipeline.run._consolidate_cdc", return_value=0)
    @patch("pipeline.run.run_validation", return_value=0)
    @patch("pipeline.run.cmd_analytics", return_value=0)
    @patch("pipeline.run.cmd_consolidate", return_value=1)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_consolidate_failure_skips_analytics(
        self,
        mock_key: MagicMock,
        mock_consolidate: MagicMock,
        mock_analytics: MagicMock,
        mock_validate: MagicMock,
        mock_consolidate_cdc: MagicMock,
        mock_normalize_cdc: MagicMock,
    ) -> None:
        args = argparse.Namespace(
            target_currency="EUR",
            fx_rate=[],
            isin=[],
            isin_map_file=[],
        )
        rc = cmd_run_consolidate_analytics(args)
        assert rc == 1
        mock_analytics.assert_not_called()
        mock_validate.assert_not_called()

    @patch("pipeline.run._normalize_cdc", return_value=0)
    @patch("pipeline.run._consolidate_cdc", return_value=0)
    @patch("pipeline.run.cmd_analytics", return_value=0)
    @patch("pipeline.run.run_validation", return_value=1)
    @patch("pipeline.run.cmd_consolidate", return_value=0)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_silver_validation_failure_skips_analytics(
        self,
        mock_key: MagicMock,
        mock_consolidate: MagicMock,
        mock_validate: MagicMock,
        mock_analytics: MagicMock,
        mock_consolidate_cdc: MagicMock,
        mock_normalize_cdc: MagicMock,
    ) -> None:
        """Silver validation failure prevents analytics from running."""
        args = argparse.Namespace(
            target_currency="EUR",
            fx_rate=[],
            isin=[],
            isin_map_file=[],
        )
        rc = cmd_run_consolidate_analytics(args)
        assert rc == 1
        mock_analytics.assert_not_called()

    @patch("pipeline.run._normalize_cdc", return_value=0)
    @patch("pipeline.run._consolidate_cdc", return_value=0)
    @patch("pipeline.run.cmd_analytics", return_value=0)
    @patch("pipeline.run.run_validation", side_effect=[0, 1])
    @patch("pipeline.run.cmd_consolidate", return_value=0)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_gold_validation_failure_returns_nonzero(
        self,
        mock_key: MagicMock,
        mock_consolidate: MagicMock,
        mock_validate: MagicMock,
        mock_analytics: MagicMock,
        mock_consolidate_cdc: MagicMock,
        mock_normalize_cdc: MagicMock,
    ) -> None:
        """Gold validation failure after analytics returns non-zero."""
        args = argparse.Namespace(
            target_currency="EUR",
            fx_rate=[],
            isin=[],
            isin_map_file=[],
        )
        rc = cmd_run_consolidate_analytics(args)
        assert rc == 1


# ---------------------------------------------------------------------------
# cmd_full / cmd_fetch / cmd_transform regression
# ---------------------------------------------------------------------------


class TestCmdFetchRegression:
    """cmd_fetch still iterates all() and uses enabled_env_var."""

    @patch("pipeline.run.fetch_connector", return_value=FetchResult.SUCCESS)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_cmd_fetch_iterates_all_connectors(
        self, mock_key: MagicMock, mock_fetch: MagicMock
    ) -> None:
        """cmd_fetch calls fetch_connector for each enabled connector."""
        # Use only the real connectors (the FakeConnector from test_connector_registry
        # doesn't have enabled_env_var, so we mock all() to exclude it).
        real_connectors = [get("ibkr"), get("trading212"), get("xtb")]
        args = argparse.Namespace(xtb_file=None)
        with (
            patch("pipeline.run.is_enabled", return_value=True),
            patch("pipeline.run.all_connectors", return_value=real_connectors),
            patch("pipeline.run.inject_secrets"),
        ):
            rc = cmd_fetch(args)
        assert rc == 0
        assert mock_fetch.call_count == 3

    @patch("pipeline.run.fetch_connector", return_value=FetchResult.SUCCESS)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_cmd_fetch_skips_disabled_connectors(
        self, mock_key: MagicMock, mock_fetch: MagicMock
    ) -> None:
        """cmd_fetch skips connectors whose enabled_env_var is false."""
        real_connectors = [get("ibkr"), get("trading212"), get("xtb")]
        args = argparse.Namespace(xtb_file=None)

        def is_enabled_side_effect(env_var: str) -> bool:
            return env_var != "XTB_ENABLED"

        with (
            patch("pipeline.run.is_enabled", side_effect=is_enabled_side_effect),
            patch("pipeline.run.all_connectors", return_value=real_connectors),
            patch("pipeline.run.inject_secrets"),
        ):
            rc = cmd_fetch(args)
        assert rc == 0
        # Should be called for ibkr and trading212, but not xtb
        assert mock_fetch.call_count == 2


class TestCmdTransformRegression:
    """cmd_transform still iterates all()."""

    @patch("pipeline.run.transform_connector", return_value=0)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_cmd_transform_iterates_all_connectors(
        self, mock_key: MagicMock, mock_transform: MagicMock
    ) -> None:
        real_connectors = [get("ibkr"), get("trading212"), get("xtb")]
        args = argparse.Namespace()
        with patch("pipeline.run.all_connectors", return_value=real_connectors):
            rc = cmd_transform(args)
        assert rc == 0
        assert mock_transform.call_count == 3


class TestCmdFullRegression:
    """cmd_full chains fetch → transform → consolidate → analytics."""

    @patch("pipeline.run._normalize_cdc", return_value=0)
    @patch("pipeline.run._consolidate_cdc", return_value=0)
    @patch("pipeline.run.cmd_analytics", return_value=0)
    @patch("pipeline.run.cmd_consolidate", return_value=0)
    @patch("pipeline.run.cmd_transform", return_value=0)
    @patch("pipeline.run.cmd_fetch", return_value=0)
    def test_cmd_full_chains_all_steps(
        self,
        mock_fetch: MagicMock,
        mock_transform: MagicMock,
        mock_consolidate: MagicMock,
        mock_analytics: MagicMock,
        mock_consolidate_cdc: MagicMock,
        mock_normalize_cdc: MagicMock,
    ) -> None:
        args = argparse.Namespace(
            xtb_file=None,
            target_currency="EUR",
            fx_rate=[],
            isin=[],
            isin_map_file=[],
        )
        rc = cmd_full(args)
        assert rc == 0
        mock_fetch.assert_called_once()
        mock_transform.assert_called_once()
        mock_consolidate.assert_called_once()
        mock_analytics.assert_called_once()

    @patch("pipeline.run._normalize_cdc", return_value=0)
    @patch("pipeline.run._consolidate_cdc", return_value=0)
    @patch("pipeline.run.cmd_analytics", return_value=0)
    @patch("pipeline.run.cmd_consolidate", return_value=0)
    @patch("pipeline.run.cmd_transform", return_value=0)
    @patch("pipeline.run.cmd_fetch", return_value=1)
    def test_cmd_full_stops_on_fetch_failure(
        self,
        mock_fetch: MagicMock,
        mock_transform: MagicMock,
        mock_consolidate: MagicMock,
        mock_analytics: MagicMock,
        mock_consolidate_cdc: MagicMock,
        mock_normalize_cdc: MagicMock,
    ) -> None:
        args = argparse.Namespace(
            xtb_file=None,
            target_currency="EUR",
            fx_rate=[],
            isin=[],
            isin_map_file=[],
        )
        rc = cmd_full(args)
        assert rc == 1
        mock_transform.assert_not_called()
        mock_consolidate.assert_not_called()
        mock_analytics.assert_not_called()
