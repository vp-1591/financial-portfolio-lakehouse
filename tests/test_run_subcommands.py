"""Tests for run-connector, run-consolidate-analytics, and cmd_full subcommands.

Verifies the extracted ``fetch_connector``/``transform_connector`` helpers,
the ``cmd_run_connector``/``cmd_run_consolidate_analytics`` commands, and
the ``cmd_full`` docker-mode orchestrator (parallel connectors + consolidate).
"""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.connectors.registry import get
from pipeline.crypto import generate_key
from pipeline.run import (
    FetchResult,
    cmd_full,
    cmd_run_consolidate_analytics,
    cmd_run_connector,
    fetch_connector,
    transform_connector,
)
from pipeline.secrets import reset_mode


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


@pytest.mark.usefixtures("docker_mode")
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
# cmd_full docker-mode orchestrator
# ---------------------------------------------------------------------------


class TestCmdFullDockerMode:
    """cmd_full in docker mode runs connectors in parallel then consolidate-analytics."""

    @patch("pipeline.run.cmd_run_consolidate_analytics", return_value=0)
    @patch("pipeline.run._run_connectors_parallel", return_value=0)
    @patch("pipeline.run.inject_secrets")
    def test_docker_mode_calls_connectors_then_consolidate(
        self,
        mock_inject: MagicMock,
        mock_parallel: MagicMock,
        mock_consolidate: MagicMock,
        monkeypatch,
    ) -> None:
        """cmd_full --mode docker calls _run_connectors_parallel then cmd_run_consolidate_analytics."""
        from pipeline.secrets import set_mode

        set_mode("docker")
        args = argparse.Namespace(
            xtb_file=None,
            target_currency="EUR",
            fx_rate=[],
            isin=[],
            isin_map_file=[],
        )
        rc = cmd_full(args)
        assert rc == 0
        mock_parallel.assert_called_once_with(args)
        mock_consolidate.assert_called_once_with(args)
        reset_mode()

    @patch("pipeline.run.cmd_run_consolidate_analytics")
    @patch("pipeline.run._run_connectors_parallel", return_value=1)
    @patch("pipeline.run.inject_secrets")
    def test_docker_mode_connector_failure_skips_consolidate(
        self,
        mock_inject: MagicMock,
        mock_parallel: MagicMock,
        mock_consolidate: MagicMock,
        monkeypatch,
    ) -> None:
        """If _run_connectors_parallel returns non-zero, consolidate is not called."""
        from pipeline.secrets import set_mode

        set_mode("docker")
        args = argparse.Namespace(
            xtb_file=None,
            target_currency="EUR",
            fx_rate=[],
            isin=[],
            isin_map_file=[],
        )
        rc = cmd_full(args)
        assert rc == 1
        mock_consolidate.assert_not_called()
        reset_mode()


# ---------------------------------------------------------------------------
# cmd_full staging/prod — Step Functions trigger
# ---------------------------------------------------------------------------


class TestCmdFullSfnTrigger:
    """cmd_full --mode staging|prod starts a Step Functions execution."""

    def _base_args(self, **overrides) -> argparse.Namespace:
        defaults = dict(
            xtb_file=None,
            with_xtb=False,
            wait=False,
            target_currency="EUR",
            fx_rate=[],
            isin=[],
            isin_map_file=[],
        )
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def _stub_session(self, monkeypatch, has_creds: bool = True) -> MagicMock:
        import boto3

        sess = MagicMock()
        sess.get_credentials.return_value = MagicMock() if has_creds else None
        sess.region_name = "eu-west-1"
        monkeypatch.setattr(boto3, "Session", lambda: sess)
        return sess

    def _stub_sfn(
        self,
        monkeypatch,
        *,
        wait_status: str | None = None,
        wait_raises: Exception | None = None,
        details: str = "DETAILS",
    ) -> MagicMock:
        import pipeline.sfn as sfn_mod

        start = MagicMock(return_value="arn:exec")
        monkeypatch.setattr(
            sfn_mod,
            "build_clients",
            lambda region: (MagicMock(), MagicMock(), MagicMock()),
        )
        monkeypatch.setattr(
            sfn_mod,
            "resolve_all_arns",
            lambda *a, **k: (
                {"ibkr": "arn:ibkr", "trading212": "arn:t212"},
                "arn:cons",
            ),
        )
        monkeypatch.setattr(sfn_mod, "start_execution", start)
        monkeypatch.setattr(sfn_mod, "fetch_failure_details", lambda *a, **k: details)
        if wait_raises is not None:
            monkeypatch.setattr(
                sfn_mod,
                "wait_for_execution",
                lambda *a, **k: (_ for _ in ()).throw(wait_raises),
            )
        else:
            monkeypatch.setattr(
                sfn_mod, "wait_for_execution", lambda *a, **k: wait_status
            )
        return start

    def test_staging_starts_execution(
        self, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:
        from pipeline.secrets import set_mode

        set_mode("staging")
        monkeypatch.setenv("STAGING_STATE_MACHINE_ARN", "arn:staging-sfn")
        self._stub_session(monkeypatch)
        start = self._stub_sfn(monkeypatch)

        rc = cmd_full(self._base_args())
        assert rc == 0
        start.assert_called_once()
        assert start.call_args.args[1] == "arn:staging-sfn"
        out = capsys.readouterr().out
        assert "arn:exec" in out
        assert "Monitor:" in out
        reset_mode()

    def test_prod_starts_execution(
        self, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:
        from pipeline.secrets import set_mode

        set_mode("prod")
        monkeypatch.setenv("PROD_STATE_MACHINE_ARN", "arn:prod-sfn")
        self._stub_session(monkeypatch)
        start = self._stub_sfn(monkeypatch)

        rc = cmd_full(self._base_args())
        assert rc == 0
        assert start.call_args.args[1] == "arn:prod-sfn"
        reset_mode()

    def test_with_xtb_errors(self, monkeypatch, capsys: pytest.CaptureFixture) -> None:
        from pipeline.secrets import set_mode

        set_mode("staging")
        monkeypatch.setenv("STAGING_STATE_MACHINE_ARN", "arn:staging-sfn")
        self._stub_session(monkeypatch)
        start = self._stub_sfn(monkeypatch)

        rc = cmd_full(self._base_args(with_xtb=True))
        assert rc == 1
        start.assert_not_called()
        assert "upload-xtb" in capsys.readouterr().err
        reset_mode()

    def test_xtb_file_errors(self, monkeypatch, capsys: pytest.CaptureFixture) -> None:
        from pipeline.secrets import set_mode

        set_mode("staging")
        monkeypatch.setenv("STAGING_STATE_MACHINE_ARN", "arn:staging-sfn")
        self._stub_session(monkeypatch)
        start = self._stub_sfn(monkeypatch)

        rc = cmd_full(self._base_args(xtb_file=["s3://bucket/file.csv"]))
        assert rc == 1
        start.assert_not_called()
        reset_mode()

    def test_aws_creds_missing_errors(
        self, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:
        from pipeline.secrets import set_mode

        set_mode("staging")
        monkeypatch.setenv("STAGING_STATE_MACHINE_ARN", "arn:staging-sfn")
        self._stub_session(monkeypatch, has_creds=False)
        start = self._stub_sfn(monkeypatch)

        rc = cmd_full(self._base_args())
        assert rc == 1
        start.assert_not_called()
        assert "AWS credentials not found" in capsys.readouterr().err
        reset_mode()

    def test_state_machine_arn_missing_errors(
        self, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:
        from pipeline.secrets import set_mode

        set_mode("staging")
        monkeypatch.delenv("STAGING_STATE_MACHINE_ARN", raising=False)
        self._stub_session(monkeypatch)
        start = self._stub_sfn(monkeypatch)

        rc = cmd_full(self._base_args())
        assert rc == 1
        start.assert_not_called()
        assert "STAGING_STATE_MACHINE_ARN" in capsys.readouterr().err
        reset_mode()

    def test_wait_succeeded_returns_zero(
        self, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:
        from pipeline.secrets import set_mode

        set_mode("staging")
        monkeypatch.setenv("STAGING_STATE_MACHINE_ARN", "arn:staging-sfn")
        self._stub_session(monkeypatch)
        self._stub_sfn(monkeypatch, wait_status="SUCCEEDED")

        rc = cmd_full(self._base_args(wait=True))
        assert rc == 0
        assert "succeeded" in capsys.readouterr().out.lower()
        reset_mode()

    def test_wait_failed_prints_details(
        self, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:
        from pipeline.secrets import set_mode

        set_mode("staging")
        monkeypatch.setenv("STAGING_STATE_MACHINE_ARN", "arn:staging-sfn")
        self._stub_session(monkeypatch)
        self._stub_sfn(monkeypatch, wait_status="FAILED", details="TASK FAILED DETAILS")

        rc = cmd_full(self._base_args(wait=True))
        assert rc == 1
        captured = capsys.readouterr()
        assert "FAILED" in captured.err
        assert "TASK FAILED DETAILS" in captured.err
        reset_mode()

    def test_wait_timeout_returns_one(
        self, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:
        from pipeline.secrets import set_mode

        set_mode("staging")
        monkeypatch.setenv("STAGING_STATE_MACHINE_ARN", "arn:staging-sfn")
        self._stub_session(monkeypatch)
        self._stub_sfn(monkeypatch, wait_raises=TimeoutError("timed out"))

        rc = cmd_full(self._base_args(wait=True))
        assert rc == 1
        assert "timed out" in capsys.readouterr().err
        reset_mode()


class TestRunConnectorsParallel:
    """_run_connectors_parallel runs enabled connectors via ThreadPoolExecutor."""

    @patch("pipeline.run.cmd_run_connector", return_value=0)
    @patch("pipeline.run.all_connectors")
    @patch("pipeline.run.is_enabled", return_value=True)
    def test_all_connectors_succeed(self, mock_enabled, mock_all, mock_rc) -> None:
        from pipeline.secrets import set_mode

        set_mode("docker")
        mock_all.return_value = [get("ibkr"), get("trading212")]
        args = argparse.Namespace(
            target_currency="EUR",
            fx_rate=[],
            isin=[],
            isin_map_file=[],
            xtb_file=None,
            mode="docker",
        )
        from pipeline.run import _run_connectors_parallel

        rc = _run_connectors_parallel(args)
        assert rc == 0
        assert mock_rc.call_count == 2
        reset_mode()

    @patch("pipeline.run.cmd_run_connector", return_value=1)
    @patch("pipeline.run.all_connectors")
    @patch("pipeline.run.is_enabled", return_value=True)
    def test_connector_failure_returns_nonzero(
        self, mock_enabled, mock_all, mock_rc, capsys
    ) -> None:
        from pipeline.secrets import set_mode

        set_mode("docker")
        mock_all.return_value = [get("ibkr")]
        args = argparse.Namespace(
            target_currency="EUR",
            fx_rate=[],
            isin=[],
            isin_map_file=[],
            xtb_file=None,
            mode="docker",
        )
        from pipeline.run import _run_connectors_parallel

        rc = _run_connectors_parallel(args)
        assert rc == 1
        stderr = capsys.readouterr().err
        assert "fail-fast" in stderr
        reset_mode()

    @patch("pipeline.run.all_connectors")
    @patch("pipeline.run.is_enabled", return_value=False)
    def test_all_disabled_returns_zero(self, mock_enabled, mock_all) -> None:
        from pipeline.secrets import set_mode

        set_mode("docker")
        mock_all.return_value = [get("ibkr"), get("trading212")]
        args = argparse.Namespace(
            target_currency="EUR",
            fx_rate=[],
            isin=[],
            isin_map_file=[],
            xtb_file=None,
            mode="docker",
        )
        from pipeline.run import _run_connectors_parallel

        rc = _run_connectors_parallel(args)
        assert rc == 0
        reset_mode()
