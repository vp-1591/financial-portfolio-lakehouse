"""Tests for run-connector, run-consolidate-analytics, and cmd_full subcommands.

Verifies the extracted ``fetch_connector``/``transform_connector`` helpers,
the ``cmd_run_connector``/``cmd_run_consolidate_analytics`` commands, and
the ``cmd_full`` docker-mode orchestrator (parallel connectors + consolidate).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
import pytest

import pipeline.sfn as sfn_mod
from pipeline import run as run_module
from pipeline import storage as storage_mod
from pipeline.connectors.registry import get
from pipeline.crypto import generate_key
from pipeline.run import (
    FetchResult,
    _run_connectors_parallel,
    cmd_full,
    cmd_run_connector,
    cmd_run_consolidate_analytics,
    fetch_connector,
    transform_connector,
)
from pipeline.secrets import reset_mode, set_mode

# ---------------------------------------------------------------------------
# Argparse dispatch
# ---------------------------------------------------------------------------


class TestArgparseDispatch:
    """run-connector and run-consolidate-analytics are present in the commands dict."""

    def test_main_dispatches_keygen(self, monkeypatch) -> None:
        """main() parses the 'keygen' subcommand and dispatches to cmd_keygen.

        Exercises the full argparse parse -> commands[args.command](args) path
        (round1-persistence §2b example 1). A dispatch-break mutation
        (key removed from the commands dict) would raise KeyError instead of
        returning 99, failing this test.
        """

        called: dict[str, bool] = {}

        def fake_keygen(args: argparse.Namespace) -> int:
            called["invoked"] = True
            return 99

        monkeypatch.setattr(run_module, "cmd_keygen", fake_keygen)
        monkeypatch.setattr(sys, "argv", ["pipeline.run", "keygen"])
        rc = run_module.main()
        assert rc == 99
        assert called.get("invoked") is True

    def test_main_dispatches_run_connector(self, monkeypatch) -> None:
        """main() parses 'run-connector' and dispatches to cmd_run_connector.

        Fails on the dispatch-break mutation where the 'run-connector' key is
        removed from the commands dict (main() raises KeyError) — the existing
        bytecode/callable checks pass under that mutation; this real dispatch
        invocation does not.
        """

        called: dict[str, bool] = {}

        def fake_run_connector(args: argparse.Namespace) -> int:
            called["invoked"] = True
            return 99

        monkeypatch.setattr(run_module, "cmd_run_connector", fake_run_connector)
        # Avoid touching real storage / S3 resolution for this dispatch test.
        monkeypatch.setattr(storage_mod, "resolve_storage", lambda: None)
        monkeypatch.setattr(
            sys, "argv", ["pipeline.run", "run-connector", "ibkr", "--mode", "docker"]
        )
        rc = run_module.main()
        assert rc == 99
        assert called.get("invoked") is True

    def test_main_dispatches_purge_account(self, monkeypatch) -> None:
        """main() parses 'purge-account' and dispatches to cmd_purge_account."""

        called: dict[str, bool] = {}

        def fake_purge_account(args: argparse.Namespace) -> int:
            called["invoked"] = True
            return 99

        monkeypatch.setattr(run_module, "cmd_purge_account", fake_purge_account)
        monkeypatch.setattr(storage_mod, "resolve_storage", lambda: None)
        monkeypatch.setattr(
            sys,
            "argv",
            ["pipeline.run", "purge-account", "xtb", "123", "--mode", "docker"],
        )
        rc = run_module.main()
        assert rc == 99
        assert called.get("invoked") is True

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


# ---------------------------------------------------------------------------
# fetch_connector / transform_connector isolation
# ---------------------------------------------------------------------------


class TestFetchConnectorIsolation:
    """fetch_connector uses connector.fetch_kwargs (no if/elif)."""

    @patch("pipeline.raw.ingest.ingest_raw", return_value=None)
    def test_uses_fetch_kwargs(
        self, mock_ingest: MagicMock, tmp_data_dir: Path
    ) -> None:
        """fetch_connector calls connector.fetch_kwargs(args) and passes each batch to fetch_snapshot."""
        connector = get("ibkr")
        args = argparse.Namespace()

        with (
            patch.object(
                connector,
                "fetch_kwargs",
                return_value=[
                    {
                        "flex_token": "t",
                        "flex_query_id": "q",
                        "flex_base_url": "u",
                    }
                ],
            ) as mock_kwargs,
            patch.object(
                connector, "fetch_snapshot", return_value=MagicMock(num_rows=1)
            ) as mock_snapshot,
            patch.object(connector, "fetch_events_kwargs", return_value={}),
        ):
            fernet_key = generate_key()
            rc = fetch_connector(connector, args, fernet_key)
            assert rc == FetchResult.SUCCESS
            mock_kwargs.assert_called_once_with(args)
            mock_snapshot.assert_called_once()

    @patch("pipeline.raw.ingest.ingest_raw", return_value=None)
    def test_skips_connector_when_kwargs_empty(
        self, mock_ingest: MagicMock, tmp_data_dir: Path
    ) -> None:
        """fetch_connector returns SKIPPED and skips when fetch_kwargs returns []."""
        connector = get("ibkr")
        args = argparse.Namespace()

        with (
            patch.object(connector, "fetch_kwargs", return_value=[]),
            patch.object(connector, "fetch_snapshot") as mock_snapshot,
        ):
            fernet_key = generate_key()
            rc = fetch_connector(connector, args, fernet_key)
            assert rc == FetchResult.SKIPPED
            mock_snapshot.assert_not_called()

    @patch("pipeline.raw.ingest.ingest_raw", return_value=None)
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
                return_value=[
                    {
                        "flex_token": "t",
                        "flex_query_id": "q",
                        "flex_base_url": "u",
                    }
                ],
            ),
            patch.object(
                connector,
                "fetch_snapshot",
                side_effect=RuntimeError("API timeout"),
            ),
            patch.object(connector, "fetch_events_kwargs", return_value={}),
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

    @patch("pipeline.run.run_validation", return_value=0)
    @patch("pipeline.run.transform_connector", return_value=0)
    @patch("pipeline.run.fetch_connector", return_value=FetchResult.SUCCESS)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_enabled_connector_calls_fetch_then_transform(
        self,
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
            tables=["ibkr_snapshot", "ibkr_events"],
            connectors=["ibkr"],
        )

    @patch("pipeline.run.run_validation", return_value=0)
    @patch("pipeline.run.transform_connector", return_value=0)
    @patch("pipeline.run.fetch_connector", return_value=FetchResult.ERROR)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_fetch_failure_skips_transform(
        self,
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
    def test_skipped_connector_returns_zero(
        self,
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
    @patch("pipeline.run.fetch_connector", return_value=FetchResult.SUCCESS)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_validation_failure_returns_nonzero(
        self,
        mock_key: MagicMock,
        mock_fetch: MagicMock,
        mock_transform: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        """If run_validation returns non-zero, cmd_run_connector returns it."""
        args = argparse.Namespace(connector="ibkr")
        rc = cmd_run_connector(args)
        assert rc == 1

    @patch("pipeline.run.run_validation")
    @patch("pipeline.run.transform_connector")
    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_xtb_without_file_returns_0(
        self,
        mock_key: MagicMock,
        mock_transform: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        """XTB without --xtb-file skips gracefully (returns 0).

        XTB is file-arrival-driven: ``run-connector xtb`` without
        ``--xtb-file`` returns SKIPPED -> exit 0, so transform and validation
        are skipped.
        """
        args = argparse.Namespace(connector="xtb", xtb_file=None)
        rc = cmd_run_connector(args)
        assert rc == 0
        mock_transform.assert_not_called()
        mock_validate.assert_not_called()

    @patch("pipeline.run.run_validation", return_value=0)
    @patch("pipeline.run.transform_connector", return_value=0)
    @patch("pipeline.run.fetch_connector", return_value=FetchResult.SUCCESS)
    @patch("pipeline.run.load_key", return_value=b"test-key")
    def test_xtb_with_file_calls_fetch(
        self,
        mock_key: MagicMock,
        mock_fetch: MagicMock,
        mock_transform: MagicMock,
        mock_validate: MagicMock,
    ) -> None:
        args = argparse.Namespace(connector="xtb", xtb_file=["report.xlsx"])
        rc = cmd_run_connector(args)
        assert rc == 0
        # D14: validation is unconditional — events table is always validated.
        mock_validate.assert_called_once_with(
            fernet_key=b"test-key",
            tables=["xtb_snapshot", "xtb_events"],
            connectors=["xtb"],
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

    @patch("pipeline.run._normalize_events", return_value=0)
    @patch("pipeline.run._consolidate_events", return_value=0)
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
        mock_consolidate_events: MagicMock,
        mock_normalize_events: MagicMock,
    ) -> None:
        args = argparse.Namespace(
            target_currency="EUR",
            fx_rate=[],
        )
        rc = cmd_run_consolidate_analytics(args)
        assert rc == 0
        mock_consolidate.assert_called_once_with(args)
        mock_analytics.assert_called_once_with(args)
        # run_validation called twice: silver then gold
        assert mock_validate.call_count == 2
        mock_validate.assert_any_call(
            fernet_key=b"test-key",
            tables=["consolidated_holdings", "events"],
            connectors=[],
        )
        mock_validate.assert_any_call(
            fernet_key=b"test-key",
            tables=[
                "portfolio_holdings",
                "dividend_income",
                "interest_income",
                "cash_flow_summary",
            ],
            connectors=[],
        )

    @patch("pipeline.run._normalize_events", return_value=0)
    @patch("pipeline.run._consolidate_events", return_value=0)
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
        mock_consolidate_events: MagicMock,
        mock_normalize_events: MagicMock,
    ) -> None:
        args = argparse.Namespace(
            target_currency="EUR",
            fx_rate=[],
        )
        rc = cmd_run_consolidate_analytics(args)
        assert rc == 1
        mock_analytics.assert_not_called()
        mock_validate.assert_not_called()

    @patch("pipeline.run._normalize_events", return_value=0)
    @patch("pipeline.run._consolidate_events", return_value=0)
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
        mock_consolidate_events: MagicMock,
        mock_normalize_events: MagicMock,
    ) -> None:
        """Silver validation failure prevents analytics from running."""
        args = argparse.Namespace(
            target_currency="EUR",
            fx_rate=[],
        )
        rc = cmd_run_consolidate_analytics(args)
        assert rc == 1
        mock_analytics.assert_not_called()

    @patch("pipeline.run._normalize_events", return_value=0)
    @patch("pipeline.run._consolidate_events", return_value=0)
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
        mock_consolidate_events: MagicMock,
        mock_normalize_events: MagicMock,
    ) -> None:
        """Gold validation failure after analytics returns non-zero."""
        args = argparse.Namespace(
            target_currency="EUR",
            fx_rate=[],
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

        set_mode("docker")
        args = argparse.Namespace(
            xtb_file=None,
            target_currency="EUR",
            fx_rate=[],
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

        set_mode("docker")
        args = argparse.Namespace(
            xtb_file=None,
            target_currency="EUR",
            fx_rate=[],
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
        defaults = {
            "xtb_file": None,
            "with_xtb": False,
            "wait": False,
            "target_currency": "EUR",
            "fx_rate": [],
        }
        defaults.update(overrides)
        return argparse.Namespace(**defaults)

    def _stub_session(self, monkeypatch, has_creds: bool = True) -> MagicMock:

        sess = MagicMock()
        sess.get_credentials.return_value = MagicMock() if has_creds else None
        sess.region_name = "eu-west-1"
        monkeypatch.setattr(boto3, "Session", lambda: sess)
        return sess

    def _stub_sfn(
        self,
        monkeypatch,
        *,
        sfn_arn: str | None = "arn:staging-sfn",
        wait_status: str | None = None,
        wait_raises: Exception | None = None,
        details: str = "DETAILS",
    ) -> MagicMock:

        start = MagicMock(return_value="arn:exec")
        monkeypatch.setattr(
            sfn_mod,
            "build_clients",
            lambda region: (MagicMock(), MagicMock(), MagicMock()),
        )
        monkeypatch.setattr(
            sfn_mod,
            "resolve_state_machine_arn",
            lambda *a, **k: sfn_arn,
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

        set_mode("staging")
        self._stub_session(monkeypatch)
        start = self._stub_sfn(monkeypatch, sfn_arn="arn:staging-sfn")

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

        set_mode("prod")
        self._stub_session(monkeypatch)
        start = self._stub_sfn(monkeypatch, sfn_arn="arn:prod-sfn")

        rc = cmd_full(self._base_args())
        assert rc == 0
        assert start.call_args.args[1] == "arn:prod-sfn"
        reset_mode()

    def test_with_xtb_errors(self, monkeypatch, capsys: pytest.CaptureFixture) -> None:

        set_mode("staging")
        self._stub_session(monkeypatch)
        start = self._stub_sfn(monkeypatch)

        rc = cmd_full(self._base_args(with_xtb=True))
        assert rc == 1
        start.assert_not_called()
        assert "upload-xtb" in capsys.readouterr().err
        reset_mode()

    def test_xtb_file_errors(self, monkeypatch, capsys: pytest.CaptureFixture) -> None:

        set_mode("staging")
        self._stub_session(monkeypatch)
        start = self._stub_sfn(monkeypatch)

        rc = cmd_full(self._base_args(xtb_file=["s3://bucket/file.csv"]))
        assert rc == 1
        start.assert_not_called()
        reset_mode()

    def test_aws_creds_missing_errors(
        self, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:

        set_mode("staging")
        self._stub_session(monkeypatch, has_creds=False)
        start = self._stub_sfn(monkeypatch)

        rc = cmd_full(self._base_args())
        assert rc == 1
        start.assert_not_called()
        assert "AWS credentials not found" in capsys.readouterr().err
        reset_mode()

    def test_state_machine_not_found_errors(
        self, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:

        set_mode("staging")
        self._stub_session(monkeypatch)
        start = self._stub_sfn(monkeypatch, sfn_arn=None)

        rc = cmd_full(self._base_args())
        assert rc == 1
        start.assert_not_called()
        # Error message is printed by resolve_state_machine_arn (tested in test_sfn.py).
        reset_mode()

    def test_wait_succeeded_returns_zero(
        self, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:

        set_mode("staging")
        self._stub_session(monkeypatch)
        self._stub_sfn(monkeypatch, wait_status="SUCCEEDED")

        rc = cmd_full(self._base_args(wait=True))
        assert rc == 0
        assert "succeeded" in capsys.readouterr().out.lower()
        reset_mode()

    def test_wait_failed_prints_details(
        self, monkeypatch, capsys: pytest.CaptureFixture
    ) -> None:

        set_mode("staging")
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

        set_mode("staging")
        self._stub_session(monkeypatch)
        self._stub_sfn(monkeypatch, wait_raises=TimeoutError("timed out"))

        rc = cmd_full(self._base_args(wait=True))
        assert rc == 1
        assert "timed out" in capsys.readouterr().err
        reset_mode()


class TestRunConnectorsParallel:
    """_run_connectors_parallel runs all connectors via ThreadPoolExecutor."""

    @patch("pipeline.run.cmd_run_connector", return_value=0)
    @patch("pipeline.run.all_connectors")
    def test_all_connectors_succeed(self, mock_all, mock_rc) -> None:

        set_mode("docker")
        mock_all.return_value = [get("ibkr"), get("trading212")]
        args = argparse.Namespace(
            target_currency="EUR",
            fx_rate=[],
            xtb_file=None,
            mode="docker",
        )

        rc = _run_connectors_parallel(args)
        assert rc == 0
        assert mock_rc.call_count == 2
        reset_mode()

    @patch("pipeline.run.cmd_run_connector", return_value=1)
    @patch("pipeline.run.all_connectors")
    def test_connector_failure_returns_nonzero(self, mock_all, mock_rc, capsys) -> None:

        set_mode("docker")
        mock_all.return_value = [get("ibkr")]
        args = argparse.Namespace(
            target_currency="EUR",
            fx_rate=[],
            xtb_file=None,
            mode="docker",
        )

        rc = _run_connectors_parallel(args)
        assert rc == 1
        stderr = capsys.readouterr().err
        assert "fail-fast" in stderr
        reset_mode()
