"""Tests for :mod:`pipeline.sfn` — Step Functions trigger and failure-detail logic.

Pure functions are tested directly; boto3 wrappers are tested with
:class:`unittest.mock.MagicMock` (dependency injection — no moto needed).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import pipeline.sfn as sfn


# ---------------------------------------------------------------------------
# Pure functions — family names, commands, execution input
# ---------------------------------------------------------------------------


class TestFamilyNames:
    def test_task_def_family_staging_maps_to_demo(self) -> None:
        assert sfn.task_def_family("staging", "ibkr") == "portfolio-pipeline-demo-ibkr"

    def test_task_def_family_prod(self) -> None:
        assert (
            sfn.task_def_family("prod", "trading212")
            == "portfolio-pipeline-prod-trading212"
        )

    def test_consolidate_task_def_family(self) -> None:
        assert (
            sfn.consolidate_task_def_family("staging")
            == "portfolio-pipeline-demo-consolidate-allocate"
        )
        assert (
            sfn.consolidate_task_def_family("prod")
            == "portfolio-pipeline-prod-consolidate-allocate"
        )

    def test_unsupported_mode_raises(self) -> None:
        with pytest.raises(ValueError):
            sfn.task_def_family("docker", "ibkr")


class TestCommandBuilders:
    def test_build_connector_command_includes_mode(self) -> None:
        assert sfn.build_connector_command("ibkr", "staging", "EUR") == [
            "run-connector",
            "ibkr",
            "--mode",
            "staging",
            "--target-currency",
            "EUR",
        ]

    def test_build_consolidate_command_includes_mode(self) -> None:
        assert sfn.build_consolidate_command("prod", "EUR") == [
            "run-consolidate-analytics",
            "--mode",
            "prod",
            "--target-currency",
            "EUR",
        ]


class TestBuildExecutionInput:
    def test_input_shape_no_demo_has_consolidate_command(self) -> None:
        connector_arns = {
            "ibkr": "arn:ibkr",
            "trading212": "arn:t212",
        }
        inp = sfn.build_execution_input(
            ["ibkr", "trading212"], connector_arns, "arn:consolidate", "staging", "EUR"
        )
        assert "demo" not in inp
        assert inp["consolidate_allocate_task_def_arn"] == "arn:consolidate"
        assert inp["consolidate_command"] == [
            "run-consolidate-analytics",
            "--mode",
            "staging",
            "--target-currency",
            "EUR",
        ]
        names = [c["name"] for c in inp["connectors"]]
        assert names == ["ibkr", "trading212"]
        for c in inp["connectors"]:
            assert c["task_def_arn"] == connector_arns[c["name"]]
            assert "--mode" in c["command"]
            assert c["command"][0] == "run-connector"


class TestConsoleUrlAndName:
    def test_console_url(self) -> None:
        url = sfn.console_url("arn:execution", "eu-west-1")
        assert "region=eu-west-1" in url
        assert url.endswith("#/executions/details/arn:execution")

    def test_execution_name_has_prefix_and_stamp(self) -> None:
        name = sfn.execution_name("staging")
        assert name.startswith("staging-")
        # Stamp = YYYYMMDD (8) + "T" (1) + HHMMSS (6) + 6 microsecond digits = 21.
        stamp = name.split("staging-", 1)[1]
        assert len(stamp) == 21


class TestResolveStateMachineArn:
    def test_staging_resolves_by_name(self) -> None:
        sfn_client = MagicMock()
        sfn_client.get_paginator.return_value.paginate.return_value = [
            {
                "stateMachines": [
                    {
                        "name": "portfolio-pipeline-orchestrator-demo",
                        "stateMachineArn": "arn:aws:states:eu-west-1:123:stateMachine:portfolio-pipeline-orchestrator-demo",
                    },
                ]
            }
        ]
        arn = sfn.resolve_state_machine_arn(sfn_client, "staging")
        assert (
            arn
            == "arn:aws:states:eu-west-1:123:stateMachine:portfolio-pipeline-orchestrator-demo"
        )

    def test_prod_resolves_by_name(self) -> None:
        sfn_client = MagicMock()
        sfn_client.get_paginator.return_value.paginate.return_value = [
            {
                "stateMachines": [
                    {
                        "name": "portfolio-pipeline-orchestrator",
                        "stateMachineArn": "arn:aws:states:eu-west-1:123:stateMachine:portfolio-pipeline-orchestrator",
                    },
                ]
            }
        ]
        arn = sfn.resolve_state_machine_arn(sfn_client, "prod")
        assert (
            arn
            == "arn:aws:states:eu-west-1:123:stateMachine:portfolio-pipeline-orchestrator"
        )

    def test_not_found_returns_none_and_prints_error(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        sfn_client = MagicMock()
        sfn_client.get_paginator.return_value.paginate.return_value = [
            {"stateMachines": []}
        ]
        arn = sfn.resolve_state_machine_arn(sfn_client, "staging")
        assert arn is None
        err = capsys.readouterr().err
        assert "portfolio-pipeline-orchestrator-demo" in err
        assert "not found" in err

    def test_unsupported_mode_returns_none_and_prints_error(
        self, capsys: pytest.CaptureFixture
    ) -> None:
        sfn_client = MagicMock()
        arn = sfn.resolve_state_machine_arn(sfn_client, "docker")
        assert arn is None
        err = capsys.readouterr().err
        assert "Unsupported mode" in err


# ---------------------------------------------------------------------------
# Failure-detail parsers (absorbed from parse_stepfunctions_event.py)
# ---------------------------------------------------------------------------


class TestParseTaskFailed:
    def test_extracts_exit_code_task_reason(self) -> None:
        cause = json.dumps(
            {
                "Containers": [{"exitCode": 1}, {"exitCode": None}],
                "taskDefinitionArn": "arn:aws:ecs:::task-definition/portfolio-pipeline-demo-ibkr:3",
                "stoppedReason": "Essential container exited",
            }
        )
        lines = sfn.parse_task_failed([{"error": "States.TaskFailed", "cause": cause}])
        assert len(lines) == 1
        line = lines[0]
        assert "error=States.TaskFailed" in line
        assert "task=portfolio-pipeline-demo-ibkr:3" in line
        assert "exitCode=1" in line
        assert "reason=Essential container exited" in line

    def test_no_exit_code_falls_back_to_na(self) -> None:
        cause = json.dumps({"Containers": [{}], "taskDefinitionArn": "arn:x/name:1"})
        lines = sfn.parse_task_failed([{"error": "E", "cause": cause}])
        assert "exitCode=N/A" in lines[0]

    def test_unparseable_cause_falls_back_to_truncated_raw(self) -> None:
        lines = sfn.parse_task_failed([{"error": "E", "cause": "not-json"}])
        assert "cause=not-json" in lines[0]


class TestParseGenericFailure:
    def test_truncates_cause_to_500(self) -> None:
        long_cause = "x" * 800
        lines = sfn.parse_generic_failure([{"error": "E", "cause": long_cause}])
        assert "cause=" + "x" * 500 in lines[0]
        assert "x" * 800 not in lines[0]


class TestFormatLogMessages:
    def test_one_per_line(self) -> None:
        assert sfn.format_log_messages(["a", "b", "c"]) == "a\nb\nc"


# ---------------------------------------------------------------------------
# boto3 wrappers — MagicMock
# ---------------------------------------------------------------------------


class TestResolveTaskDefArn:
    def test_calls_describe_with_family_and_returns_arn(self) -> None:
        ecs = MagicMock()
        ecs.describe_task_definition.return_value = {
            "taskDefinition": {"taskDefinitionArn": "arn:family:5"}
        }
        arn = sfn.resolve_task_def_arn(ecs, "portfolio-pipeline-demo-ibkr")
        ecs.describe_task_definition.assert_called_once_with(
            taskDefinition="portfolio-pipeline-demo-ibkr"
        )
        assert arn == "arn:family:5"


class TestResolveAllArns:
    def test_resolves_connectors_and_consolidate(self) -> None:
        ecs = MagicMock()

        def _desc(taskDefinition: str) -> dict:
            return {"taskDefinition": {"taskDefinitionArn": f"arn:{taskDefinition}"}}

        ecs.describe_task_definition.side_effect = _desc
        connector_arns, consolidate_arn = sfn.resolve_all_arns(
            ecs, "staging", ["ibkr", "trading212"]
        )
        assert connector_arns == {
            "ibkr": "arn:portfolio-pipeline-demo-ibkr",
            "trading212": "arn:portfolio-pipeline-demo-trading212",
        }
        assert consolidate_arn == "arn:portfolio-pipeline-demo-consolidate-allocate"
        described = {
            c.kwargs["taskDefinition"]
            for c in ecs.describe_task_definition.call_args_list
        }
        assert "portfolio-pipeline-demo-consolidate-allocate" in described


class TestStartExecution:
    def test_passes_arn_name_and_serialized_input(self) -> None:
        sfn_client = MagicMock()
        sfn_client.start_execution.return_value = {"executionArn": "arn:exec"}
        inp = {"connectors": [], "consolidate_command": []}
        arn = sfn.start_execution(sfn_client, "arn:sfn", inp, "staging-x")
        sfn_client.start_execution.assert_called_once_with(
            stateMachineArn="arn:sfn",
            name="staging-x",
            input=json.dumps(inp),
        )
        assert arn == "arn:exec"


class TestWaitForExecution:
    def test_succeeded_after_running(self) -> None:
        sfn_client = MagicMock()
        sfn_client.describe_execution.side_effect = [
            {"status": "RUNNING"},
            {"status": "SUCCEEDED"},
        ]
        with patch("pipeline.sfn.time.sleep") as sleep:
            status = sfn.wait_for_execution(
                sfn_client, "arn:exec", timeout_seconds=900, interval_seconds=30
            )
        assert status == "SUCCEEDED"
        assert sleep.call_count == 1

    def test_failed_returns_failed(self) -> None:
        sfn_client = MagicMock()
        sfn_client.describe_execution.return_value = {"status": "FAILED"}
        with patch("pipeline.sfn.time.sleep"):
            assert sfn.wait_for_execution(sfn_client, "arn:exec") == "FAILED"

    def test_timeout_raises(self) -> None:
        sfn_client = MagicMock()
        sfn_client.describe_execution.return_value = {"status": "RUNNING"}
        with patch("pipeline.sfn.time.sleep"):
            with pytest.raises(TimeoutError):
                sfn.wait_for_execution(
                    sfn_client, "arn:exec", timeout_seconds=60, interval_seconds=30
                )


class TestFetchFailureDetails:
    def test_queries_history_and_each_log_group(self) -> None:
        sfn_client = MagicMock()
        start = datetime(2026, 7, 23, 10, 0, 0, tzinfo=timezone.utc)
        sfn_client.get_execution_history.return_value = {
            "events": [
                {
                    "type": "TaskFailed",
                    "taskFailedEventDetails": {
                        "error": "States.TaskFailed",
                        "cause": json.dumps(
                            {
                                "Containers": [{"exitCode": 1}],
                                "taskDefinitionArn": "arn::portfolio-pipeline-demo-ibkr:1",
                                "stoppedReason": "boom",
                            }
                        ),
                    },
                }
            ]
        }
        sfn_client.describe_execution.return_value = {"startDate": start}

        logs_client = MagicMock()
        logs_client.filter_log_events.return_value = {
            "events": [{"message": "line one"}, {"message": "line two"}]
        }

        out = sfn.fetch_failure_details(sfn_client, logs_client, "arn:exec", "staging")

        # History surfaced.
        assert "=== Execution History ===" in out
        assert "exitCode=1" in out
        # All three log groups queried with the scoped start time.
        expected_start_ms = int(start.timestamp() * 1000)
        queried_groups = [
            c.kwargs["logGroupName"]
            for c in logs_client.filter_log_events.call_args_list
        ]
        assert queried_groups == [
            "/ecs/portfolio-pipeline-demo-ibkr",
            "/ecs/portfolio-pipeline-demo-trading212",
            "/ecs/portfolio-pipeline-demo-consolidate-allocate",
        ]
        for c in logs_client.filter_log_events.call_args_list:
            assert c.kwargs["startTime"] == expected_start_ms
        # Log messages rendered.
        assert "line one" in out
        assert "line two" in out

    def test_log_fetch_failure_is_best_effort(self) -> None:
        sfn_client = MagicMock()
        sfn_client.get_execution_history.return_value = {"events": []}
        sfn_client.describe_execution.return_value = {
            "startDate": datetime(2026, 7, 23, 10, 0, 0, tzinfo=timezone.utc)
        }
        logs_client = MagicMock()
        logs_client.filter_log_events.side_effect = RuntimeError("boom")
        out = sfn.fetch_failure_details(sfn_client, logs_client, "arn:exec", "prod")
        assert "failed to fetch logs" in out
        assert "boom" in out


class TestBuildClients:
    def test_build_clients_uses_default_chain(self) -> None:
        with (
            patch("pipeline.sfn.boto3.client") as mock_client,
        ):
            sfn.build_clients("eu-west-1")
            services = [c.args[0] for c in mock_client.call_args_list]
            assert services == ["stepfunctions", "ecs", "logs"]
            for c in mock_client.call_args_list:
                assert c.kwargs["region_name"] == "eu-west-1"
