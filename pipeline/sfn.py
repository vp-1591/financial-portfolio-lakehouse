"""Step Functions orchestration for staging/prod ``full`` runs.

When ``cmd_full`` runs in staging or prod mode it does not execute the
pipeline locally — instead it starts a Step Functions execution that runs
each connector as an ECS Fargate task and then runs the
``run-consolidate-analytics`` task.  The caller's machine only needs AWS
credentials with ``states:StartExecution`` permission; broker secrets are
injected into the ECS containers by SSM at task launch time.

This module is split into:

- **Pure functions** (no boto3, no I/O) — command builders, execution-input
  assembly, family-name math, and the failure-detail parsers absorbed from
  ``.github/scripts/parse_stepfunctions_event.py`` and
  ``.github/scripts/format_log_events.py``.  These are unit-tested without
  AWS.
- **boto3 wrappers** — each takes its client as a parameter (dependency
  injection) so tests use :class:`unittest.mock.MagicMock` rather than moto.

Clients are built with boto3's default credential chain (the base
``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` env vars exported by the
``configure-aws-credentials`` GitHub Action), not via
:func:`pipeline.secrets.resolve_aws_credentials`.  The credential
isolation between environments is handled at the SSM / ECS level; the SFN
trigger only needs IAM
``states:StartExecution`` / ``ecs:DescribeTaskDefinition`` /
``logs:FilterLogEvents`` permissions, which the same access key provides in
either environment.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import boto3

# ``--mode`` value → ECS environment label used in task definition families
# and CloudWatch log group names.  Staging mode runs against the demo
# infrastructure (env label "demo"); prod mode against prod.
MODE_TO_ENV_LABEL: dict[str, str] = {"staging": "demo", "prod": "prod"}

# Connectors run by ``full --mode staging|prod``.  XTB is excluded — it
# requires an uploaded file and is triggered by the EventBridge S3 file
# arrival rule, not by the CI/manual ``full`` run.
DEFAULT_CONNECTORS: list[str] = ["ibkr", "trading212"]

DEFAULT_TIMEOUT_SECONDS = 900
DEFAULT_POLL_INTERVAL_SECONDS = 30

TASK_FAMILY_TEMPLATE = "portfolio-pipeline-{env_label}-{name}"
CONSOLIDATE_FAMILY_NAME = "consolidate-allocate"

TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}


# ---------------------------------------------------------------------------
# Pure functions — no boto3, no I/O
# ---------------------------------------------------------------------------


def _env_label(mode: str) -> str:
    try:
        return MODE_TO_ENV_LABEL[mode]
    except KeyError as exc:
        raise ValueError(f"Unsupported mode for SFN trigger: {mode!r}") from exc


def task_def_family(mode: str, connector_name: str) -> str:
    """Return the ECS task definition family for a connector in a mode.

    e.g. ``staging`` + ``ibkr`` → ``portfolio-pipeline-demo-ibkr``.
    """
    return TASK_FAMILY_TEMPLATE.format(env_label=_env_label(mode), name=connector_name)


def consolidate_task_def_family(mode: str) -> str:
    """Return the consolidate-allocate task definition family for a mode."""
    return task_def_family(mode, CONSOLIDATE_FAMILY_NAME)


def build_connector_command(name: str, mode: str, target_currency: str) -> list[str]:
    """Build the ``run-connector`` command array for the SFN execution input."""
    return ["run-connector", name, "--mode", mode, "--target-currency", target_currency]


def build_consolidate_command(mode: str, target_currency: str) -> list[str]:
    """Build the ``run-consolidate-analytics`` command for the SFN input."""
    return [
        "run-consolidate-analytics",
        "--mode",
        mode,
        "--target-currency",
        target_currency,
    ]


def build_execution_input(
    connectors: list[str],
    connector_arns: dict[str, str],
    consolidate_arn: str,
    mode: str,
    target_currency: str,
) -> dict:
    """Assemble the Step Functions execution input dict.

    Schema (matches the orchestrator ASL)::

        {
          "connectors": [{"name", "task_def_arn", "command"}, ...],
          "consolidate_allocate_task_def_arn": str,
          "consolidate_command": [str, ...]
        }

    The vestigial ``demo`` field is intentionally absent — the ASL never
    references ``$.demo``.  ``consolidate_command`` is consumed by the
    ConsolidateAllocate state via ``"Command.$": "$.consolidate_command"``.
    """
    return {
        "connectors": [
            {
                "name": name,
                "task_def_arn": connector_arns[name],
                "command": build_connector_command(name, mode, target_currency),
            }
            for name in connectors
        ],
        "consolidate_allocate_task_def_arn": consolidate_arn,
        "consolidate_command": build_consolidate_command(mode, target_currency),
    }


def console_url(execution_arn: str, region: str) -> str:
    """Build a clickable Step Functions console URL for an execution ARN."""
    return (
        f"https://{region}.console.aws.amazon.com/states/home"
        f"?region={region}#/executions/details/{execution_arn}"
    )


def execution_name(prefix: str) -> str:
    """Build a unique SFN execution name: ``{prefix}-<UTC microsecond stamp>``.

    Step Functions requires unique execution names per state machine; the
    microsecond precision avoids same-second collisions for manual runs.
    """
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    return f"{prefix}-{stamp}"


def state_machine_arn(mode: str) -> str | None:
    """Resolve the state machine ARN for a mode from the environment.

    Reads ``STAGING_STATE_MACHINE_ARN`` (staging) or
    ``PROD_STATE_MACHINE_ARN`` (prod).  Returns ``None`` and prints an
    actionable error if the variable is unset.
    """
    env_var = (
        "STAGING_STATE_MACHINE_ARN" if mode == "staging" else "PROD_STATE_MACHINE_ARN"
    )
    arn = os.environ.get(env_var)
    if not arn:
        source = "terraform/demo" if mode == "staging" else "terraform/prod"
        print(
            f"{env_var} is not set. Set it from the {source} "
            "`state_machine_arn` Terraform output in your .env or CI secrets.",
            file=sys.stderr,
        )
    return arn


# ---------------------------------------------------------------------------
# Failure-detail parsers — absorbed from parse_stepfunctions_event.py
# ---------------------------------------------------------------------------


def parse_task_failed(events: list[dict]) -> list[str]:
    """Parse ``TaskFailed`` event details into human-readable summary lines.

    Each ``cause`` field is a JSON string containing the ECS task detail;
    extract the exit code (first container with one), the task definition
    short name, and the stopped reason.  Falls back to a truncated raw
    cause when the JSON cannot be parsed.
    """
    lines: list[str] = []
    for d in events:
        error = d.get("error", "unknown")
        cause = d.get("cause", "")
        try:
            j = json.loads(cause)
            containers = j.get("Containers", [{}])
            exit_code = next(
                (
                    c.get("exitCode")
                    for c in containers
                    if c.get("exitCode") is not None
                ),
                "N/A",
            )
            task_def = j.get("taskDefinitionArn", "N/A").split("/")[-1]
            reason = j.get("stoppedReason", "N/A")
            lines.append(
                f"  error={error}  task={task_def}  exitCode={exit_code}  reason={reason}"
            )
        except (json.JSONDecodeError, AttributeError):
            lines.append(f"  error={error}  cause={cause[:500]}")
    return lines


def parse_generic_failure(events: list[dict]) -> list[str]:
    """Parse ``TaskTimedOut`` / ``ExecutionFailed`` events — error + truncated cause."""
    return [
        f"  error={d.get('error', 'unknown')}  cause={d.get('cause', '')[:500]}"
        for d in events
    ]


# ---------------------------------------------------------------------------
# Log formatting — absorbed from format_log_events.py
# ---------------------------------------------------------------------------


def format_log_messages(messages: list[str]) -> str:
    """Render CloudWatch log messages one per line."""
    return "\n".join(messages)


# ---------------------------------------------------------------------------
# boto3 wrappers — each takes its client as a parameter (dependency injection)
# ---------------------------------------------------------------------------


def build_clients(region: str) -> tuple[Any, Any, Any]:
    """Build ``(sfn, ecs, logs)`` boto3 clients using the default credential chain."""
    sfn = boto3.client("stepfunctions", region_name=region)
    ecs = boto3.client("ecs", region_name=region)
    logs = boto3.client("logs", region_name=region)
    return sfn, ecs, logs


def resolve_task_def_arn(ecs_client: Any, family: str) -> str:
    """Resolve the latest active task definition ARN for a family name."""
    resp = ecs_client.describe_task_definition(taskDefinition=family)
    return resp["taskDefinition"]["taskDefinitionArn"]


def resolve_all_arns(
    ecs_client: Any,
    mode: str,
    connectors: list[str],
) -> tuple[dict[str, str], str]:
    """Resolve connector + consolidate-allocate task definition ARNs.

    Returns ``(connector_arns, consolidate_arn)``.
    """
    connector_arns = {
        name: resolve_task_def_arn(ecs_client, task_def_family(mode, name))
        for name in connectors
    }
    consolidate_arn = resolve_task_def_arn(
        ecs_client, consolidate_task_def_family(mode)
    )
    return connector_arns, consolidate_arn


def start_execution(
    sfn_client: Any,
    state_machine_arn: str,
    input_dict: dict,
    name: str,
) -> str:
    """Start a Step Functions execution and return the execution ARN."""
    resp = sfn_client.start_execution(
        stateMachineArn=state_machine_arn,
        name=name,
        input=json.dumps(input_dict),
    )
    return resp["executionArn"]


def wait_for_execution(
    sfn_client: Any,
    execution_arn: str,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
) -> str:
    """Poll an execution until it reaches a terminal status.

    Returns the terminal status (``SUCCEEDED`` / ``FAILED`` / ``TIMED_OUT`` /
    ``ABORTED``).  Raises :class:`TimeoutError` if the timeout elapses first.
    """
    elapsed = 0
    while elapsed <= timeout_seconds:
        status = sfn_client.describe_execution(executionArn=execution_arn)["status"]
        if status in TERMINAL_STATUSES:
            return status
        time.sleep(interval_seconds)
        elapsed += interval_seconds
    raise TimeoutError(
        f"Step Function execution {execution_arn} did not finish within "
        f"{timeout_seconds}s"
    )


def _filter_events(
    history: list[dict], event_type: str, details_key: str
) -> list[dict]:
    return [
        e[details_key]
        for e in history
        if e.get("type") == event_type and details_key in e
    ]


def fetch_failure_details(
    sfn_client: Any,
    logs_client: Any,
    execution_arn: str,
    mode: str,
) -> str:
    """Collect diagnostic output for a failed SFN execution.

    Absorbs the logic from the ``deploy-staging.yml`` "Wait" and "Print
    container logs on failure" steps:

    1. Fetch execution history and surface ``TaskFailed`` / ``TaskTimedOut``
       / ``ExecutionFailed`` events via the parsers above.
    2. Scope CloudWatch log queries to the execution start time
       (``describe_execution`` returns a ``datetime``; convert to epoch ms).
    3. For each connector + the consolidate-allocate task, fetch and print
       container logs from ``/ecs/portfolio-pipeline-{env_label}-{name}``.
    """
    env_label = _env_label(mode)
    sections: list[str] = []

    history = sfn_client.get_execution_history(executionArn=execution_arn)["events"]
    task_failed = _filter_events(history, "TaskFailed", "taskFailedEventDetails")
    task_timed_out = _filter_events(history, "TaskTimedOut", "taskTimedOutEventDetails")
    exec_failed = _filter_events(
        history, "ExecutionFailed", "executionFailedEventDetails"
    )

    sections.append("=== Execution History ===")
    if task_failed:
        sections.append("--- Task Failures ---")
        sections.extend(parse_task_failed(task_failed))
    if task_timed_out:
        sections.append("--- Task Timeouts ---")
        sections.extend(parse_generic_failure(task_timed_out))
    if exec_failed:
        sections.append("--- Execution Failure ---")
        sections.extend(parse_generic_failure(exec_failed))

    start_date = sfn_client.describe_execution(executionArn=execution_arn)["startDate"]
    start_ms = int(start_date.timestamp() * 1000)

    for name in [*DEFAULT_CONNECTORS, CONSOLIDATE_FAMILY_NAME]:
        log_group = f"/ecs/portfolio-pipeline-{env_label}-{name}"
        sections.append(f"=== Container logs: {name} ===")
        sections.append(f"Log group: {log_group}")
        try:
            resp = logs_client.filter_log_events(
                logGroupName=log_group,
                startTime=start_ms,
                limit=500,
            )
            messages = [e["message"] for e in resp.get("events", [])]
            sections.append(format_log_messages(messages))
        except Exception as exc:  # noqa: BLE001 — log fetch is best-effort diagnostics
            sections.append(f"(failed to fetch logs from {log_group}: {exc})")
        sections.append("")

    return "\n".join(sections)
