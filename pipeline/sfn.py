"""Step Functions orchestration for staging/prod ``full`` runs.

When ``cmd_full`` runs in staging or prod mode it does not execute the
pipeline locally — instead it starts a Step Functions execution that runs
each connector as an ECS Fargate task and then runs the
``run-consolidate-analytics`` task.  The caller's machine needs AWS
credentials with ``states:ListStateMachines`` / ``states:StartExecution``
permission; broker secrets are injected into the ECS containers by SSM at
task launch time.

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
isolation between environments is handled at the SSM / ECS level.

The IAM permissions the trigger needs differ by environment (ADR 0053 /
0093):

- **staging** — the ``pipeline-staging-cicd`` policy grants the full set:
  ``states:ListStateMachines``, ``states:StartExecution``,
  ``states:DescribeExecution``, ``states:GetExecutionHistory``,
  ``ecs:DescribeTaskDefinition``, and ``logs:FilterLogEvents``.  Staging
  is CI-triggered on every merge to ``main`` and polls with ``--wait``, so
  it can start executions and read history/logs.
- **prod** — the ``pipeline-cicd`` policy grants **only**
  ``ecs:DescribeTaskDefinition``.  Production runs on a daily EventBridge
  schedule, not CI/manual trigger, so the prod key deliberately lacks
  Step Functions permissions (a compromised prod key cannot trigger the
  production state machine).  Running ``full --mode prod`` locally against
  the prod ``pipeline`` user therefore fails with ``AccessDeniedException``
  on ``states:ListStateMachines``; per ADR 0093 the caller's IAM user must
  have the SFN permissions added to use that path.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import UTC, datetime
from typing import Any

import boto3

# ``--mode`` value → Step Functions state machine name.  The ARN is resolved
# at runtime via ``list_state_machines`` so no env var or hardcoded ARN is
# needed — the names come from Terraform
# (``terraform/staging/main.tf``: ``state_machine_name =
# "portfolio-pipeline-orchestrator-staging"``,
# ``terraform/prod/main.tf``: ``state_machine_name =
# "portfolio-pipeline-orchestrator"``).
STATE_MACHINE_NAMES: dict[str, str] = {
    "staging": "portfolio-pipeline-orchestrator-staging",
    "prod": "portfolio-pipeline-orchestrator",
}

# Connectors run by ``full --mode staging|prod``.  XTB is excluded: it is
# driven solely by the EventBridge S3 file-arrival trigger (fetch + transform
# only when a new file is uploaded), so scheduled/CI runs do not launch a
# no-op ``run-connector xtb`` task.  ``run-consolidate-analytics`` still reads
# XTB silver (``xtb_snapshot``/``xtb_events``) on every run via the connector
# registry, whenever present.
# Decision: docs/adr/0110-xtb-file-arrival-only-ingestion.md
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
    if mode not in {"staging", "prod"}:
        raise ValueError(f"Unsupported mode for SFN trigger: {mode!r}")
    return mode


def task_def_family(mode: str, connector_name: str) -> str:
    """Return the ECS task definition family for a connector in a mode.

    e.g. ``staging`` + ``ibkr`` → ``portfolio-pipeline-staging-ibkr``.
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

    The vestigial ``staging`` field is intentionally absent — the ASL never
    references ``$.staging``.  ``consolidate_command`` is consumed by the
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
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
    return f"{prefix}-{stamp}"


def resolve_state_machine_arn(sfn_client: Any, mode: str) -> str | None:
    """Resolve the state machine ARN for a mode via the SFN API.

    Looks up the state machine by its well-known name (see
    :data:`STATE_MACHINE_NAMES`) using ``list_state_machines``.  Returns
    ``None`` and prints an actionable error if the state machine is not found.
    """
    name = STATE_MACHINE_NAMES.get(mode)
    if name is None:
        print(f"Unsupported mode for SFN trigger: {mode!r}", file=sys.stderr)
        return None

    paginator = sfn_client.get_paginator("list_state_machines")
    for page in paginator.paginate():
        for sm in page.get("stateMachines", []):
            if sm["name"] == name:
                return sm["stateMachineArn"]

    print(
        f"State machine {name!r} not found in AWS. "
        f"Ensure the {mode} infrastructure is deployed "
        f"(terraform/{_env_label(mode)}/main.tf).",
        file=sys.stderr,
    )
    return None


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

    exec_desc = sfn_client.describe_execution(executionArn=execution_arn)
    start_ms = int(exec_desc["startDate"].timestamp() * 1000)

    # Derive the connector list from the failed execution's own input so XTB
    # container logs are still captured for failed *file-arrival* executions
    # (where XTB runs) even though XTB is no longer in DEFAULT_CONNECTORS.
    # Fall back to DEFAULT_CONNECTORS if the input cannot be parsed or is not
    # a JSON object (e.g. "[]"). An input that parses but yields no connectors
    # (e.g. "{}") queries only the consolidate-allocate log group.
    # Decision: docs/adr/0110-xtb-file-arrival-only-ingestion.md
    try:
        connector_names = [
            c["name"]
            for c in json.loads(exec_desc.get("input") or "{}").get("connectors", [])
        ]
    except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
        connector_names = list(DEFAULT_CONNECTORS)

    for name in [*connector_names, CONSOLIDATE_FAMILY_NAME]:
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
        except Exception as exc:
            sections.append(f"(failed to fetch logs from {log_group}: {exc})")
        sections.append("")

    return "\n".join(sections)
