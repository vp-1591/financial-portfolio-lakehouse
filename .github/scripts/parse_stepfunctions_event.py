"""Parse Step Functions event details from a JSON file.

Usage: python3 parse_stepfunctions_event.py <event_type> <json_file>

event_type: task_failed | task_timed_out | execution_failed
"""

import json
import sys


def parse_task_failed(data: list[dict]) -> None:
    """Parse TaskFailed events, extracting exit code, task definition, and reason."""
    for d in data:
        error = d.get("error", "unknown")
        cause = d.get("cause", "")
        try:
            j = json.loads(cause)
            containers = j.get("Containers", [{}])
            exit_code = next(
                (
                    i.get("exitCode")
                    for i in containers
                    if i.get("exitCode") is not None
                ),
                "N/A",
            )
            task_def = j.get("taskDefinitionArn", "N/A").split("/")[-1]
            reason = j.get("stoppedReason", "N/A")
            print(
                f"  error={error}  task={task_def}  exitCode={exit_code}  reason={reason}"
            )
        except (json.JSONDecodeError, AttributeError):
            print(f"  error={error}  cause={cause[:500]}")


def parse_generic(data: list[dict]) -> None:
    """Parse TaskTimedOut and ExecutionFailed events — print error and truncated cause."""
    for d in data:
        error = d.get("error", "unknown")
        cause = d.get("cause", "")[:500]
        print(f"  error={error}  cause={cause}")


HANDLERS = {
    "task_failed": parse_task_failed,
    "task_timed_out": parse_generic,
    "execution_failed": parse_generic,
}


def main() -> None:
    event_type = sys.argv[1]
    with open(sys.argv[2]) as f:
        data = json.load(f)

    handler = HANDLERS.get(event_type)
    if handler is None:
        print(f"Unknown event type: {event_type}", file=sys.stderr)
        sys.exit(1)

    handler(data)


if __name__ == "__main__":
    main()
