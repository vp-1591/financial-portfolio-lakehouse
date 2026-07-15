# 0081: Fix Deploy Log Readability in GitHub Actions

> **Supersedes [ADR 0067](./0067-fix-step-function-failure-logging-in-deploy-workflow.md)** — ADR 0067 added the failure-logging steps but the output was unreadable: ECS task JSON blobs in the execution history and tab-concatenated CloudWatch logs. This ADR replaces the raw output with parsed, human-readable formats.

## Context

The deploy-staging workflow already fetches CloudWatch container logs on failure (ADR 0062/0067). However, the output is nearly unusable for debugging:

1. **Step Functions execution history** — The `taskFailedEventDetails.cause` field contains the entire ECS task JSON (network interfaces, container ARNs, etc.) as a monolithic string. The useful information (`exitCode`, `stoppedReason`, `taskDefinitionArn`) is buried in this blob and not scannable at a glance.

2. **CloudWatch log output** — `aws logs filter-log-events --output text` dumps all log events as a single tab-separated string with no line breaks between events. Multiple Step Functions retry attempts (3 per connector) are concatenated into an unreadable wall of text. Python tracebacks are technically present but extremely hard to find.

3. **Error-swallowing patterns** — `|| true` after `get-execution-history`, `2>/dev/null` on `filter-log-events`, and `2>/dev/null || echo "0"` on `startDate` parsing make it impossible to distinguish "no data" from "the command failed."

4. **`startDate` parsing is broken** — `date -d {} +%s%3N` via `xargs` fails on AWS CLI v2's epoch-seconds timestamp format (e.g., `1721234567.890`). The `2>/dev/null` masks the error, and `|| echo "0"` silently falls back to epoch 0.

An AI debug agent reviewing a failed run concluded it needed AWS CloudWatch access because it couldn't find the application errors in the GitHub Actions output — despite the errors being present but buried in unreadable formatting.

## Decision

1. **Parse ECS task JSON in execution history** — Use `--output json` with `python3 -c` to extract `exitCode`, `stoppedReason`, and `taskDefinitionArn` from the ECS task JSON blob in `taskFailedEventDetails.cause`. Print one line per failure: `error=States.TaskFailed task=portfolio-pipeline-demo-ibkr exitCode=1 reason=Essential container in task exited`.

2. **Format CloudWatch logs per-line** — Use `--output json` and `python3 -c` to print each log event on its own line instead of a tab-concatenated wall of text.

3. **Fix `startDate` parsing** — Handle AWS CLI v2's epoch-seconds format directly in bash using parameter expansion (`${START_SECONDS%%.*}` and `${START_SECONDS#*.}`) instead of piping through `date -d`. Print the computed `START_MS` for debugging visibility. Fall back to `"0"` with a `::warning::` annotation on failure.

4. **Remove error-swallowing** — Replace `|| true` with `|| echo "(failed to fetch ...)"` on execution history queries. Replace `2>/dev/null` on `filter-log-events` with explicit error handling that prints `::warning::` annotations. This makes diagnostic failures visible in the GitHub Actions UI.

5. **Add CLAUDE.md note** — A short note directing agents and developers to the "Print container logs on failure" step for application errors.

## Constraints

- The deploy workflow must not hang indefinitely — the existing 15-minute timeout is preserved.
- No new IAM permissions are needed; `logs:FilterLogEvents` and `states:GetExecutionHistory` are already granted.
- `python3` is available on the `ubuntu-latest` runner used by the workflow.
- The fix only changes how existing data is queried and displayed.

## Consequences

- **Positive**: Failure diagnostics are now scannable — the key information (exit code, stopped reason, Python traceback) is immediately visible without scrolling through walls of JSON or tab-separated text.
- **Positive**: Diagnostic command failures are now visible as `::warning::` annotations in the GitHub Actions UI, making it clear when the logging itself has issues.
- **Positive**: The `startDate` parsing now works correctly with AWS CLI v2's timestamp format, ensuring log queries are scoped to the right time window.
- **Neutral**: Three `python3 -c` inline scripts are added to the workflow. These are simple JSON parsers (< 10 lines each) and `python3` is always available on `ubuntu-latest`.

## Validation

1. Push to a feature branch and trigger the deploy-staging workflow.
2. If the pipeline fails, verify in the GitHub Actions logs that:
   - `START_MS` is printed and is a reasonable epoch millisecond value (not `0`).
   - Task Failure output shows `error=States.TaskFailed task=... exitCode=1 reason=...` instead of the full ECS JSON blob.
   - CloudWatch log output has one log event per line with readable Python tracebacks.
   - Any failures in the diagnostic commands produce `::warning::` annotations, not silent empty output.
3. Verify that `ruff check --fix .` and `ruff format .` pass (no Python changes).