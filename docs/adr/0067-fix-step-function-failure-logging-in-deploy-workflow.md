# 0067: Fix Step Function Failure Logging in Deploy Workflow

> **Superseded by [ADR 0081](./0081-fix-deploy-log-readability.md)** — The execution history and container log output from ADR 0067 was unreadable: ECS task JSON blobs and tab-concatenated CloudWatch logs. ADR 0081 replaces the raw output with parsed, human-readable formats.

## Context

The `Wait for demo pipeline` step in `deploy-staging.yml` prints Step Functions execution history on failure using this JMESPath query:

```
events[?type==`TaskFailed` || type==`ExecutionFailed`].{name:name, error:error, cause:cause}
```

This query projects top-level `error` and `cause` fields from the event objects, but for ECS `RunTask` failures, those fields don't exist at the top level. Step Functions nests the error details inside type-specific event detail objects:

- `TaskFailed` → `taskFailedEventDetails.error` and `taskFailedEventDetails.cause`
- `ExecutionFailed` → `executionFailedEventDetails.error` and `executionFailedEventDetails.cause`
- `TaskTimedOut` → `taskTimedOutEventDetails.error` and `taskTimedOutEventDetails.cause`

The top-level `name` field also doesn't exist on these events. The result is a table showing `None` for every column:

```
| cause | error | name |
|-------|-------|------|
| None  | None  | None |
| None  | None  | None |
```

This makes the execution history step useless for debugging — the actual error (e.g., `run.py: error: argument command: invalid choice: 'run-consolidate-allocate'`) is only visible in the CloudWatch container logs printed by the separate `Print container logs on failure` step.

## Decision

Replace the single broken JMESPath query with three separate queries, each targeting a specific failure event type and projecting from the correct nested detail object:

1. **TaskFailed** — `events[?type==\`TaskFailed\`].{id:id, error:taskFailedEventDetails.error, cause:taskFailedEventDetails.cause}`
2. **TaskTimedOut** — `events[?type==\`TaskTimedOut\`].{id:id, error:taskTimedOutEventDetails.error, cause:taskTimedOutEventDetails.cause}`
3. **ExecutionFailed** — `events[?type==\`ExecutionFailed\`].{id:id, error:executionFailedEventDetails.error, cause:executionFailedEventDetails.cause}`

This also adds `TaskTimedOut` coverage (missing from the original query) and uses `id` instead of `name` since `name` is not a field on Step Functions history events.

## Constraints

- The deploy workflow must not hang indefinitely — the existing 15-minute timeout is preserved.
- No new IAM permissions are needed; `states:GetExecutionHistory` is already granted.
- The fix only changes how the existing history data is queried and displayed.

## Consequences

- **Positive**: Failure details (error type and cause) are now visible directly in the GitHub Actions log alongside the execution status, making it possible to diagnose failures without navigating to the AWS Console or relying solely on the container logs step.
- **Positive**: `TaskTimedOut` events are now also captured, covering the case where an ECS task exceeds its timeout.
- **Neutral**: Three separate CLI calls instead of one, but each is cheap (the data is already fetched; JMESPath filtering is client-side) and only runs on failure.

## Validation

1. Push a change that triggers the deploy workflow with a known-bad command (e.g., the current `run-consolidate-allocate` which is not a valid `run.py` subcommand).
2. The "Wait for demo pipeline" step should show a table like:

   ```
   --- Task Failures ---
   |  id  |  error              |  cause                                                          |
   |------|--------------------|------------------------------------------------------------------|
   |  6   |  States.TaskFailed  |  {"Error":"States.TaskFailed","Cause":"run.py: error: ..."}      |
   ```

   instead of `None` for every column.
3. The "Print container logs on failure" step should continue to show the full container stderr.