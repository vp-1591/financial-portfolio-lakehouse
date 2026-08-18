# 0110 — XTB Driven Solely by the File-Arrival Trigger

## Context

XTB ingestion has two triggers: the EventBridge S3 file-arrival rule (runs
the full pipeline when a report is uploaded) and the scheduled/CI run. Two
problems surfaced:

1. **The file-arrival trigger was broken in demo and prod.** The EventBridge
   `input_transformer` template embedded the `<xtb_file>` substitution
   placeholder inside a Terraform `jsonencode()` call. Go's `encoding/json`
   HTML-escapes `<`, `>`, `&` to `<`, `>`, `&`, so the stored
   template contained `<xtb_file>` instead of the literal `<xtb_file>` token
   EventBridge's input transformer matches. No substitution happened; every
   file-arrival execution passed the literal string `<xtb_file>` as the S3
   key, got a 404, and failed (`States.TaskFailed` after the ASL retries).

2. **The scheduled `run-connector xtb` step was a no-op that contradicted
   ADR 0091.** The schedule passes no `--xtb-file`, so `fetch_connector`
   returns `SKIPPED` and `cmd_run_connector` returns 0 without transform or
   validation — an ECS task that does nothing. ADR 0108 D19/D21 had added XTB
   to the schedule and called it a "required connector", contradicting ADR
   0091 decision #6 ("Default connectors: `ibkr`, `trading212` — XTB
   excluded, driven by the EventBridge file-arrival trigger"). The
   required-gate claims were already stale: commit `fee7cde` removed `xtb`
   from `_REQUIRED_CDC_BROKERS` without updating the ADR, and `b84612a`
   fixed only code comments.

## Decision

XTB **fetch + transform** runs only on the EventBridge S3 file-arrival
trigger — a new file is re-parsed only when one is uploaded. XTB is removed
from `DEFAULT_CONNECTORS` (`pipeline/sfn.py`) and `schedule_connectors`
(`terraform/{prod,demo}/connectors.auto.tfvars`), so cron/CI runs no longer
launch a no-op XTB fetch+transform task.

**consolidate+analytics still runs over ALL silver tables — including
`xtb_snapshot`/`xtb_cdc` — on every cron/CI and file-arrival run.** XTB
silver is the cache of the latest uploaded file; `cmd_consolidate` iterates
the connector registry (`all_connectors()`), not the scheduled-connectors
list, so it reads XTB silver whenever present without re-parsing. Only a
file-arrival run refreshes XTB silver.

`xtb_cdc` is removed from `NON_EMPTY_REQUIRED`
(`pipeline/analytics/quality.py`), closing issue #132: XTB is fully optional
until a file arrives and is not in any required gate. This completes the
removal started by `fee7cde` (which dropped `xtb` from
`_REQUIRED_CDC_BROKERS`).

`fetch_failure_details` (`pipeline/sfn.py`) now derives the connector list
from the failed execution's own `input` (a JSON string with
`{"connectors":[{...}]}`) instead of `DEFAULT_CONNECTORS`, so XTB container
logs are still captured for failed *file-arrival* executions (where XTB
runs) even though XTB is no longer in `DEFAULT_CONNECTORS`. It falls back to
`DEFAULT_CONNECTORS` on unparseable input.

This restores consistency with ADR 0091 decision #6 and clarifies ADR 0107's
consequence note ("XTB is now a required scheduled connector that skips
gracefully when no file has arrived" — no longer true).

**Alternatives considered:**

- Keep XTB in the schedule and let it skip (status quo): launches a no-op ECS
  task every run and contradicts ADR 0091 #6.
- Keep `xtb_cdc` in `NON_EMPTY_REQUIRED` as an explicit "ingested at least
  once" check: a bare `pipeline validate` fails on missing/empty `xtb_cdc`
  before any file has arrived — a dormant landmine (issue #132).

## Constraints

- The file-arrival `input_template` must NOT pass `<...>` placeholders through
  `jsonencode()` — Go's `encoding/json` HTML-escapes `<`, `>`, `&`. Use a
  sentinel that survives `jsonencode` (e.g. `__XTB_FILE__`) and `replace()` it
  with the literal `<xtb_file>` token at render time.
- `fetch_failure_details` must not hardcode `DEFAULT_CONNECTORS` for log
  groups; derive from the failed execution's input so file-arrival failures
  still surface XTB logs.
- `file_arrival_connectors` keeps `["ibkr","trading212","xtb"]` — the
  file-arrival run still fetches + transforms XTB.
- The rest of ADR 0108 remains in force: the new-format parser, shared bronze
  (D17), `account_id` derivation (D18), and the upload-path change (D20)
  carry forward unchanged (originally decided in ADR 0108 §Decision).

## Consequences

- Cron/CI runs no longer launch a no-op XTB ECS task (prod schedule is
  monthly `cron(0 6 1 * ? *)`; demo is `scheduled=false`).
- XTB silver freshness is tied to file-arrival executions: if a file-arrival
  execution fails at IBKR/T212 before reaching XTB, XTB silver stays stale
  until the next successful file-arrival — the cron/CI run cannot compensate
  (it never fetches XTB). This gap predates this change.
- A bare `pipeline validate` no longer fails on missing/empty `xtb_cdc`
  before the first XTB file arrives.
- ADR 0108 D15/D19/D21 are superseded; the rest of 0108 stays in force.

## Validation

- `tests/test_sfn.py::TestFetchFailureDetails` — log groups derived from the
  execution input (incl. xtb for a file-arrival execution); new fallback test
  for unparseable input → `DEFAULT_CONNECTORS`.
- `tests/test_quality.py::test_non_empty_required_registry` — asserts
  `xtb_cdc` NOT in `NON_EMPTY_REQUIRED`.
- `tests/test_run_subcommands.py::_stub_sfn` — no xtb ARN resolved.
- Full suite: 794 tests pass; `ruff check --fix . && ruff format .` clean;
  `pyright pipeline/ tests/` 0 errors.
- `terraform validate` passes for the orchestrator module; `terraform console`
  confirms `replace(jsonencode([...__XTB_FILE__...]), "__XTB_FILE__",
  "<xtb_file>")` renders the literal `<xtb_file>` token.
- Post-deploy (user/ops): re-apply Terraform for demo/prod, re-upload an XTB
  file, confirm the SFN execution SUCCEEDED and `xtb_snapshot`/`xtb_cdc`
  silver refreshed.
