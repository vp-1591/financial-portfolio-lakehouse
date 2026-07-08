# Plan: Phase 2 PR 2 — Generic `run-connector` CLI subcommand

> Implementation plan for **PR 2 of 3** of `docs/roadmaps/phase-2-step-functions-orchestration.md`.
> Workflow stage: `implement` → `ADR` → `review`.

## Context

Phase 2 moves the portfolio pipeline to a Step Functions orchestrator. The orchestrator runs
each connector as its own Fargate task using a generic `run-connector <name>` subcommand (each
task runs fetch+transform for one connector in a single process, cutting cold starts). This PR
adds that subcommand plus `run-consolidate-allocate`, and extracts shared helpers from the
existing `cmd_fetch`/`cmd_transform` so they can be reused by both the orchestrator subcommand and
the existing commands.

It also fixes XTB S3 key decoding at the boundary: EventBridge object keys arrive percent-encoded,
and the shared `parse_s3_uri` does not unquote (it's shared with locally-typed keys that are
already decoded, so unquoting there risks double-decoding literal `%`).

**Depends on PR 1** (`docs/roadmaps/plans/phase-2-pr1-connector-protocol.md`) being merged — it
consumes the `fetch_kwargs`, `fetch_cdc_kwargs`, and `enabled_env_var` connector methods. **No
behavior change to existing commands** — `full`/`fetch`/`transform`/`consolidate`/`allocate` still
work identically; they now route through the extracted helpers.

## Reused, not rebuilt

- `pipeline/run.py` — `cmd_fetch` (107), `cmd_transform` (230), `cmd_consolidate` (288),
  `cmd_allocate` (366), `cmd_full` (414), `cmd_upload_xtb` (428).
- `pipeline/connectors/registry.py` — `get(name)`, `all()`.
- PR 1 methods: `connector.fetch_kwargs(args)`, `connector.fetch_cdc_kwargs()`,
  `connector.enabled_env_var`.
- `pipeline/connectors/xtb/fetch.py` `_read_file_bytes` — already accepts `s3://`.
- `pipeline/storage/s3.py` `parse_s3_uri` (s3.py:19) — does NOT unquote; stays untouched.
- `Dockerfile` — `ENTRYPOINT ["python","-m","pipeline.run"]`, already supports subcommand args.
- `tests/` patterns: `conftest.py` `tmp_data_dir`/`fernet_key`/`env_key`; in-process monkeypatch
  (see `test_pipeline_integration.py`).

## Step 1 — Extract `fetch_connector` and `transform_connector` helpers

Refactor `pipeline/run.py` (no behavior change to existing commands):

1. Extract `fetch_connector(connector, args: argparse.Namespace, fernet_key) -> int` from
   `cmd_fetch`'s loop body (114–226) — now calls `connector.fetch_kwargs(args)` (no `if/elif`),
   then `connector.fetch_snapshot(**kwargs)`, ingests raw, tries CDC. The XTB multi-file append
   loop moves inside `XtbConnector.fetch_kwargs`/`fetch_snapshot` (or the helper handles
   `args.xtb_file` list — pick the cleaner spot during impl). Preserve error-to-stderr behavior.
2. Extract `transform_connector(connector, fernet_key) -> int` from `cmd_transform`'s loop body
   (240–284).
3. `cmd_fetch`/`cmd_transform` iterate `all()` and call the helpers — unchanged behavior.

## Step 2 — Add `run-connector` subcommand

4. **One** new subcommand `run-connector` with positional `connector` (connector name) + the
   `--xtb-file` (append) and `common_parser` args. `cmd_run_connector(args)`:
   `connector = get(args.connector)`; if `not is_enabled(connector.enabled_env_var)`: log +
   return 0 (runtime gate, matches existing behavior — uses the connector's `enabled_env_var`
   attribute so `trading212` maps to `T212_ENABLED`, not `TRADING212_ENABLED`);
   `rc = fetch_connector(...)`; `return rc if rc else transform_connector(...)`. For XTB without
   `--xtb-file`: print error, return **1** (dedicated subcommand fails loudly, unlike
   `cmd_fetch`'s silent skip).

## Step 3 — Add `run-consolidate-allocate` subcommand

5. `cmd_run_consolidate_allocate(args)` — `cmd_consolidate(args)` then `cmd_allocate(args)` (both
   idempotent full-overwrite; reuse unchanged). Register subcommand `run-consolidate-allocate`
   with `common_parser` + `--fx-rate`/`--isin`/`--isin-map-file`.
6. Add both new subcommands to the `commands` dict. **No per-connector subcommands** — a 4th
   connector needs zero CLI changes. `full` stays for local dev.

## Step 4 — Update `cmd_upload_xtb` print message

7. Update `cmd_upload_xtb` print (`run.py:457`): replace the "future phase" wording with
   "EventBridge will trigger the orchestrator on this file's arrival."

## Step 5 — XTB S3 key percent-decoding (at the boundary, not shared helpers)

EventBridge object keys arrive percent-encoded; `parse_s3_uri` (s3.py:19) does NOT unquote and is
shared with `upload_to_staging`/`read_s3_bytes` (locally-typed keys are already decoded → naive
unquote there risks double-decoding literal `%` sequences). **Decode once in the XTB fetch path:**
in `pipeline/connectors/xtb/fetch.py` `_read_file_bytes`, for the `s3://` branch,
`urllib.parse.unquote` the key before reading. `parse_s3_uri`/`read_s3_bytes` stay untouched.
Document the caveat (XTB report filenames should not contain literal `%`). This is the
single-decode point.

## Tests (`tests/test_run_subcommands.py`, new + update connector tests)

In-process monkeypatch, reuse `tmp_data_dir`/`fernet_key`/`env_key`:

1. Argparse dispatch — `run-connector` + `run-consolidate-allocate` present in `commands` dict;
   `run-connector ibkr` resolves via `get("ibkr")`.
2. `fetch_connector`/`transform_connector` isolation — monkeypatch `get("ibkr")` methods; assert
   only `ibkr_*` raw/normalized written; `trading212_*` untouched. Verify `fetch_kwargs` is called
   on the connector (no if/elif in the helper).
3. `cmd_run_connector` for each connector — set `*_ENABLED=1` + mock secrets; fetch+transform that
   connector only. XTB without `--xtb-file` returns 1.
4. `cmd_run_consolidate_allocate` — seed normalized fixtures for two connectors; assert
   `consolidated_holdings` + `portfolio_allocation` exist.
5. `cmd_full` + existing `cmd_fetch`/`cmd_transform` regression — unchanged behavior (helpers
   iterate `all()`).
6. XTB s3 key decoding — unit test `_read_file_bytes` with a percent-encoded s3 key unquotes
   correctly (mock `read_s3_bytes`).

Run `ruff check --fix . && ruff format .`, then
`.venv/Scripts/python -m pytest tests/ -v`.

## Verification

1. `.venv/Scripts/python -m pytest tests/ -v` — existing + new tests pass.
2. `ruff check .` and `ruff format --check .` clean.
3. `docker build -t portfolio-pipeline:pr2 .` succeeds.
4. `docker run --rm portfolio-pipeline:pr2 run-connector --help` and
   `run-consolidate-allocate --help` print usage; `run-connector ibkr` (with mocked env) runs.
5. `python -m pipeline.run full` regression — identical output to pre-PR behavior.

## ADR

No new ADR required for this PR — it is part of the larger Phase 2 decision recorded in ADR 0051
(created in PR 3). If a reviewer asks, the rationale is "generic subcommand + extracted helpers so
adding a connector needs zero CLI changes."