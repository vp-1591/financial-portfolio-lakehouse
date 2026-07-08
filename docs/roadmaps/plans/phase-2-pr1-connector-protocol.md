# Plan: Phase 2 PR 1 — Connector self-description protocol

> Implementation plan for **PR 1 of 3** of `docs/roadmaps/phase-2-step-functions-orchestration.md`.
> Workflow stage: `implement` → `ADR` → `review`.

## Context

Phase 2 moves the portfolio pipeline to a Step Functions orchestrator. The orchestrator runs
each connector as its own Fargate task via a generic `run-connector <name>` subcommand, so
connectors must be **self-describing** — each connector must know how to build its own fetch
kwargs, which secrets it needs, and how to extract its holdings from a normalized table. Today
that knowledge is baked into `if connector.name == "ibkr"/elif "trading212"/elif "xtb"` branches
in `pipeline/run.py` and `pipeline/normalized/extract.py`.

This PR adds the self-description methods to the `BrokerConnector` protocol and implements them
in all three connectors, then refactors `extract_holdings` to delegate to connectors. **No behavior
change** — existing `full`/`fetch`/`transform`/`consolidate`/`allocate` commands produce
identical results. This is the modularity foundation; PR 2 adds the CLI subcommands that consume
it.

This PR depends on nothing and is the first of three. PR 2 (CLI) and PR 3 (Terraform) build on it.

## Reused, not rebuilt

- `pipeline/connectors/registry.py` — `get(name)`, `all()`, `register()` decorator.
- `pipeline/connectors/base.py` — `BrokerConnector` Protocol (`@runtime_checkable`): attributes
  `name`, `display_name`; methods `fetch_snapshot`, `fetch_cdc`, `transform_snapshot`,
  `transform_cdc`.
- `pipeline/connectors/ibkr/connector.py`, `trading212/connector.py`, `xtb/connector.py` — the
  three registered connectors.
- `pipeline/secrets.py` — `resolve_secret`, `get_env`, `is_demo`, `DEMO_SECRET_MAP`,
  `REQUIRED_SECRETS`.
- `pipeline/normalized/extract.py` — `extract_holdings(broker, table_path, fernet_key)` with the
  per-broker branch ladder at lines 86-134.
- `tests/` patterns: `conftest.py` `tmp_data_dir`/`fernet_key`/`env_key`; in-process monkeypatch
  (see `test_pipeline_integration.py`).

## Step 1 — Add self-description methods to the connector protocol

Add four members to `BrokerConnector` in `pipeline/connectors/base.py` and implement in each
connector (`ibkr`, `trading212`, `xtb`):

1. `fetch_kwargs(self, args: argparse.Namespace) -> dict` — builds the connector-specific
   snapshot kwargs currently hardcoded as the `if connector.name == "ibkr"/elif "trading212"/elif
   "xtb"` block in `cmd_fetch` (`pipeline/run.py:128-196`). `args` is the argparse `Namespace` from
   the CLI parser. IBKR resolves `IBKR_FLEX_TOKEN`/`IBKR_FLEX_QUERY_ID`/`IBKR_FLEX_BASE_URL`; T212
   resolves `T212_API_KEY`/`T212_API_SECRET` + `is_demo()` base URL; XTB reads `args.xtb_file`.
   Each connector imports `resolve_secret`/`get_env`/`is_demo` itself (no central if/elif). Also
   add `fetch_cdc_kwargs(self) -> dict` — T212 returns its snapshot kwargs; others `{}`.
2. `required_secrets(self) -> list[str]` — the base secret env-var names (e.g. IBKR returns
   `["IBKR_FLEX_TOKEN","IBKR_FLEX_QUERY_ID"]`). Used by a future validate step and to document SSM
   param names; cheap to add now.
3. `extract_holdings(self, df: pl.DataFrame, fernet_key: bytes) -> list[Holding]` — moves the
   per-broker branch ladder from `pipeline/normalized/extract.py:86-134` onto the connector. Each
   connector knows its display name, description column, and security_currency source.
4. `enabled_env_var` — a class/instance attribute on each connector declaring the `*_ENABLED`
   environment variable name:
   - `IbkrConnector.enabled_env_var = "IBKR_ENABLED"`
   - `Trading212Connector.enabled_env_var = "T212_ENABLED"`
   - `XtbConnector.enabled_env_var = "XTB_ENABLED"`

   Avoids deriving the env var name from the connector registry name — the mapping is not 1:1
   (`trading212` → `T212_ENABLED`, not `TRADING212_ENABLED`).

## Step 2 — Refactor `extract_holdings` to delegate to connectors

`pipeline/normalized/extract.py` `extract_holdings(broker, table_path, fernet_key)` becomes a thin
shim that reads the normalized table to a DataFrame and calls
`get(broker).extract_holdings(df, fernet_key)`. **Public signature preserved** so `cmd_consolidate`
and existing tests (`test_consolidate.py`, `test_consolidate_pipeline.py`) keep working unchanged.

Port the per-broker expectations (display name, description column, security_currency source) from
the existing `extract.py:86-134` branches into each connector's `extract_holdings`.

## Step 3 — Secrets registry note

`pipeline/secrets.py`: any new connector adds its secrets to `DEMO_SECRET_MAP` (and
`REQUIRED_SECRETS` if that list exists — verify during impl). No central registry edit needed for
the three existing connectors; this is forward guidance only.

## Tests (`tests/test_connector_protocol.py`, new + update connector tests)

In-process monkeypatch, reuse `tmp_data_dir`/`fernet_key`/`env_key`:

1. `connector.fetch_kwargs()` — unit tests per connector (new methods from Step 1); assert
   IBKR/T212/XTB each build correct kwargs given a mock argparse `Namespace`. For T212, verify
   `is_demo()` selects the demo base URL.
2. `connector.fetch_cdc_kwargs()` — T212 returns its snapshot kwargs; IBKR/XTB return `{}`.
3. `connector.required_secrets()` — assert each returns the expected env-var name list.
4. `connector.enabled_env_var` — assert each connector declares the right env var name.
5. `connector.extract_holdings()` — port the expectations from existing `extract_holdings`
   behavior; assert IBKR/T212/XTB each produce correct `Holding` lists from a normalized
   DataFrame fixture.
6. `extract_holdings(broker, table_path, fernet_key)` regression — the thin-shim path produces
   identical results to the pre-refactor behavior (`test_consolidate.py`,
   `test_consolidate_pipeline.py` unchanged and passing).

Run `ruff check --fix . && ruff format .`, then
`.venv/Scripts/python -m pytest tests/ -v`.

## Verification

1. `.venv/Scripts/python -m pytest tests/ -v` — existing + new tests pass.
2. `ruff check .` and `ruff format --check .` clean.
3. `docker build -t portfolio-pipeline:pr1 .` succeeds.
4. `python -m pipeline.run full` (with mocked env) produces identical output to pre-PR behavior
   (regression — the protocol refactor must be behavior-neutral).

## ADR

No new ADR required for this PR — it is a behavior-neutral refactor that is part of the larger
Phase 2 decision recorded in ADR 0051 (created in PR 3). If a reviewer asks, the rationale is
"modularity foundation for the registry-driven orchestrator."