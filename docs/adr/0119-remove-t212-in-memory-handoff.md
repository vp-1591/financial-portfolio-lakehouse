# 0119: Remove the T212 In-Memory Fetch Handoff

## Context

ADR 0116 built an in-memory encrypted fetch handoff so the trading212 transform
did not re-read the whole accumulated `raw/trading212` table every run — the
accumulated table grew by a full copy of the events history each run, and the
re-read was the measured memory driver behind the staging OOMs (1039 MB
transform peak). The handoff added a `handoff_supported` capability flag to the
`BrokerConnector` protocol, a pre-dedup encrypted-table return from
`ingest_raw`, and threading through `cmd_run_connector`.

Merge-on-key retention (ADR 0117) removed the driver the handoff was built for:
a re-fetched endpoint now replaces its row in place, so the accumulated table
no longer grows with run count and the transform's single bronze read (AD-6)
is bounded by the broker's current state. The handoff is now redundant
complexity — a protocol flag, a non-obvious return shape, and a second data
path that must stay in sync with the table write.

## Decision

The goal is a single, simple data path from fetch to transform. The handoff is
removed:

1. `handoff_supported` is deleted from the `BrokerConnector` protocol and all
   three connectors (trading212, ibkr, xtb).
2. `fetch_connector` returns `FetchResult` alone — no handoff dict, no
   fetch-times tuple.
3. `transform_connector` drops the `raw_tables` parameter; the single bronze
   table read (AD-6, cached per run) is the only path.
4. `ingest_raw` returns `None` (it keeps the `connector_name` parameter and the
   merge/VACUUM behavior of ADR 0117).

This is a **measured experiment (AD-8)**: the removal is kept only if the
transform's memory peak and runtime stay within budget of the ADR 0116
baseline (1039 MB transform peak). If either regresses materially, the handoff
is restored. The measurement requires a live T212 fetch and was **not
executed** in the implementation sandbox (no `T212_API_KEY`/network); the
measurement plan and decision rule are documented in the story's Dev Agent
Record, and no numbers were fabricated. The decision rule is: keep the removal
if peak memory and runtime are within budget; restore the handoff otherwise.

## Constraints

- Transforms still Fernet-decrypt; the single bronze read is the only input
  path — no per-broker branch in `run.py`.
- The events-fetch fail-loud behavior (any endpoint failure raises
  `RuntimeError` → `FetchResult.ERROR` → exit 1) is unchanged.
- ibkr and xtb keep reading the accumulated table — their transforms are
  designed around it (out-of-window events survive only there).

## Consequences

- **Positive:** the protocol is simpler; there is one data path, so the
  handoff and the table write cannot drift.
- **Positive:** the transform's input is bounded by the broker's current state
  (ADR 0117), so the memory driver ADR 0116 addressed stays gone.
- **Negative (behavior change):** trading212 normalized events now reflect the
  current history — an event deleted from T212's API (e.g. a cancelled order)
  disappears from the normalized events table instead of surviving in the
  accumulated history. This is arguably more correct and was already the
  handoff's behavior; it is now the only behavior.
- **Follow-up:** the AD-8 measurement must run against a live T212 fetch
  before this removal is considered final; if it regresses, restore the
  handoff.

## Validation

- `tests/test_transform_connector_handoff.py` (rewritten, 5 tests): golden
  regression — the table-read transform output equals the
  `t212_normalized_snapshot` fixture; empty raw table skips without rewriting;
  a real `ingest_raw` → transform boundary produces identical output; the
  events-fetch branch works; `ingest_raw` returns `None` and a re-fetch does
  not grow the table.
- `tests/test_connector_protocol.py`, `tests/test_connector_registry.py`,
  `tests/test_run_subcommands.py`, `tests/test_pipeline_integration.py`,
  `tests/test_trading212_connector.py` updated for the removed handoff.
- Full suite: `pytest tests/ -q -rf` → 922 passed; `ruff` clean; `pyright`
  0 errors.

Supersedes: ADR 0116. Carried forward unchanged: single bronze raw table per
broker (ADR 0114), phase-level RSS observability (ADR 0115), merge-on-key
retention and single bronze read (ADR 0117).
