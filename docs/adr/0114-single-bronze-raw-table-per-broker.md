# 0114: Single Bronze (Raw) Table Per Broker

## Context

The raw layer today holds six Delta tables: `raw/{broker}_snapshot` +
`raw/{broker}_events` for each of ibkr, trading212, and xtb — all carrying the
identical `RAW_SCHEMA` (`fetched_at, broker, source, payload, payload_hash,
source_file`, ADR 0047). XTB is a special case in every layer: the
`if connector.name == "xtb":` branch in `fetch_connector` (run.py), the
`events_raw_layer = "snapshot"` override on `BrokerConnector`, and
never-invoked `fetch_events`/`fetch_events_kwargs` stubs. XTB was already a
*de facto* shared bronze (ADR 0110 D17 carried forward from ADR 0108: one
`XTB_REPORT` raw row feeds both silver tables), but the other brokers kept the
per-layer split. `docs/table-lineage.md` even drew a false `raw/xtb_events`
edge to a table that was never written.

The raw `source` column already discriminates data kind — IBKR `flex` vs
`flex_events`, the five Trading 212 request paths, XTB `XTB_REPORT` — so the
per-broker × per-layer multiplication is redundant: every raw row routes to the
correct silver transform by `source` alone. Multiplicity and special-casing add
maintenance cost and drift risk (see the six alias schemas and six path
constants that had to stay in sync).

This is the implementation PR for issue #144 / PR #150.

## Decision

**One raw Delta table per broker.** Every broker lands every raw payload —
snapshot, positions, and events alike — in exactly one table `raw/{broker}`
(alias `{broker}_raw`), discriminated by `source`. XTB is a first-class broker
under the same contract as IBKR and Trading 212: no `name == "xtb"` branches in
`fetch_connector`/`transform_connector`, no `events_raw_layer` override, no
events stubs. The merge is shipped by migration
`pipeline/migrations/migrate_single_bronze.py`, which runs per environment
BEFORE the renamed-path code deploys (ADR-0113 A1 migration convention; deploy
gate).

Concretely:

- `pipeline/raw/models.py` keeps the single `RAW_SCHEMA`; the six `*_raw_schema`
  aliases and six `RAW_*_SNAPSHOT`/`RAW_*_EVENTS` path constants are deleted.
- `get_raw_path(connector_name)` returns `raw/{connector_name}`; both the fetch
  and transform loops read/write one table per broker.
- **Fetch contract:** `BrokerConnector.fetch_kwargs` returns one or more kwarg
  batches (one per `--xtb-file` for XTB, one per endpoint otherwise); the
  generic fetch path iterates them, appending each to `raw/{broker}`.
  `fetch_events`/`fetch_events_kwargs` are CONCRETE methods on IBKR/Trading 212
  only — the events fetch is gated with
  `getattr(connector, "fetch_events_kwargs", None)`; XTB simply has none.
- **Transform routing by exact source gates:** IBKR keeps `source != "flex"`
  / `!= "flex_events"`; Trading 212 gates are prefix-anchored
  (`source.startswith("/equity/...")`) per ADR 0102's declared request paths —
  pagination-tolerant, never free substring; XTB keeps exact `XTB_REPORT` gates
  and its per-`account_id` latest logic (ADR 0108 D18).
- **`filter_latest_snapshot` keeps per-`source` keying** (ADR 0100 #1 carried
  via 0108) — never a global max; XTB's per-`account_id` latest stays.
- **Migration:** `pipeline/migrations/migrate_single_bronze.py` merges
  `raw/{broker}_snapshot` + `raw/{broker}_events` into `raw/{broker}` per broker,
  dedup-keyed on `(broker, source, payload_hash)` keeping the latest
  `fetched_at` row and its `source_file` (so XTB's `account_id` derivation,
  ADR 0108 D18, survives), then removes every per-layer raw path — including the
  orphaned `raw/xtb_events` — via batched S3 delete, refusing to delete unless
  the merged table was written and verified. Idempotent, `--dry-run` support,
  destination-schema conflict raises rather than clobbers (ADR 0113 A1); the
  destination must also be empty or a row-subset of the sources (checked on the
  dedup key) and a partial per-key delete failure raises instead of reporting
  success — review-hardening of the same never-clobber decision.

Goal: the raw layer holds exactly one table per broker; no per-broker-name logic
in fetch/transform; every raw row reaches the correct silver table without
ambiguity; `{broker}_raw` surfaces to query consumers via alias auto-discovery.

**Alternatives considered and rejected:**

- **Keep the six-table status quo.** The per-layer split adds nothing once
  `source` discriminates kind; it forces six schema aliases, six path constants,
  and XTB special-cases to stay in sync, and it is what produced the false
  `xtb_events` raw edge.
- **A `kind`/`layer` column in one table.** Adds a redundant discriminator that
  the `source` column already provides, and changes `RAW_SCHEMA` (ADR 0047
  immutability).
- **Per-broker raw schemas.** Rejected — `RAW_SCHEMA` is one shared schema
  (ADR 0102) and nothing in the story needs per-broker raw fields.

## Constraints

- **Silver layer immutable:** two tables per broker (`normalized/{broker}_snapshot`,
  `normalized/{broker}_events`), same schemas and contents — only the raw read
  path changes.
- **`RAW_SCHEMA` fields and payload formats unchanged** (ADR 0047): raw stores
  original bytes unmodified; `account_id` stays a silver-layer concept.
- **Source vocabulary unchanged** (ADR 0102): `flex`/`flex_events`/the five
  Trading 212 paths/`XTB_REPORT` stay exactly; fetch sets `source`, transform
  never rewrites it.
- **Per-endpoint fetch isolation preserved:** the five Trading 212 endpoints
  and IBKR's two Flex queries stay separate fetches; per-endpoint
  `try/except` write-what-succeeded stays; T212's all-events-endpoints-empty
  `RuntimeError` survives unchanged.
- **`filter_latest_snapshot` per-source keying** and XTB per-`account_id` latest
  are preserved — never a global max.
- **`transform_connector` empty-raw WARN + skip** (ADR 0087 #5), `DEFAULT_CONNECTORS`,
  `sfn.py`, `storage.py`, `query.py` alias mechanism, terraform, workflows unchanged.
- **Migration artifacts as history:** `migrate_cdc_to_events.py` and its tests
  keep their old table names (historical inputs); do not rename them.
- **Grease bar (AC-5):** `grep -rniE "raw/(ibkr|trading212|xtb)_(snapshot|events)|xtb_events"`
  over `pipeline/ tests/ docs/ README.md` returns zero outside the carve-outs
  (`docs/adr/`, the new migration script + its test, `_bmad-output/`).
- **No new dependencies:** pinned stack unchanged (deltalake 1.6.0, polars
  1.42.0, pyarrow 24.0.0 — pyarrow for schemas + S3 fs only; `write_deltalake`
  accepts `pl.DataFrame` directly).

## Consequences

- **Positive:** raw multiplicity collapses from six to three tables; the XTB
  special cases (branch, override, stubs) disappear; routing is uniform and
  exact; `docs/table-lineage.md` and `docs/architecture.md` show only tables
  that exist.
- **Positive:** the merged `raw/{broker}` lets a snapshot and events transform
  run off one table, so IBKR's `flex`/`flex_events` rows and Trading 212's
  positions vs history rows provably cannot leak into the wrong silver
  transform (regression guards).
- **Negative:** environments must run the migration per environment, in a
  maintenance window with connectors idle, before the renamed-path code deploys
  — a skipped migration or a live fetch running old code can recreate per-layer
  paths after removal. Sequencing is documented in the migration docstring.
- **Negative:** the historical names persist in exempt artifacts — the old
  migration + its tests, prior ADRs — which are the intended inputs to the
  one-time migration.

## Validation

- `tests/test_migrate_single_bronze.py` — 21 tests: merges both sources into
  one table, dedup key, latest-`fetched_at` tie-break preserving `source_file`,
  source deletion, orphan `xtb_events` purge, idempotent no-op, absent source,
  `--dry-run` writes nothing, destination-schema conflict raises, verification-
  failure refuses delete, destination-row clobber refusal, destination row-
  subset re-run, partial-delete failure raises.
- `tests/test_single_bronze_routing.py` — 10 regression guards: Trading 212
  `/equity/positions` list payload never reaches the events silver; IBKR
  `flex`/`flex_events` in one merged `raw/ibkr` route to the right silver;
  per-`source` dedup keyedness in `filter_latest_snapshot`; both silver tables
  per broker unchanged in schema and contents from a merged raw table.
- `tests/test_xtb_connector.py::test_shared_bronze_no_xtb_events_raw` — the
  pre-existing XTB end-state test, kept.
- Full suite: 865 tests pass; `ruff check --fix . && ruff format .` clean;
  `pyright pipeline/ tests/` 0 errors; grep bar zero outside carve-outs.
- Manual per environment: `python -m pipeline.migrations.migrate_single_bronze
  --mode <docker|staging|prod> --dry-run`, then for real, then verify
  `raw/{broker}` counts == sum of the two source tables via
  `pipeline.run query "SELECT broker, source, COUNT(*) FROM {broker}_raw GROUP
  BY broker, source" --decrypt --mode staging`, then deploy the renamed-path
  code (see the migration docstring for the full AD-7 ordering).
