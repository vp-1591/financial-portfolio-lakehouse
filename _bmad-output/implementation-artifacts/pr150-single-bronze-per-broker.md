# Story pr150: Single bronze (raw) table per broker

Status: review

<!-- Note: Validation is optional. Run validate-create-story for quality check before dev-story. -->

## Story

As a data engineer,
I want each broker to land every raw payload — snapshot, positions, and events alike — in exactly one Delta table `raw/{broker}` discriminated by `source`, with XTB treated as a first-class broker,
so that the raw layer loses its per-broker × per-layer multiplicity (`{broker}_snapshot` + `{broker}_events`, six identical-schema tables), the `name == "xtb"` branch and the `events_raw_layer` override are deleted, and every raw row routes to the correct silver table (`normalized/{broker}_snapshot` / `normalized/{broker}_events`) without ambiguity.

**This is the implementation PR for issue #144 / PR #150.** Deliver as ONE PR (explicit user direction; the issue's suggested 5-PR sequence is superseded).

## Acceptance Criteria

### AC-1 — CAP-1: One raw Delta table per broker; `source` discriminates data kind (AD-1, AD-5)

**Given** the current per-broker raw tables `raw/{broker}_snapshot` and `raw/{broker}_events` (XTB: `raw/xtb_snapshot` only),
**When** the code, the merge migration, and the storage layout are changed to one table `raw/{broker}` per broker (query alias `{broker}_raw`),
**Then** after the migration, `raw/ibkr`, `raw/trading212`, `raw/xtb` each hold rows from both fetch kinds, `SELECT DISTINCT source` over each table returns the expected per-broker values, and no `{broker}_snapshot` or `{broker}_events` raw path remains (enforced by the grep bar in AC-5).

### AC-2 — CAP-2: XTB is no longer a special case in fetch/transform (AD-6)

**Given** the current `if connector.name == "xtb":` branch in `fetch_connector` (run.py:130), the `events_raw_layer = "snapshot"` override (base.py:23, xtb/connector.py:26), and XTB's never-invoked `fetch_events` / `fetch_events_kwargs` stubs (xtb/connector.py:78-82),
**When** `fetch_connector` and `transform_connector` carry no per-broker-name logic and `BrokerConnector` has no raw-layer override and no events stubs,
**Then** XTB's multi-file `--xtb-file` support is preserved through the uniform contract (AD-6: `fetch_kwargs` returns one or more kwarg batches and the generic fetch path iterates them, appending each batch to `raw/xtb`), every connector writes `raw/{name}`, its transforms route by `source`, and the full test suite passes.

### AC-3 — CAP-3: Rows route to the correct silver transform after the merge (AD-2, AD-3, AD-4)

**Given** a merged `raw/{broker}` holding snapshot and events rows,
**When** `transform_snapshot` and `transform_events` run,
**Then** both transforms include-filter on `source` BEFORE any payload unwrap/parse (never substring luck; IBKR exact `flex`/`flex_events`, Trading 212 prefix-anchored on the declared request paths, XTB exact `XTB_REPORT`), Trading 212 `/equity/positions` list payloads provably cannot reach the events silver (regression guard), `filter_latest_snapshot` stays keyed per distinct `source` value (with a regression guard), XTB keeps its per-`account_id` latest logic (ADR 0108 D18, AD-4), and both silver tables per broker are unchanged in schema and contents.

### AC-4 — The merge migration is the deploy gate (AD-7, ADR 0113 A1 convention)

**Given** the live per-layer raw tables and the migration convention in `pipeline/migrations/migrate_cdc_to_events.py` + `_storage_options.py`,
**When** `pipeline/migrations/migrate_single_bronze.py` merges `raw/{broker}_snapshot` + `raw/{broker}_events` into `raw/{broker}` and then removes every per-layer raw path (including the orphaned `raw/xtb_events` — never written, `docs/table-lineage.md` draws a false edge to it),
**Then** the migration is idempotent (exit 0 on absent or already-migrated tables, raise on auth/region/permission errors or unexpected schema), dedup-keyed on `(broker, source, payload_hash)` tie-breaking to the latest `fetched_at` and preserving `source_file` so XTB's `account_id` derivation (ADR 0108 D18) survives, supports `--dry-run`, runs via `python -m pipeline.migrations.migrate_single_bronze --mode <docker|staging|prod> [--dry-run]` (never hand-constructed `DeltaTable()`), a destination conflict raises rather than clobbers, and post-migration `pipeline.run query --decrypt --mode staging` row counts per broker match the sum of the pre-migration tables.

### AC-5 — Clean tree, green suite, zero stale raw names (spec Success signal + NFR10)

**Given** all code, migration, test, fixture, and doc changes are complete,
**When** `grep -rniE "raw/(ibkr|trading212|xtb)_(snapshot|events)|xtb_events" pipeline/ tests/ docs/ README.md` runs and the full check suite runs,
**Then** the grep returns zero outside the carve-outs (`docs/adr/`, the migration script + its test, and `_bmad-output/`), `ruff check --fix . && ruff format .`, then `pyright pipeline/ tests/`, then `pytest tests/ -q -rf` all pass (tests re-run after linting to catch auto-fix regressions), and the success signal holds: exactly one raw Delta table per broker (`raw/{broker}`, alias `{broker}_raw`), XTB special-casing gone from the code, Trading 212 positions provably unable to reach the events silver, both silver tables per broker keeping pre-merge contents.

### AC-6 — Docs tell the truth and the decision is recorded (conventions + adr-workflow)

**Given** `docs/table-lineage.md` (draws the false `xtb_events` raw edge), `docs/architecture.md` (lists 6 raw tables + `{broker}_snapshot_raw`-style aliases), and the broker docs that name raw paths,
**When** the docs are updated to show `raw/{broker}` (alias `{broker}_raw`) feeding both silver transforms,
**Then** no false raw edges remain, the docs show only tables that exist, and a new ADR recording the single-bronze decision is created via `manage-adr` after implementation (per the adr-workflow: plan for it now, invoke the skill after the code lands).

## Tasks / Subtasks

- [x] T1: Merge migration `pipeline/migrations/migrate_single_bronze.py` (AC-4, AD-7)
  - [x] T1.1 Read `pipeline/migrations/migrate_cdc_to_events.py` + `_storage_options.py` and copy the convention: argparse `--mode docker|staging|prod`, `--dry-run`, `pipeline.secrets.load_env()` + `set_mode`, `get_storage_options_with_credentials()`, exit 0 on absent/already-migrated, `RuntimeError`/`ClientError` → `SystemExit(1)`.
  - [x] T1.2 Merge each broker: read `raw/{broker}_snapshot` + `raw/{broker}_events` (deltalake `DeltaTable` with storage options), concat (polars), dedup on `(broker, source, payload_hash)` keeping latest `fetched_at` and preserving `source_file`, then `write_deltalake(raw/{broker}, merged, mode="overwrite", schema_mode="overwrite")` — `pl.DataFrame` accepted directly, do NOT convert to `pa.Table`.
  - [x] T1.3 Idempotent recovery: the merge is a deterministic overwrite of `raw/{broker}` — re-running against the still-present sources with the same dedup key reproduces the same destination, so an interrupted run re-succeeds (no partial state). If the destination already exists with a schema that does NOT equal `RAW_SCHEMA` (order-sensitive), raise rather than clobber (ADR 0113 A1 conflict convention).
  - [x] T1.4 Remove every per-layer raw path after the merged table is verified (`raw/{broker}_snapshot`, `raw/{broker}_events`, and the orphaned `raw/xtb_events`) via batched S3 delete (mirror `_delete_objects`, `_DELETE_CHUNK=1000`), refusing to delete if the merged `raw/{broker}` was not written/verified. `--dry-run` prints the plan and writes nothing.
  - [x] T1.5 Migration script name is exactly `migrate_single_bronze.py` and runs via `python -m pipeline.migrations.migrate_single_bronze`.
  - [x] T1.6 Tests: `tests/test_migrate_single_bronze.py` modeled on `tests/test_migrate_cdc_to_events.py` (FakeS3 / FakeBackend / `_fake_config` pattern): merges both sources into one table, dedup key, latest-`fetched_at` tie-break preserving `source_file`, source deletion, orphan `xtb_events` purge, idempotent no-op, absent source, dry-run does not write, conflict raises, verification-failure refuses delete.

- [x] T2: Raw-layer collapse (AC-1, AD-1)
  - [x] T2.1 `pipeline/raw/models.py`: keep the single `RAW_SCHEMA` (fields `fetched_at, broker, source, payload, payload_hash, source_file` — unchanged, ADR 0047), delete the six `*_raw_schema` aliases (`ibkr_snapshot_raw_schema`, `ibkr_events_raw_schema`, `trading212_snapshot_raw_schema`, `trading212_events_raw_schema`, `xtb_snapshot_raw_schema`, `xtb_events_raw_schema`) and their re-exports in `pipeline/raw/__init__.py`.
  - [x] T2.2 `pipeline/run.py` `get_raw_path` (run.py:381): collapse to `raw/{connector_name}` — `config.raw_path(connector_name)`. Update all four call sites (run.py:137, 163, 185, 217) to the new signature; delete the now-unused `layer` param and the events/snapshot layer distinction.
  - [x] T2.3 `pipeline/paths.py`: remove the six `RAW_*_SNAPSHOT`/`RAW_*_EVENTS` constants (mapping lines 27-32); update `tests/test_storage_config.py:570` which asserts `RAW_IBKR_SNAPSHOT` (either drop the assertion or point it at the collapsed path).
  - [x] T2.4 `pipeline/raw/ingest.py`: dedup key stays `(broker, source, payload_hash)` (already correct — ingest.py:46-52); no change needed unless the merge exposes a gap.

- [x] T3: Fetch path unifies — every connector writes `raw/{name}` (AC-2, AD-5, AD-6)
  - [x] T3.1 `pipeline/run.py` `fetch_connector` (lines 114-198): delete the `if connector.name == "xtb":` branch (lines 130-149). The generic path becomes the ONLY path: `fetch_kwargs(args)` returns one or more kwarg batches, each batch fetched and appended to `raw/{name}`; the events fetch (when the connector has one) appends its rows to the SAME `raw/{name}`.
  - [x] T3.2 XTB multi-file: `XtbConnector.fetch_kwargs` (xtb/connector.py:28-36) currently returns kwargs for only the FIRST `--xtb-file`. Under the uniform contract it returns one batch per file (list-iterate in the generic path), preserving `--xtb-file` append semantics — every uploaded file's single `XTB_REPORT` row lands in `raw/xtb`.
  - [x] T3.3 `BrokerConnector` (base.py): remove the `events_raw_layer` attribute (line 23) and its docstring (D17 override). Drop `fetch_events`/`fetch_events_kwargs` from the Protocol (AD-6 "no events stubs") and delete XTB's never-invoked stubs (xtb/connector.py:78-82). Gate the events fetch in `fetch_connector` with `getattr(connector, "fetch_events_kwargs", None)` — IBKR/T212 still fetch events (IBKR `flex_events`; T212 the three history endpoints), XTB simply has no events fetch (shared bronze feeds both silvers).
  - [x] T3.4 Per-endpoint write-what-succeeded `try/except` isolation stays; Trading 212 `fetch.py:120-124` all-events-endpoints-empty `RuntimeError` survives unchanged (write it to `raw/trading212` now).
  - [x] T3.5 IBKR `fetch_snapshot`/`fetch_events` keep returning `pa.Table` rows tagged `flex` / `flex_events`; T212 keeps its five endpoints with full captured request-path `source` values; XTB keeps `XTB_REPORT` with `source_file` — the source vocabulary (AD-2) is defined once per broker and does NOT change.
  - [x] T3.6 `pipeline/sfn.py` `DEFAULT_CONNECTORS` and `build_connector_command` unchanged (no raw names there — verify with grep).

- [x] T4: Transform routing — single raw read + exact source gates (AC-3, AD-2, AD-3, AD-4)
  - [x] T4.1 `pipeline/run.py` `transform_connector` (lines 201-265): delete the `raw_layer = layer if layer == "snapshot" else connector.events_raw_layer` override (line 216) — both loops read `get_raw_path(connector.name)` = `raw/{name}`. Normalized output paths (`config.normalized_path(f"{connector.name}_{layer}")`, line 245) unchanged — silver stays two tables per broker.
  - [x] T4.2 IBKR `transform.py`: keep the exact gates `source != "flex"` (line 84) and `source != "flex_events"` (line 267) — they now operate on the merged table; no change beyond the raw path. Add/keep a regression test that an `flex_events` row cannot leak into `transform_snapshot` and vice-versa when both live in the same raw table.
  - [x] T4.3 Trading 212 `transform.py`: tighten snapshot gates from substring (`"/account/summary" in source` line 50, `"/positions" in source` line 52) and events gates (lines 269-273) to prefix-anchored matching on the declared request paths (`source.startswith("/equity/account/summary")`, `/equity/positions`, `/equity/history/orders`, `/equity/history/dividends`, `/equity/history/transactions`) per AD-2 (pagination-tolerant; never free substring). The `/equity/metadata/instruments` fixture row (legacy) stays skipped.
  - [x] T4.4 XTB `transform.py`: `_latest_per_account` and both transforms keep exact `source == "XTB_REPORT"` gates and per-`account_id` latest logic (D18) — unchanged; they just read `raw/xtb` now.
  - [x] T4.5 `pipeline/connectors/transform_utils.py`: `filter_latest_snapshot` keeps per-`source` keying (line 93, `max().over("source")` — do NOT convert to a global max). Add the AD-4 regression guard: a test asserting dedup is per distinct `source` (two different `source` rows with different `fetched_at` both survive).

- [x] T5: Query aliases, fixtures, docs, and the grep bar (AC-1, AC-5, AC-6)
  - [x] T5.1 `pipeline/query.py`: NO code change needed — aliases auto-discover `raw/{broker}` → `{broker}_raw` (parse_alias strips the `_raw` suffix). Update docstring examples (`ibkr_snapshot_raw` → `ibkr_raw` where they illustrate production naming) and verify `tests/test_query_list_tables.py` still passes (it tests the generic mechanism; `parse_alias("ibkr_snapshot_raw")` remains valid).
  - [x] T5.2 `tests/conftest.py` `tmp_data_dir` (lines 100-105): replace the six raw dirs with `raw/ibkr`, `raw/trading212`, `raw/xtb`. Update `tests/test_consolidate_pipeline.py:35-40` raw-dir writes likewise. Keep normalized dirs unchanged.
  - [x] T5.3 `tests/fixtures/`: fixtures are already RAW_SCHEMA-shaped per broker (`t212_raw_snapshot`, `xtb_raw_snapshot`, `ibkr_raw_snapshot` + `ibkr_raw_events`) — repurpose into the merged-table shape where a test needs both kinds in one table (e.g. IBKR: concat `ibkr_raw_snapshot` + `ibkr_raw_events` rows into one fixture). Update fixture docs/doctrings that reference `{broker}_snapshot` raw paths.
  - [x] T5.4 `tests/test_xtb_connector.py::test_shared_bronze_no_xtb_events_raw` (line 914) already encodes the end state (no `xtb_events` raw); keep it. Add the mirror assertions for ibkr/trading212 (no `*_events` raw table needed; both kinds in `raw/{broker}`).
  - [x] T5.5 Docs: `docs/table-lineage.md` — replace the six raw nodes (lines 19-24) with three (`raw/ibkr`, `raw/trading212`, `raw/xtb`), each feeding `transform_snapshot` → normalized snapshot AND `transform_events` → normalized events; delete the `xtb_events` raw node and its false edge (line 51). `docs/architecture.md` — raw layer table list (lines 27-41) becomes `raw/{broker}` "Encrypted API payloads, one table per broker, `source` discriminates snapshot/events"; alias table (lines 44-67) becomes `ibkr_raw`, `trading212_raw`, `xtb_raw`. Sweep `docs/brokers/{ibkr,trading212,xtb}.md`, `docs/configuration.md` for raw table names and update.
  - [x] T5.6 `tests/test_connector_registry.py:15` (`events_raw_layer = "events"`) — delete the attribute from the fake connector; `tests/test_connector_protocol.py` — update any `fetch_events_kwargs`/`fetch_events` protocol assertions (line 176-190) to the post-merge protocol.

- [x] T6: CAP-3 regression guards (AC-3)
  - [x] T6.1 Regression: Trading 212 `/equity/positions` list payload never reaches the events silver — a merged `raw/trading212` holding a positions row produces NO rows in `transform_events` output.
  - [x] T6.2 Regression: IBKR `flex` and `flex_events` in the same merged `raw/ibkr` table route to the right silver (snapshot transform emits no event, events transform emits no snapshot).
  - [x] T6.3 Regression: per-`source` dedup keyedness (T4.5).
  - [x] T6.4 Regression: both silver tables per broker unchanged in schema and contents after the merge (run the existing snapshot/events transform tests against a merged raw table).

- [ ] T7: Full checks, deploy sequencing, ADR, one PR (AC-5, AC-6, NFR10)
  - [ ] T7.1 `ruff check --fix . && ruff format .`; then `pyright pipeline/ tests/`; then `pytest tests/ -q -rf`; tests re-run after lint.
  - [ ] T7.2 Grep bar: `grep -rniE "raw/(ibkr|trading212|xtb)_(snapshot|events)|xtb_events"` over `pipeline/ tests/ docs/ README.md` returns zero outside carve-outs (`docs/adr/`, the migration script + its test, `_bmad-output/`).
  - [ ] T7.3 Document the deploy sequence (AD-7): migration runs manually per environment (`--mode docker|staging|prod [--dry-run]` then for real) BEFORE the renamed-path transform code deploys there, so no live fetch re-creates a per-layer path after removal. Include the pre/post count-verification queries (`pipeline.run query --decrypt --mode staging`).
  - [ ] T7.4 New ADR via `manage-adr` after implementation records the single-bronze-per-broker convention (do NOT hand-write the ADR; record as the final step after the PR merges).

## Dev Notes

### Current state (verified 2026-08-20)

- **Two raw tables per broker today** — `raw/{broker}_snapshot` + `raw/{broker}_events`, one identical `RAW_SCHEMA` (`pipeline/raw/models.py:12-21`: `fetched_at, broker, source, payload, payload_hash, source_file`), plus six `*_raw_schema` aliases (lines 24-29) re-exported by `pipeline/raw/__init__.py`.
- **`get_raw_path`** lives in `pipeline/run.py:381-388` (NOT paths.py): `config.raw_path(f"{connector_name}_{layer}")`. Callers: run.py:137, 163, 185, 217. The deprecated `pipeline/paths.py` module (`__getattr__` shim) still exposes `RAW_*_SNAPSHOT`/`RAW_*_EVENTS` constants (lines 27-32), referenced by `tests/test_storage_config.py:570`.
- **`fetch_connector`** (run.py:114-198): XTB-only branch at line 130 (`if connector.name == "xtb":`) iterates `--xtb-file` and appends each to `raw/xtb_snapshot` via `ingest_raw`; generic path (lines 151-198) writes snapshot → `raw/{name}_snapshot` and events → `raw/{name}_events`.
- **`transform_connector`** (run.py:201-265): loops `("snapshot", "events")`; `raw_layer = layer if layer == "snapshot" else connector.events_raw_layer` (line 216); reads `DeltaTable(get_raw_path(name, raw_layer))`, WARN+skip on missing/empty (ADR 0087 #5); normalized write is `config.normalized_path(f"{name}_{layer}")` (line 245) — silver paths do NOT change.
- **`BrokerConnector`** (base.py:14, Protocol): members include `events_raw_layer: str` (line 23, D17 docstring) and `fetch_events_kwargs`/`fetch_events` (structural). IBKR/T212 set `events_raw_layer = "events"`; XTB = `"snapshot"` (xtb/connector.py:26). XTB also carries never-invoked `fetch_events`/`fetch_events_kwargs` stubs (lines 78-82).
- **Source vocabulary (AD-2, the routing contract)** — unchanged by this story:

  | Broker | Snapshot `source` | Events `source` | Transform gate today |
  | --- | --- | --- | --- |
  | ibkr | `flex` | `flex_events` | exact `!=` (transform.py:84, 267) |
  | trading212 | `/equity/account/summary`, `/equity/positions` (one row per endpoint/page, `nextPagePath` suffixes possible) | `/equity/history/orders`, `/dividends`, `/transactions` | substring `in` (transform.py:50, 52, 269-273) |
  | xtb | `XTB_REPORT` (one row per uploaded file; `source_file` = filename) | same raw/same source (shared bronze) | exact `!= "XTB_REPORT"` (transform.py:116, 354) |

  Broker column values: `"IBKR"`, `"Trading 212"`, `"XTB"` (display names, not `name` attr).
- **Raw write dedup** (`pipeline/raw/ingest.py:23-64`): `dedup_raw` drops rows whose `(broker, source, payload_hash)` already exist; `ingest_raw` (lines 67-104) appends (`mode="append"`), creating a schema-only Delta table when all rows dedupe. `payload_hash = sha256(payload).hexdigest()` computed at fetch time.
- **`filter_latest_snapshot`** (`transform_utils.py:55-97`): dedups `fetched_at` latest per `source` (`max().over("source")`), never global. `dedup_events` (lines 390-435): `unique(subset, keep="first")` on `fetched_at`-descending sort (ADR 0105) — do not touch.
- **Query aliases** (`pipeline/query.py`): fully auto-discovered — `list_tables()` scans `{layer}/` dirs, `parse_alias` strips `_analytics|_normalized|_raw`; a `raw/ibkr` dir surfaces as `ibkr_raw` with zero code change. No hardcoded alias registry.
- **Migration pattern to copy** — `pipeline/migrations/migrate_cdc_to_events.py` + `_storage_options.py`: argparse `--mode docker|staging|prod` + `--dry-run`; `pipeline.secrets.load_env()` then `set_mode`; S3 client via `_build_s3_client()`; S3 copy-verify-then-delete rename of Delta dirs (size-conflict raises, post-copy verification before delete, `_DELETE_CHUNK=1000`); idempotent absent/already-migrated → exit 0. `get_storage_options_with_credentials()` in `_storage_options.py` injects real AWS creds into deltalake storage options.
- **Migration test pattern** — `tests/test_migrate_cdc_to_events.py`: `FakeS3` client double (in-memory objects) + `_FakeBackend`/`_fake_config` with `use_storage(...)`, `storage_options` returns fake creds so the boto3 chain is skipped. Reuse this for the new migration tests.
- **Docs today**: `docs/table-lineage.md` draws SIX raw nodes including the false `xtb_events` → normalized edge (lines 19-24, 44-51); `docs/architecture.md` lists `raw/{broker}_snapshot` + `raw/{broker}_events` (lines 27-41) and a raw alias table with `*_raw` aliases (lines 44-67).
- **Tests/conftest today**: `tests/conftest.py:100-105` creates the six-raw-dir tree; `tests/test_consolidate_pipeline.py:35-40` writes raw dirs; `tests/test_xtb_connector.py::test_shared_bronze_no_xtb_events_raw` (line 914) already asserts the target end-state for XTB.

### What the developer MUST NOT change (preserve exactly)

- **Silver (normalized) layer**: two tables per broker, same schemas/contents — only the raw read path changes. `events_normalized_schema`, `snapshot_normalized_schema` untouched.
- **`RAW_SCHEMA` fields and payload formats** (ADR 0047: raw stores original bytes unmodified; parsing lives in the transform; `account_id` is a silver-layer concept, never added to `RAW_SCHEMA`).
- **The source vocabulary** (AD-2): fetch sets `source`, transform never rewrites it; `flex`/`flex_events`/the five T212 paths/`XTB_REPORT` stay exactly.
- **Per-endpoint fetch isolation** (AD-5): IBKR's two Flex queries and Trading 212's five endpoints stay separate fetches; per-endpoint `try/except` write-what-succeeded stays; T212's all-events-endpoints-empty `RuntimeError` (fetch.py:120-124) survives unchanged.
- **`filter_latest_snapshot` per-source keying** (ADR 0100 #1 carried via 0108, AD-4) and XTB's per-`account_id` latest (D18) — add regression guards, never convert to global max.
- **`transform_connector` empty-raw WARN + skip** (ADR 0087 #5), `DEFAULT_CONNECTORS`, `sfn.py`, `storage.py`, `query.py` mechanism, terraform, workflows.
- **Migration artifacts as history**: the rename migration (`migrate_cdc_to_events.py`) and its tests keep their old table names (they're historical inputs); do NOT rename them to the merged scheme.

### Deploy sequencing (AD-7, migration-first)

1. Write + test the migration, deploy the CODE that only merges writes to `raw/{broker}` LAST in each environment. The ordering per environment: (a) run `migrate_single_bronze --mode <env> --dry-run`, (b) verify, then run for real, (c) confirm `raw/{broker}` counts == sum of the two source tables, (d) then deploy the renamed-path code (which now reads/writes `raw/{broker}`). If the migration runs AFTER the code, `transform_connector` will read only the merged table and quality gates will flag missing per-layer tables; if the old fetch still runs it can recreate a per-layer path after the migration removed it (connectors are idle across the window — run the migration in a maintenance window).
2. Verify with `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m pipeline.run query "SELECT broker, source, COUNT(*) FROM {broker}_raw GROUP BY broker, source" --decrypt --mode staging` — per-broker distinct sources match the table in Dev Notes.

### Check & workflow commands (run from the repo root)

```bash
# project venv (never system Python)
.venv/Scripts/python -m ruff check --fix . && .venv/Scripts/python -m ruff format .
.venv/Scripts/python -m pyright pipeline/ tests/
.venv/Scripts/python -m pytest tests/ -q -rf
# grep bar
grep -rniE "raw/(ibkr|trading212|xtb)_(snapshot|events)|xtb_events" pipeline/ tests/ docs/ README.md
```

### Testing standards

- pytest + fixtures in `tests/fixtures/{ibkr,trading212,xtb}.py` (all `pa.Table` shaped to `RAW_SCHEMA`).
- Migration tests use the FakeS3/FakeBackend pattern (`tests/test_migrate_cdc_to_events.py`) — do NOT mock deltalake/polars; write real local Delta tables in `tmp_path`.
- New behavior (merge routing, per-source dedup guard, positions-never-events) gets focused tests; existing tests updated only where they name raw paths or the protocol (`events_raw_layer`, stubs).
- Run all three checks before the PR; re-run tests after linting.

## Project Structure Notes

- The structural target (from `ARCHITECTURE-SPINE.md` "Structural Seed"):

```text
pipeline/
  connectors/
    {broker}/            # per-broker: connector.py · fetch.py · transform.py
    base.py              # BrokerConnector protocol — uniform, no raw-layer override (AD-6)
    registry.py          # all()/get() — single source of truth for connector sets
    transform_utils.py   # filter_latest_snapshot — per-source keying (AD-4)
  raw/
    models.py            # RAW_SCHEMA — one shared schema, unchanged (AD-1)
    ingest.py            # dedup_raw on (broker, source, payload_hash)
  run.py                 # get_raw_path → raw/{name}; fetch/transform_connector, no name branches (AD-6)
  migrations/            # migrate_single_bronze.py — idempotent merge + xtb_events purge (AD-7)
  query.py               # alias auto-discovery: raw/{broker} → {broker}_raw
```

- Naming: raw tables `raw/{broker}` → aliases `{broker}_raw` (auto-discovered). Silver unchanged (`normalized/{broker}_snapshot`, `normalized/{broker}_events`). No `{broker}_snapshot`/`{broker}_events` raw paths in code/docs/tests (carve-out: migration artifacts + `docs/adr/`).
- No new dependencies; reuse the pinned stack (deltalake 1.6.0, polars 1.42.0, pyarrow 24.0.0 — pyarrow for schemas + S3 fs only; `write_deltalake` accepts `pl.DataFrame` directly).

### Previous work / git intelligence

- The closest analogue is PR #145 (CDC→events + demo→staging rename, `worktree-spec-rename-cdc-events-demo-staging`): it shipped the same-shaped migration (`migrate_cdc_to_events.py`), the same FakeS3 test pattern, and the grep-zero bar discipline. Copy its migration + test structure and its "docs/adr never rewritten" carve-out.
- Recent merges: #149 (remove empty variables.tf), #148 (remove hardcoded connector requirements → registry is the single source of truth), #147 (remove S3_PREFIX storage-prefix entirely), #146 (task-role S3 policy for empty staging prefix), #145 (the renames). The project has converged on "remove multiplicity / registry-derived / single source of truth" — this story is the next step of that pattern.
- XTB already proves the end state (ADR 0110 D17 shared bronze): `tests/test_xtb_connector.py::test_shared_bronze_no_xtb_events_raw` (line 914) asserts events come from a single `XTB_REPORT` raw row with no `xtb_events` table. Generalize this shape to all brokers.

### References

- `_bmad-output/specs/spec-single-bronze-per-broker/SPEC.md` — capabilities CAP-1/2/3, constraints, success signal
- `_bmad-output/specs/spec-single-bronze-per-broker/ARCHITECTURE-SPINE.md` — AD-1..AD-7, invariants, capability→architecture map, structural seed (this is the authoritative build contract)
- `_bmad-output/specs/spec-single-bronze-per-broker/architecture-diagrams.md` — before/after raw shape
- `docs/adr/0110-xtb-file-arrival-only-ingestion.md` (D17 shared bronze carried forward), `0113-rename-cdc-events-and-demo-staging.md` (events naming final; A1 migration convention), `0102` (one shared snapshot schema), `0108` (D18 per-account latest), `0100` (per-source dedup), `0047` (raw stores bytes), `0105` (events dedup latest-wins)
- Code: `pipeline/run.py`, `pipeline/connectors/base.py`, `pipeline/raw/{models,ingest}.py`, `pipeline/connectors/transform_utils.py`, `pipeline/paths.py`, `pipeline/migrations/migrate_cdc_to_events.py`, `tests/conftest.py`, `tests/test_migrate_cdc_to_events.py`, `docs/{table-lineage,architecture}.md`

## Dev Agent Record

### Agent Model Used

Orchestrated via Claude Code with staged fresh-context subagents (T1, T2–T4,
T5–T6) + orchestrator T7. Environment: Windows, project venv (main repo
`.venv\Scripts\python.exe`, Python 3.11; worktree has no local venv).

### Debug Log References

- T1 report: `$CLAUDE_JOB_DIR/tmp/t1-report.md`
- T2–T4 report: `$CLAUDE_JOB_DIR/tmp/t2-report.md`
- T5–T6 report: `$CLAUDE_JOB_DIR/tmp/t3-report.md`

### Completion Notes List

- T1 merge migration (`migrate_single_bronze.py`, 18 tests) delivered first
  (file-disjoint); then T2–T4 code collapse (raw models/paths/run/connectors);
  then T5–T6 fixtures/conftest/docs + 10 CAP-3 regression guards
  (`tests/test_single_bronze_routing.py`).
- Full suite 865 tests pass; `ruff check --fix . && ruff format .` clean
  (tests re-run after lint); `pyright pipeline/ tests/` 0 errors.
- Grep bar zero outside carve-outs: only the migration script + its test, the
  historical `migrate_cdc_to_events.py` + its test, `docs/adr/`, and
  legitimate silver `xtb_events` (`normalized/xtb_events`) references.
- Deploy sequencing (AD-7 migration-first) documented in the migration script
  docstring: per environment `--dry-run` → real run → count verification →
  then deploy renamed-path code.
- ADR 0114 recorded via manage-adr (single bronze raw table per broker).
- Worktree venv note: all subagents used the main repo venv.

### File List

- New: `pipeline/migrations/migrate_single_bronze.py`,
  `tests/test_migrate_single_bronze.py`,
  `tests/test_single_bronze_routing.py`,
  `docs/adr/0114-single-bronze-raw-table-per-broker.md`.
- Modified: `pipeline/{run,paths,storage,query}.py`,
  `pipeline/raw/{models,__init__}.py`,
  `pipeline/connectors/{base.py, transform_utils.py}`,
  `pipeline/connectors/{ibkr,trading212,xtb}/{connector,fetch,transform}.py`,
  `pipeline/analytics/quality.py` (verify-only), `docs/{table-lineage,architecture}.md`,
  `docs/brokers/xtb.md`, `docs/adr/README.md`, `tests/conftest.py`,
  `tests/fixtures/{ibkr,trading212}.py`, and 13 test files (path/protocol
  updates + regression guards), `_bmad-output/implementation-artifacts/
  {pr150-single-bronze-per-broker.md, sprint-status.yaml}`.
