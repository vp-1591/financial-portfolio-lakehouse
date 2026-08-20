---
id: SPEC-single-bronze-per-broker
companions:
  - architecture-diagrams.md   # before/after raw-layer shape and transform routing
  - ARCHITECTURE-SPINE.md      # feature architecture spine: invariants, routing contract, migration gate
sources: []        # input was GitHub issue vp-1591/financial-portfolio-lakehouse#144 (URL, not a file); traceability in .memlog.md entry 1
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability only — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Single Bronze (Raw) Table per Broker

## Why

The raw layer still carries per-broker × per-layer multiplicity: each broker has two raw tables (`{broker}_snapshot`, `{broker}_events`), all six sharing one identical `RAW_SCHEMA` and differing only in the `source` column values. XTB already proves a single bronze works — `xtb_snapshot` feeds both silver transforms via the `events_raw_layer = "snapshot"` override — but only as a special case: a `name == "xtb"` branch in `fetch_connector`, the `events_raw_layer` protocol attribute, and the raw-layer selection in `transform_connector`. Every transform already filters by `source` today, and the project has converged on unification everywhere else (one shared snapshot schema per ADR 0102, one shared events schema, registry-derived connector lists, quality gates keyed on normalized tables). Bronze is the last per-broker multiplicity and the last XTB special case. Unifying to one raw table per broker makes the connector contract one-table-per-broker, deletes the XTB branch, and makes `docs/table-lineage.md` true (it currently draws an edge to the never-written `xtb_events` raw table). This is a refactor of write targets and routing only — no change to what data is fetched or how it is parsed.

## Capabilities

- **CAP-1** — One raw Delta table per broker; `source` discriminates data kind.
  - **intent:** Operator stores every payload a broker returns — snapshot, positions, and events alike — in a single raw table `raw/{broker}`, discriminated by `source`, with no per-layer raw plumbing.
  - **success:** After migration, `raw/ibkr`, `raw/trading212`, `raw/xtb` each hold rows from both fetch kinds; no `{broker}_snapshot` or `{broker}_events` raw paths remain; `SELECT DISTINCT source` over each table returns the expected per-broker values.
- **CAP-2** — XTB is no longer a special case in fetch/transform.
  - **intent:** Developer runs fetch and transform for any broker through one generic path, with no broker-name branch, no raw-layer override attribute, and no stub-method scaffolding.
  - **success:** The `name == "xtb"` branch in `fetch_connector` is deleted, the `events_raw_layer` attribute is removed, `transform_connector` reads a single raw path, and the XTB connector conforms to the same protocol as IBKR and Trading 212; the full test suite passes.
- **CAP-3** — Rows route to the correct silver transform after the merge.
  - **intent:** System sends each raw row to its silver table — snapshots to `normalized/{broker}_snapshot`, events to `normalized/{broker}_events` — based on `source`.
  - **success:** Regression tests prove Trading 212 `/equity/positions` list payloads never reach the events silver; both events transforms include-filter only event sources; both silver tables per broker are unchanged in schema and contents.

## Constraints

- **Bronze only.** Silver stays two normalized tables per broker (different schemas, consumers, freshness gates). Do not merge silver.
- **API calls do not merge — only the write target.** IBKR still issues distinct Flex queries; Trading 212 still calls its five endpoints (account summary, positions, orders, dividends, transactions). Preserve write-what-succeeded, surface-the-failure isolation (per-endpoint `try/except` stays).
- **Merge migration is idempotent, dedup-keyed on `(broker, source, payload_hash)`, supports `--dry-run`**, and purges the orphaned `xtb_events` raw table (never written since ADR 0108; `docs/table-lineage.md` currently draws a false edge to it).
- **Migration must deploy before renamed-path transforms run**, or `transform_connector` silently skips and quality gates flag missing tables.
- **`filter_latest_snapshot` stays per-`source`** — a future global max would silently drop streams. Add a regression guard.
- **Trading 212's all-events-endpoints-empty `RuntimeError` (fetch.py:121) must survive the merge** — the required-non-empty behavior depends on it.
- **Raw table is named `{broker}` under `raw/`** (query alias `{broker}_raw` via alias auto-discovery). Do not keep the misleading `{broker}_snapshot` name XTB currently uses.
- **Explicit source gates:** events transforms include-filter event sources (IBKR `flex_events`; Trading 212 `/orders` | `/dividends` | `/transactions`) before payload unwrapping — never substring luck.

## Non-goals

- No single global bronze across brokers (rejected option C — broker discrimination would move into every query/filter and blur failure isolation).
- No merge or collapse of per-broker API fetches (IBKR's two Flex queries and Trading 212's five endpoints stay separate fetches).
- No change to `RAW_SCHEMA` fields or payload formats.

## Success signal

One PR lands: the migration produces exactly one raw Delta table per broker (`raw/{broker}`, alias `{broker}_raw`) in staging and prod; XTB special-casing is gone from the code; Trading 212 positions provably cannot reach the events silver; both silver tables per broker keep their pre-merge contents; `ruff`, `pyright`, and `pytest` all pass.

## Assumptions

- Issue #144 details were verified against main (2026-08-20) rather than taken at face value: "cdc" terms in the issue map to today's "events" naming (ADR 0113), and IBKR's `transform_events` already carries an explicit `flex_events` source gate.
- Single-PR delivery reflects explicit user direction; the issue's suggested 5-PR sequence is superseded.
