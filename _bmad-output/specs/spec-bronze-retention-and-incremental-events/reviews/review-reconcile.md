# Reconcile Review — ARCHITECTURE-SPINE.md vs SPEC / Contract / Decision Matrix / Memlog

- **Spine reviewed:** `ARCHITECTURE-SPINE.md` (bounded-bronze-incremental-events, epic, status draft)
- **Inputs reconciled against:** `SPEC.md`, `retention-and-events-contract.md`, `handoff-decision-matrix.md`, `.memlog.md`
- **External facts spot-verified:** parent spine `spec-single-bronze-per-broker/ARCHITECTURE-SPINE.md` (ADR 0114), ADR 0047, ADR 0105, ADR 0116, `pipeline/raw/ingest.py`, `pipeline/connectors/trading212/{client,fetch}.py`, `pipeline/connectors/xtb/transform.py`, `pipeline/normalized/models.py`
- **Date:** 2026-08-22

---

## Verdict: GAPS — not fully reconciled

The spine faithfully carries all five SPEC capabilities, every one of the nine SPEC constraints (with two partial carries, see F5/F8), all but one non-goal explicitly, and every final memlog `(decision)` maps to an AD-1..AD-8. The two supersedes of the parent spine (RAW_SCHEMA/ADR 0047; parent AD-5 append) are correctly registered. The VACUUM-timing re-reading of "before every fetch" is a **defensible reconciliation**, not a contradiction.

The blockers are two **HIGH** gaps in the spine's own core mechanism (AD-1's write-time merge-on-key):

1. **AD-1 cannot deliver CAP-1's bounded Trading 212 retention** for paginated endpoints, because paginated raw rows carry per-run `source` values (`nextPagePath` cursors) that the merge-on-`source` neither replaces nor deletes. The "exactly one row per retention key" paradigm is contradicted by the current data shape.
2. **Active ADR 0116 constraints are superseded without registration** ("append-only raw write, `dedup_raw` stays, no `DeltaTable.merge`"), while the spine claims "No other inherited invariant is weakened."

Plus five medium/low findings (dropped fail-loud contract, wrong internal AD cross-references, missing CAP-4 verification test, two dropped non-goals, XTB event identity under-specification).

---

## 1. Capability coverage — PASS

| Capability | Spine map row | Governed by | Verdict |
|---|---|---|---|
| CAP-1 bounded bronze storage | `raw/{broker}` write path | AD-1, AD-2, AD-3 | Present; mechanism gap (H1) |
| CAP-2 incremental events | `normalize.py` events write | AD-4 | Present; XTB identity gap (F8) |
| CAP-3 current-fetch freshness | `quality.py`, `run.py` | AD-5 | Present |
| CAP-4 explicit Delta retention | `retention.py`, per-broker fetch | AD-1, AD-3 | Present; verification gap (F5) |
| CAP-5 single bronze read | `transform_connector` | AD-6 | Present |

All five capabilities appear in the Capability → Architecture Map and are governed by at least one AD. Extra rows (T212 handoff → AD-8, raw schema → AD-2/AD-7, migrations → AD-7) are a faithful addition.

## 2. Constraint coverage

All nine SPEC constraints trace to an AD, convention, inherited invariant, or Deferred. Two carries are incomplete:

1. "Trading 212 endpoint responses treated as complete… partial-response handling not added without evidence" → Deferred carries the completeness assumption, but the **fail-loud enforcement** the matrix demands is not carried (F6).
2. "Normalized event history for **every broker** is append-preserved by `event_id`" → AD-4 binds every broker's events write but names the identity subset only for IBKR/Trading 212, omitting XTB's existing `(event_type, event_id, account_id)` (F8).

Full trace:

| Spec constraint | Spine home | Status |
|---|---|---|
| One raw table per broker; no observability table unless proven insufficient | Inherited AD-1; Deferred ("second observability table"); AD-5 "no new metadata table" | Carried |
| `RAW_SCHEMA` adds nullable `account_id`, drops `source_file`; XTB fetch populates from filename; IBKR/T212 NULL; transform grouping + parse-recovery + `fetched_at`/`payload_hash` tie-break | AD-2 | Carried |
| Never treat `(source, account_id)` null as one cross-broker key | AD-2 Rule | Carried |
| Every broker's event history append-preserved by `event_id`; never overwritten from only latest response | AD-4 | Carried, but XTB identity omitted (F8) |
| Snapshot + events consume one shared bronze read per run; no independent re-read/reparse | AD-6 | Carried |
| T212 handoff removed for a measurement experiment; baseline; restore on material regression | AD-8 | Carried |
| T212 complete per contract; XTB account identity is the retention boundary; partial/correction handling not added without evidence | Deferred | Carried; fail-loud enforcement dropped (F6) |
| Delta defaults not a retention policy; 1-week deleted-file, 30-day log, VACUUM not automatic; distinguish logical/physical/log | AD-3 Rule | Carried |
| Silver schemas and source vocabulary unchanged unless required | Inherited (source vocab), AD-7 (events tables need no migration) | Carried |

## 3. Non-goals — one dropped, one carried implicitly

- No second metadata/observability table → carried in Deferred + AD-5. PASS.
- **No semantic canonicalization of broker payloads before hashing → dropped** — not in Deferred, not restated anywhere in the spine (F9). Not contradicted (AD-1 hashes raw bytes unchanged), but a quiet non-goal the spine lost.
- No deletion/correction semantics beyond stated completeness → carried in Deferred ("Broker corrections / partial responses / truncation — non-goal"). PASS.
- No guarantee of replaying historical broker payloads after the retention window; normalized rows are the durable history → carried only implicitly by the paradigm ("bounded cache … silver event layer is the durable history"); not listed in Deferred (F10). Not contradicted.

No non-goal was silently turned into a decision.

## 4. Memlog decision → AD mapping — PASS (with one registration gap)

| Memlog decision | Spine | Verdict |
|---|---|---|
| One broker-scoped bronze table, broker-specific retention/dedup | inherited AD-1 + paradigm | Mapped |
| XTB latest report per account, T212 latest response per endpoint, IBKR latest report per source | AD-1 | Mapped; T212 mechanics fail (H1) |
| IBKR events dedup-append by `event_id` instead of overwrite | AD-4 | Mapped |
| Delta defaults (7-day file, 30-day log, VACUUM not auto) are not retention | AD-3 | Mapped |
| Raw retention maintenance runs before every fetch: remove/compact then VACUUM | AD-3 re-reads "before every fetch" as "once per run, end of run" | Reconciled (see §6) |
| `RAW_SCHEMA` adds nullable `account_id`, drops `source_file`; XTB filename population; transform fallback | AD-2 | Mapped |
| VACUUM 7-day default, `retention_hours` omitted, `enforce_retention_duration=True` | AD-3 | Mapped |
| Events merge: native `DeltaTable.merge` on event identity; `dedup_events` pre-dedup; update only when newer; never delete | AD-4 | Mapped |
| Freshness run-aware; no metadata table | AD-5 | Mapped |
| Migration backfills XTB `account_id` from `source_file` before drop | AD-7 | Mapped |
| Write-time merge-on-key; in-batch `(source, payload_hash)` dedup stays; cross-run `dedup_raw` scan deleted | AD-1 | Mapped, but conflicts with active ADR 0116 without registration (H2) |
| Remove T212 handoff as measured experiment; rollback on material regression | AD-8 | Mapped |

The memlog itself records the end-of-run VACUUM reconciliation (decision: "tombstone created by the merge means vacuum lands after the write"), so AD-3 does not contradict the memlog.

## 5. Success signal achievability — PARTIAL

The seven concrete proof items map to ADs: retention demo (AD1/AD2), bounded storage + reduced read/dedup work (AD1), one shared bronze read (AD6), T2 measured baseline (AD8), Flex-window preservation (AD4), no false stale warning (AD5), Delta retention/VACUUM verified (AD3). **H1 blocks "shows bounded raw storage and materially reduced read/dedup work across repeated fetches"** for Trading 212 unless the merge-on-key is redesigned for paginated raw rows, and **F5** means "verifies configured Delta retention/VACUUM behavior" has no test mapped in the conventions row.

## 6. Delta retention facts — honored

- 7-day tombstone default, 30-day log default, VACUUM not automatic → AD-3 states all three and keeps them distinct ("Logical removal (merge replace), physical removal (VACUUM), and transaction-log history (30-day Delta default) remain three distinct mechanisms").
- "Before every fetch: remove/compact then VACUUM" vs spine's end-of-run VACUUM → **defensible reconciliation, not a contradiction**: the merge creates the tombstone during the write; at the 7-day threshold a tombstone is ineligible for VACUUM for 7 days, so running VACUUM at start-of-fetch vs end-of-run changes nothing functionally. The memlog already records this reading, and AD-3 documents it. The only residue is that CAP-4's literal wording ("before every fetch") is re-read without the SPEC/contract being edited to match — a documentation note, not a design defect.

---

## 7. Findings

### H1 — HIGH — AD-1's merge-on-`source` cannot bound Trading 212 raw storage for paginated endpoints; CAP-1's "latest response per endpoint" is not implemented

**Anchor:** SPEC CAP-1 success ("Trading 212 retains the latest response per endpoint", SPEC line 21); contract broker-policy table ("Trading 212 | endpoint `source` | Latest complete response per endpoint", line 7); spine AD-1 Rule and the paradigm line "exactly one row per broker retention key (… Trading 212 endpoint …)" (spine lines 20, 79).

**Why:** The current T212 events fetch emits one raw row per captured HTTP response, and paginated endpoints follow `nextPagePath` (client.py `_fetch_paginated`), so page-2+ rows carry `source` = the base path plus a per-run cursor token (e.g., fixture `nextPagePath: "/equity/history/orders?cursor=abc"`). The parent spine (inherited as AD-2 here) even states the vocabulary includes "pagination suffixes possible". Under AD-1's merge-on-`source`:
- page-1 row (source = base endpoint path) is replaced each run — OK;
- page-2+ rows (source = base-path-`?cursor=…`) differ every run, so the current batch matches nothing, the untouched-key rule keeps every stale cursor row, and new cursor rows are inserted every run.

Result: T212 raw storage and the transform's single-bronze read keep growing with pagination depth per run — the exact unbounded-growth failure CAP-1 and the success signal ("bounded raw storage … materially reduced read/dedup work") exist to kill. The spine's "one row per retention key" invariant does not hold for the current raw data shape, and the spine does not reconcile it (no page-collapse, no key redesign, no fetch change).

**Required:** decide and state one of: (a) store one raw row per endpoint per fetch (concatenated pages, keyed on the stable endpoint path), (b) merge on `(source, payload_hash)` and delete the source's rows absent from the current batch (replace-the-endpoint semantics), or (c) document the retention key for paginated sources and amend CAP-1's success wording. This is the largest dropped reconciliation in the spine.

### H2 — HIGH — Active ADR 0116 constraints are superseded by AD-1 without being registered in "Conflicts surfaced"

**Anchor:** ADR 0116 (active) Constraint: "The raw table append write and dedup behavior remain append-only; `dedup_raw` uses `(source, payload_hash)` (projected key scan only, **no `DeltaTable.merge`**/bounded scans)"; spine "Conflicts surfaced" (line 40) claims "No other inherited invariant is weakened" while AD-1 deletes the `dedup_raw` scan and replaces append with `DeltaTable.merge`.

**Why:** The spine inherits AD-1's "the removed `dedup_raw` accumulation scan", which directly contradicts an explicit, active ADR constraint, and the spine's conflict list names only ADR 0047 and parent AD-5. The memlog records the write-on-key decision, so the intent is recorded — but the spine that claims to be the single governing document for the epic neither lists ADR 0116's dedup/append constraint as superseded nor inherits-and-supersedes it. A reviewer applying the ADR/roadmap consistency rule would flag the violation.

### M3 — MEDIUM — Fail-loud T212 fetch contract and per-endpoint write-what-succeeded isolation dropped when parent AD-5 was superseded

**Anchor:** handoff matrix (line 13, Remove column): "the current fail-loud T212 fetch contract must remain if partial responses are unsafe"; parent AD-5 ("Per-endpoint `try/except` write-what-succeeded isolation stays; Trading 212's all-events-endpoints-empty `RuntimeError` survives").

**Why:** the spine supersedes parent AD-5 (append → merge) but does not restate or retire AD-5's load-bearing clauses. The contract's completeness assumption is carried in Deferred, but the enforcement mechanism (any events-endpoint failure aborts the run — `RuntimeError` at fetch.py:127) is not named anywhere. Under the events merge and untouched-key raw semantics the *data* consequence is largely mitigated (missing endpoint keeps its prior raw row and its events are append-preserved), but the fail-loud exit is a user-visible behavior the matrix explicitly keeps, and the spine neither keeps it nor records a decision to drop it.

### M4 — MEDIUM — Wrong internal AD cross-references in the governing document

**Anchors:** spine line 40 "write-on-key (**AD-3 here**)" — write-on-key is **AD-1** here (AD-3 is the VACUUM AD); spine line 37 "run-aware freshness (**AD-6 here**) falls back to it" — run-aware freshness is **AD-5** here (AD-6 is the single-read AD). Also the Inherited table's "From parent" for ADR 0072 lists ADR 0114, but the parent spine (ADR 0114) does not carry ADR 0072 (it carries ADR 0105; ADR 0072 is not in the parent's standing-constraints table).

**Why:** these are the exact references an implementer or reviewer uses to find the governing rule; a wrong AD number in a spine whose purpose is to bind the implementation is a real cost. The AD-numbering mismatch (and the "from" provenance) should be corrected.

### M5 — MEDIUM — CAP-4's "documented and tested or verified for every environment" has no test/verification mapped

**Anchor:** SPEC CAP-4 success ("the behavior is documented and tested or verified for every environment"); spine Consistency Conventions "Tests & regression guards" row lists no VACUUM/retention test (only no-op-merge, stale-warning, key-preservation, `SELECT DISTINCT source`, migrated-XTB tests).

**Why:** AD-3 documents the mechanism (VACUUM per run, default retention, per environment) but the success signal "verifies configured Delta retention/VACUUM behavior" is not carried into the conventions, so the spine under-specifies how the PR proves CAP-4. The mechanism is sound; the verification convention is missing.

### L1 — LOW — Non-goal "No semantic canonicalization of broker payloads before hashing" is dropped

**Anchor:** SPEC non-goal line 50. The spine's Deferred section lists observability, corrections/partials, merge granularity, log history, rollback — but not canonicalization. Not contradicted (AD-1 hashes raw payloads directly), but a listed non-goal was silently dropped from the distilled record. Add to Deferred.

### L2 — LOW — Non-goal "No guarantee of replaying historical broker payloads after the configured retention window" carried only implicitly

**Anchor:** SPEC non-goal line 52. The paradigm ("bounded cache … silver event layer is the durable history") implies it but Deferred does not list it. Not contradicted; add to Deferred for completeness.

### L3 — LOW — AD-4 under-specifies XTB's event identity subset

**Anchor:** SPEC constraint "Normalized event history for **every broker** is append-preserved by `event_id`" (line 40); contract line 21 "The target behavior for **every broker** event table is to merge …"; spine AD-4 Rule "the per-broker event identity — the existing subset containing `event_id` (IBKR, Trading 212 per ADR 0105)".

**Why:** XTB has an events table (`xtb_events`, via shared `XTB_REPORT`) and an existing dedup subset `(event_type, event_id, account_id)` (xtb/transform.py). AD-4's parenthetical names only IBKR and T212, so an implementer must discover XTB's identity from code rather than from the spine — exactly the load-bearing input the spine should carry.

### L4 — LOW — VACUUM timing: re-read of "before every fetch" is a defensible reconciliation (informational)

**Anchor:** contract line 17 ("Before every fetch, retention maintenance must remove or compact … then run VACUUM") vs AD-3 ("VACUUM lands at the end of the run"). **Not a contradiction.** The merge creates the tombstone during the write, and at the 7-day default the position of VACUUM (start-of-fetch vs end-of-run) changes nothing — the memlog decision records exactly this. The re-read should stay visible in the contract/SPEC wording at merge time so the success criterion "documented … for every environment" is literal.

---

## 8. Recommended follow-ups

1. Redesign or restate AD-1's T212 (and check IBKR Flex pagination) retention mechanism before implementation (H1).
2. Register ADR 0116's dedup/append constraints as superseded in "Conflicts surfaced" (H2).
3. Decide and record the fate of the fail-loud T212 fetch contract and write-what-succeeded isolation (F6/M3).
4. Fix the two wrong internal AD references and the "From parent ADR 0114" for ADR 0072 (M4).
5. Add a VACUUM/retention verification test or explicit per-environment check to the conventions (M5).
6. Add the dropped non-goals and XTB event identity to Deferred/AD-4 (L1/L2/L3).
