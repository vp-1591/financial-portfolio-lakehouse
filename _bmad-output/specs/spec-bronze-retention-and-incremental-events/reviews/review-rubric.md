# Rubric-Walker Review — bounded-bronze-incremental-events (epic spine)

Reviewed: `ARCHITECTURE-SPINE.md` (draft, 2026-08-22), parent spine
`spec-single-bronze-per-broker` (final), `SPEC.md`, companions
`retention-and-events-contract.md`, `handoff-decision-matrix.md`, `.memlog.md`.
Brownfield verified against `pipeline/` source, `docs/adr/`, `pyproject.toml`, and
the pinned deltalake 1.6.0 in the venv.

## Verdict

**REVISE — 1 HIGH, 3 MEDIUM, 2 LOW.** The spine is structurally strong: the
bounded-bronze / durable-events paradigm is coherent, the supersede of ADR 0047 and
parent AD-5 is surfaced rather than silent, the stack is pinned and verified, and
CAP-1..CAP-5 each have a governing AD. But two AD Rules as written would let a
story implement a *silent functional no-op* (VACUUM that never deletes; an events
merge that cannot preserve what it claims), one AD points at the wrong module, and
the per-broker event identity plus null-key retention handling are under-pinned.

## Findings

### HIGH — F1. AD-3's Rule is unenforceable as written: `DeltaTable.vacuum()` defaults to `dry_run=True`, so "invoke vacuum with the default retention" deletes nothing.

- AD-3 Rule: "each broker run invokes `DeltaTable.vacuum()` with the default
  retention (tombstone 7-day; `retention_hours` omitted, `enforce_retention_duration`
  stays True)".
- Verified in the venv (deltalake 1.6.0): `vacuum(self, retention_hours=None,
  dry_run=True, enforce_retention_duration=True, ...)` — `dry_run=True` is the
  default. A story implementer following the Rule literally calls `vacuum()` and
  physically deletes nothing. CAP-4's success ("removes or compacts out-of-policy
  raw data and then runs VACUUM … verified for every environment") silently fails,
  and AD-3's own "Prevents" ("out-of-policy rows surviving on disk indefinitely")
  is not met. Delta's 7-day tombstone ceiling + dry-run VACUUM is exactly the
  no-maintenance failure mode the SPEC names.
- Fix the spine: state `dry_run=False` explicitly and add a regression guard to the
  Tests & regression conventions row asserting tombstoned files are actually
  removed (spec CAP-4 demands "tested or verified for every environment"; the
  spine currently pins no vacuum test).

### MEDIUM — F2. AD-4 misidentifies the events-write site: the `mode="overwrite"` that discards events absent from the current response is `pipeline/run.py` `transform_connector`, not `normalize.py:218`.

- The spine claims the CAP-2 failure is "today's `mode="overwrite"` events tables
  (`normalize.py:218`) discarding every event absent from the current broker
  response — the Flex window-loss failure". Verified brownfield:
  - `pipeline/run.py:301-306` (`transform_connector`) writes each `{broker}_events`
    table with `mode="overwrite"` from the current run's rows — **this** is the
    destructive write that drops out-of-window events.
  - `pipeline/normalized/normalize.py:218` is `normalize_currency`, a
    read-modify-write of the *existing* consolidated events table (it rewrites the
    rows it just read; it discards nothing).
- Consequence: AD-4's Binds ("`normalize.py`, `dedup_events`") and the Structural
  Seed ("`normalize.py` — events write becomes `DeltaTable.merge`") point stories
  at the wrong module. Changing `normalize.py`'s write to a merge is behaviorally
  inert; the destructive overwrite in `run.py` `transform_connector` is never
  bound, so the Flex-window-loss it claims to prevent survives. This is a missed
  brownfield divergence point (checklist item 1/4).

### MEDIUM — F3. AD-4 under-pins the per-broker event identity; XTB and T212 scoping is dropped and IBKR is misattributed to ADR 0105.

- Rule: "merge … on the per-broker event identity — the existing subset containing
  `event_id` (IBKR, Trading 212 per ADR 0105)". Actual per-broker subsets in the
  code:
  - IBKR: `["event_id"]` (`ibkr/transform.py:349`).
  - Trading 212: `["event_type", "event_id"]` (`trading212/transform.py:299`,
    ADR 0105 — `event_type` scopes separate ID spaces).
  - XTB: `["event_type", "event_id", "account_id"]` (`xtb/transform.py:369`).
- The spine omits XTB entirely and drops the `event_type` (T212) and `account_id`
  (XTB) components. ADR 0105 is Trading 212-only; IBKR's subset is not from it.
- Consequence: a merge keyed on `event_id` alone (the literal reading of "subset
  containing `event_id`") is **not idempotent** against the in-batch
  `dedup_events` keys and collapses same-ID events across XTB accounts and across
  T212 order/dividend/transaction ID spaces — exactly the cross-broker collapse
  AD-2/ADR 0105 guard against. The parent's standing-constraints row already
  recorded the correct contract ("XTB adds `account_id` to the subset"); the epic
  dropped it. Fix: state the merge key = each broker's exact dedup subset
  (`event_id` for IBKR; `(event_type, event_id)` for T212; `(event_type, event_id,
  account_id)` for XTB) and that in-batch dedup key == merge key.

### MEDIUM — F4. Null retention keys (unparseable XTB filenames) have no rule in AD-1/AD-2; the memlog's fallback decision was dropped in distillation.

- `retention-and-events-contract.md` names the XTB risk: "Filename may not provide
  an account ID". The `.memlog.md` records the decision: "NULL retention keys
  (unparseable XTB account) fall back to append + in-batch dedup". The spine's
  AD-1 merge rule is silent on NULL-keyed rows.
- Consequence: a Delta MERGE predicate `account_id = account_id` never matches
  NULLs (SQL three-valued logic), so a NULL-keyed XTB row would **insert on every
  run** — unbounded raw growth for exactly the file the broker contract says can
  occur. Two stories reading AD-1 would diverge (one appends, one merges-on-null
  and duplicates). The spine should either carry the memlog fallback into AD-1 (or
  AD-2) or surface it as an OPEN QUESTION — a whole divergence point left silent.
  (Relatedly, `DecodedRow`/`iter_raw_payloads` read `source_file` at
  `transform_utils.py:157`; dropping the column binds "every raw reader" but the
  Structural Seed does not list `transform_utils.py`'s `source_file` removal, so
  that read-site change is only implicit.)

### LOW — F5. Operational envelope is named but not concretely mapped to the orchestrator.

- The spine does place the logic: merge at write (`ingest.py`, AD-1), VACUUM per
  broker run (AD-3, binding "every environment (docker/MinIO, staging, prod)"),
  events merge in the run (AD-4), migration per environment with connectors idle
  (AD-7). So the deployment/ops dimension is not *silent*.
- Gaps: (a) the boundary of a "run" is not mapped to the Step Functions envelope —
  staging/prod executes one ECS/Fargate task per connector (SFN Map state,
  ADRs 0051/0052/0091), while XTB runs in a *separate* EventBridge file-arrival
  task (ADR 0110), and docker mode runs connectors in parallel threads; AD-3's
  "VACUUM lands at the end of the run" is ambiguous across those three trigger
  shapes (does XTB's file-arrival run vacuum too? which task owns "end of run"
  relative to the validate step?). (b) AD-7 says "connectors idle" but never says
  the scheduled SFN executions must be paused during the per-environment migration
  window. (c) the State conventions row says "VACUUM per run with default
  retention" without a raw/silver scope qualifier while AD-3 binds only raw
  maintenance — a story could vacuum silver events and, although live files are
  unreachable to VACUUM, the tombstone/compact behavior there is undefined. These
  are fixable with two sentences in AD-3/AD-7; leaving them open invites
  orchestration drift.

### LOW — F6. Citation/table errors that will mislead stories.

- "Inherited Invariants" table lists "From parent: ADR 0114" for both the ADR 0105
  dedup row and the ADR 0072 freshness row; ADR 0072 is not in the parent's
  standing-constraints table at all, and the ADR 0105 row should cite ADR 0105.
- "Conflicts surfaced" says parent AD-5 "is superseded at the raw layer by
  write-on-key (AD-3 here)"; write-on-key is AD-1, not AD-3.

## Checklist-by-checklist

1. **Divergence points fixed?** Mostly. Merge key, migration, freshness, shared
   read, handoff are pinned. Misses: events merge key per broker (F3); null-key
   XTB rows (F4); events-write site (F2).
2. **Rules enforceable & prevent their stated divergence?** No for AD-3 (F1) and
   AD-4's identity (F3). AD-1/AD-2/AD-5/AD-6/AD-7/AD-8 are enforceable.
3. **Deferred safe?** Yes — content-time keying, merge granularity, log history,
   handoff rollback are all either non-goals or engine-choice defaults; nothing
   under Deferred can let two units diverge.
4. **Ratifies brownfield?** `dedup_raw` accumulation scan, `normalize.py:218`
   overwrite, freshness=max, deltalake 1.6.0 — all real and correctly named, with
   one misattribution (F2). The planned change builds on the real ADR 0116 t212
   handoff baseline (verified `handoff_supported`, `base.py`, `run.py`).
5. **CAP-1..CAP-5 covered?** Yes — CAP-1 (AD-1/2/3), CAP-2 (AD-4), CAP-3 (AD-5),
   CAP-4 (AD-3 — see F1 for the dry-run hole and missing per-environment
   verification guard), CAP-5 (AD-6/AD-8). The T212 handoff experiment (spec
   constraint) is AD-8 with a rollback rule.
6. **Supersede + parent line held?** Correctly surfaced: AD-2 explicitly replaces
   ADR 0047's "account_id is a silver concept" clause and drops `source_file`, and
   the supersede is recorded in `.memlog.md` (verified present) — not a silent
   override. Parent AD-1..AD-4, AD-6, AD-7 are held; parent AD-5 (append) is
   superseded at the raw layer and that supersede is stated. (F6 note: one AD-3/AD-1
   reference slip.)
7. **Stack pinned/verified?** Verified against `pyproject.toml` and the venv:
   deltalake 1.6.0 (vacuum/merge/delete present), polars 1.42.0, pyarrow 24.0.0,
   duckdb 1.5.4, ruff/pyright 0.16.0/1.1.411. Claim "no new dependencies" holds.
8. **Dimensions owned?** Data/format/state/naming/conventions are covered;
   deployment/environments are *named* but the SFN/ECS/file-arrival run-boundary
   and migration-vs-scheduled-run interplay are not concretely mapped (F5).

## Priorities

F1 then F3 then F2 then F4 (order of user-visible failure), then F5/F6 copy fixes.
