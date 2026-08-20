---
id: SPEC-remove-hardcoded-connectors
companions: [trigger-connector-matrix.md]
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability only — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# PR #148 Fix: Connector-Agnostic Pipeline without Silent XTB Data Loss

## Why

PR #148 ("Remove hardcoded connector requirements") threads the enabled connector set through event consolidation and validation, replacing hardcoded broker gate lists. It introduced a regression: a scheduled/manual/CI run fetches only `ibkr` + `trading212`, so `consolidate_events` skips `xtb_events` and its overwrite-mode write drops XTB rows that a prior file-arrival upload consolidated — silently removing XTB from `events`, `dividend_income`, and `interest_income` until the next upload. That contradicts the PR's own purpose (config-driven, not hardcoded, connectors) and violates the active ADR 0110 contract: consolidation+analytics read all silver via the registry, not the scheduled-connectors list. The goal is a connector-agnostic pipeline that never silently loses a broker's already-ingested data.

## Capabilities

- **CAP-1** — Event consolidation covers every broker with silver data, not only the brokers the run fetched.
  - **intent:** Scheduled/manual/CI runs keep already-ingested broker data (e.g. XTB) present in `normalized/events` and in analytics derived from it.
  - **success:** Seed `xtb_events` silver with rows, run a scheduled-style consolidation that declares only `ibkr`/`trading212`, and assert `normalized/events` still contains the XTB rows (and `dividend_income`/`interest_income` rebuilt from `events` include them).
- **CAP-2** — Per-trigger connector sets are declared, not gated in Python.
  - **intent:** An operator changes which brokers a trigger runs by editing tfvars, with no Python gate lists (required/optional) to update.
  - **success:** No required/optional broker gate lists exist in `pipeline/` code; editing `schedule_connectors` / `file_arrival_connectors` changes the per-trigger set with zero code change.
- **CAP-3** — Missing or empty enabled event tables are a WARN, not a FAIL.
  - **intent:** A broker that has not produced events yet does not fail the run.
  - **success:** `run_validation(connectors=[...])` returns WARN (and exit 0) for a missing or empty enabled `<broker>_events` table.

## Constraints

- **C1 (ADR 0091 #6, ADR-0110):** XTB fetch+transform runs only on the file-arrival trigger; scheduled/CI runs must not launch a no-op `run-connector xtb` task. Rules out re-adding XTB to the scheduled set.
- **C2 (ADR-0110 §Decision):** Consolidation+analytics read all registered silver tables via the registry (`all_connectors()`), independent of the scheduled-connectors list.
- **C3 (ADR-0107):** Broker lists live in committed auto-tfvars (`schedule_connectors`, `file_arrival_connectors`); orchestrator Terraform passes them into the SFN input.
- **C4:** `consolidate_events` writes `normalized/events` with `mode="overwrite"` (no merge) — the consolidated set must be complete before the write.
- **C5 (adr-workflow):** Altering an active ADR requires superseding via `manage-adr`, never hand-editing the ADR file.

## Non-goals

- XTB does **not** return to the scheduled connector set; no scheduled no-op XTB task.
- `file_arrival_connectors` stays `["ibkr","trading212","xtb"]`; file-arrival trigger behavior unchanged.
- No re-architecture of the connector registry, broker naming, or the tfvars schema.
- No schedule/cadence changes (staging `scheduled=false`; prod monthly `cron(0 6 1 * ? *)`).
- `DEFAULT_CONNECTORS` (`pipeline/sfn.py`) stays as the manual/CI trigger default; not config-derived in this work.

## Success signal

After an XTB file upload, every later scheduled/manual/CI run — which fetches only `ibkr`+`trading212` — leaves the XTB rows present in `normalized/events` and in the analytics rebuilt from it. A regression test seeds `xtb_events` silver and proves the scheduled-style run keeps those rows. No hardcoded broker gate lists remain in pipeline code; ADR-0110 stays active (no supersede needed). The three checks (ruff, pyright, pytest) pass; PR #148 is updated and merged.

## Decisions

- **D1:** All-silver consolidation is the permanent general semantics — any future broker whose refresh decouples from the schedule is covered by consolidation via the registry (user: Q1 confirmed).
- **D2:** The fix is registry-based consolidation over all silver while keeping per-trigger fetch/validate lists; Fix 1 (re-adding XTB to scheduled sets) is excluded (user: Q2 confirmed).
- **D3:** The fix keeps ADR-0110 active and does not re-hardcode XTB — it restores compliance with ADR-0110 §Decision, so no supersede is needed.
