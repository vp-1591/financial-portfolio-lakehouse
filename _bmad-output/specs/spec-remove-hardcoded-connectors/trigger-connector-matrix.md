# Trigger × Connector Matrix (PR #148 state)

Load-bearing companion to SPEC-remove-hardcoded-connectors: the per-trigger
connector sets, and where the regression lives.

| Trigger | Fetch + transform connectors | Consolidate + validate connectors (post-PR) | Cadence | XTB regression? |
|---|---|---|---|---|
| **File arrival** (EventBridge S3 rule) | `file_arrival_connectors` = `[ibkr, trading212, xtb]` | `file_arrival_connectors` (incl. `xtb`) via input_transformer | On XTB file upload (prod + demo) | No |
| **Schedule** (EventBridge cron target) | `schedule_connectors` = `[ibkr, trading212]` | `schedule_connectors` (no `xtb`) | prod `scheduled=true`, monthly `cron(0 6 1 * ? *)`; staging `scheduled=false` | **Yes — overwrite drops XTB from `events`** |
| **CLI `full --mode staging\|prod`** (SFN trigger, `_trigger_sfn_execution`) | `DEFAULT_CONNECTORS` = `[ibkr, trading212]` | `DEFAULT_CONNECTORS` (no `xtb`) via `build_execution_input` | Manual / CI | **Yes — same drop** |
| **Docker `full`** (`cmd_full`) | `all_connectors()` = `[ibkr, trading212, xtb]` | `all_connectors()` (`run.py:442`) | Local | No |
| **Standalone `run-connector <name>`** | one broker | validate own tables only (`run.py:674-678`); no `connectors=` passed → `enabled_event_tables={}` | Manual | Fix 2 gap: no non-empty WARN |

## Current-conditions notes

- **Internal inconsistency:** `cmd_consolidate` (holdings) iterates the registry
  (`all_connectors()`, `pipeline/run.py:294`), so holdings keep XTB; event
  consolidation reads only the passed list — holdings and events disagree.
- **Registry is registration-driven, not hardcoded:** `pipeline/connectors/registry.py`
  `all()` returns whatever connectors register at import. Iterating it for
  consolidation is connector-agnostic by design (ADR 0110 C2), not a hardcoded list.
- `consolidate_events` writes `mode="overwrite"` (C4) — an incomplete input set
  permanently erases the brokers not listed.
