# 0115: Phase-Level RSS Memory Observability in Pipeline Runs

## Context

The staging deploy after merging PR #152 failed: the trading212 connector
Fargate task (256 CPU / 512 MB) was OOM-killed three times (exit 137,
`OutOfMemoryError: container killed due to memory usage`) while ibkr in the
same Step Functions Map state succeeded. The container's CloudWatch log stream
ended abruptly ~30s after startup — only the six-line secrets preamble, then
silence — so nothing in the existing logs said *which phase* crossed the
512 MB ceiling.

Local measurement alone was not enough: the raw Trading 212 API fetch peaks at
only 73 MB, yet the full fetch→dedup→write→transform path later proved to peak
at ~1 GB. The pipeline needed a way for a dying container to leave a record of
the last phase it reached.

## Decision

Add **code-level phase RSS logging** (PR #153): a small psutil-based module
(`pipeline/observability.py`) whose `log_memory()` prints
`[mem] phase=<name> rss_mb=<x> peak_mb=<y>` lines to **stderr** at phase
boundaries in `pipeline/run.py` (`cmd_run_connector`, `cmd_full`,
`cmd_run_consolidate_analytics`), plus a `MemorySampler` context manager that
samples RSS every 100 ms over a span to catch intra-phase spikes. psutil is
declared in the pipeline extra.

The goal is a diagnosable log trail for OOM-class failures: a container that
dies from memory pressure names its last phase instead of stopping silently.

CloudWatch **Container Insights was considered and rejected**:
- Its ECS metrics publish at ~1-minute granularity with aggregation delay, so
  a ~30 s OOM-killed task yields only "spiked to the limit and died" — already
  known, not attributable to a phase.
- It adds a per-task monthly fee plus extra CloudWatch log ingestion.
- Code logs survive the kill: the awslogs driver streams stderr
  asynchronously, and the dead container's stream already preserved its
  secrets preamble. A `[mem]` line written just before the spike is persisted
  the same way.

## Constraints

- No terraform or AWS resource changes (Container Insights stays disabled).
- No change to pipeline behavior or exit codes — observability only.
- Works on the `python:3.11-slim-bookworm` container and Windows dev
  (psutil is cross-platform).
- Log lines must reach CloudWatch even for a SIGKILLed container, hence
  stderr + flush — the same channel the secrets preamble already uses.

## Consequences

- **Positive**: OOM kills now name the spike phase (verified: trading212 peaks
  at ~1039 MB locally vs the 512 MB task limit; ibkr at ~275 MB).
- **Positive**: Phase deltas make memory regressions visible in normal
  successful runs too, without new infrastructure.
- **Negative**: No long-term per-task memory *trend* metrics (the one thing
  Container Insights would add); trend detection is deferred until a real
  need appears.
- **Follow-up**: With the phase data in hand, the trading212 task memory must
  be raised (1024–2048 MB) in `terraform/staging/main.tf` and
  `terraform/prod/main.tf` — a separate change.

## Validation

1. `pytest tests/ -q -rf` passes (868 tests; no behavior change).
2. Local run `pipeline.run run-connector trading212 --mode staging` with
   staging creds emits `[mem]` lines at every boundary; trading212 peak ~1 GB,
   ibkr peak ~275 MB — matching the deploy outcomes (OOM vs success).
3. Staging run #94 after the encrypted handoff completed successfully at
   **207.8 MB peak RSS**, versus **at least 511.1 MB** before the fix: a
   **303.3 MB (59.3%) reduction**, with 9 passes, 0 warnings, and 0 failures.
4. After deploy, an OOM-killed container's last CloudWatch `[mem]` line names
   the phase it died in.
