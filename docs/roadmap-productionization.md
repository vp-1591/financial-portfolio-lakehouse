# Roadmap: Pipeline Architecture & Productionization

## Goal

Make the portfolio pipeline production-ready: reliable orchestration, repeatable
execution, environment-aware configuration, and a report that proves the pipeline
is doing real work.

## Current state

The pipeline already has the core engineering foundation:

- three broker connectors
- raw → normalized → analytics flow
- encrypted Delta storage
- automated tests
- a working CLI entrypoint

What remains is operational maturity: orchestrated runs, scheduleable execution,
and a polished reporting artifact delivered without manual intervention.

---

## What remains to build

### 1. Deployment and environment strategy

A clear deployment model that separates staging from production and enables
repeatable execution.

#### Branch & environment flow

`main` **is** the staging branch. Production deployments are triggered by version tags.

```
feature branch → PR to main → auto-deploy to staging → tag release → deploy to production
```

| Gate | Trigger | What runs |
|---|---|---|
| Feature PR | PR opened/updated against `main` | Lint, unit tests, Docker build |
| Merge to `main` | PR merged | Deploy to staging AWS, run pipeline with demo creds, data quality checks |
| Production | Git tag (e.g. `v1.2.0`) | Deploy to production AWS |

No `stage` or `prod` branches — avoids three-way merge headaches. `main` is always staging-ready, and tags pin production releases.

**Deploy vs run are separate concerns:**

- **Deploy** = push the new pipeline code/container to the environment (happens on merge/tag)
- **Run** = execute the pipeline steps: fetch → transform → consolidate → allocate

A deployment makes a new version available; it doesn't trigger a pipeline run. Runs happen on triggers (schedule, manual, or file arrival).

#### Environment-aware config

Planned work:

- environment-aware config for test, staging, and production-style runs
- configurable storage paths and credentials
- infrastructure for AWS-backed execution and scheduling
- a simple deployment model with clear environment boundaries

### 2. Orchestration: Per-connector Step Functions

Each connector runs as its own Step Function with its own trigger. Each Step
Function runs two ECS Fargate tasks: one for the connector's fetch+transform,
then one for consolidate+allocate.

```
IBKR  — schedule / manual trigger → [fetch+transform] → [consolidate+allocate]
T212  — schedule / manual trigger → [fetch+transform] → [consolidate+allocate]
XTB   — file arrival trigger      → [fetch+transform] → [consolidate+allocate]
```

Each bracketed step is one Fargate task running the existing Docker image with
a different subcommand (e.g. `python -m pipeline run-full-ibkr`).

**Why per-connector, not per-step?**

The pipeline has four logical steps (fetch, transform, consolidate, allocate),
but running each as a separate Fargate task means four cold starts per connector
run — roughly 3–4 minutes of overhead for a pipeline whose actual work takes
under a minute. Grouping into two tasks per connector (connector work, then
consolidation) cuts cold starts from 4 to 2 while preserving all the orchestration
benefits:

- **Fewer cold starts** — two per connector run instead of four, cutting
  overhead from ~3–4 minutes to ~1.5–2 minutes
- **Always-current analytics** — consolidate+allocate runs after each connector
  completes, so the analytics layer is always up-to-date rather than waiting for
  all connectors to finish
- **Idempotent by design** — consolidate and allocate are full-snapshot
  overwrites, so running them after every connector is safe and correct
- **Independent triggers** — XTB is file-driven, IBKR/T212 are API-driven;
  mixing them would require manual coordination before every run
- **Per-broker retry** — one broker's flaky API doesn't block the others;
  Step Functions retry with exponential backoff on the connector task, then
  re-run consolidation
- **Failure isolation** — IBKR down doesn't block T212 or XTB
- **Event-driven XTB** — file arrival triggers the pipeline automatically

Step-level durations (how long each step took) move from the Step Functions
graph to structured CloudWatch logs. This is an acceptable trade-off: for a
daily personal pipeline, log-based observability is sufficient and avoids
per-step cold starts that would add 2+ minutes of overhead per run.

**Connector subcommands**

Each connector needs a thin CLI wrapper that runs fetch+transform sequentially
within a single process:

| Subcommand | What it runs |
|---|---|
| `run-ibkr` | IBKR fetch → IBKR transform |
| `run-t212` | T212 fetch → T212 transform |
| `run-xtb` | XTB fetch → XTB transform |
| `run-consolidate-allocate` | Consolidate → allocate |

The existing `full` subcommand (fetch → transform → consolidate → allocate for
all enabled connectors) remains for local development and testing.

#### AWS resources needed

- **Step Functions** — one state machine per connector (ibkr, t212, xtb)
- **ECS Fargate** — two task definitions: connector task and consolidate-allocate task
- **EventBridge** — scheduled triggers for IBKR/T212; S3 file arrival trigger for XTB
- **S3** — already in use for Delta Lake storage (separate buckets for staging vs prod); `staging/xtb/` prefix for XTB file arrival trigger
- **IAM roles** — task execution role, Step Functions role, per-connector least-privilege policies

#### Cost estimate (personal use)

- Step Functions: ~$0.25/month per state machine (state transitions are cheap)
- ECS Fargate: ~$0.50-1/month (2 tasks per connector run, ~2 minutes each, daily schedule)
- S3: negligible for portfolio-sized data

### 3. Data quality checks (staging gate)

After the pipeline runs in staging, validate the output before promoting to prod:

- **Schema validation** — Delta tables have expected columns and types
- **Null/range checks** — no unexpected nulls, values within sane bounds
- **Idempotency** — running twice produces identical output
- **Row count stability** — row counts don't drop unexpectedly vs previous run
- **Smoke test** — end-to-end pipeline completes without error using demo creds

These run as a separate Step Functions task after `allocate`.

Planned work:

- schema and business-rule checks for the output tables
- simple data quality summaries in the report
- clear security documentation that distinguishes local encryption from cloud production controls

### 4. Reporting baseline with a concrete portfolio feature

The immediate reporting milestone is a single HTML report that proves the
pipeline can produce useful output. The report should include:

- a portfolio summary for the current run
- a simple allocation view by broker and currency
- a position-type breakdown for the feature idea folded into this roadmap

#### Concrete feature idea: position-type classification

Add a lightweight position classification layer that tags each position as one
of a few simple categories such as ETF, stock, gold, thematic, or cash-like.
Use that classification to generate:

- a pie chart of portfolio composition by position type
- a short cheat sheet that explains the mix in plain English
- a small summary section in the HTML report

This shows how automation, classification, and reporting can be layered onto the
existing system without changing the core architecture.

### 5. Delivery and operational visibility

The report should not stop at being generated locally. The next step is to make
it easy to receive and inspect.

Planned work:

- email delivery of the generated report
- manual trigger support for pipeline and report generation
- optional scheduled execution where Step Functions are configured for it
- run metadata and basic error visibility for operational review

---

## Suggested phases

### Phase 1 — Deployment strategy and environment setup

Establish the branch/tag/environment model and make the pipeline
environment-aware so it can be deployed to different AWS environments without
code changes.

### Phase 2 — Step Functions orchestration

Move the core pipeline execution flow to per-connector Step Functions so
connector runs can be triggered manually, scheduled, or event-driven (file
arrival for XTB). Each Step Function runs two Fargate tasks: connector
fetch+transform, then consolidate+allocate. Add connector subcommands
(`run-ibkr`, `run-t212`, `run-xtb`, `run-consolidate-allocate`) and set up
ECS task definitions, EventBridge triggers, and IAM roles.

### Phase 3 — Staging data quality gates

Add pipeline validation after `allocate` and integrate it as a blocking gate
before production promotions. Verify schemas, nullability, idempotency, and row
count stability.

### Phase 4 — Reporting baseline

Deliver a single self-contained HTML report that proves the pipeline can produce
useful output and include the position-type classification feature.

### Phase 5 — Delivery and automation

Add email delivery and make report generation and pipeline runs easy to trigger
and schedule.

---

## Alternatives considered

| Approach | Why rejected |
|----------|-------------|
| **Per-step Fargate tasks** (4 per connector) | Adds ~3–4 minutes of cold-start overhead per connector run. Steps are fast and sequential, so per-step granularity provides little benefit over log-based observability. |
| **Single monolithic Step Function** (all connectors in one run) | XTB has no API and must wait for manual file upload — it would block IBKR/T212 or require coordination before every run. |
| **Lambda functions** | Requires rewriting all pipeline code for Lambda constraints (deployment package size limits, no native Delta Lake support, 15-minute timeout). The existing Docker image runs unmodified on Fargate. |
| **Long-running ECS service** (always-on container) | Costs orders of magnitude more for a pipeline that runs once daily. Paying for idle compute contradicts the event-driven model. |

### Future (v3, optional)

Dagster for asset-based orchestration and local development UX. Adds lineage
visualization and a web UI. Worth considering if the project grows beyond 4
steps or needs cross-pipeline scheduling. Not included now to keep scope
focused — Step Functions is sufficient and cheaper for a single pipeline.