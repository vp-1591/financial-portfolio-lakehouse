# Roadmap: Pipeline Architecture & Productionization

## Goal

This roadmap is meant to make the portfolio pipeline look credible to DE hiring
managers. The core story is operational maturity: reliable orchestration,
repeatable execution, and a report that proves the pipeline is doing real work.
Analytics is a supporting story that shows the system can produce useful output,
but the primary audience is engineering.

## Current state

The pipeline already has the core engineering foundation:

- three broker connectors
- raw → normalized → analytics flow
- encrypted Delta storage
- automated tests
- a working CLI entrypoint

What remains is to make it feel production-like: orchestrated runs, scheduleable
execution, and a polished reporting artifact that can be delivered without manual
intervention.

---

## What remains to build

The work below is intentionally focused on DE credibility rather than a broad DA
product roadmap.

### 1. Deployment and environment strategy

The foundation for everything else: a clear deployment model that separates
staging from production and enables repeatable execution.

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
- a simple deployment path that can be explained in one minute to a hiring manager

### 2. Orchestration: Per-connector Step Functions

Each connector runs as its own Step Function with its own trigger. This
avoids the XTB-blocking-IBKR problem (XTB has no API, must wait for manual
upload) and isolates per-broker failures.

```
IBKR  — schedule / manual trigger → ibkr_fetch → ibkr_transform → consolidate → allocate
T212  — schedule / manual trigger → t212_fetch → t212_transform → consolidate → allocate
XTB   — file arrival trigger      → xtb_fetch  → xtb_transform  → consolidate → allocate
```

Each Step Function:
- Runs each step as a separate ECS Fargate task (same Docker image, different subcommand)
- Has per-step retry with exponential backoff (broker APIs are flaky)
- Catches and isolates per-broker failures (one broker down doesn't block others)
- Reports step-level duration and status in the AWS console

Consolidate and allocate read **all** normalized tables (not just the
connector that triggered), so the output is always a complete picture.
Running them multiple times per window is fine — they are idempotent
overwrites.

**Why per-connector Step Functions?**

- **Independent triggers** — XTB is file-driven (no API), IBKR/T212 are
  API-driven. Mixing them in one monolithic command means XTB blocks the
  others or requires manual file upload before every run.
- **Per-broker retry** — one broker's flaky API doesn't block the others.
- **Step-level observability** — each step reports duration and status in the
  AWS console.
- **Event-driven XTB** — file arrival triggers the pipeline automatically,
  no manual workflow dispatch needed.

#### AWS resources needed

- **Step Functions** — one state machine per connector (ibkr, t212, xtb)
- **ECS Fargate** — task definition for the pipeline container, scheduled by Step Functions
- **EventBridge** — scheduled triggers for IBKR/T212; S3 file arrival trigger for XTB
- **S3** — already in use for Delta Lake storage (separate buckets for staging vs prod); `staging/xtb/` prefix for XTB file arrival trigger
- **IAM roles** — task execution role, Step Functions role, per-connector least-privilege policies

#### Cost estimate (personal use)

- Step Functions: ~$0.25/month per state machine (state transitions are cheap)
- ECS Fargate: ~$1-2/month (only runs during pipeline execution)
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

This demonstrates AI augmentation skills in a data engineering pipeline: a
small, concrete feature that shows how automation, classification, and reporting
can be layered onto the existing system without changing the core architecture.

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
arrival for XTB). Set up ECS Fargate tasks, EventBridge triggers, and IAM roles.

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

## Why this roadmap is strong for DE hiring

This roadmap is useful because it highlights the skills that matter most for DE
roles:

- **Real pipeline orchestration** — per-connector Step Functions with independent triggers shows event-driven architecture and failure isolation, not just running a monolithic script end-to-end
- **Clear operational boundaries** — separating deploy from run, staging from production, and per-broker concerns
- **Repeatable execution and scheduling** — automated staging gates, environment-aware config, and scheduled/manual/event-driven triggers
- **Infrastructure-aware deployment thinking** — ECS Fargate, EventBridge, Step Functions, IAM isolation, and cost awareness
- **A visible artifact** — a polished HTML report that proves the system works and can support downstream analytics needs

The report and the position-type feature are valuable because they show the
pipeline can support adjacent analytics needs, but the core narrative remains
engineering-first.

### Future (v3, optional)

Dagster for asset-based orchestration and local development UX. Adds lineage visualization and a web UI. Worth considering if the project grows beyond 4 steps or needs cross-pipeline scheduling. Not included now to keep scope focused — Step Functions is sufficient and cheaper for a single pipeline.
