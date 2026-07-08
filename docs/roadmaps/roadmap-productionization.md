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

### 2. Orchestration: Orchestrator Step Function with connector fan-out/fan-in

A single orchestrator Step Function runs all enabled connectors in parallel,
waits for them to complete, then runs consolidate+allocate once — producing a
consistent snapshot across all enabled connectors. Triggers and connector
inclusion are explicit configuration, not derived from each other.

#### Configuration

The orchestrator is driven by four explicit flags (Terraform variables that
map to the existing app-level env vars and to EventBridge rule creation):

| Flag | Controls | Default |
|---|---|---|
| `scheduled` | Whether an EventBridge daily-schedule trigger fires the orchestrator | `false` when `xtb_enabled`, else `true` |
| `ibkr_enabled` | Whether the orchestrator includes the IBKR fetch+transform task | `true` |
| `t212_enabled` | Whether the orchestrator includes the T212 fetch+transform task | `true` |
| `xtb_enabled` | Whether the orchestrator includes XTB, and whether the S3 file-arrival trigger is created | `true` |

The orchestrator fans out to every `*_enabled` connector, waits for all to
complete, then runs consolidate+allocate. A deployment with only IBKR sets
`t212_enabled=false` and `xtb_enabled=false`; the orchestrator runs IBKR,
waits, then consolidates. No manual "disable from prerequisites" step.

#### Triggers

Two independent trigger sources, each created only when its flag is set:

- **S3 file arrival** (`xtb_enabled=true`) — `upload-xtb` writes to
  `staging/xtb/`, EventBridge detects `s3:ObjectCreated`, fires the
  orchestrator. This is the "all data is ready" signal: the manual XTB upload
  means IBKR and T212 should be fetched fresh in the same run for a consistent
  snapshot across all three brokers.
- **Daily schedule** (`scheduled=true`) — EventBridge fires the orchestrator
  on a cron expression. Used when there is no manual gating connector.

```
Orchestrator (single state machine, two possible triggers)
  trigger: S3 file arrival (if xtb_enabled) AND/OR daily schedule (if scheduled)
    ├─ XTB fetch+transform (if xtb_enabled)     ─┐
    ├─ IBKR fetch+transform (if ibkr_enabled)   ─┤
    ├─ T212 fetch+transform (if t212_enabled)   ─┤
    └─ wait for all enabled connectors             │
    └─ consolidate+allocate  ◄─────────────────────┘
```

Each bracketed step is one Fargate task running the existing Docker image with
a different subcommand.

#### Recommended combinations and the consistency trade-off

| `scheduled` | `xtb_enabled` | Recommended? | Behavior |
|---|---|---|---|
| `false` | `true` | ✅ Default | Manual trigger via XTB upload; all brokers fetched fresh together — fully consistent |
| `true` | `false` | ✅ Default | Daily schedule; IBKR+T212 fetched fresh — consistent (no XTB) |
| `true` | `true` | ⚠️ Only if XTB extraction is automated | Daily schedule includes XTB's *last* normalized data (no new file), so XTB is stale relative to IBKR/T212. Acceptable once XTB report download is automated so a fresh file is always staged before the schedule fires. |
| `false` | `false` | ❌ | No trigger at all — orchestrator never runs. Use manual Step Function execution instead. |

**Why orchestrator fan-out/fan-in, not per-connector consolidate+allocate?**

Running consolidate+allocate after each connector means analytics reflects
partial data (e.g., IBKR's new data mixed with T212's previous snapshot). The
orchestrator waits for all enabled connectors to complete, then runs
consolidate+allocate once for a consistent snapshot.

**Why is XTB file arrival the recommended trigger when XTB is enabled?**

XTB is the only connector without an API — its report must be downloaded
manually and uploaded. That manual upload is the natural "all data is ready"
signal. Triggering the orchestrator on XTB file arrival fetches IBKR and T212
fresh in the same run, so the snapshot is consistent across all three brokers.
Setting `scheduled=true` with `xtb_enabled=true` is allowed but produces a
stale-XTB snapshot unless XTB extraction is automated.

**Automating XTB report extraction (future)**

If XTB report extraction is automated later (e.g., a scheduled job that logs
into XTB and downloads the report before the orchestrator's schedule fires),
set `scheduled=true` and `xtb_enabled=true` for a fully automated, consistent
daily run. Until then, leave `scheduled=false` when `xtb_enabled=true` and use
the manual upload as the trigger.

**Why two Fargate tasks per connector, not four?**

The pipeline has four logical steps (fetch, transform, consolidate, allocate),
but running each as a separate Fargate task means four cold starts per connector
run — roughly 3–4 minutes of overhead for a pipeline whose actual work takes
under a minute. Grouping into two tasks (connector work, then consolidation)
cuts cold starts from 4 to 2 while preserving orchestration benefits:

- **Fewer cold starts** — two per connector run instead of four, cutting
  overhead from ~3–4 minutes to ~1.5–2 minutes
- **Consistent snapshots** — the orchestrator waits for all enabled connectors,
  then runs consolidate+allocate once
- **Idempotent by design** — consolidate and allocate are full-snapshot
  overwrites, so re-running the orchestrator is safe and correct
- **Per-broker retry** — one broker's flaky API doesn't block the others;
  Step Functions retry with exponential backoff on the connector task, then
  the orchestrator retries
- **Failure isolation** — IBKR down doesn't block T212; a failed connector
  task is retried independently within the orchestrator run

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

#### AWS resource isolation

AWS resources follow a hybrid model — shared orchestration, isolated data access:

| Resource | Location | Rationale |
|---|---|---|
| ECR repository | `shared/` | Both environments use the same Docker image with different tags (staging-latest, production-latest) |
| Step Functions | `shared/` | State machines are orchestration wiring, not data. `DEMO` parameter selects the environment. |
| ECS task definitions | `prod/` and `demo/` | Task definitions include `DEMO` flag and reference environment-specific IAM roles and S3 buckets. |
| IAM roles (task execution) | `prod/` and `demo/` | Production tasks access production S3; demo tasks access demo S3. No cross-environment access. |
| S3 buckets | `prod/` and `demo/` | Already isolated (ADR 0037, 0038). |
| EventBridge rules | `shared/` | S3 file arrival rule (created when `xtb_enabled=true`) and daily schedule rule (created when `scheduled=true`) are environment-agnostic. Rule creation is gated by the config flags. |

The pattern follows the existing ECR approach (ADR 0049): `terraform/shared/`
defines the shared infrastructure, and `prod/`/`demo/` look up shared
resources by name via `data` sources. ECS task definitions in `prod/` and
`demo/` pass `DEMO=true` or `DEMO=false` as an environment variable, matching
the environment selector established in ADR 0049. The `scheduled` and
`*_enabled` flags are Terraform variables in `shared/` that control which
EventBridge rules and ECS connector tasks are created.

#### AWS resources needed

- **Step Functions** — one orchestrator state machine. Triggered by S3 file
  arrival (if `xtb_enabled=true`) and/or daily schedule (if `scheduled=true`).
- **ECS Fargate** — four task definitions per environment: one per connector
  (ibkr, t212, xtb) and one for consolidate-allocate
- **EventBridge** — S3 file arrival rule (created if `xtb_enabled=true`) and/or
  daily schedule rule (created if `scheduled=true`) targeting the orchestrator
- **S3** — already in use for Delta Lake storage (separate buckets for staging
  vs prod); `staging/xtb/` prefix for XTB file arrival trigger
- **IAM roles** — task execution role per environment (prod + demo), Step
  Functions role, per-connector least-privilege policies

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

### Phase 1 — Deployment strategy and environment setup *[status: done]*

Establish the branch/tag/environment model and make the pipeline
environment-aware so it can be deployed to different AWS environments without
code changes.

### Phase 2 — Step Functions orchestration

Move the core pipeline execution to an orchestrator Step Function that fans out
to enabled connectors, waits for all to complete, then runs
consolidate+allocate once for a consistent snapshot. Triggers and connector
inclusion are explicit config flags (`scheduled`, `ibkr_enabled`,
`t212_enabled`, `xtb_enabled`): S3 file arrival fires the orchestrator when
`xtb_enabled=true`, a daily schedule fires it when `scheduled=true`. Add
connector subcommands (`run-ibkr`, `run-t212`, `run-xtb`,
`run-consolidate-allocate`) and set up ECS task definitions (in `prod/` and
`demo/`), a shared state machine (in `shared/`), EventBridge triggers, and
IAM roles.

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
| **Per-connector consolidate+allocate** | Analytics shows inconsistent snapshots — IBKR's new data + T212's old data between runs. Defeats the purpose of a consistent portfolio view. |
| **Per-step Fargate tasks** (4 per connector) | Adds ~3–4 minutes of cold-start overhead per connector run. Steps are fast and sequential, so per-step granularity provides little benefit over log-based observability. |
| **Single monolithic Step Function** (all connectors in one run) | XTB has no API and must wait for manual file upload — it would block IBKR/T212 or require coordination before every run. |
| **Deriving the trigger from `xtb_enabled` (no explicit `scheduled` flag)** | Magic/implicit behavior — the user can't schedule with XTB enabled even if they've automated XTB extraction. An explicit `scheduled` flag makes the trigger a deliberate config choice with the consistency trade-off documented above. |
| **Separate XTB Step Function running consolidate+allocate** | Produces a snapshot with XTB's new data + IBKR/T212's stale daily data — not consistent. Folding XTB into the orchestrator run fetches all connectors fresh together. |
| **Lambda functions** | Requires rewriting all pipeline code for Lambda constraints (deployment package size limits, no native Delta Lake support, 15-minute timeout). The existing Docker image runs unmodified on Fargate. |
| **Long-running ECS service** (always-on container) | Costs orders of magnitude more for a pipeline that runs once daily. Paying for idle compute contradicts the event-driven model. |
| **Fully isolated orchestration per environment** | Doubles Terraform for Step Functions (one set per env). State machines are orchestration wiring — the `DEMO` flag already selects the environment. Isolation belongs at the data layer (S3, IAM), not the orchestration layer. |

### Future (v3, optional)

Dagster for asset-based orchestration and local development UX. Adds lineage
visualization and a web UI. Worth considering if the project grows beyond 4
steps or needs cross-pipeline scheduling. Not included now to keep scope
focused — Step Functions is sufficient and cheaper for a single pipeline.