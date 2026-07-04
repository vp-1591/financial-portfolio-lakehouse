# XTB Report → Cloud Pipeline: Upload Roadmap

## Goal

Pass an XTB `.xlsx` report to the cloud pipeline without manual AWS console
logins or S3 uploads — and make XTB an event-driven connector that triggers
the downstream pipeline automatically.

## Architecture

Each connector runs as its own Step Function with its own trigger:

```
IBKR  — schedule / manual trigger → ibkr_fetch → ibkr_transform → consolidate → allocate
T212  — schedule / manual trigger → t212_fetch → t212_transform → consolidate → allocate
XTB   — file arrival trigger      → xtb_fetch  → xtb_transform  → consolidate → allocate
```

Every connector goes through its own `fetch.py` to produce a raw Delta table
row.  Consolidate and allocate read **all** normalized tables (not just the
connector that triggered), so the output is always a complete picture.
Running them multiple times per window is fine — they are idempotent
overwrites.

### Why per-connector Step Functions?

- **Independent triggers** — XTB is file-driven (no API), IBKR/T212 are
  API-driven.  Mixing them in one monolithic `full` command means XTB blocks
  the others or requires manual file upload before every run.
- **Per-broker retry** — one broker's flaky API doesn't block the others.
- **Step-level observability** — each step reports duration and status in the
  AWS console.
- **Event-driven XTB** — file arrival triggers the pipeline automatically,
  no manual workflow dispatch needed.

## XTB upload flow

The raw Delta table is the system of record.  The `.xlsx` bytes live inside
an encrypted `payload` column in a Parquet file managed by Delta Lake — there
is no permanent S3 landing bucket duplicating data.

### Option A — Direct trigger (no S3 staging)

```
upload-xtb  →  fetch.py  →  append to raw Delta table
             →  start Step Function execution (AWS SDK)
```

- `upload-xtb` reads the local `.xlsx`, calls `fetch_snapshot()` /
  `fetch_cdc()`, and appends to the raw Delta table (S3 or local depending
  on config).
- Then starts the XTB Step Function execution directly.
- No S3 staging area, no EventBridge rule, no transient files.
- Requires Step Functions permissions on the local CLI.

### Option B — S3 staging + EventBridge (transient, event-driven)

```
upload-xtb      →  uploads .xlsx to s3://bucket/staging/xtb/<filename>
                →  (returns immediately)

EventBridge      →  detects s3:ObjectCreated in staging/xtb/
                →  triggers XTB Step Function

Step Function    →  xtb_fetch: reads .xlsx from S3 staging, encrypts,
                    appends to raw Delta table
                →  xtb_transform: decrypts + parses, writes normalized
                →  consolidate: merges all normalized tables
                →  allocate: calculates allocations
                →  (optional) clean up staging file
```

- More "DE portfolio impressive" — shows event-driven architecture.
- `upload-xtb` only needs S3 write permissions; it doesn't start Step
  Functions itself.
- The staging prefix is **ephemeral** — files are cleaned up after fetch
  succeeds.  The raw Delta table is the permanent storage.
- Requires an EventBridge rule and IAM permissions for S3 → Step Functions.

**Recommendation:** Option B for portfolio value.  The event-driven pattern
demonstrates real DE skills (S3 triggers, Step Functions, IAM isolation).
Option A is a valid fallback for local development or simpler deployments.

## Current flow (painful)

1. Log into XTB portal → download report
2. Log into AWS console → find bucket → upload file
3. Go to GitHub → trigger workflow → type/paste S3 path

## Target flow (one command)

```
python -m pipeline upload-xtb ~/Downloads/XTB_Report.xlsx
```

This uploads the file and the pipeline picks it up automatically.

## Implementation steps

### 1. CLI command: `pipeline upload-xtb`

- New subcommand in `pipeline/run.py`
- For **Option A**: reads the local `.xlsx`, calls `fetch_snapshot()` /
  `fetch_cdc()`, appends to the raw Delta table, then starts the XTB Step
  Function via AWS SDK.
- For **Option B**: uploads the `.xlsx` to
  `s3://<bucket>/staging/xtb/<filename>`.  EventBridge handles the rest.
- Uses existing `resolve_aws_credentials()` and `get_storage()` from
  `pipeline/secrets.py` and `pipeline/storage.py`.
- Outputs the S3 URI (Option B) or confirmation message (Option A).

### 2. XTB fetch: accept S3 URIs (Option B only)

- Modify `pipeline/connectors/xtb/fetch.py` to accept `s3://` paths.
- Download the file from S3 to a temp location using existing S3 credentials.
- Parse it with the existing parser (unchanged).
- Clean up the staging file after successful fetch (optional, via Step
  Function cleanup step).

### 3. EventBridge rule (Option B only)

- S3 ObjectCreated event on `<bucket>/staging/xtb/` prefix.
- Target: XTB Step Function.

### 4. Step Function: XTB pipeline

- State machine: `xtb_fetch → xtb_transform → consolidate → allocate`
- Each step runs as a separate ECS Fargate task (same Docker image,
  different subcommand).
- Per-step retry with exponential backoff.
- Catches and isolates failures (XTB down doesn't block IBKR/T212).

### 5. Step Functions: IBKR and T212 pipelines (separate roadmap)

- IBKR: `ibkr_fetch → ibkr_transform → consolidate → allocate`
  (schedule or manual trigger)
- T212: `t212_fetch → t212_transform → consolidate → allocate`
  (schedule or manual trigger)
- See `docs/roadmap-add-staging.md` for full details.

## Files to touch

| File | Change |
|------|--------|
| `pipeline/run.py` | New `upload-xtb` subcommand |
| `pipeline/connectors/xtb/fetch.py` | Accept `s3://` URIs (Option B) |
| `pipeline/connectors/xtb/connector.py` | Pass S3 creds to fetch if needed |
| `.github/workflows/pipeline.yml` | Remove `xtb-s3-path` input (not needed) |
| `infra/main.tf` | EventBridge rule + Step Function (Option B) |

## Why not alternatives

| Approach | Problem |
|----------|---------|
| Base64 in workflow input | 10MB GitHub limit, awkward to paste |
| Manual S3 upload | Requires AWS console login (the pain point) |
| HTTP URL input | Still need to host the file somewhere |
| Landing bucket (permanent) | Duplicates data — raw Delta table is the system of record |