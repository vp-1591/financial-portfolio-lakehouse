# XTB Report → Cloud Pipeline: Upload Roadmap

## Phase 1 — ✅ Completed

Code changes for S3 staging upload and S3-aware fetch (no infrastructure):

- `pipeline/storage.py` — `staging_path()` on `StorageBackend` protocol, `S3Backend`, `LocalBackend`, and `StorageConfig`; uses `staging`/`staging_demo` prefix for production/demo isolation
- `pipeline/s3.py` — New module: `parse_s3_uri()`, `upload_to_staging()`, `read_s3_bytes()`, `delete_from_staging()` using PyArrow's `S3FileSystem`
- `pipeline/connectors/xtb/fetch.py` — `_read_file_bytes()` helper dispatches on `s3://` prefix; both `fetch_snapshot` and `fetch_cdc` accept S3 URIs
- `pipeline/run.py` — `upload-xtb` subcommand uploads `.xlsx` to S3 staging; `--xtb-file` help mentions S3 URI support
- `docs/adr/0048-xtb-cloud-upload.md` — ADR documenting the decision

## Goal

Pass an XTB `.xlsx` report to the cloud pipeline without manual AWS console
logins or S3 uploads — and make XTB an event-driven connector that triggers
the downstream pipeline automatically.

## Architecture

XTB is one of three per-connector Step Functions described in
`docs/roadmaps/0001-productionization.md`.  The XTB-specific trigger is file arrival
(S3 ObjectCreated → EventBridge → Step Function), while IBKR and T212 use
schedule/manual triggers.

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

Step Function    →  xtb_fetch+transform: reads .xlsx from S3 staging,
                    encrypts, appends to raw Delta table, decrypts + parses,
                    writes normalized
                →  consolidate+allocate: merges all normalized tables,
                    calculates allocations
                →  (optional) clean up staging file
```

- Event-driven architecture — S3 triggers Step Functions automatically.
- `upload-xtb` only needs S3 write permissions; it doesn't start Step
  Functions itself.
- The staging prefix is **ephemeral** — files are cleaned up after fetch
  succeeds.  The raw Delta table is the permanent storage.
- Requires an EventBridge rule and IAM permissions for S3 → Step Functions.

**Recommendation:** Option B.  The event-driven pattern provides automatic
triggering, least-privilege IAM, and ephemeral staging.  Option A is a valid
fallback for local development or simpler deployments.

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

### 4. Step Function + IBKR/T212 pipelines

See `docs/roadmaps/0001-productionization.md` Phase 2 for the full orchestration
plan.  The XTB Step Function follows the same two-task pattern
(`xtb_fetch+transform → consolidate+allocate`) with a file-arrival trigger
instead of a schedule.

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