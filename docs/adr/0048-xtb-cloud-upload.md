# 0048: XTB Cloud Upload — S3 Staging + EventBridge

## Context

The XTB connector requires a local `.xlsx` file passed via `--xtb-file`. To make XTB an event-driven connector that triggers the pipeline automatically when a report is uploaded, we need:

1. A CLI command (`upload-xtb`) to upload `.xlsx` files to S3 staging
2. XTB fetch to accept S3 URIs in addition to local file paths
3. AWS infrastructure (EventBridge + Step Functions) to trigger the pipeline on file arrival

The roadmap at `docs/roadmaps/0002-xtb-cloud-upload.md` describes two options:
- **Option A** — Direct trigger: `upload-xtb` reads the file locally, calls `fetch_snapshot()`, and starts a Step Function execution via AWS SDK
- **Option B** — S3 staging + EventBridge: `upload-xtb` uploads to S3 staging, EventBridge detects file arrival and triggers the Step Function

## Decision

We chose **Option B** (S3 staging + EventBridge) for the following reasons:

1. **Portfolio value** — demonstrates event-driven architecture, a core data engineering skill
2. **Least privilege** — `upload-xtb` only needs S3 write permissions; it does not need Step Functions permissions
3. **Ephemeral staging** — the staging prefix is transient; files are cleaned up after successful fetch. The raw Delta table is the system of record
4. **Demo isolation** — production uses `staging/` prefix, demo uses `staging_demo/`, consistent with the existing `pipeline`/`pipeline_demo` pattern

Phase 1 (this ADR) implements the code changes only. Infrastructure (EventBridge, Step Functions, ECS) is Phase 2.

## Consequences

- `pipeline/s3.py` provides S3 helpers using PyArrow's `S3FileSystem` (already a dependency) — no new dependencies for Phase 1
- `pipeline/connectors/xtb/fetch.py` now accepts both local paths and `s3://` URIs via `_read_file_bytes()` — backward compatible
- `pipeline/storage.py` gains `staging_path()` on both backends and `StorageConfig` — production paths use `staging/`, demo paths use `staging_demo/`
- `upload-xtb` subcommand validates S3 storage is configured before uploading
- `--xtb-file` help text now mentions S3 URI support
- Phase 2 will add boto3 as a dependency when Step Functions are introduced

## Validation

- All existing tests pass
- New tests for `parse_s3_uri`, `staging_path`, and S3 URI fetch (mocked)
- `ruff check --fix .` and `ruff format .` pass cleanly
- Demo isolation: staging paths use `staging_demo/` prefix in demo mode