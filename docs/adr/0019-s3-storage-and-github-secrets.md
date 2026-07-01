# 0019: S3 Storage and GitHub Secrets

## Context

The pipeline previously used Bitwarden Vault CLI (`bw`) for secrets and local
filesystem for data storage. Two problems drove this change:

1. **Bitwarden added complexity** — CI required a `bw` session token, and the
   coding agent had to be blocked from calling `bw`. GitHub Actions already has
   first-class secret management that's simpler and more natural for CI.
2. **No cloud storage** — `StorageBackend` was designed for S3/GCS extension
   but only `LocalBackend` existed. A GitHub Actions workflow can't write to
   the local filesystem, so S3 is needed for production runs.

The user also noted that secrets should never be stored in S3 — the encryption
key comes from `ENCRYPTION_KEY` env var, not from the S3 bucket.

## Decision

1. **Remove Bitwarden completely.** `pipeline/secrets.py` no longer calls `bw`.
   Secrets come from environment variables, set by GitHub Actions (via GitHub
   Secrets) or loaded from a `.env` file (local dev, via `python-dotenv`).
   The `.env` file is gitignored.

2. **Add `S3Backend`** implementing the `StorageBackend` protocol.
   - `table_path()` returns `s3://bucket/prefix/layer/table` URIs
   - `ensure_parent()` is a no-op (S3 doesn't need parent directories)
   - Detection: `S3_BUCKET` env var triggers S3 mode; absent = local mode
   - AWS credentials from standard env vars: `AWS_ACCESS_KEY_ID`,
     `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
   - No `boto3` dependency — `deltalake` handles S3 I/O via its Rust crate

3. **`StorageConfig` fields change from `Path` to `str`.** S3 URIs like
   `s3://bucket/prefix` are not valid `Path` objects (especially on Windows).
   All path construction goes through backend convenience methods
   (`raw_path()`, `normalized_path()`, `analytics_path()`).

4. **Replace all `Path(...).parent.mkdir()` calls** with
   `backend.ensure_parent()`. This is a no-op for S3 and preserves local
   filesystem behavior for `LocalBackend`.

5. **GitHub Actions workflow** with `workflow_dispatch` trigger runs the
   pipeline on demand. Secrets come from GitHub Secrets. S3 configuration
   comes from `S3_BUCKET` (secret) and `S3_PREFIX`/`AWS_REGION` (variables).

6. **Terraform** provisions the S3 bucket and IAM user with least-privilege
   access. The IAM access key goes into GitHub Secrets as
   `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

7. **`query.py` supports S3** — DuckDB S3 credentials are configured via
   `SET s3_access_key_id=...` etc. when the table path starts with `s3://`.

8. **`keygen` command is local-only** — in S3 mode, it prints a message
   telling the user to set `ENCRYPTION_KEY` env var. The encryption
   key is never stored in S3.

9. **Local dev still works** — `LocalBackend` is the default when `S3_BUCKET`
   is not set. `PIPELINE_DATA_DIR` continues to work for custom local paths.
   A `.env` file in the project root provides secrets for local development.

## Consequences

- **Bitwarden is no longer a dependency.** CI runs without `bw` installed.
- **`.claude/hooks/block-secret-access.sh` is deleted.** It blocked `bw` calls
  which no longer exist.
- **`S3Backend` enables cloud pipeline execution** with zero code changes
  beyond setting `S3_BUCKET` + AWS credentials.
- **`StorageConfig` field types change from `Path` to `str`.** This is a
  breaking change for any code that used `config.raw_dir / "table"` instead
  of `config.raw_path("table")`.
- **`inject_secrets()` is no longer a mutating function.** It loads `.env`
  and validates which secrets are available, but doesn't fetch from external
  sources.
- **`.env` file is gitignored.** Local developers create it manually with
  their API keys and encryption key.
- **Secrets are never in S3.** The encryption key (`ENCRYPTION_KEY`)
  comes from GitHub Secrets or `.env`, never from the S3 bucket. Delta table
  values are Fernet-encrypted at rest, and the key to decrypt them is not
  stored alongside the data.
- **Terraform state is local.** For this single-user project, a remote backend
  is not needed yet.

## Validation

- All 235 tests pass
- `TestS3Backend` tests verify URI format and no-op `ensure_parent`
- `TestResolveStorage` tests verify `S3_BUCKET` env var detection
- `TestInjectSecrets` tests verify `.env` loading and env var priority
- Manual end-to-end: set `S3_BUCKET` + AWS creds →
  `python -m pipeline.run transform` writes Delta tables to S3