# 0032: Docker support and query CLI subcommand

## Context

The pipeline currently requires manual Python environment setup (venv, pip install,
.env, keygen). For a portfolio project, "easy to launch" is a key goal. Docker
provides a one-command experience: `docker compose run --rm pipeline full`.

Additionally, the `pipeline/query.py` module has a rich Python API
(`get_connection()`, `decrypt_df()`, `list_tables()`) but no CLI entrypoint.
Reviewers who run the pipeline want to see the results, but without a query
command they need to write Python code to inspect Delta tables.

Docker volume mounts on Windows (NTFS via virtiofs) do not support the atomic
renames that Delta Lake's `object_store` crate requires for both data file
writes and commit log operations. This causes "Upload aborted" errors on
Docker Desktop for Windows. MinIO (S3-compatible storage) avoids this entirely
by using the S3 protocol instead of local filesystem operations.

## Decision

### Dockerfile (multi-stage)

Add a multi-stage Dockerfile:

- Stage 1 (builder): installs `[pipeline]` dependencies into site-packages
- Stage 2 (runtime): copies installed packages and source, runs as non-root
  `pipeline` user (UID 1000)
- Base image: `python:3.11-slim-bookworm` (glibc required by deltalake/pyarrow)
- `PYTHONPATH=/app` ensures `PROJECT_ROOT` resolves to `/app/`, so `.env`,
  `data/`, and `.secrets/` paths work correctly inside the container
- Entry point: `python -m pipeline.run`

### .dockerignore

Excludes `.env`, `.secrets`, `data/`, `.git`, `tests/`, `docs/`, `terraform/`,
and build artifacts. No secrets or local data enter the image.

### docker-compose.yml

Three services: `minio`, `create-bucket`, and `pipeline`.

MinIO service:

- `minio/minio` image with `/data` volume and health check
- Console on port 9001, API on port 9000
- Default credentials: `minioadmin` / `minioadmin`

Bucket creation:

- `minio/mc` init container that creates the `pipeline` bucket
- Runs after MinIO is healthy

Pipeline service:

- `depends_on: create-bucket` ensures bucket exists before pipeline starts
- `env_file: .env` (with `required: false`) — secrets are optional at startup
- S3 environment variables route all Delta table operations through MinIO:
  `S3_BUCKET=pipeline`, `S3_ENDPOINT_URL=http://minio:9000`, `S3_ALLOW_HTTP=true`
- Volume mount for `.secrets/` (keygen key file)
- No `data/` bind mount — data lives in MinIO's Docker volume

### S3Backend MinIO support

Add `S3_ENDPOINT_URL` and `S3_ALLOW_HTTP` environment variables to
`S3Backend.storage_options` and DuckDB's S3 SECRET configuration:

- `S3_ENDPOINT_URL` maps to `aws_endpoint_url` in deltalake storage options
  and `ENDPOINT` + `URL_STYLE 'path'` in DuckDB's S3 SECRET
- `S3_ALLOW_HTTP=true` maps to `aws_allow_http` in deltalake and
  `USE_SSL false` in DuckDB's S3 SECRET

### LocalBackend robustness

- `LocalBackend.storage_options` returns `{"allow_unsafe_rename": "true"}` for
  filesystems that don't support atomic renames (safe for single-writer usage)
- `LocalBackend.ensure_parent` rescues orphaned parquet files from failed
  writes: if a table directory has parquet files but no `_delta_log/`, the
  entire directory is moved to `.rescue/<table_name>_<timestamp>/` under the
  data directory so the next `write_deltalake` starts fresh, while keeping
  the orphaned data recoverable

### query subcommand

Add `pipeline query <SQL> [--decrypt] [--format table|csv|json]`:

- Executes SQL against Delta tables via DuckDB
- `--decrypt` auto-detects and decrypts Fernet-encrypted binary columns
- `--format` controls output (default: human-readable table)
- Calls `refresh()` before query to discover new tables
- S3-compatible: queries work against both local and MinIO storage

### CI smoke test

Add a `docker` job to ci.yml that builds the image and verifies `--help`,
`query --help`, and `keygen` work.

## Consequences

- Data is stored in MinIO (S3-compatible) instead of a bind-mounted `data/`
  directory — no NTFS filesystem issues on Docker Desktop for Windows
- MinIO console available at http://localhost:9001 for data browsing
- `S3_ENDPOINT_URL` and `S3_ALLOW_HTTP` enable MinIO or other S3-compatible
  stores for local development and CI
- Image size ~400–500MB (deltalake + pyarrow + duckdb native libraries)
- MinIO adds ~100MB to the Docker setup
- XTB `--xtb-file` paths must be container-relative (mount reports directory)
- Encryption key can come from `ENCRYPTION_KEY` env var (recommended in Docker)
  or `.secrets/encryption.key` file (via volume mount)
- Container runs as non-root user (UID 1000)
- `refresh()` call before every query ensures table discovery is current
- `.dockerignore` ensures no secrets or local data enter the image
- Docker build requires `pipeline/` and `pyproject.toml` in the build context
- The `PYTHONPATH=/app` env var is necessary so that `PROJECT_ROOT` resolves
  correctly for `.env` loading and data directory defaults
- `LocalBackend` changes (allow_unsafe_rename, orphan rescue to `.rescue/`) remain
  for local development on Windows without Docker

## Validation

- Docker build succeeds: `docker build -t pipeline .`
- Container starts: `docker run --rm pipeline --help`
- keygen runs: `docker run --rm pipeline keygen`
- Query subcommand works: `docker run --rm pipeline query --help`
- MinIO bucket creation succeeds
- Pipeline writes to and reads from MinIO via S3Backend
- All existing tests pass (236 tests)
- CI docker job passes on GitHub Actions