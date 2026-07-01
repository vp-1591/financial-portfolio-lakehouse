# 0032: Docker support and query CLI subcommand

## Context

The pipeline currently requires manual Python environment setup (venv, pip install,
.env, keygen). For a portfolio project, "easy to launch" is a key goal. Docker
provides a one-command experience: `docker compose run --rm pipeline full`.

Additionally, the `pipeline/query.py` module has a rich Python API
(`get_connection()`, `decrypt_df()`, `list_tables()`) but no CLI entrypoint.
Reviewers who run the pipeline want to see the results, but without a query
command they need to write Python code to inspect Delta tables.

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

Single `pipeline` service:

- `env_file: .env` (with `required: false`) injects secrets at runtime; `.env` is optional so `keygen` works before secrets are configured
- `PIPELINE_DATA_DIR=/app/data` for explicit path resolution
- Volume mounts: `./data:/app/data`, `./.secrets:/app/.secrets`
- No command override — subcommands specified via `docker compose run --rm pipeline <command>`

### query subcommand

Add `pipeline query <SQL> [--decrypt] [--format table|csv|json]`:

- Executes SQL against Delta tables via DuckDB
- `--decrypt` auto-detects and decrypts Fernet-encrypted binary columns
- `--format` controls output (default: human-readable table)
- Calls `refresh()` before query to discover new tables

### CI smoke test

Add a `docker` job to ci.yml that builds the image and verifies `--help`,
`query --help`, and `keygen` work.

## Consequences

- Image size ~400–500MB (deltalake + pyarrow + duckdb native libraries)
- XTB `--xtb-file` paths must be container-relative (mount reports directory)
- Encryption key can come from `ENCRYPTION_KEY` env var (recommended in Docker)
  or `.secrets/encryption.key` file (via volume mount); `keygen` works without
  `.env` because `env_file` uses `required: false`
- Container runs as non-root user (UID 1000)
- `refresh()` call before every query ensures table discovery is current
- `.dockerignore` ensures no secrets or local data enter the image
- Docker build requires `pipeline/` and `pyproject.toml` in the build context
- The `PYTHONPATH=/app` env var is necessary so that `PROJECT_ROOT` resolves
  correctly for `.env` loading and data directory defaults

## Validation

- Docker build succeeds: `docker build -t pipeline .`
- Container starts: `docker run --rm pipeline --help`
- keygen runs: `docker run --rm pipeline keygen`
- Query subcommand works: `docker run --rm pipeline query --help`
- All existing tests pass (231 tests)
- Query subcommand tests pass (7 tests)
- CI docker job passes on GitHub Actions