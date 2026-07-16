# Local Development

## Prerequisites

- Python 3.11+
- Docker (optional, for MinIO)

## Option A: Local venv

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[pipeline]"
.venv\Scripts\python -m pipeline.run keygen   # generate encryption key (once)
```

## Option B: Docker

```bash
docker compose build
docker compose up minio -d
docker compose run --rm pipeline full
docker compose run --rm pipeline query "SELECT * FROM portfolio_holdings_analytics"
```

Data persists in the `minio-data` Docker volume. Secrets come from `.env`
(via `env_file` in docker-compose). MinIO console at http://localhost:9001
(login: `minioadmin` / `minioadmin`).

## Run the pipeline

**Local:**
```powershell
.venv\Scripts\python -m pipeline.run full
```

**Cloud (S3):**
```powershell
$env:S3_BUCKET = "your-bucket"
$env:AWS_ACCESS_KEY_ID = "..."
$env:AWS_SECRET_ACCESS_KEY = "..."
$env:ENCRYPTION_KEY = "..."
.venv\Scripts\python -m pipeline.run full
```

**GitHub Actions (manual dispatch):**
Go to Actions → Pipeline → Run workflow. Secrets are injected automatically
from GitHub Secrets.

## Querying data

```bash
# List all tables
python -m pipeline.run query "SHOW TABLES"

# Query gold table (values are encrypted; use --decrypt for human-readable output)
python -m pipeline.run query "SELECT * FROM portfolio_holdings_analytics" --decrypt

# Query percentage column only (no decryption needed)
python -m pipeline.run query "SELECT ticker, percentage, position_type FROM portfolio_holdings_analytics"

# Decrypt encrypted columns
python -m pipeline.run query "SELECT * FROM ibkr_snapshot_normalized" --decrypt

# Export as CSV or JSON
python -m pipeline.run query "SELECT ticker, percentage FROM portfolio_holdings_analytics" --format csv
```

For the full table naming convention, see [Architecture](../architecture.md#table-naming-convention).
