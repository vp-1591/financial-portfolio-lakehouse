# Investment Portfolio Dashboard

Utilities for consolidating broker assets into a single portfolio view.

## Medallion Pipeline

The `pipeline/` package implements a medallion architecture (raw → normalized →
analytics) with Delta tables and Fernet encryption for sensitive financial data.

### Data flow

```mermaid
flowchart TD
    classDef source fill:#4472c4,stroke:#2f5496,color:#fff
    classDef raw fill:#ed7d31,stroke:#c55a11,color:#fff
    classDef norm fill:#70ad47,stroke:#4f7d28,color:#fff
    classDef fx fill:#7030a0,stroke:#5b2280,color:#fff
    classDef analytics fill:#5b9bd5,stroke:#2e75b6,color:#fff

    IBKR["IBKR Flex Web Service<br/>positions · cash"]:::source
    T212["Trading 212 API<br/>account · positions · instruments"]:::source
    XTB["XTB Excel Report<br/>open positions · cash ops"]:::source

    IBKR -->|"encrypt payloads"| RS["ibkr<br/>snapshot · cdc"]:::raw
    T212 -->|"encrypt payloads"| RT["trading212<br/>snapshot · cdc"]:::raw
    XTB -->|"parse & encrypt"| RX["xtb<br/>snapshot · cdc"]:::raw

    RS -->|"parse · normalize · re-encrypt values"| NS["ibkr_snapshot"]:::norm
    RT -->|"parse · normalize · re-encrypt values"| NT["trading212_snapshot"]:::norm
    RX -->|"parse · normalize · re-encrypt values"| NX["xtb_snapshot"]:::norm

    NS -->|"convert currency · normalize tickers · ISIN overrides"| CH["consolidated_holdings"]:::norm
    NT --> CH
    NX --> CH

    FX1["Frankfurter API"]:::fx -->|"FX rates"| CH
    FX2["Yahoo Finance"]:::fx -->|"fallback"| CH

    CH -->|"sum values · calculate %"| PA["portfolio_allocation"]:::analytics
```

Each layer stores data in Delta tables under `data/`:

| Layer | Node color | Table | Contents |
|-------|-----------|-------|----------|
| 🔵 Sources | Blue | — | Broker APIs and files |
| 🟠 Raw | Orange | `raw/{broker}_snapshot` | Encrypted API payloads with fetch metadata |
| 🟠 Raw | Orange | `raw/{broker}_cdc` | Encrypted change-data-capture payloads |
| 🟢 Normalized | Green | `normalized/{broker}_snapshot` | Structured positions & cash rows; financial values remain Fernet-encrypted |
| 🟢 Normalized | Green | `normalized/consolidated_holdings` | Cross-broker holdings converted to target currency; financial values remain Fernet-encrypted |
| 🟣 FX Rates | Purple | — | Frankfurter API (primary) / Yahoo Finance (fallback) |
| 🔵 Analytics | Light blue | `analytics/portfolio_allocation` | Ticker percentages by broker |

### IBKR Flex Web Service

IBKR data is fetched through the Flex Web Service API — no local gateway
process or browser login is required. Data has a 15–30 minute delay compared
to real-time positions.

#### IBKR Flex Query setup

1. Log in to [IBKR Client Portal](https://portal.interactivebrokers.com).
2. Navigate to **Performance & Reports → Flex Queries**.
3. Click the **+** icon in the **Activity Flex Query** section to create a new
   query named `get-open-positions`.
4. In the **Open Positions** section, select these fields: Account ID, Currency,
   FX Rate To Base, Asset Class, Symbol, Description, Conid, ISIN, Listing
   Exchange, Report Date, Quantity, Mark Price, Position Value, Cost Basis
   Price, Cost Basis Money, Percent of NAV, Unrealized P/L, Side.
5. In the **Account Information** section, select: Net Liquidation Value,
   Cash Balance, Currency.
6. Set **Format** to XML, **Period** to Last Business Day, and
   **Include Currency Rates** to Yes.
7. Click **Continue → Create** and note the **Query ID** (a numeric ID).
8. On the same page, click the **gear icon ⚙️** next to **Flex Web Service
   Configuration**, toggle it to **Enable**, and click **Generate A New Token**.
   Copy the token immediately — it is shown only once.

### Setup

Create a venv and install dependencies:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[pipeline]"
```

Generate an encryption key (only needed once):

```powershell
.venv\Scripts\python -m pipeline.run keygen
```

### Docker

Run the pipeline in a container without installing Python. Data is stored in
MinIO (S3-compatible storage) to avoid filesystem compatibility issues with
Docker volume mounts on Windows.

```bash
# Build and start MinIO + pipeline
docker compose build
docker compose up minio -d   # start MinIO in background

# First-time setup: generate encryption key
docker compose run --rm pipeline keygen

# Run the full pipeline
docker compose run --rm pipeline full

# Query Delta tables (human-readable)
docker compose run --rm pipeline query "SELECT * FROM portfolio_allocation_analytics"

# Query with decryption
docker compose run --rm pipeline query "SELECT * FROM ibkr_snapshot_normalized" --decrypt

# Export as CSV
docker compose run --rm pipeline query "SELECT * FROM portfolio_allocation_analytics" --format csv

# MinIO console (browse data at http://localhost:9001)
# Login: minioadmin / minioadmin
```

Data persists in the `minio-data` Docker volume. Secrets come from `.env`
(via `env_file` in docker-compose). MinIO credentials are configured in
`docker-compose.yml` (default: `minioadmin` / `minioadmin`).

### Secrets Management

**Secrets (API keys, encryption keys) are never stored in config files or S3.**
They come from environment variables, set by one of two sources:

1. **`.env` file (local dev)** — create a `.env` file in the project root
   (gitignored) with your secrets. The pipeline loads it automatically at
   startup via `python-dotenv`:

   ```bash
   # .env (never committed — copy from .env.example)
   IBKR_FLEX_TOKEN=your_token_here
   IBKR_FLEX_QUERY_ID=your_query_id_here
   T212_API_KEY=your_key_here
   T212_API_SECRET=your_secret_here
   ENCRYPTION_KEY=your_fernet_key_here
   ```

2. **GitHub Secrets (CI)** — set in your repository settings. The pipeline
   workflow injects them as environment variables at runtime.

Environment variables always take priority over `.env` file values.

| Variable | Purpose |
|----------|---------|
| `IBKR_FLEX_TOKEN` | IBKR Flex Web Service token *(required)* |
| `IBKR_FLEX_QUERY_ID` | IBKR Flex Query ID *(required)* |
| `IBKR_FLEX_BASE_URL` | IBKR Flex Web Service base URL (default: `https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService`) |
| `IBKR_ENABLED` | Enable/disable IBKR connector (default: enabled) |
| `T212_API_KEY` | Trading 212 API key *(required)* |
| `T212_API_SECRET` | Trading 212 API secret *(required)* |
| `T212_BASE_URL` | Trading 212 API base URL (auto-derived from `DEMO`) |
| `T212_ENABLED` | Enable/disable Trading 212 connector (default: enabled) |
| `XTB_ENABLED` | Enable/disable XTB connector (default: enabled) |
| `ENCRYPTION_KEY` | Fernet key for encrypting financial values *(required)* |
| `DEMO` | Run in demo mode — uses `_DEMO` secrets and separate storage (default: `false`) |
| `STORAGE_TYPE` | Storage backend: `cloud`, `minio`, or `local` (default: `cloud` if `S3_BUCKET` set, `local` otherwise) |
| `S3_BUCKET` | S3 bucket for cloud storage (required when `STORAGE_TYPE=cloud`) |
| `AWS_ACCESS_KEY_ID` | AWS credential for S3 |
| `AWS_SECRET_ACCESS_KEY` | AWS credential for S3 |
| `AWS_REGION` | AWS region (default: `eu-west-1`) |
| `S3_ENDPOINT_URL` | Custom S3 endpoint (for MinIO or other S3-compatible stores) |
| `S3_ALLOW_HTTP` | Allow non-HTTPS connections to S3 endpoint (set to `true` for MinIO) |

**Demo mode variables** (used only when `DEMO=true`):

| Variable | Purpose |
|----------|---------|
| `IBKR_FLEX_TOKEN_DEMO` | IBKR Flex token for demo mode |
| `IBKR_FLEX_QUERY_ID_DEMO` | IBKR Flex Query ID for demo mode |
| `T212_API_KEY_DEMO` | Trading 212 API key for demo mode |
| `T212_API_SECRET_DEMO` | Trading 212 API secret for demo mode |
| `ENCRYPTION_KEY_DEMO` | Fernet key for demo mode |
| `S3_BUCKET_DEMO` | S3 bucket for demo data (defaults to `{S3_BUCKET}-demo`) |
| `S3_PREFIX_DEMO` | S3 prefix for demo data (defaults to `pipeline_demo`) |
| `AWS_ACCESS_KEY_ID_DEMO` | AWS credential for demo S3 |
| `AWS_SECRET_ACCESS_KEY_DEMO` | AWS credential for demo S3 |

All connectors are **enabled by default**. Set a toggle to `0`, `false`, or
`no` to disable it.

### Cloud Storage (S3)

When `S3_BUCKET` is set, the pipeline uses `S3Backend` to store Delta tables
in S3. AWS credentials come from `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, and `AWS_REGION`. No additional dependencies
are needed — `deltalake` handles S3 natively via its Rust `object_store`
crate.

For S3-compatible stores like MinIO, set `S3_ENDPOINT_URL` to the server
URL (e.g. `http://minio:9000`) and `S3_ALLOW_HTTP=true` to allow non-HTTPS
connections. The Docker setup uses MinIO by default — see the Docker section.

The `keygen` command only works in local mode. For S3, set
`ENCRYPTION_KEY` as an environment variable — **the encryption
key is never stored in S3.**

### Configuration

All configuration is through environment variables. No config files needed —
set env vars in your shell, `.env` file, or GitHub Actions workflow.

Connectors are **enabled by default**. Set `IBKR_ENABLED=0`, `T212_ENABLED=0`,
or `XTB_ENABLED=0` to disable a connector.

See the [Secrets Management](#secrets-management) section for the full list of
environment variables.

### Run the pipeline

**Local (default — uses `data/` directory):**

```powershell
.venv\Scripts\python -m pipeline.run full
```

**Local with custom data directory:**

```powershell
$env:PIPELINE_DATA_DIR = "C:\path\to\data"
.venv\Scripts\python -m pipeline.run full
```

**Cloud (S3) — set environment variables:**

```powershell
$env:S3_BUCKET = "your-bucket"
$env:AWS_ACCESS_KEY_ID = "..."
$env:AWS_SECRET_ACCESS_KEY = "..."
$env:ENCRYPTION_KEY = "..."
.venv\Scripts\python -m pipeline.run full
```

**GitHub Actions (manual dispatch):**

Go to Actions → Pipeline → Run workflow. Secrets are injected automatically
from GitHub Secrets. See `.github/workflows/pipeline.yml`.

### Querying data

After running the pipeline, query Delta tables with SQL:

```bash
# List all tables (Python API)
python -m pipeline.run query "SHOW TABLES"

# Query with human-readable output
python -m pipeline.run query "SELECT * FROM portfolio_allocation_analytics"

# Decrypt encrypted columns
python -m pipeline.run query "SELECT * FROM ibkr_snapshot_normalized" --decrypt

# Export as CSV
python -m pipeline.run query "SELECT ticker, percentage FROM portfolio_allocation_analytics" --format csv

# Export as JSON
python -m pipeline.run query "SELECT * FROM ibkr_snapshot_normalized" --format json --decrypt
```

Table names follow the `{name}_{layer}` convention:

| Table | Layer |
|-------|-------|
| `ibkr_snapshot_raw` | Raw |
| `ibkr_snapshot_normalized` | Normalized |
| `trading212_snapshot_raw` | Raw |
| `trading212_snapshot_normalized` | Normalized |
| `xtb_snapshot_raw` | Raw |
| `xtb_snapshot_normalized` | Normalized |
| `consolidated_holdings_normalized` | Normalized |
| `portfolio_allocation_analytics` | Analytics |

Use `--decrypt` to decode Fernet-encrypted binary columns (financial values
in normalized tables, payloads in raw tables). Without `--decrypt`, encrypted
columns appear as binary data.

### Infrastructure

The `terraform/` directory contains separate Terraform configurations for
production and demo environments:

- **`terraform/prod/`** — production S3 bucket and IAM user
- **`terraform/demo/`** — demo S3 bucket and IAM user (isolated credentials)

The S3 backend bucket name is not committed to the repo (it contains an
account identifier). Copy the sample config and fill in your bucket name:

**Production:**

```bash
cd terraform/prod
cp backend.tf.sample backend.tf
# Edit backend.tf — set bucket to your S3 state bucket name
terraform init
terraform plan
terraform apply
```

**Demo:**

```bash
cd terraform/demo
cp backend.tf.sample backend.tf
# Edit backend.tf — set bucket to your S3 state bucket name
terraform init
terraform plan
terraform apply
```

After applying, store the outputs in GitHub:

| Output | GitHub type | Production | Demo |
|--------|-------------|------------|------|
| `s3_bucket` | Variable | `S3_BUCKET` | `S3_BUCKET_DEMO` |
| `s3_prefix` | Variable | `S3_PREFIX` | `S3_PREFIX_DEMO` |
| `access_key_id` | Secret | `AWS_ACCESS_KEY_ID` | `AWS_ACCESS_KEY_ID_DEMO` |
| `access_key_secret` | Secret | `AWS_SECRET_ACCESS_KEY` | `AWS_SECRET_ACCESS_KEY_DEMO` |

### Tests & Linting

```powershell
.venv\Scripts\python -m pytest
.venv\Scripts\python -m ruff check .
.venv\Scripts\python -m ruff format --check .
```