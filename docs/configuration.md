# Configuration

## Environment Variables

Secrets (API keys, encryption keys) are **never stored in config files or S3.**
They come from environment variables, set by one of two sources:

1. **`.env` file (local dev)** — create a `.env` file in the project root
   (gitignored) with your secrets. The pipeline loads it automatically at
   startup via `python-dotenv`.

2. **GitHub Secrets (CI)** — set in your repository settings. The pipeline
   workflow injects them as environment variables at runtime.

See [`.env.example`](../.env.example) for the complete list of variables with
inline documentation.

### Variable categories

- **Broker secrets** — `IBKR_FLEX_TOKEN`, `T212_API_KEY`, `T212_API_SECRET`, and
  their `_ENABLED` toggles
- **Encryption** — `ENCRYPTION_KEY` (Fernet key, generated via `keygen` command)
- **Storage** — `STORAGE_TYPE`, `S3_BUCKET`, AWS credentials, S3 endpoint
- **Demo mode** — `DEMO` toggle and `_DEMO`-suffixed variables for each secret

### Connector toggles

All connectors are **enabled by default**. Set a toggle to `0`, `false`, or
`no` to disable it:

- `IBKR_ENABLED` — IBKR Flex Web Service
- `T212_ENABLED` — Trading 212 API
- `XTB_ENABLED` — XTB Excel report upload

## Broker Setup

### IBKR Flex Web Service

IBKR data is fetched through the Flex Web Service API — no local gateway
process or browser login is required. Data has a 15–30 minute delay compared
to real-time positions.

To set up: log in to [IBKR Client Portal](https://portal.interactivebrokers.com),
navigate to **Performance & Reports → Flex Queries**, create an **Activity Flex
Query** named `get-open-positions` with the Open Positions and Account
Information fields you need, set Format to XML and Period to Last Business Day.
Enable **Flex Web Service Configuration** and generate a token.

Required environment variables: `IBKR_FLEX_TOKEN`, `IBKR_FLEX_QUERY_ID`,
`IBKR_FLEX_BASE_URL` (optional, has a default).

For detailed field configuration, see:
- [Flex Query Required Fields](ibkr/flex-query-required-fields.md)
- [Flex Query Required Fields (CDC)](ibkr/flex-query-required-fields-cdc.md)

### Trading 212 API

Trading 212 provides a REST API for retrieving account data, positions, and
instruments. Requires an API key and secret.

Required environment variables: `T212_API_KEY`, `T212_API_SECRET`,
`T212_BASE_URL` (auto-derived from `DEMO` setting).

For API key permissions, see [API Key Permissions](trading212/api-key-permissions.md).

### XTB

XTB does not provide a live API. Data is ingested from Excel report exports
uploaded to the `xtb-report-sample/` directory. No API key is required.

Required environment variable: `XTB_ENABLED` (optional, enabled by default).

## Storage Configuration

### Local filesystem

Default storage. Delta tables are written to the `data/` directory in the
project root. No additional configuration needed.

### MinIO (Docker)

When running with Docker Compose, the pipeline uses MinIO (an S3-compatible
store) running in a separate container. Data persists in the `minio-data`
Docker volume. MinIO console at http://localhost:9001 (login: `minioadmin` /
`minioadmin`).

Set `STORAGE_TYPE=minio` and configure `S3_ENDPOINT_URL` and `S3_ALLOW_HTTP=true`
in your `.env` file.

### AWS S3

When `S3_BUCKET` is set, the pipeline uses `S3Backend` to store Delta tables
in S3. AWS credentials come from `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, and `AWS_REGION`. No additional dependencies
are needed — `deltalake` handles S3 natively via its Rust `object_store`
crate.

For S3-compatible stores like MinIO, set `S3_ENDPOINT_URL` to the server
URL (e.g. `http://minio:9000`) and `S3_ALLOW_HTTP=true` to allow non-HTTPS
connections.

The `keygen` command only works in local mode. For S3, set
`ENCRYPTION_KEY` as an environment variable — **the encryption
key is never stored in S3.**

## Demo Mode

Set `DEMO=true` to run in demo mode. This uses `_DEMO`-suffixed environment
variables (e.g. `IBKR_FLEX_TOKEN_DEMO`, `ENCRYPTION_KEY_DEMO`) and a separate
storage prefix. There is no fallback between demo and production variables —
each mode reads only its own set.
