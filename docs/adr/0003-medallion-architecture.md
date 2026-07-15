# 0003: Medallion Architecture Pipeline

> **Superseded by [ADR 0084](./0084-encrypt-gold-value-columns.md)** — The "Analytics layer: no encryption" clause is superseded by ADR 0084, which encrypts gold value columns as `pa.binary()` while keeping metadata columns plaintext.
>
> **Drifted** — Pipeline has expanded well beyond the ADR's scope: new modules (s3.py, secrets.py, storage.py, report/, analytics/quality|cdc|holdings|models, normalized/consolidate_cdc, connectors/transform_utils), new CLI commands (validate, report, upload-xtb, run-connector, run-consolidate-analytics), expanded BrokerConnector protocol, and paths.py is now deprecated.

## Context

The investment portfolio dashboard was a set of CLI scripts that fetch live broker
data, normalize it, convert currencies, and print a percentage table — with no
persistence, no history, and no data pipeline. To enable historical tracking,
CDC (change data capture) from broker transaction history, and dashboard
readiness, we needed a proper data pipeline.

## Decision

Replaced the broker-specific CLI scripts with a decoupled pipeline package
using a medallion-style architecture named by purpose rather than tier labels:

- **`raw/`** — encrypted, append-only broker payloads (immutable source of truth)
- **`normalized/`** — parsed, deduplicated, schema-enforced data with encrypted
  financial columns
- **`analytics/`** — portfolio allocation percentages, ready for dashboards

The pipeline uses:

- **Delta tables** (via `deltalake`) for ACID-guaranteed storage with DuckDB
  queryability
- **Fernet encryption** for financial value columns at the normalized layer and
  entire payloads at the raw layer
- **BrokerConnector protocol** for decoupled, self-contained broker connectors
  under `pipeline/connectors/`
- **PyArrow schemas** for type-safe table definitions
- **Connector registry** for auto-discovery and registration of new brokers

### Directory structure

```
pipeline/
  connectors/          # Decoupled broker connectors (ibkr, trading212, xtb)
  raw/                 # Raw table schemas and ingestion logic
  normalized/          # Normalized schemas and consolidation
  analytics/           # Analytics schema and allocation
  crypto.py            # Fernet encrypt/decrypt (only module importing cryptography)
  keygen.py             # CLI: generate encryption key
  paths.py             # Path constants for data/ directories
  query.py             # DuckDB query helpers with decryption
  run.py               # Unified CLI (fetch / transform / allocate / full)
```

### Connector design

Each broker is a self-contained package implementing the `BrokerConnector`
protocol with `fetch_snapshot()`, `fetch_cdc()`, `transform_snapshot()`, and
`transform_cdc()` methods. Adding a new broker requires creating a new directory
under `pipeline/connectors/` and calling `register()` — no changes to the main
pipeline code.

### CDC scope

- **Trading 212**: Snapshot + CDC (orders, dividends, transactions)
- **XTB**: Snapshot + CDC (cash operations from XLS)
- **IBKR**: Snapshot only; CDC schema stub exists, fetcher raises `NotImplementedError`

### Encryption

- Raw layer: entire `payload` column is Fernet-encrypted
- Normalized layer: only `value`/`amount`/`quantity` columns are encrypted
- Analytics layer: no encryption
- Key stored in `.secrets/encryption.key` (gitignored) or `ENCRYPTION_KEY` env var

### What was removed

The following scripts were removed because their logic was migrated into the
pipeline connectors:

- `scripts/ibkr_net_worth.py` → `pipeline/connectors/ibkr/client.py`, `fetch.py`, `transform.py`
- `scripts/trading212_net_worth.py` → `pipeline/connectors/trading212/client.py`, `fetch.py`, `transform.py`
- `scripts/xtb_net_worth.py` → `pipeline/connectors/xtb/parser.py`, `fetch.py`, `transform.py`
- `scripts/portfolio_connectors.py` → `pipeline/normalized/consolidate.py`, `pipeline/analytics/allocation.py`

`scripts/portfolio_percentages.py` is preserved but will read from the analytics
Delta table instead of fetching live data.

### Old tests replaced

The following test files were removed because they tested the old script-based
approach and have been replaced by pipeline-targeted tests:

- `tests/test_ibkr_net_worth.py` → `tests/test_ibkr_connector.py`
- `tests/test_trading212_net_worth.py` → `tests/test_trading212_connector.py`
- `tests/test_xtb_net_worth.py` → `tests/test_xtb_connector.py`
- `tests/test_portfolio_percentages.py` → `tests/test_consolidate.py`

New test files added:

- `tests/conftest.py` — shared Fernet key and temp directory fixtures
- `tests/test_crypto.py` — Fernet encrypt/decrypt roundtrip tests
- `tests/test_connector_registry.py` — connector registry and discovery tests
- `tests/test_consolidate.py` — currency conversion, aggregation, ISIN override tests

### Dependencies

Added `pyproject.toml` with optional pipeline dependencies (`deltalake`,
`duckdb`, `cryptography`, `pyarrow`). Existing `pytest.ini` removed in favor
of `[tool.pytest.ini_options]` in `pyproject.toml`.

### `.gitignore` updates

Added `data/` and `.secrets/` to exclude pipeline data and encryption keys from
version control.

## Consequences

- Historical portfolio data is now persisted in Delta tables, enabling time-series
  analysis and dashboard queries
- Broker connectors are decoupled: adding a new broker requires only a new
  directory under `pipeline/connectors/`
- Financial values are encrypted at rest in raw and normalized layers
- DuckDB can query Delta tables directly for ad-hoc analysis
- IBKR CDC is not yet implemented (schema stub only)
- The old `scripts/` files still exist but are superseded by the pipeline;
  they will be removed once `portfolio_percentages.py` is updated to read from
  the analytics table
- `pytest.ini` is replaced by `pyproject.toml` config

## Validation

- All 117 tests pass (14 crypto + 12 IBKR + 22 Trading 212 + 11 XTB +
  6 registry + 16 consolidation + 6 original scripts)
- `python -m pipeline.keygen` creates `.secrets/encryption.key`
- Connector protocol is enforced via the `BrokerConnector` runtime-checkable
  protocol
- Delta table schemas use `pa.timestamp("us", tz="UTC")` (PyArrow-compatible)
- `deltalake.write_deltalake()` used for all Delta writes (API compatibility
  verified with deltalake 1.6.0)