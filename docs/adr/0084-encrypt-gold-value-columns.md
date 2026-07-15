# 0084: Encrypt Gold Value Columns

> **Supersedes** the "Analytics layer: no encryption" clause in [ADR 0003](./0003-medallion-architecture.md).

## Context

ADR 0003 established the medallion architecture with Fernet encryption at the raw and normalized layers but explicitly stated "Analytics layer: no encryption." This means gold Delta tables (`portfolio_holdings`, `dividend_income`, `interest_income`, `cash_flow_summary`) store financial values as plaintext `pa.float64()`. An attacker with S3 access but no encryption key can read all monetary amounts directly from gold tables — defeating the purpose of encrypting the raw and normalized layers.

Phases 1 and 2 of the gold cleanup roadmap folded `portfolio_allocation` into `portfolio_holdings` and added a `percentage` column. The `percentage` column represents each position's weight in the total portfolio and is derived from `target_value`. This column is non-sensitive metadata — it reveals relative allocation but not absolute monetary amounts.

## Decision

Encrypt gold value columns as `pa.binary()` using `encrypt_float()`, matching the normalized-layer pattern. Only numeric value columns are encrypted; metadata columns remain plaintext so that allocation charts and the positions chart render without the decryption key.

**Encrypted columns (changed from `pa.float64()` to `pa.binary()`):**

| Table | Columns |
|---|---|
| `portfolio_holdings` | `security_value`, `target_value` |
| `dividend_income` | `cash_amount`, `target_value` |
| `interest_income` | `cash_amount`, `target_value` |
| `cash_flow_summary` | `cash_amount`, `target_value` |

**Plaintext columns (unchanged):**

| Table | Columns |
|---|---|
| `portfolio_holdings` | `percentage`, `ticker`, `broker`, `security_ccy`, `target_ccy`, `position_type`, `identifier`, `description`, `calculated_at` |
| `dividend_income` | `calculated_at`, `period_month`, `period_quarter`, `broker`, `ticker`, `isin`, `description`, `security_ccy`, `instrument_ccy`, `target_ccy`, `event_count` |
| `interest_income` | `calculated_at`, `period_month`, `period_quarter`, `broker`, `security_ccy`, `target_ccy`, `event_count` |
| `cash_flow_summary` | `calculated_at`, `period_month`, `period_quarter`, `broker`, `event_type`, `security_ccy`, `target_ccy`, `event_count` |

The report loader decrypts value columns after reading gold tables via DuckDB, so chart and renderer code continues to receive plaintext DataFrames.

Allocation donut charts (`allocation_by_broker`, `allocation_by_currency`) and the positions chart now use the `percentage` column instead of `target_value`, eliminating the need for decryption in those chart functions.

## Constraints

- The `data_quality` table has no financial value columns and is not encrypted.
- Encryption uses the same Fernet key as raw and normalized layers.
- A full `pipeline run full` is required to rebuild gold tables with encrypted values; old plaintext tables will not match the new `pa.binary()` schemas.
- The `percentage` column must remain `pa.float64()` (plaintext) so allocation charts render without the decryption key.

## Consequences

- Gold Delta tables now have binary value columns instead of float columns. DuckDB ad-hoc queries require `--decrypt` to see human-readable values.
- The report loader (`load_all()`) decrypts after loading, so chart and renderer code is unchanged from its perspective — it still receives plaintext floats.
- Schema checks in `pipeline/analytics/quality.py` automatically pick up `pa.binary()` types since they import schemas directly from `models.py`.
- Allocation charts using `percentage` render without decryption, reducing the report's dependency on the encryption key.
- This supersedes ADR 0003's "Analytics layer: no encryption" clause. The analytics layer now encrypts value columns but keeps metadata columns plaintext.

## Validation

- `pipeline run validate` passes with `pa.binary()` schemas for gold value columns.
- `pipeline run full` then `pipeline run report` produces a report with correctly rendered charts.
- `pipeline run query "SELECT * FROM portfolio_holdings_analytics" --decrypt` returns decrypted float values.
- `pipeline run query "SELECT * FROM portfolio_holdings_analytics"` (without `--decrypt`) shows binary blobs for `security_value` and `target_value`, but readable `percentage`, `ticker`, `broker`, etc.
- All existing tests pass after updates to handle binary value columns.