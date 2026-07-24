## Goal

Single dashboard consolidating assets from different brokers. Medallion architecture: raw (encrypted broker payloads → Delta) → normalized (parsed, consolidated, FX-converted) → analytics (portfolio aggregations). All financial values are Fernet-encrypted at rest. Delta Lake for storage, DuckDB for queries, Polars for all data manipulation, PyArrow only for table schemas (`models.py`) and S3 filesystem (`s3.py`). `write_deltalake` accepts `pl.DataFrame` directly — do not convert to `pa.Table` for writes.

## Checks

Always use the project venv — never the system Python (`C:\Python314`). On Windows, use `python.exe -m pip` instead of bare `pip`:

```bash
.venv/Scripts/python.exe -m pip install <package>
```

Before committing, run all three checks. After linting, re-run tests to ensure auto-fixes didn't break anything:

```bash
ruff check --fix . && ruff format .
.venv/Scripts/python -m pyright pipeline/ tests/
.venv/Scripts/python -m pytest tests/ -v
```

Run a single test file or specific test:

```bash
.venv/Scripts/python -m pytest tests/test_consolidate.py -v
.venv/Scripts/python -m pytest tests/test_consolidate.py::test_consolidate_holdings -v
```

- When changing portfolio math, broker data normalization, or dashboard output, add or update focused tests that cover the changed behavior and any reported regression.

## Useful commands

```bash
.venv/Scripts/python -m pipeline.run query "SELECT * FROM portfolio_holdings" --decrypt --mode staging
.venv/Scripts/python -m pipeline.run report --mode staging --open
```

@~/.claude/shared/adr-workflow.md

## Roadmap workflow

Roadmaps live in `docs/roadmaps/<number>-<topic>.md` and follow the template in
`docs/roadmaps/roadmap-template.md`. Use `/create-roadmap` to create or update one —
it clarifies ambiguities before drafting.

The workflow order is: `analyze → roadmap → plan → implement → ADR → review`.

## Schema migrations

When a table schema changes (column types, added/removed columns), create a migration script under `pipeline/migrations/` that rewrites the existing Delta table to match the new schema. This ensures the deploy can succeed against pre-existing tables so that quality checks don't flag mismatches between the expected and actual schema.

## Deploy logs

When a staging deploy fails, the application error (Python tracebacks) is in the "Print container logs on failure". Check the `=== Container logs: <connector> ===` sections.

## AWS Guidance

- Prefer the AWS MCP Server for AWS interactions — it provides sandboxed execution, observability, and audit logging. If unavailable, use the AWS CLI directly.
- Before starting a task, check whether a relevant AWS skill is available. Load the skill with `retrieve_skill` and prefer its guidance over general knowledge.
- When uncertain about specific AWS details (API parameters, permissions, limits, error codes), verify against documentation rather than guessing. State uncertainty explicitly if you cannot confirm.
- When creating infrastructure, prefer infrastructure-as-code (Terraform in this project, or AWS CDK / CloudFormation) over direct CLI commands.
- When working with infrastructure, follow AWS Well-Architected Framework principles.
- Do not use em dashes in AWS resource names or descriptions. Use hyphens instead.

### Secret Safety

- MUST load the `creating-secrets-using-best-practices` skill first for any secret, credential, API key, token, or password task. MUST NOT call `secretsmanager get-secret-value` or `batch-get-secret-value` directly, and MUST NOT hit the Secrets Manager Agent daemon directly. Prefer `{{resolve:secretsmanager:secret-id:SecretString:json-key}}` with `asm-exec` so the secret resolves at runtime without entering context.
