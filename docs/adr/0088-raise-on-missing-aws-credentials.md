# 0088: Raise on Missing AWS Credentials for DuckDB S3

> **Supersedes [ADR 0055](./0055-iam-role-credential-fallback.md)** — The production-mode
> branch of `_configure_s3()` now raises `RuntimeError` instead of silently skipping
> DuckDB SECRET creation. ADR 0055's demo-mode branch (empty credentials blocking
> fallback) and its `to_storage_options()`/`to_pyarrow_kwargs()` changes (omit keys for
> IAM role fallback) remain in force — only the DuckDB path changes.

## Context

ADR 0055 enabled IAM role credential fallback for ECS tasks by omitting credential
keys when both `key_id` and `secret_key` are `None`. This works correctly for
`deltalake`'s `object_store` and PyArrow, which have their own credential chains that
reach `~/.aws/credentials`, AWS SSO, and instance metadata.

However, DuckDB's `delta_scan()` extension does **not** use boto3's credential chain.
When no DuckDB SECRET is created and no `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`
environment variables are set, DuckDB tries the instance metadata endpoint (IMDS)
directly. On local development machines, IMDS is unreachable, producing a confusing
timeout error:

```
Error interacting with object store: Generic S3 error:
Error performing PUT http://169.254.169.254/latest/api/token
```

This affects `pipeline query`, `pipeline validate`, and `pipeline report` when running
locally against S3 with credentials in `~/.aws/credentials` rather than environment
variables. The error gives no indication that AWS credentials are missing or how to
fix it.

On ECS deployments, AWS credentials are injected as environment variables from SSM
parameters, so `key_id` and `secret_key` are always non-`None` and this branch is
never reached.

## Decision

When both `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are `None` (not in
environment variables or `.env`) and the pipeline is **not** in demo mode,
`_configure_s3()` in `pipeline/query.py` now raises a `RuntimeError` with an
actionable message:

```
AWS credentials not found. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
in your .env file or environment variables. For local development without
S3, use STORAGE_TYPE=local.
```

This replaces the previous behavior of silently skipping DuckDB SECRET creation
and logging a debug message. The demo-mode branch (creating an empty SECRET to
block production fallback) is unchanged.

## Constraints

- ECS deployments set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from SSM
  parameters, so this branch is never reached in production ECS.
- Local development without S3 (`S3_BUCKET` not set) defaults to `STORAGE_TYPE=local`,
  so `_configure_s3()` is never called.
- `deltalake`'s `object_store` and PyArrow continue to omit credential keys when
  both are `None` (ADR 0055), allowing IAM role fallback in those subsystems.
  This change only affects the DuckDB path.

## Consequences

- **Positive**: Local developers running `pipeline report` or `pipeline query` against
  S3 without AWS environment variables get a clear, actionable error message instead
  of a confusing IMDS timeout.
- **Positive**: ECS deployments are unaffected — they always set explicit credentials
  via SSM environment variables.
- **Positive**: Demo mode isolation is preserved — the empty SECRET branch is unchanged.
- **Negative**: Any deployment that relied on DuckDB's IMDS fallback (IAM role without
  explicit env vars) will now get a `RuntimeError` instead of attempting IMDS auth.
  This is intentional: such deployments should set `AWS_ACCESS_KEY_ID` and
  `AWS_SECRET_ACCESS_KEY` explicitly.

## Validation

1. `test_raises_when_credentials_absent` — verifies `RuntimeError` is raised when
   both `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are absent from environment.
2. `test_raises_when_credentials_empty` — verifies `RuntimeError` is raised when
   both are set to empty strings (normalized to `None`).
3. `test_demo_mode_no_fallback_to_prod` — unchanged, verifies demo isolation still
   works (empty SECRET blocks production fallback).
4. Manual: run `pipeline report --open` without AWS env vars and `S3_BUCKET` set —
   should produce a clear error message, not an IMDS timeout.