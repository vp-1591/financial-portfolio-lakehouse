# 0103: Prod boto3 Default-Chain Credential Fallback

> **Supersedes [ADR 0088](./0088-raise-on-missing-aws-credentials.md)** — The production-mode
> branch no longer raises immediately when `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` are
> absent from the environment. `resolve_aws_credentials()` now falls back to boto3's default
> credential chain before raising. ADR 0088's demo-mode empty-SECRET branch carries forward
> unchanged, see 0103 §Decision.

## Context

ADR 0088 made `_configure_s3()` raise a `RuntimeError` in production mode when both AWS
credentials were `None`, because DuckDB's `delta_scan()` extension cannot read
`~/.aws/credentials` or AWS SSO on its own — silently skipping SECRET creation produced a
confusing IMDS timeout instead of an actionable error. On ECS, credentials are always
injected as environment variables from SSM, so the raise branch was never reached in
deployments.

The gap ADR 0088 left open: a developer running `pipeline report --mode prod` (or
`query` / `validate`) locally with AWS SSO or `~/.aws/config` configured but no
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables set hits that raise.
Yet the `full` subcommand works in the same setup, because `cmd_full` calls
`boto3.Session()` directly (run.py), using boto3's default credential chain (env vars →
`~/.aws/credentials` → `~/.aws/config` / SSO → IMDS). The data-plane path
(`_configure_s3`, `S3Backend.storage_options`, `s3._make_s3fs`, `_discover_tables_s3`)
all resolve credentials through `resolve_aws_credentials()`, which read environment
variables only — never boto3 / AWS config / SSO.

A boto3-fallback pattern already existed in
`pipeline/migrations/migrate_snapshot_schema_unify.py:_get_storage_options_with_credentials`,
which used `boto3.Session().get_credentials().get_frozen_credentials()` to bridge boto3's
chain into deltalake's `object_store` storage options. That pattern was not shared with
the main credential resolver.

## Decision

`resolve_aws_credentials()` (pipeline/secrets.py) now falls back to boto3's default
credential chain when both `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are absent
**and the active mode is `prod`**. The fallback is performed by a new shared private helper,
`_boto3_default_chain_credentials(region) -> tuple[str, str, str | None] | None`, which
returns `(key_id, secret_key, session_token)` (or `None` when boto3 finds nothing). Both
`resolve_aws_credentials()` and the `migrate_snapshot_schema_unify` migration now call
this helper, eliminating the previously duplicated boto3 logic (DRY).

A `session_token` field was added to `AwsCredentials` and threaded through all three
data-plane adapters so SSO / temporary credentials (which always carry a session token)
work across the data plane:

- `to_duckdb_secret_parts()` emits `SESSION_TOKEN 'value'` (DuckDB S3 SECRET keyword).
- `to_storage_options()` emits `aws_session_token` (object_store lowercase key).
- `to_pyarrow_kwargs()` emits `session_token` (PyArrow convention).

The raise branch in `_configure_s3()` (ADR 0088) is retained as the **final fallback**:
it now fires only when boto3's chain *also* finds nothing. Its message was updated to
suggest `aws configure` / `aws sso login` in addition to setting env vars.

The demo-mode empty-SECRET branch (creating a SECRET with empty KEY_ID/SECRET to block
production fallback) remains unchanged (originally decided in ADR 0055, §Decision, and
reaffirmed in ADR 0088). The `to_storage_options()` / `to_pyarrow_kwargs()` omit-keys
behavior for IAM role fallback (ADR 0055) also remains in force.

### Alternatives considered

- **Fall back in every non-demo mode (prod + staging).** Rejected: staging's
  demo-isolation intent is that missing credentials fail rather than silently use the
  developer's real AWS credentials against the demo bucket. The boto3 fallback would
  inject real credentials into the empty-SECRET demo branch, breaking that isolation.
  Gating to `prod` only preserves staging behavior exactly.
- **Let each caller decide whether to invoke the fallback.** Rejected: the prod gate
  would be duplicated across four call sites (`_configure_s3`, `S3Backend.storage_options`,
  `s3._make_s3fs`, `_discover_tables_s3`). Centralizing the fallback in the shared
  resolver keeps one decision point and one `@cache`d result.
- **Use DuckDB's `credential_chain` SECRET provider.** Rejected: it relies on DuckDB's
  own AWS SDK wiring, which is uneven across platforms and was the original cause of the
  IMDS timeout (ADR 0088). Resolving via boto3 in Python and injecting explicit
  credentials keeps the data plane uniformly authenticated.

## Constraints

- The boto3 fallback is gated to `get_mode() == "prod"`. Staging/demo and docker/MinIO
  must never invoke it: docker uses `S3_ENDPOINT_URL` + MinIO keys, staging preserves
  demo isolation via the empty-SECRET branch.
- ECS deployments inject `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` from SSM, so the
  fallback is not reached in production ECS — env vars win. The fallback is for local /
  non-ECS prod runs with AWS config/SSO.
- `resolve_aws_credentials()` is `@cache`d; the boto3 chain is read once per process.
  Tests clear the cache in `setup_method`.
- Static `~/.aws/credentials` keys produce no session token; SSO / STS temporary
  credentials do. The `session_token` field is `None` for the former and threaded through
  for the latter.

## Consequences

- **Positive**: `pipeline report` / `query` / `validate` in prod now work with AWS
  config / SSO set up and no explicit env vars, matching the `full` subcommand. The
  original user-reported bug is fixed.
- **Positive**: One shared boto3 credential helper replaces two copies of the same
  logic (the resolver + the migration). DRY; future changes have one location.
- **Positive**: SSO / temporary credentials now work across the entire data plane
  (DuckDB, deltalake/object_store, PyArrow), not just deltalake writes — the
  `session_token` was previously dropped by `AwsCredentials`.
- **Negative**: A dependency on a working boto3 default chain in prod-local runs. If
  SSO is expired or `~/.aws/config` is misconfigured, the raise branch fires with the
  updated message — acceptable, since it is actionable.
- **Negative**: `resolve_aws_credentials()` becomes mode-aware (reads `get_mode()`),
  giving it a second reason to change. Accepted because the alternative (scattering
  the prod gate across callers) is worse.
- **Neutral**: The migration helper now imports from `pipeline.secrets` instead of
  `boto3` directly; behavior is preserved.

## Validation

1. `test_prod_mode_falls_back_to_boto3` — prod, env vars absent, mocked boto3 returns
   SSO credentials with a session token: `_configure_s3` creates a SECRET with the
   boto3 KEY_ID and a `session_token` field, no raise.
2. `test_prod_mode_raises_when_boto3_also_empty` — prod, env vars absent, boto3 returns
   `None`: `RuntimeError` is still raised (final fallback).
3. `test_prod_mode_env_vars_skip_boto3` — prod, env vars set: the
   `_boto3_default_chain_credentials` helper is never invoked (env vars win).
4. `TestBoto3DefaultChainCredentials` — unit tests for the helper: returns the 3-tuple
   with/without token, returns `None` when boto3 finds nothing or access key is empty.
5. `TestAwsCredentialsSessionToken` — `session_token` threads through
   `to_storage_options` (`aws_session_token`), `to_pyarrow_kwargs` (`session_token`),
   `to_duckdb_secret_parts` (`SESSION_TOKEN '...'`, with single-quote escaping), and is
   omitted when `None`.
6. Existing `test_raises_when_credentials_absent` / `test_raises_when_credentials_empty`
   (docker mode) and `test_staging_mode_no_credentials_creates_empty_secret` (staging)
   unchanged — the fallback is prod-gated, so docker/staging behavior is preserved.
7. Manual: `.venv/Scripts/python -m pipeline.run report --open --mode prod` with AWS
   SSO / `~/.aws/config` and no `AWS_*` env vars now authenticates and runs instead of
   raising "AWS credentials not found".
8. Full suite: `pytest tests/ -q` — 756 passed.