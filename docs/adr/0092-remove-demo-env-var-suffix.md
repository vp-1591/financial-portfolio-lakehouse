# 0092 — Remove _DEMO env var suffix pattern

## Context

The pipeline used `_DEMO`-suffixed environment variable names (`IBKR_FLEX_TOKEN_DEMO`, `AWS_ACCESS_KEY_ID_DEMO`, etc.) to isolate staging credentials from production. The `DEMO_SECRET_MAP` in `secrets.py` mapped base names to `_DEMO` variants, and `resolve_secret()` swapped to the `_DEMO` variant when in staging mode. SSM parameter names also carried the suffix (`/portfolio/demo/IBKR_FLEX_TOKEN_DEMO`), as did ECS task environment variables and GitHub Secrets.

Phases 2 and 3 (ADRs 0090, 0091) replaced the `DEMO` and `STORAGE_TYPE` env vars with the `--mode` CLI flag and made `cmd_full` trigger Step Functions for staging/prod. But the `_DEMO` suffix remained — a double indirection where SSM path, env var name, and Python code all duplicated the `_DEMO` pattern.

Demo and prod ECS tasks are **completely separate containers** that never share an environment. The SSM path prefix (`/portfolio/demo/` vs `/portfolio/prod/`) already provides isolation; the env var suffix is redundant.

## Decision

1. **Remove `_DEMO` suffix from SSM parameter names.** Renamed from `/portfolio/demo/<NAME>_DEMO` to `/portfolio/demo/<NAME>` (e.g., `/portfolio/demo/IBKR_FLEX_TOKEN_DEMO` → `/portfolio/demo/IBKR_FLEX_TOKEN`). ECS tasks inject secrets under base names (`IBKR_FLEX_TOKEN`, not `IBKR_FLEX_TOKEN_DEMO`).

2. **Delete `DEMO_SECRET_MAP` and derived constants.** `DEMO_SECRET_MAP`, `REQUIRED_SECRETS_DEMO`, `REQUIRED_SECRETS_S3_DEMO`, and `REQUIRED_SECRETS_DEMO_NON_AWS` are removed from `secrets.py`. `resolve_secret()` now reads `os.environ.get(name)` directly — no suffix swap. `inject_secrets()` validates `REQUIRED_SECRETS` unconditionally.

3. **Remove `S3_BUCKET_DEMO` and `S3_PREFIX_DEMO`.** The staging branch of `resolve_storage()` reads `S3_BUCKET` and `S3_PREFIX` (default `"pipeline_demo"`). Terraform demo `common_environment` sets `S3_BUCKET` and `S3_PREFIX` instead of `S3_BUCKET_DEMO`/`S3_PREFIX_DEMO`.

4. **Rename GitHub Secrets from `_DEMO` to `_STAGING`.** `AWS_ACCESS_KEY_ID_DEMO` → `AWS_ACCESS_KEY_ID_STAGING`, `AWS_SECRET_ACCESS_KEY_DEMO` → `AWS_SECRET_ACCESS_KEY_STAGING`, `DEMO_STATE_MACHINE_ARN` → `STAGING_STATE_MACHINE_ARN`.

5. **`is_demo()` stays as `get_mode() == "staging"`.** It is still meaningful for mode-based behavior (demo S3 bucket, synthetic deposit injection, T212 demo API URL, encryption-key file fallback guard) — only the `_DEMO` env var swap is removed.

## Constraints

- The T212 demo API URL (`_DEMO_BASE_URL`) and IBKR synthetic deposit amount (`_DEMO_INITIAL_DEPOSIT_AMOUNT`) are runtime behavior constants, not env var patterns — they are unchanged.
- The `staging_path()` method's `"staging_demo"` prefix within the demo bucket is a data-layout choice, not part of the env var pattern — it is unchanged.
- The SSM parameter rename requires a Terraform migration: copy real values to new names out-of-band, then `state rm` + `import` to adopt without destroying old params. Old `_DEMO` params are deleted only after ECS tasks reference the new names.
- GitHub Secret names must be recreated by the operator in the `staging` environment under the `_STAGING` names.

## Consequences

- Simpler mental model: one name per secret, environment isolation via SSM path prefix.
- `resolve_secret()` is a simple `os.environ.get(name)` call — no mode-dependent logic.
- SSM migration requires operator coordination (copy values before Terraform apply).
- ADRs 0037, 0039, 0040, 0042, 0044 are superseded; 0038 and 0043 updated in place.

## Validation

- All existing tests pass after removing `DEMO_SECRET_MAP` and `_DEMO` env var references.
- `grep -rn "_DEMO" pipeline/ terraform/ .github/ docs/ .env.example` returns only the intentionally kept items (`_DEMO_BASE_URL`, `_DEMO_INITIAL_DEPOSIT_AMOUNT`, superseded ADR bodies, this ADR).
- `terraform -chdir=terraform/demo plan` shows SSM params adopted (no destroy/recreate) and new ECS task def revisions.
- After staging deploy, a `run-connector ibkr --mode staging` ECS task reads `IBKR_FLEX_TOKEN` (base name) from the `/portfolio/demo/IBKR_FLEX_TOKEN` SSM parameter and succeeds.