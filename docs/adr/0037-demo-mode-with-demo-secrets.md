# 0037: Demo Mode with `_DEMO` Secrets and Isolated Storage

## Context

The pipeline had a narrow `T212_DEMO` flag that only switched the Trading 212 API URL between live and demo endpoints. This was insufficient for a full demo mode because:

- Other brokers (IBKR) had no demo mechanism
- Demo and production data shared the same storage (same S3 bucket, same local directory)
- All secrets used the same env vars, so demo runs would hit production APIs if demo secrets were missing
- There was no way to toggle demo mode from the GitHub Actions UI

The user needed a general `DEMO` boolean that completely isolates demo runs from production: separate credentials, separate storage, and a toggle in the GitHub Actions workflow.

## Decision

Replace `T212_DEMO` with a general `DEMO` boolean flag. When `DEMO=true`:

1. **Secrets**: Use `_DEMO`-suffixed secrets exclusively (e.g., `T212_API_KEY_DEMO` instead of `T212_API_KEY`). No fallback to production secrets — missing demo credentials are a hard error.

2. **Storage**: Write to a separate demo bucket/ducket directory:
   - S3: `{S3_BUCKET}_demo` bucket with `pipeline_demo` prefix (or `S3_BUCKET_DEMO`/`S3_PREFIX_DEMO` if set)
   - Local: `{data_dir}_demo` (or `PIPELINE_DATA_DIR_DEMO` if set)

3. **AWS credentials**: Use `AWS_ACCESS_KEY_ID_DEMO`/`AWS_SECRET_ACCESS_KEY_DEMO` exclusively. No fallback to base keys.

4. **T212 API URL**: Default to `https://demo.trading212.com/api/v0` instead of `https://live.trading212.com/api/v0`.

5. **GitHub Actions**: Add a `demo` boolean workflow_dispatch input that sets `DEMO=true` and passes all `_DEMO` secrets.

The `DEMO_SECRET_MAP` in `pipeline/secrets.py` maps each base secret name to its `_DEMO` variant. The `resolve_secret()` function enforces strict isolation: in demo mode, it raises `EnvironmentError` if a `_DEMO` secret is missing rather than falling back to the base secret.

## Consequences

- **Complete isolation**: Demo runs cannot accidentally access production APIs or production data.
- **Hard errors on misconfiguration**: If `DEMO=true` but a `_DEMO` secret is missing, the pipeline fails immediately rather than silently using production credentials.
- **Breaking change**: `T212_DEMO` is removed. Users currently using `T212_DEMO=true` must switch to `DEMO=true` and set `T212_API_KEY_DEMO` and `T212_API_SECRET_DEMO`.
- **More env vars**: Each secret now has a `_DEMO` variant, doubling the number of env vars to configure. The `.env.example` and README document all variants.
- **GitHub Secrets**: The user must add all `_DEMO` secrets to the repository's GitHub Secrets and `S3_BUCKET_DEMO`/`S3_PREFIX_DEMO` as GitHub Variables.

## Validation

- `tests/test_secrets.py`: `TestIsDemo`, `TestResolveSecret` (including `EnvironmentError` on missing demo secret), `TestInjectSecretsDemoMode`
- `tests/test_storage_config.py`: `TestDemoStorage` (local demo suffix, custom demo dir, S3 demo bucket, explicit overrides, non-demo unchanged)
- Manual: Set `DEMO=true` with `_DEMO` secrets and verify data lands in the demo bucket/directory
- Manual: Verify GitHub Actions workflow shows the `demo` toggle in the UI