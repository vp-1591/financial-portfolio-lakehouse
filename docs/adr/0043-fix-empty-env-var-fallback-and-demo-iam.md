# 0043: Fix Empty-String Env Var Fallback and Broaden Demo IAM Policy

## Context

After fixing the demo bucket naming (ADR 0042, hyphen instead of underscore), the demo pipeline still failed with **403 AccessDenied** on `s3:PutObject`. Two root causes:

1. **Empty-string env vars bypass defaults**: `os.environ.get("S3_PREFIX_DEMO", "pipeline_demo")` returns `""` when the env var is set to an empty string (e.g., by GitHub Actions `${{ vars.S3_PREFIX_DEMO }}` when the variable is undefined). This caused the S3 prefix to be empty, making the pipeline write to `raw/...` instead of `pipeline_demo/raw/...`, which didn't match the IAM policy's `pipeline_demo/*` path pattern.

2. **IAM policy too restrictive**: The Terraform IAM policy for the `pipeline-demo` user only granted `s3:PutObject` on `${bucket_arn}/${var.s3_prefix}/*`. Since the demo bucket is dedicated (not shared with production), the prefix restriction was unnecessary.

The empty-string pattern was a recurring bug — `os.environ.get` with a default only falls back when the key is missing, not when the value is empty. This was the third time encountering this class of bug.

## Decision

1. **Rename `get_config()` → `get_env()`** and change its implementation to treat empty strings as unset:

   ```python
   def get_env(name: str, default: str | None = None) -> str | None:
       value = os.environ.get(name)
       if value:  # non-empty string
           return value
       return default
   ```

   This returns the default when the env var is missing **or empty**, preventing CI systems that set empty strings for undefined variables from silently bypassing defaults.

2. **Use `get_env()` everywhere** instead of `os.environ.get()` in `pipeline/storage.py` and `pipeline/secrets.py`. Environment variable reading is now centralized in `secrets.py`.

3. **Broaden the demo IAM policy** to grant full bucket access. The policy now has two statements:
   - `s3:ListBucket` on the bucket ARN
   - `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on `${bucket_arn}/*`

   The `s3_prefix` variable is no longer used in the policy (kept as a Terraform output only).

## Consequences

- **No more empty-string bypass**: Env vars set to empty strings by CI systems now correctly fall back to defaults.
- **Demo IAM policy is simpler and correct**: The `pipeline-demo` user has full access to the dedicated demo bucket.
- **Breaking change**: `get_config()` is renamed to `get_env()`. Any code importing `get_config` must update to `get_env`.
- **Centralized env var access**: `storage.py` no longer reads env vars directly — it uses `get_env()` from `secrets.py`.

## Validation

- `tests/test_secrets.py`: `TestGetEnv` (renamed from `TestGetConfig`) now includes empty-string fallback tests
- `tests/test_storage_config.py`: New tests for empty-string env vars (`S3_PREFIX_DEMO`, `S3_PREFIX`, `S3_BUCKET_DEMO`, `PIPELINE_DATA_DIR_DEMO`)
- All 321 existing tests pass
- After Terraform apply, the demo pipeline should no longer get 403 errors