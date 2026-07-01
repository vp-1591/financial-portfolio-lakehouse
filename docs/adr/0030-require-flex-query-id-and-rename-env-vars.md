# 0030 — Require IBKR Flex Query ID and rename PORTFOLIO_ENCRYPTION_KEY

## Context

Two environment variables had inconsistent treatment:

1. **`IBKR_FLEX_QUERY_ID`** had a hardcoded default of `1554188` in three places
   (`client.py`, `fetch.py`, `connector.py`) and in `run.py`. This made it appear
   optional, but every IBKR Flex query actually requires a valid query ID — the
   default `1554188` was a specific user's query, not a sensible universal default.
   Meanwhile, `IBKR_FLEX_TOKEN` was already required (connector skips if missing).

2. **`PORTFOLIO_ENCRYPTION_KEY`** was verbose and inconsistent with the simpler
   naming of other env vars like `IBKR_FLEX_TOKEN`, `T212_API_KEY`, etc. The
   `PORTFOLIO_` prefix added no disambiguation value since this project only has
   one encryption key.

## Decision

1. **Make `IBKR_FLEX_QUERY_ID` required**, matching the pattern used for
   `IBKR_FLEX_TOKEN`. Remove all hardcoded defaults (`"1554188"`). If the env var
   is not set, the IBKR connector is skipped with a debug log message — same
   behavior as a missing `IBKR_FLEX_TOKEN`. Add it to `REQUIRED_SECRETS` so
   `inject_secrets()` warns when it is missing.

2. **Rename `PORTFOLIO_ENCRYPTION_KEY` to `ENCRYPTION_KEY`** across all source,
   test, CI, and documentation files. The config field `encryption_key_file`
   and the file path `.secrets/encryption.key` are unchanged — only the env var
   name changes.

3. **Rename `infra/` directory to `terraform/`** for clarity — the directory
   contains Terraform configuration and `terraform/` is more descriptive.

4. **Add linting commands to the README Tests section**, renaming it to
   "Tests & Linting" and adding `ruff check` and `ruff format --check` commands.

## Consequences

- **Breaking change**: Users must set `IBKR_FLEX_QUERY_ID` and `ENCRYPTION_KEY`
  (instead of `PORTFOLIO_ENCRYPTION_KEY`) environment variables. The old names
  are no longer recognized.
- The IBKR connector no longer silently uses a hard-coded query ID that may not
  belong to the user.
- GitHub Actions secret names must be updated: `PORTFOLIO_ENCRYPTION_KEY` →
  `ENCRYPTION_KEY` in the repository settings.
- Any `infra/` path references in deployment scripts or documentation must be
  updated to `terraform/`.

## Validation

- All 221 tests pass after the changes.
- `grep -r "1554188"` returns no results in source or test files.
- `grep -r "PORTFOLIO_ENCRYPTION_KEY"` returns no results.
- `grep -r "infra/"` in code and docs returns no directory path references.