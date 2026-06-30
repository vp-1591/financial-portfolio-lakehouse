# 0022: Fix review findings in S3 storage PR

## Context

Review of PR #3 (feat/s3-storage-and-github-secrets) identified four issues:

1. **Workflow argparse mismatch**: `.github/workflows/pipeline.yml` unconditionally passes `--target-currency` to all pipeline commands, but `fetch` and `transform` subcommands don't accept this argument. Dispatching those commands fails with "unrecognized arguments: --target-currency EUR".

2. **Dead `import traceback`**: `pipeline/run.py` CDC error handler imports `traceback` but never uses it.

3. **Duplicated keygen logic**: `pipeline/run.py::cmd_keygen` and `pipeline/keygen.py::main` contain identical S3Backend isinstance checks and print messages. Both implement the full keygen flow independently.

4. **Repeated boolean env-var parsing**: The pattern `get_config(..., "false").lower() == "true"` appears 5× in `pipeline/run.py` for parsing boolean config values.

## Decision

1. **Fix workflow**: Only pass `--target-currency` to commands that accept it (full, consolidate, allocate). Use a shell conditional to branch based on the command.

2. **Remove dead import**: Delete the unused `import traceback`.

3. **Deduplicate keygen**: `cmd_keygen` now delegates to `keygen_main()` from `pipeline/keygen.py`. Remove the duplicated S3Backend check and print statements. Also remove the now-unused `S3Backend` and `generate_key` imports from `run.py`.

4. **Extract `parse_bool()` helper**: Add `parse_bool(name, default=False)` to `pipeline/secrets.py`. It returns the default if the env var is unset; when set, `true`/`1`/`yes` (case-insensitive) are True, everything else is False. Replace all 5 inline boolean parsing expressions in `run.py` with `parse_bool()` calls. Add `TestParseBool` test class.

## Consequences

- Workflow dispatch now works for all 5 commands (full, fetch, transform, consolidate, allocate).
- `cmd_keygen` is a one-liner delegating to `keygen_main()`.
- Boolean config parsing is consistent and tested.
- All 237 tests pass.

## Validation

- `python -m pytest tests/ -v`: 237 passed, 1 skipped
- Manually verified workflow YAML conditional for target-currency