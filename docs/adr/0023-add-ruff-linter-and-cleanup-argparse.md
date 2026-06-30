# 0023: Add ruff linter to CI and clean up argparse

## Context

The codebase had accumulated lint issues (unused imports, unused variables, inconsistent formatting, undefined type annotations) that were never caught automatically. The `--target-currency` argument was defined on three separate subparsers (`consolidate`, `allocate`, `full`), requiring a shell conditional in the GitHub Actions workflow to avoid passing it to `fetch` and `transform`.

## Decision

1. **Add ruff to CI**: A new `lint` job in `.github/workflows/ci.yml` runs `ruff check .` and `ruff format --check --diff .` on every push and PR to main.

2. **Add ruff to dev dependencies**: `ruff>=0.11.0` added to `[project.optional-dependencies]` in `pyproject.toml`, with `[tool.ruff]` config for target version and line length.

3. **Move `--target-currency` to parent parser**: Instead of defining it on three subparsers and using a shell conditional in the workflow, `--target-currency` is now a top-level argument. All commands accept it; `fetch`, `transform`, and `keygen` simply ignore it. The workflow is now a one-liner:
   ```yaml
   run: python -m pipeline.run ${{ inputs.command }} --target-currency "${{ inputs.target-currency }}"
   ```

4. **Fix all 74 lint errors**: Removed unused imports, unused variables, added `TYPE_CHECKING` guards for type-only imports, added `# noqa: E402` for `sys.path`-gated test imports, and applied `ruff format` across the codebase.

5. **Add linting instruction to CLAUDE.md**: Document that linting should run at end of session before committing, not during focused work.

## Consequences

- CI catches lint issues before merge; no shell conditional needed in workflow YAML.
- All code passes `ruff check` and `ruff format` cleanly.
- 237 tests still pass after all changes.

## Validation

- `ruff check .` → All checks passed
- `ruff format --check .` → 67 files already formatted
- `python -m pytest tests/ -v` → 237 passed, 1 skipped