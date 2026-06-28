# 0016: GitHub Actions CI

## Context

The project had no CI/CD pipeline. Tests were run manually before commits, and there was no branch protection on `main`. PRs could be merged without automated verification.

## Decision

1. Add `.github/workflows/ci.yml` that:
   - Triggers on every push to `main` and every pull request targeting `main`
   - Runs on `ubuntu-latest` with Python 3.11
   - Creates a virtual environment, installs `[dev]` dependencies, and runs `pytest`
   - Uses the project's venv pattern (`.venv/`) as specified in CLAUDE.md

2. The CI workflow does **not** use a matrix strategy for Python versions initially. Python 3.11 matches the minimum required version in `pyproject.toml` and can be expanded later.

3. Branch protection on `main` should be configured in GitHub Settings to require the `test` job to pass before merging. This is a manual GitHub setting, not a file change.

## Consequences

- Every PR to `main` is automatically tested before merge
- Tests use `use_storage()` with `tmp_path`, so no real data or secrets are needed in CI
- Branch protection (when configured) prevents merging with failing tests
- No caching step yet — install times are acceptable for the current dependency set

## Validation

- The workflow file is syntactically valid YAML
- The `pip install -e ".[dev]"` command installs all required dependencies including `deltalake`, `duckdb`, `polars`, and `pytest`
- Tests pass in a clean environment via `pytest tests/ -v`