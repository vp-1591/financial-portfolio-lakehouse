## Goal
To create a single dashboard that consolidates assests from different brokers.

## Test maintenance

- When changing portfolio math, broker data normalization, or dashboard output, add or update focused tests that cover the changed behavior and any reported regression.
- Run the relevant tests before finishing changes.

## Linting

- Before committing, run `ruff check --fix .` and `ruff format .` to fix lint issues.
- After running ruff, re-run tests to ensure the auto-fixes didn't break anything.

@~/Documents/Vadym/GitRep/agents-artifacts/claude-config/adr-workflow.md

## Environment

Always use the project's Python virtual environment for dependency installs and code execution:

```
.venv/Scripts/python
.venv/Scripts/pip
```

Never use the system Python (`C:\Python314`). Always prefix commands with the venv, e.g.:

```bash
.venv/Scripts/python -m pytest tests/ -v
.venv/Scripts/pip install <package>
```
