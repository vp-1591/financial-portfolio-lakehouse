## Goal
To create a single dashboard that consolidates assests from different brokers.

## Test maintenance

- When changing portfolio math, broker data normalization, or dashboard output, add or update focused tests that cover the changed behavior and any reported regression.
- Run the relevant tests before finishing changes.

## Linting

- Before committing, run `ruff check --fix .` and `ruff format .` to fix lint issues.
- After running ruff, re-run tests to ensure the auto-fixes didn't break anything.

@~/.claude/shared/adr-workflow.md

## Roadmap workflow

Roadmaps live in `docs/roadmap-<topic>.md` and follow the template in
`docs/roadmap-template.md`. Use `/create-roadmap` to create or update one —
it clarifies ambiguities before drafting.

The workflow order is: `analyze → roadmap → plan → implement → ADR → review`.

## Deploy logs

When a staging deploy fails, the application error (Python tracebacks) is in the "Print container logs on failure". Check the `=== Container logs: <connector> ===` sections.

## Environment

Always use the project's Python virtual environment for dependency installs and code execution:

```
.venv/Scripts/python
.venv/Scripts/pip
```

Never use the system Python (`C:\Python314`). Always prefix commands with the venv, e.g.:

```bash
.venv/Scripts/python -m pytest tests/ -v
```

Note: on Windows, use `python.exe -m pip` instead of bare `pip` — the venv's `pip` scripts are not directly callable from Git Bash:

```bash
.venv/Scripts/python.exe -m pip install <package>
```
