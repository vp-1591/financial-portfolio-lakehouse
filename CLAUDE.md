## Goal
To create a single dashboard that consolidates assests from different brokers.

## Test maintenance

- When changing portfolio math, broker data normalization, or dashboard output, add or update focused tests that cover the changed behavior and any reported regression.
- Run the relevant tests before finishing changes, using a command-level watchdog for any command that may hang.

## Architecture Decision Records

Record every feature, fix, infrastructure change, behavior change, or notable implementation decision in `docs/adr/`.

Use one Markdown file per decision with a descriptive kebab-case name, such as `docs/adr/0001-add-local-kafka-transform-tests.md`. Include the context, decision, consequences, and any validation performed.

Before making any change, check existing ADR filenames in `docs/adr/` and review any ADRs related to the area being touched. If a relevant ADR conflicts with the current user requirements, stop and notify the user, then ask how to proceed before continuing.

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

## Git workflow
- Never commit directly to `main`. Always branch first: `git checkout -b feat/<short-description>`
- After local tests pass, open a PR: `gh pr create --fill`
- Wait for CI to go green before merging — don't merge with a red check.
