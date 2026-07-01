## Goal
To create a single dashboard that consolidates assests from different brokers.

## Test maintenance

- When changing portfolio math, broker data normalization, or dashboard output, add or update focused tests that cover the changed behavior and any reported regression.
- Run the relevant tests before finishing changes.

## Linting

- Before committing, run `ruff check --fix .` and `ruff format .` to fix lint issues.
- After running ruff, re-run tests to ensure the auto-fixes didn't break anything.

## Architecture Decision Records

Record every feature, fix, infrastructure change, behavior change, or notable implementation decision in `docs/adr/`.

Use one Markdown file per decision with a descriptive kebab-case name, such as `docs/adr/0001-add-local-kafka-transform-tests.md`. Include the context, decision, consequences, and any validation performed.

Before making any change, check existing ADR filenames in `docs/adr/` and review any ADRs related to the area being touched. If a relevant ADR conflicts with the current user requirements, stop and notify the user, then ask how to proceed before continuing.

### ADR-aware implementation workflow

When implementing a feature or making a decision that warrants an ADR:

1. **Read the ADR index** — Check `docs/adr/README.md` for the list of active ADRs. If the README doesn't exist yet, read all files in `docs/adr/` directly.
2. **Identify relevant ADRs** — Find ADRs whose topic overlaps with the change you're about to make. Read those ADRs for context.
3. **Respect active ADRs** — If an active ADR conflicts with the planned change, stop and ask the user how to proceed. Do not silently deviate from an active ADR.
4. **Skip superseded ADRs** — ADRs marked as superseded (they have a `> **Superseded by ADR XXXX**` notice at the top) are historical context only. Do not treat them as current guidance.
5. **Write a new ADR** — After implementing the change, create a new numbered ADR file in `docs/adr/`. Use the next available number. Include `## Context`, `## Decision`, `## Consequences`, and `## Validation` sections.
6. **Do NOT mark old ADRs as superseded** — Supersession detection is handled by the `/optimize-adrs` skill. Your job is to write the new ADR; the optimize workflow determines what it supersedes and updates the index accordingly.

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
