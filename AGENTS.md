## Goal
To create a single dashboard that consolidates assests from different brokers.

## Python environment

A virtual environment lives at `.venv/`. Run commands through it directly:

```powershell
.venv\Scripts\python -m pytest
.venv\Scripts\python -m pipeline.run full --ibkr --t212-api-key "KEY" --xtb-file "report.xlsx"
```

Pipeline dependencies (`deltalake`, `duckdb`, `cryptography`, `pyarrow`,
`pandas`) are installed in this venv under the `[pipeline]` extra.

## Test maintenance

- When changing portfolio math, broker data normalization, or dashboard output, add or update focused tests that cover the changed behavior and any reported regression.
- Run the relevant tests before finishing changes, using a command-level watchdog for any command that may hang.

## Architecture Decision Records

Record every feature, fix, infrastructure change, behavior change, or notable implementation decision in `docs/adr/`.

Use one Markdown file per decision with a descriptive kebab-case name, such as `docs/adr/0001-add-local-kafka-transform-tests.md`. Include the context, decision, consequences, and any validation performed.

Before making any change, check existing ADR filenames in `docs/adr/` and review any ADRs related to the area being touched. If a relevant ADR conflicts with the current user requirements, stop and notify the user, then ask how to proceed before continuing.
