## Goal
To create a single dashboard that consolidates assests from different brokers.

## Test maintenance

- When changing portfolio math, broker data normalization, or dashboard output, add or update focused tests that cover the changed behavior and any reported regression.
- Run the relevant tests before finishing changes, using a command-level watchdog for any command that may hang.

## Architecture Decision Records

Record every feature, fix, infrastructure change, behavior change, or notable implementation decision in `docs/adr/`.

Use one Markdown file per decision with a descriptive kebab-case name, such as `docs/adr/0001-add-local-kafka-transform-tests.md`. Include the context, decision, consequences, and any validation performed.
