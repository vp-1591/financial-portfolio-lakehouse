# 0032 — ADR index and optimize-adrs workflow

## Context

The project has 31 ADR files in `docs/adr/` with no index, no status tracking, and no way to know which ADRs are still active vs superseded. Several ADRs explicitly supersede others (e.g., ADR 0007 supersedes 0008/0009; ADR 0020 removes the YAML config that 0018/0019 introduced), but this information is buried in prose and requires reading every file to discover.

Additionally, when implementing features, there is no structured process for consulting existing ADRs or knowing which ones are still current guidance.

## Decision

1. **Created `/optimize-adrs` skill** (`.claude/skills/optimize-adrs/SKILL.md`) — A manually triggered slash command that:
   - Creates or updates `docs/adr/README.md` with a full index table (ADR number, title, created date, status, superseded by)
   - Detects superseded ADRs using explicit cross-references, same-component replacement, and bugfix-subsumed-by-redesign heuristics
   - Adds `> **Superseded by ADR XXXX**` notices to superseded ADR files
   - Sets a `last-indexed` watermark for incremental runs
   - Opens a PR with a rationale table for every superseded ADR
   - Uses the `opus` model for nuanced supersession analysis

2. **Updated `CLAUDE.md`** with an ADR-aware implementation workflow that instructs Claude to:
   - Read the ADR index before implementing features
   - Respect active ADRs and stop if they conflict
   - Skip superseded ADRs (historical context only)
   - Write new ADRs after implementation
   - Not mark old ADRs as superseded (the `/optimize-adrs` workflow handles that)

3. **README.md index format** includes:
   - Metadata header with `last-indexed` (ISO 8601) and `total` count
   - Full table with ADR, Title, Created, Status (active/superseded), Superseded by columns
   - Duplicate number disambiguation using `a`/`b` suffixes with a mapping comment
   - The `last-indexed` value serves as a watermark for incremental git log analysis

## Consequences

- **ADR status is discoverable** — The README.md index makes it immediately clear which ADRs are active vs superseded.
- **Supersession is tracked** — Both in the index table and as a notice in each superseded ADR file.
- **Incremental updates** — The `last-indexed` watermark means subsequent `/optimize-adrs` runs only process new commits.
- **Separation of concerns** — Feature implementation writes new ADRs; the `/optimize-adrs` workflow determines supersession. This prevents incorrect supersession during implementation.
- **PR transparency** — Every `/optimize-adrs` PR includes a rationale table explaining why each ADR was marked as superseded.

## Validation

- The skill file exists at `.claude/skills/optimize-adrs/SKILL.md`
- `CLAUDE.md` includes the ADR-aware implementation workflow section
- The `/optimize-adrs` slash command appears in Claude Code and can be invoked
- First run creates `docs/adr/README.md` from scratch; subsequent runs are incremental