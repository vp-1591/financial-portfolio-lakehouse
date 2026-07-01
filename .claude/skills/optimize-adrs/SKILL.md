---
name: optimize-adrs
description: Re-index ADRs, detect superseded records, update README.md, mark superseded ADRs, and open a PR
model: opus
disable-model-invocation: true
---

You are an ADR index maintainer. Your job is to bring `docs/adr/README.md` up to date, detect superseded ADRs, mark them in both the index and the individual files, and open a PR with a clear summary including the rationale for every supersession.

## Step 1 — Read or create the index

Read `docs/adr/README.md`. If it does not exist, this is a **first run** — you will build the entire index from scratch. If it exists, extract the `last-indexed` ISO 8601 datetime from the metadata table.

## Step 2 — Gather git history

- **First run:** List all ADR files in `docs/adr/` and get the creation date of each with `git log --format="%ai" --diff-filter=A -- <file>`.
- **Incremental run:** Run `git log --since="<last-indexed>" --oneline --name-only` to find commits that touched ADR files or related code since the last indexing.

## Step 3 — Read every ADR file

For each `.md` file in `docs/adr/`:
1. Extract the title (first `#` heading or filename-derived).
2. Read the `## Context`, `## Decision`, `## Consequences` sections.
3. Look for explicit cross-references: "supersedes", "replaces", "merged from", "obsoletes" followed by ADR numbers.
4. Note which component/module the ADR affects (e.g., config system, T212 connector, IBKR connector, query API, CI).

## Step 4 — Determine supersession relationships

Apply these heuristics in priority order. A higher-priority heuristic overrides a lower one if they conflict:

### Heuristic 1 — Explicit statements (highest confidence)
If ADR X explicitly says "supersedes ADR Y" or "merged from ADR Y", mark Y as superseded by X. Also check `## Merged from` sections.

### Heuristic 2 — Same-component, later-replaces-earlier
If two ADRs affect the same component and the later one removes or replaces what the earlier one introduced, and the later ADR references the earlier one in its Context section, mark the earlier as superseded.

### Heuristic 3 — Bugfix subsumed by redesign
If a bugfix ADR modified code introduced by an earlier ADR, and a subsequent ADR redesigns the entire subsystem, the bugfix ADR is superseded by the redesign ADR.

### Heuristic 4 — Do NOT mark as superseded
- Do not mark a foundational ADR as superseded just because later ADRs build on it.
- Do not mark an ADR as superseded just because the codebase has evolved past it — only when a later ADR explicitly replaces or contradicts it.

For each supersession, **write a one-sentence rationale** explaining why the ADR is superseded (e.g., "ADR 0018 introduced YAML config; ADR 0020 removed it entirely").

## Step 5 — Build the index table

Create or update `docs/adr/README.md` with this structure:

```markdown
# Architecture Decision Records

This index tracks all ADRs in `docs/adr/`. Run `/optimize-adrs` to update it.

| Field | Value |
|-------|-------|
| last-indexed | <ISO 8601 datetime> |
| total | <number> |

## Index

| ADR | Title | Created | Status | Superseded by |
|-----|-------|---------|--------|---------------|
| 0001 | Disable Pytest Cache Provider | 2026-06-15 | active | — |
| ... | ... | ... | ... | ... |
| 0018 | Bitwarden Secrets and YAML Config | 2026-06-28 | superseded | 0020 |
| ... | ... | ... | ... | ... |

<!-- Duplicate-number mapping
  0002a → 0002-add-consolidate-step-and-fix-duplicates.md
  0002b → 0002-use-broker-native-identifiers-in-portfolio-report.md
  ... 
-->
```

**Duplicate numbers:** If two files share a number, disambiguate with `a`/`b` suffixes sorted alphabetically by filename. Document the mapping in an HTML comment below the table. Do NOT rename actual files.

**`last-indexed`:** Set to the current ISO 8601 datetime (with timezone) of when you are running this workflow.

**`Created` dates:** Use `git log --format="%ai" --diff-filter=A -- <file>` to get each file's creation date.

## Step 6 — Add superseded notices to ADR files

For each ADR newly marked as superseded (i.e., not previously marked in the existing README or not already having a "Superseded by" notice), insert this block right after the title `#` heading of the file:

```markdown
> **Superseded by [ADR XXXX](./XXXX-filename.md)** — <one-sentence rationale>.
```

Do NOT modify any other content in the ADR file. If a "Superseded by" notice already exists and is still correct, leave it unchanged.

## Step 7 — Commit and open a PR

1. Create a feature branch: `git checkout -b adr/optimize-YYYY-MM-DD` (use today's date).
2. Stage all changes: `git add docs/adr/`.
3. Commit with a descriptive message.
4. Push the branch.
5. Open a PR with `gh pr create`. The PR body **must include a section listing every ADR marked as superseded with its rationale**, formatted like:

```markdown
### Superseded ADRs

| ADR | Superseded by | Rationale |
|-----|---------------|-----------|
| 0018 | 0020 | Introduced YAML config; ADR 0020 removed it entirely |
| 0019 | 0020 | Introduced S3/GitHub secrets alongside YAML; ADR 0020 removed YAML config |
| ... | ... | ... |
```

Also include in the PR body a summary of any index corrections, new ADRs added since last indexing, and the new `last-indexed` timestamp.

## Step 8 — If nothing changed

If the index is already up to date and no ADRs need to be newly marked as superseded, report that to the user and do not create a branch or PR.