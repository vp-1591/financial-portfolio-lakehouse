---
name: optimize-adrs
description: Re-index ADRs, detect superseded records, update README.md, mark superseded ADRs, and open a PR
model: opus
disable-model-invocation: true
---

You are an ADR index maintainer. `docs/adr/README.md` is append-only maintained
by the feature-implementation workflow — every new ADR already has a row with
`status: active`. Your job is NOT to discover new ADRs. Your job is to decide
which *existing* active rows should flip to `superseded`, mark them in both
the index and the individual files, and open a PR with a clear rationale for
every supersession.

## Step 1 — Read the index

Read `docs/adr/README.md`.

- **If it exists:** extract the `last-indexed` ISO 8601 datetime from the
  metadata table. This run is incremental.
- **If it does not exist:** this is a one-time bootstrap for a repo that
  predates this workflow. List all files in `docs/adr/`, get each one's
  creation date with `git log --format="%ai" --diff-filter=A -- <file>`, and
  build the index from scratch with every ADR marked `active`. Then proceed
  to Step 4 as normal — supersession detection runs the same way on a fresh
  index as an existing one.

## Step 2 — Consistency check (cheap, not a re-read)

List files in `docs/adr/` and compare filenames against ADR numbers already
in the index table. This is a filename diff, not a content read.

- **Orphan files** (exist on disk, no index row): the feature workflow should
  have added these but didn't. Add a row for each: number, title (from the
  file's first `#` heading), creation date via `git log --diff-filter=A`,
  status `active`, superseded-by `—`. Note these in the PR body under a
  "Recovered missing entries" section so the person can see the append step
  is being skipped somewhere upstream.
- **Orphan rows** (in index, no matching file): flag in the PR body, do not
  guess or delete — a human should confirm whether the file was removed
  intentionally.

Do not open or read the body of any ADR file in this step.

## Step 3 — Gather git history since last index

Run `git log --since="<last-indexed>" --oneline --name-only` to find commits
since the last run. Discard commits that only touch tests, CI config,
lockfiles, or paths with no relation to any ADR's subject matter — you're
building a candidate list of *active* ADRs that might now be stale, not a
full changelog.

For each active ADR row in the index, check whether any surviving commit
touches the component/module that ADR concerns. Build a shortlist of
"at-risk" active ADRs — only these get read in full in Step 4. Active ADRs
with no related commits since last-indexed are left untouched.

## Step 4 — Read at-risk ADRs and determine supersession

For each ADR in the Step 3 shortlist only:
1. Read `## Context`, `## Decision`, `## Consequences`.
2. Look for explicit cross-references: "supersedes", "replaces", "merged
   from", "obsoletes" followed by ADR numbers.
3. Compare against the commits identified in Step 3 for that ADR's area.

Apply these heuristics in priority order — a higher-priority heuristic
overrides a lower one if they conflict:

### Heuristic 1 — Explicit statements (highest confidence)
If ADR X explicitly says "supersedes ADR Y" or "merged from ADR Y", mark Y as
superseded by X. Also check `## Merged from` sections.

### Heuristic 2 — Same-component, later-replaces-earlier
If a later ADR affects the same component as an earlier active ADR and its
Decision section uses words like "Delete", "Remove", "Replace", "Rewrite", or
"Redesign" for a module, config format, or API the earlier ADR introduced,
mark the earlier one superseded — even without an explicit reference by
number.

Example: a later ADR's Decision says "Delete `pipeline/config.py`" and
"Remove `pyyaml` dependency" → the earlier ADR that introduced that config
system is superseded, even if not cited by number.

### Heuristic 3 — Bugfix subsumed by redesign
If an active bugfix ADR modified code that a later ADR's redesign has since
removed or replaced entirely, the bugfix ADR is superseded by the redesign.

Example: an ADR fixed DuckDB S3 credential propagation; a later ADR
redesigned the query API with "native DuckDB connection, decrypt utility,
drop wrappers." The code the bugfix touched no longer exists — superseded by
the redesign.

### Heuristic 4 — Do NOT mark as superseded
- Do not mark a foundational ADR as superseded just because later ADRs build
  on it.
- Do not mark an ADR as superseded just because the codebase evolved past it
  — only when a later ADR explicitly replaces or contradicts it.

For each supersession, write a one-sentence rationale (e.g., "Introduced YAML
config; the later ADR removed it entirely").

## Step 5 — Update the index

Edit only the rows that changed status in Step 4 (plus any orphan rows from
Step 2). Do not rewrite rows that didn't change. Update `last-indexed` to now.

```markdown
# Architecture Decision Records

This index tracks all ADRs in `docs/adr/`. New ADRs are appended here
automatically when created. Run `/optimize-adrs` to detect and mark
superseded records.

| Field | Value |
|-------|-------|
| last-indexed | <ISO 8601 datetime> |

## Index

| ADR | Title | Created | Status | Superseded by |
|-----|-------|---------|--------|---------------|
| 0001 | ... | 2026-06-15 | active | — |
| ... | ... | ... | ... | ... |
| 0018 | ... | 2026-06-28 | superseded | 0020 |
| ... | ... | ... | ... | ... |

<!-- Duplicate-number mapping
  0002a → 0002-add-consolidate-step-and-fix-duplicates.md
  0002b → 0002-use-broker-native-identifiers-in-portfolio-report.md
-->
```

No `total` field — row count is trivially visible from the table itself and
a separately-tracked count only invites drift.

**Duplicate numbers:** if two files share a number, disambiguate with `a`/`b`
suffixes sorted alphabetically by filename, documented in the HTML comment.
Do NOT rename actual files.

## Step 6 — Add superseded notices to ADR files

For each ADR newly marked superseded in this run, insert this block right
after the title `#` heading of the file:

```markdown
> **Superseded by [ADR XXXX](./XXXX-filename.md)** — <one-sentence rationale>.
```

Do not modify any other content in the file. If a notice already exists and
is still correct, leave it unchanged.

## Step 7 — Commit and open a PR

1. `git checkout -b adr/optimize-YYYY-MM-DD` (today's date).
2. `git add docs/adr/`.
3. Commit with a descriptive message.
4. Push the branch.
5. `gh pr create`. The PR body must include:

```markdown
### Superseded ADRs

| ADR | Superseded by | Rationale |
|-----|---------------|-----------|
| 0018 | 0020 | Introduced YAML config; the later ADR removed it entirely |

### Recovered missing entries (if any)

| ADR | Issue |
|-----|-------|
| 0024 | File existed but had no index row — added |

### Orphan rows flagged (if any)

| ADR | Issue |
|-----|-------|
| 0009 | Row exists in index, no matching file found — needs human review |
```

Also include the new `last-indexed` timestamp.

## Step 8 — If nothing changed

If no active ADR was flagged for supersession and no orphans were found,
report that to the user and do not create a branch or PR.