---
name: optimize-adrs
description: Reconcile ADR index, detect supersession and drift, and surface implementation mismatches
model: sonnet
disable-model-invocation: true
---

Reconcile the ADR index with the repository and detect:
- unrecorded supersessions
- ADRs whose implementation no longer exists or diverged significantly
- ADRs describing unimplemented ("ghost") functionality

## 1. Load the index

Read `docs/adr/README.md`.

- If present, read `last-indexed`.
- Otherwise bootstrap by indexing all files in `docs/adr/`, using
  `git log --diff-filter=A` for creation dates.

## 2. Reconcile index structure

Compare ADR filenames with index rows.

- Missing row → add as `active`.
- Missing file → report in PR (do not delete row).

Do not read ADR contents in this step.

## 3. Identify candidates

Run:

```bash
git log --since="<last-indexed>" --oneline --name-only
```

Ignore commits affecting only lockfiles, formatting, or unrelated paths.

From remaining commits, identify ADRs whose referenced components or files may
have changed. Only these ADRs are analyzed in Step 4.

## 4. ADR verification (Explore agents)

For each candidate ADR, dispatch an Explore (Haiku) agent to gather evidence:

- Identify modules/files/APIs referenced by the ADR
- Determine whether they exist, were modified, or were removed
- Summarize relevant commits affecting those areas
- Check whether implementation matches the ADR's Decision section

Return evidence only. Do not classify or decide status.

## 5. Classification (primary model)

Using collected evidence, classify each ADR as:

- **Valid**: implementation matches ADR
- **Superseded**: later work replaced or removed the implementation
- **Drifted**: implementation exists but no longer matches ADR significantly
- **Unimplemented (ghost ADR)**: describes functionality not present in code
- **Needs review**: evidence insufficient or ambiguous

Rules:
- Mark ADR as superseded only when clearly replaced or removed.
- Do not mark ADRs superseded merely due to evolution or extension.
- Drifted and Unimplemented ADRs must NOT be marked superseded.

Record a one-sentence rationale for any non-valid classification.

## 6. Update ADR metadata

- Update only index rows whose status changed.
- Add recovered missing rows.
- Update `last-indexed`.
- For newly superseded ADRs, insert below title:

```markdown
> **Superseded by [ADR XXXX](./XXXX-filename.md)** — <reason>.
```

Do not modify other ADR content.

## 7. Open PR

If any changes exist:

Create a branch and PR including:

### Superseded ADRs

| ADR | Superseded by | Reason |
|-----|---------------|--------|

### Drifted ADRs

| ADR | Issue |
|-----|-------|

### Unimplemented ADRs (ghost ADRs)

| ADR | Missing implementation |
|-----|------------------------|

### Recovered index entries

| ADR | Issue |
|-----|-------|

### Orphan rows

| ADR | Issue |
|-----|-------|

Include updated `last-indexed`.

If no changes were found, report this and do not open a PR.