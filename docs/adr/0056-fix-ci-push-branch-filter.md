# 0056: Fix CI Push Branch Filter to Eliminate Duplicate Runs

> **Supersedes** — This updates the `push` trigger scope from ADR [0016](./0016-github-actions-ci.md), which defined the original CI workflow triggering on every push without a branch filter.

## Context

The CI workflow (`ci.yml`) triggers on `push:` without a branch filter and `pull_request: branches: [main]`. When a developer pushes a commit to a PR branch, GitHub fires both a `push` event and a `pull_request` event, causing the `lint` and `test` jobs to run twice for the same code. This produces 6 checks (1 skipped) on every PR commit — wasting runner minutes and cluttering the GitHub checks UI.

The `docker` job has `if: github.event_name == 'pull_request'`, so it correctly runs only on PR events and is skipped on push events. However, the skipped check is visible as noise.

The concurrency group (`github.workflow + pull_request.number || github.ref`) does not cancel either run because `push` events on PR branches resolve to `github.ref` (e.g. `refs/heads/feature-branch`) while `pull_request` events resolve to the PR number — producing different concurrency keys.

## Decision

Add `branches: [main]` to the `push:` trigger in `ci.yml`, restricting push-triggered CI to the `main` branch only:

```yaml
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
```

The `docker` job's `if: github.event_name == 'pull_request'` condition is left unchanged — it's a PR-only smoke test, and on `main` pushes the `deploy-staging` workflow already builds and pushes the Docker image.

## Constraints

- CI must still run on every PR targeting `main` (via `pull_request` event)
- CI must still run on every push to `main` (post-merge verification before deploy-staging runs)
- PR branch pushes must not trigger duplicate CI runs

## Consequences

- PR branch pushes now trigger 3 CI checks (lint, test, docker) instead of 6
- Merge to main still triggers 4 checks (3 CI + 1 deploy-staging), with `docker` correctly skipped
- No more duplicate lint/test runs wasting runner minutes
- The "Skipped" docker check on push events remains — this is by design and cosmetic only

## Validation

- Push a branch and open a PR — confirm only 3 CI checks appear (lint, test, docker), all from the `pull_request` event
- Merge the PR — confirm 4 checks appear (3 CI + 1 deploy-staging), with `docker` skipped
- Confirm no duplicate lint/test runs on either event