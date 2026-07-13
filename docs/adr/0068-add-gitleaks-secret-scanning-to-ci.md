# ADR 0068: Add Gitleaks Secret Scanning to CI

## Context

The CI pipeline runs lint, tests, and Docker build checks on every PR, but has no secret scanning. Accidentally committed API keys, tokens, or other credentials could go undetected until someone notices — or worse, until they're exploited. We need automated detection of leaked secrets in PR diffs.

## Decision

Add a `gitleaks` job to the CI workflow that runs on every pull request using the official `gitleaks/gitleaks-action@v2` GitHub Action. This action:

- Automatically detects PR context and scans only the diff against the base branch.
- Fails the CI job (`exit-code 1`) when secrets are found.
- Requires full git history (`fetch-depth: 0`) to compute the diff correctly.

No custom `.gitleaks.toml` configuration is added — the default built-in rules cover most common secret patterns (AWS keys, GitHub tokens, private keys, etc.). We can add a config file later if false positives become an issue.

## Constraints

- The job only runs on `pull_request` events — there is no diff to scan on a push to main.
- Uses `GITHUB_TOKEN` from the default secrets (no additional secrets or permissions needed).
- Must not slow down CI significantly — gitleaks runs in seconds.

## Consequences

- **Positive:** Any PR that introduces a secret (API key, token, etc.) will be caught before merge, and the CI status will turn red.
- **Negative:** Potential false positives from default rules can block legitimate PRs. This can be mitigated by adding a `.gitleaks.toml` with allowlists.
- **Positive:** No additional GitHub secrets or permissions are required — `GITHUB_TOKEN` is automatically available.

## Validation

- Push the change on a feature branch and open PR #62.
- Confirm the `gitleaks` job appears in CI and passes (green) on a clean diff.
- If needed, temporarily commit a fake AWS key to verify the job turns red with a finding.