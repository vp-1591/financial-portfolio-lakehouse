# Disable Pytest Cache Provider

## Context

Running pytest in this repository creates `.pytest_cache` or redirected
`pytest-cache-files-*` directories. These generated cache directories clutter
the workspace and are not needed for the current test suite.

## Decision

Add a repository-level `pytest.ini` that disables pytest's cache provider with
`-p no:cacheprovider` for all local pytest runs.

## Consequences

Pytest no longer writes cache directories during normal test runs in this
repository. Features that depend on pytest's cache provider, such as failed-test
reruns using `--lf`, are unavailable unless the option is overridden manually.

## Validation

Run the focused pytest suite and confirm no new pytest cache directory is
created.
