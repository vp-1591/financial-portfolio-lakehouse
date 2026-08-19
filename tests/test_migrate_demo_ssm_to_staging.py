"""Tests for the B2 SSM migration (demo -> staging parameter path swap).

Covers the unit-testable ``copy_parameters()`` and
``retire_demo_parameters()`` entry points against an in-memory fake SSM
client (the script's boto3 wrappers take the client as a parameter, mirroring
the dependency-injection pattern in ``pipeline/sfn.py``).  The fake records
created and deleted parameters so the tests assert values are copied
byte-for-byte, never printed, and that the retire step is confined to
``/portfolio/demo/``.
"""

from __future__ import annotations

import pytest

from pipeline.migrations.migrate_demo_ssm_to_staging import (
    _DEST_PREFIX,
    _SECRETS,
    _SOURCE_PREFIX,
    _confirm_retire,
    copy_parameters,
    retire_demo_parameters,
)


class _FakeSSM:
    """Minimal in-memory SSM client: get/put/delete + get_parameters_by_path."""

    class exceptions:
        class ParameterNotFound(Exception):
            pass

    def __init__(self, params: dict[str, dict] | None = None) -> None:
        self.params: dict[str, dict] = dict(params or {})
        self.deleted: list[str] = []

    def get_parameter(self, Name: str, WithDecryption: bool = True) -> dict:
        if Name not in self.params:
            raise self.exceptions.ParameterNotFound(Name)
        # Real AWS omits KeyId from get_parameter even for SecureString
        # parameters; the script must resolve it via describe_parameters.
        return {
            "Parameter": {k: v for k, v in self.params[Name].items() if k != "KeyId"}
        }

    def describe_parameters(
        self, ParameterFilters: list[dict], MaxResults: int = 10
    ) -> dict:
        name = ParameterFilters[0]["Values"][0]
        if name in self.params:
            return {"Parameters": [dict(self.params[name])]}
        return {"Parameters": []}

    def put_parameter(self, **kwargs: object) -> None:
        self.params[str(kwargs["Name"])] = dict(kwargs)

    def get_paginator(self, operation: str):
        assert operation == "get_parameters_by_path"
        return self

    def paginate(self, Path: str, Recursive: bool = False):
        return [
            {
                "Parameters": [
                    {"Name": n} for n in sorted(self.params) if n.startswith(Path)
                ]
            }
        ]

    def delete_parameter(self, Name: str) -> None:
        self.params.pop(Name, None)
        self.deleted.append(Name)


def _demo_store(values: dict[str, str]) -> dict[str, dict]:
    """Build a source parameter store under /portfolio/demo/ with a KMS key."""
    return {
        f"{_SOURCE_PREFIX}{name}": {
            "Name": f"{_SOURCE_PREFIX}{name}",
            "Value": value,
            "Type": "SecureString",
            "KeyId": "kms-key-demo",
        }
        for name, value in values.items()
    }


@pytest.fixture
def secret_values() -> dict[str, str]:
    return {name: f"secret-{name.lower()}" for name in _SECRETS}


def test_copy_creates_staging_parameters_with_same_key(
    secret_values: dict[str, str],
) -> None:
    ssm = _FakeSSM(_demo_store(secret_values))
    assert ssm.params  # source params present

    created = copy_parameters(ssm)

    assert created == len(_SECRETS)
    for name in _SECRETS:
        dest = f"{_DEST_PREFIX}{name}"
        assert dest in ssm.params
        # Value copied byte-for-byte, never regenerated.
        assert ssm.params[dest]["Value"] == secret_values[name]
        assert ssm.params[dest]["Type"] == "SecureString"
        # Same KMS key as the source parameter.
        assert ssm.params[dest]["KeyId"] == "kms-key-demo"
    # The copy step never touches the source parameters.
    assert all(f"{_SOURCE_PREFIX}{name}" in ssm.params for name in _SECRETS)


def test_copy_normalizes_default_ssm_kms_key(secret_values: dict[str, str]) -> None:
    # Parameters encrypted with the default SSM key report KeyId "aws/ssm"
    # (via describe_parameters); the destination must omit KeyId so SSM uses
    # the default key, matching the source.
    store = _demo_store(secret_values)
    for param in store.values():
        param["KeyId"] = "aws/ssm"
    ssm = _FakeSSM(store)

    created = copy_parameters(ssm)

    assert created == len(_SECRETS)
    for name in _SECRETS:
        assert "KeyId" not in ssm.params[f"{_DEST_PREFIX}{name}"]


def test_copy_is_idempotent_when_destination_matches(
    secret_values: dict[str, str],
) -> None:
    store = _demo_store(secret_values)
    for name in _SECRETS:
        store[f"{_DEST_PREFIX}{name}"] = {
            "Name": f"{_DEST_PREFIX}{name}",
            "Value": secret_values[name],
            "Type": "SecureString",
            "KeyId": "kms-key-demo",
        }
    ssm = _FakeSSM(store)

    created = copy_parameters(ssm)

    assert created == 0
    assert len(ssm.params) == 2 * len(_SECRETS)  # nothing added, nothing removed


def test_copy_raises_when_destination_differs(secret_values: dict[str, str]) -> None:
    store = _demo_store(secret_values)
    store[f"{_DEST_PREFIX}ENCRYPTION_KEY"] = {
        "Name": f"{_DEST_PREFIX}ENCRYPTION_KEY",
        "Value": "a-different-key",
        "Type": "SecureString",
        "KeyId": "kms-key-demo",
    }
    ssm = _FakeSSM(store)

    with pytest.raises(RuntimeError, match="different value"):
        copy_parameters(ssm)
    # The mismatched destination was not overwritten.
    assert ssm.params[f"{_DEST_PREFIX}ENCRYPTION_KEY"]["Value"] == "a-different-key"


def test_copy_skips_missing_source() -> None:
    # Only a subset of the five secrets exists on the source path.
    subset = {"IBKR_FLEX_TOKEN": "token"}
    ssm = _FakeSSM(_demo_store(subset))

    created = copy_parameters(ssm)

    assert created == 1
    assert f"{_DEST_PREFIX}IBKR_FLEX_TOKEN" in ssm.params
    assert all(
        f"{_DEST_PREFIX}{name}" not in ssm.params
        for name in _SECRETS
        if name != "IBKR_FLEX_TOKEN"
    )


def test_copy_dry_run_creates_nothing(secret_values: dict[str, str]) -> None:
    ssm = _FakeSSM(_demo_store(secret_values))

    created = copy_parameters(ssm, dry_run=True)

    assert created == 0
    assert all(not n.startswith(_DEST_PREFIX) for n in ssm.params)


def test_retire_deletes_only_demo_parameters(secret_values: dict[str, str]) -> None:
    store = _demo_store(secret_values)
    for name in _SECRETS:
        store[f"{_DEST_PREFIX}{name}"] = {"Name": f"{_DEST_PREFIX}{name}", "Value": "x"}
    ssm = _FakeSSM(store)

    retired = retire_demo_parameters(ssm, assume_yes=True)

    assert retired == len(_SECRETS)
    assert sorted(ssm.deleted) == sorted(f"{_SOURCE_PREFIX}{n}" for n in _SECRETS)
    # Staging (and prod, had any existed) parameters are untouched.
    assert all(n.startswith(_DEST_PREFIX) for n in ssm.params)


def test_retire_nothing_to_retire() -> None:
    ssm = _FakeSSM()

    retired = retire_demo_parameters(ssm, assume_yes=True)

    assert retired == 0
    assert ssm.deleted == []


def test_retire_dry_run_deletes_nothing(secret_values: dict[str, str]) -> None:
    ssm = _FakeSSM(_demo_store(secret_values))

    retired = retire_demo_parameters(ssm, dry_run=True, assume_yes=True)

    assert retired == 0
    assert ssm.deleted == []
    assert len(ssm.params) == len(_SECRETS)


def test_retire_aborts_without_confirmation(
    secret_values: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    ssm = _FakeSSM(_demo_store(secret_values))
    monkeypatch.setattr(
        "pipeline.migrations.migrate_demo_ssm_to_staging._confirm_retire", lambda: False
    )

    retired = retire_demo_parameters(ssm, assume_yes=False)

    assert retired == 0
    assert ssm.deleted == []
    assert len(ssm.params) == len(_SECRETS)


def test_confirm_retire_raises_on_non_interactive_stdin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # pytest runs with non-interactive stdin, so no input() should be reached.
    def _fail_input(_prompt: str) -> str:
        raise AssertionError("input() must not be called when stdin is not a TTY")

    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    monkeypatch.setattr("builtins.input", _fail_input)

    with pytest.raises(RuntimeError, match="Non-interactive stdin"):
        _confirm_retire()
