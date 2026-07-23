#!/usr/bin/env python
"""Tests for the block-dotenv-reads hook.

Run with: .venv/Scripts/python.exe .claude/hooks/test_block_dotenv_reads.py
"""

import json
import subprocess
import sys
from pathlib import Path

HOOK = str(Path(__file__).parent / "block-dotenv-reads.py")

# Use variables to avoid triggering the hook on this test file itself
_DOTENV = ".env"
_DOTENV_EXAMPLE = ".env.example"
_DOTENV_LOCAL = ".env.local"
_DOTENV_PROD = ".env.production"
_DOTENVRC = ".envrc"


def run_hook(tool, tool_input):
    payload = json.dumps({"tool_name": tool, "tool_input": tool_input})
    result = subprocess.run(
        [sys.executable, HOOK],
        input=payload,
        capture_output=True,
        text=True,
    )
    return result.returncode


def test_read_tool():
    assert run_hook("Read", {"file_path": f"/app/{_DOTENV}"}) == 2, "Read .env blocked"
    assert run_hook("Read", {"file_path": f"/app/{_DOTENV_LOCAL}"}) == 2, (
        "Read .env.local blocked"
    )
    assert run_hook("Read", {"file_path": f"/app/{_DOTENV_PROD}"}) == 2, (
        "Read .env.production blocked"
    )
    assert run_hook("Read", {"file_path": f"/app/{_DOTENV_EXAMPLE}"}) == 0, (
        "Read .env.example allowed"
    )
    assert run_hook("Read", {"file_path": f"C:\\project\\{_DOTENV}"}) == 2, (
        "Read .env Windows blocked"
    )
    assert run_hook("Read", {"file_path": f"C:\\project\\{_DOTENV_EXAMPLE}"}) == 0, (
        "Read .env.example Windows allowed"
    )
    assert run_hook("Read", {"file_path": "/app/src/main.py"}) == 0, (
        "Read non-env allowed"
    )
    assert run_hook("Read", {"file_path": f"/app/{_DOTENVRC}"}) == 0, (
        "Read .envrc allowed (not .env)"
    )


def test_grep_tool():
    assert run_hook("Grep", {"path": "/app", "glob": _DOTENV}) == 2, (
        "Grep .env glob blocked"
    )
    assert run_hook("Grep", {"path": "/app", "glob": _DOTENV_EXAMPLE}) == 0, (
        "Grep .env.example glob allowed"
    )
    assert run_hook("Grep", {"path": f"/app/{_DOTENV}", "glob": "*.py"}) == 2, (
        "Grep .env path blocked"
    )
    assert run_hook("Grep", {"path": f"/app/{_DOTENV_EXAMPLE}", "glob": "*.py"}) == 0, (
        "Grep .env.example path allowed"
    )


def test_bash_tool():
    assert run_hook("Bash", {"command": f"cat {_DOTENV}"}) == 2, "cat .env blocked"
    assert run_hook("Bash", {"command": f"cat {_DOTENV_EXAMPLE}"}) == 0, (
        "cat .env.example allowed"
    )
    assert run_hook("Bash", {"command": f"cat {_DOTENV_LOCAL}"}) == 2, (
        "cat .env.local blocked"
    )
    assert run_hook("Bash", {"command": f"source {_DOTENV}"}) == 2, (
        "source .env blocked"
    )
    assert run_hook("Bash", {"command": f"source {_DOTENV_EXAMPLE}"}) == 0, (
        "source .env.example allowed"
    )
    assert run_hook("Bash", {"command": f". {_DOTENV}"}) == 2, "dot-source .env blocked"
    assert run_hook("Bash", {"command": f". {_DOTENV_EXAMPLE}"}) == 0, (
        "dot-source .env.example allowed"
    )
    assert (
        run_hook("Bash", {"command": f'git commit -m "update {_DOTENV_EXAMPLE}"'}) == 0
    ), "git commit .env.example allowed"
    assert run_hook("Bash", {"command": f"git add {_DOTENV_EXAMPLE}"}) == 0, (
        "git add .env.example allowed"
    )
    assert run_hook("Bash", {"command": "echo hello"}) == 0, "echo (no dotenv) allowed"
    assert run_hook("Bash", {"command": f"type {_DOTENV}"}) == 2, "type .env blocked"
    assert run_hook("Bash", {"command": f"type {_DOTENV_EXAMPLE}"}) == 0, (
        "type .env.example allowed"
    )
    # git commit is NOT a read command, so even mentioning .env is fine
    assert run_hook("Bash", {"command": f'git commit -m "fix {_DOTENV}"'}) == 0, (
        "git commit (not a read cmd) allowed"
    )


def main():
    failures = []
    for name, fn in [
        ("test_read_tool", test_read_tool),
        ("test_grep_tool", test_grep_tool),
        ("test_bash_tool", test_bash_tool),
    ]:
        try:
            fn()
        except AssertionError as e:
            failures.append(f"  {name}: {e}")
            print(f"FAIL: {name}")
        else:
            print(f"PASS: {name}")

    if failures:
        print(f"\n{len(failures)} test(s) failed:")
        for f in failures:
            print(f)
        sys.exit(1)
    else:
        print("\nAll tests passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
