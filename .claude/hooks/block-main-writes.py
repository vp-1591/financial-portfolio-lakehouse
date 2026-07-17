#!/usr/bin/env python
"""Pre-tool-use hook that blocks git commit/push on main/master branches.

Reads JSON from stdin: {"tool": "...", "tool_input": {...}}.
Exits with code 0 (allow) or 2 (block).
"""

import json
import re
import subprocess
import sys


def get_current_branch() -> str:
    """Return the current git branch name, or empty string if not in a git repo."""
    try:
        result = subprocess.run(
            ["git", "branch", "--show-current"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.stdout.strip()
    except (subprocess.SubprocessError, FileNotFoundError):
        return ""


def is_write_command(cmd: str) -> bool:
    """Check if the command is a git commit or push."""
    return bool(re.search(r"\bgit (commit|push)\b", cmd))


def main() -> None:
    data = json.load(sys.stdin)
    tool_input = data.get("tool_input", {})
    command = tool_input.get("command", "")

    branch = get_current_branch()
    if branch in ("main", "master") and is_write_command(command):
        print(
            f"Blocked: can't commit/push while on '{branch}'. "
            "Create a feature branch first.",
            file=sys.stderr,
        )
        sys.exit(2)

    sys.exit(0)


if __name__ == "__main__":
    main()
