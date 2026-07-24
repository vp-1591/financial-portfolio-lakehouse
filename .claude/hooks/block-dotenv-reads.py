#!/usr/bin/env python
"""Pre-tool-use hook that blocks reads from .env files.

Allows .env.example (contains placeholders, not real secrets).
Blocks .env, .env.local, .env.production, and all other .env* variants.

Works for both Read/Grep tools (file_path / glob) and Bash tool (commands).
Reads JSON from stdin: {"tool_name": "...", "tool_input": {...}}.
Exits with code 0 (allow) or 2 (block).
"""

import json
import re
import sys

# ---------------------------------------------------------------------------
# Shared: .env filename detection
# ---------------------------------------------------------------------------

# Captures the full .env filename including suffix (e.g. ".env", ".env.local",
# ".env.example"). Uses lookbehind/ahead so it works inside both file paths
# and command strings without requiring a path-separator prefix.
_DOTENV_FILENAME_RE = re.compile(
    r"(?<![a-zA-Z0-9_])(\.env(?:\.[a-zA-Z0-9_]+)?)(?![a-zA-Z0-9_])",
    re.IGNORECASE,
)

# Filenames that are safe to read (templates / examples, no real secrets)
_SAFE_DOTENV_NAMES = frozenset({".env.example"})


def _is_real_dotenv(text: str) -> bool:
    """Return True if *text* references a real .env file (not .env.example).

    Scans for any .env filename. The first match that isn't in the safe set
    causes an immediate return of True.  Returns False only if every .env
    reference is safe (or there are none).
    """
    for m in _DOTENV_FILENAME_RE.finditer(text):
        filename = m.group(1).lower()
        if filename not in _SAFE_DOTENV_NAMES:
            return True
    return False


# ---------------------------------------------------------------------------
# Bash: command-level detection
# ---------------------------------------------------------------------------

_READ_COMMANDS = frozenset(
    {
        "cat",
        "less",
        "more",
        "head",
        "tail",
        "nl",
        "tac",
        "rev",
        "nano",
        "vim",
        "vi",
        "nvim",
        "code",
        "notepad",
        "explorer",
        "xdg-open",
        "start",
        "open",
        "type",
        "grep",
        "rg",
    }
)

# <read-command> ... <dotenv-filename><boundary>
_READ_CMD_RE = re.compile(
    r"(?:^|[\|;&])\s*("
    + "|".join(re.escape(c) for c in _READ_COMMANDS)
    + r")\s+.*?\.env(?:\.[a-zA-Z0-9_]+)?(?:[^a-zA-Z0-9.]|$)",
    re.IGNORECASE,
)

# source / .  ...  <dotenv-filename><boundary>
_SOURCE_RE = re.compile(
    r"(?:source|\.)\s+.*?\.env(?:\.[a-zA-Z0-9_]+)?(?:[^a-zA-Z0-9.]|$)",
    re.IGNORECASE,
)


def _is_read_dotenv_command(cmd: str) -> bool:
    """Return True if *cmd* reads or sources a real .env file."""
    for m in _READ_CMD_RE.finditer(cmd):
        if _is_real_dotenv(m.group(0)):
            return True
    for m in _SOURCE_RE.finditer(cmd):
        if _is_real_dotenv(m.group(0)):
            return True
    return False


# ---------------------------------------------------------------------------
# Tool dispatch
# ---------------------------------------------------------------------------

_BLOCKED_MSG = "Blocked: reading .env files is not allowed (security: secrets)."


def _check_read_tool(tool_input: dict) -> None:
    """Block if the Read tool targets a real .env file."""
    if _is_real_dotenv(tool_input.get("file_path", "")):
        print(_BLOCKED_MSG, file=sys.stderr)
        sys.exit(2)


def _check_grep_tool(tool_input: dict) -> None:
    """Block if the Grep tool targets a real .env file or glob."""
    path = tool_input.get("path", "")
    glob_pattern = tool_input.get("glob", "")
    if _is_real_dotenv(path) or _is_real_dotenv(glob_pattern):
        print(_BLOCKED_MSG, file=sys.stderr)
        sys.exit(2)


def _check_bash_tool(tool_input: dict) -> None:
    """Block if the Bash command reads or sources a real .env file."""
    if _is_read_dotenv_command(tool_input.get("command", "")):
        print(_BLOCKED_MSG, file=sys.stderr)
        sys.exit(2)


_TOOL_CHECKERS = {
    "Read": _check_read_tool,
    "Grep": _check_grep_tool,
    "Bash": _check_bash_tool,
}


def main() -> None:
    data = json.load(sys.stdin)
    tool = data.get("tool_name", "")
    checker = _TOOL_CHECKERS.get(tool)
    if checker:
        checker(data.get("tool_input", {}))
    sys.exit(0)


if __name__ == "__main__":
    main()
