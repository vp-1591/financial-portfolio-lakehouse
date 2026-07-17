#!/usr/bin/env python
"""Pre-tool-use hook that blocks reads from .env files.

Works for both Read tool (file_path) and Bash tool (commands that read .env).
Reads JSON from stdin: {"tool_name": "...", "tool_input": {...}}.
Exits with code 0 (allow) or 2 (block).
"""

import json
import re
import sys

# Patterns for .env file paths (matches .env, .env.local, .env.production, etc.)
DOTENV_RE = re.compile(r"(^|[/\\])\.env([^a-zA-Z0-9]|$)", re.IGNORECASE)

# Bash commands that read file contents
READ_COMMANDS = {
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

# Pattern to match a read command followed by a .env path
READ_CMD_RE = re.compile(
    r"(?:^|[\|;&])\s*("
    + "|".join(re.escape(c) for c in READ_COMMANDS)
    + r")\s+.*?\.env(?:[^a-zA-Z0-9]|$)",
    re.IGNORECASE,
)

# Pattern to match sourcing .env files
SOURCE_RE = re.compile(r"(?:source|\.)\s+.*\.env", re.IGNORECASE)


def is_dotenv_path(path: str) -> bool:
    return bool(DOTENV_RE.search(path))


def is_read_command(cmd: str) -> bool:
    return bool(READ_CMD_RE.search(cmd)) or bool(SOURCE_RE.search(cmd))


def main() -> None:
    data = json.load(sys.stdin)
    tool = data.get("tool_name", "")
    tool_input = data.get("tool_input", {})

    if tool == "Read":
        file_path = tool_input.get("file_path", "")
        if is_dotenv_path(file_path):
            print(
                "Blocked: reading .env files is not allowed (security: secrets).",
                file=sys.stderr,
            )
            sys.exit(2)

    if tool == "Bash":
        command = tool_input.get("command", "")
        if is_read_command(command):
            print(
                "Blocked: reading .env files is not allowed (security: secrets).",
                file=sys.stderr,
            )
            sys.exit(2)

    if tool == "Grep":
        path = tool_input.get("path", "")
        glob = tool_input.get("glob", "")
        if is_dotenv_path(path) or is_dotenv_path(glob):
            print(
                "Blocked: reading .env files is not allowed (security: secrets).",
                file=sys.stderr,
            )
            sys.exit(2)

    sys.exit(0)


if __name__ == "__main__":
    main()
