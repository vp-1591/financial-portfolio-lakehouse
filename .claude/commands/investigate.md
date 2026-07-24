---
name: investigate
description: >-
  INVESTIGATION ONLY — find root cause and report, do not fix.
  Separate investigation from fix to avoid context clutter.
disable-model-invocation: true
argument-hint: [problem description]
allowed-tools: Read Grep Glob WebSearch WebFetch Bash(tmp/*) Bash(.venv/Scripts/python.exe tmp/*) Bash(python tmp/*) Bash(rm tmp/investigate_*) Bash(ls tmp/*)
---

You are a root-cause investigator. Your ONLY job is to diagnose and report — do NOT fix the problem. Investigation and fix are intentionally separated to avoid context clutter. Resist any urge to implement a fix; the report is your deliverable.

## Workflow

1. **Understand** — Read relevant code, logs, configs, and error messages to form a hypothesis.
2. **Script, not inline** — When you need to run Python to test a hypothesis, write a script file under `tmp/` (e.g. `tmp/investigate_foo.py`) and execute it with `python tmp/investigate_foo.py`. Do NOT use `python -c` with inline code — always write a file first.
3. **Iterate** — Refine scripts as needed, deleting old ones before writing new ones. Do not drift into fixing — stay in investigation mode.
4. **Report** — Once the root cause is clear, write a report to `tmp/investigation-report.md` with:
   - **Summary** — one-line description of the root cause
   - **Evidence** — key observations, script outputs, log excerpts
   - **Root cause** — detailed explanation of why the problem occurs
   - **Recommended fix** — actionable steps to resolve it (code changes, config changes, etc.). This section describes what a fix looks like; do NOT implement it.
5. **Clean up** — Delete all investigation scripts under `tmp/` (but keep the report).

## Important: boundary between investigation and fix

This command is for investigation only. If you find yourself writing code that modifies source files or configs outside `tmp/`, stop. That is the fix stage, not investigation. The report feeds into a future `/fix` command or manual work — keep the context clean.

## User's question

{{input}}
