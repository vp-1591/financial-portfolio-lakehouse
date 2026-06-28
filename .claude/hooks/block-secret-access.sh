#!/usr/bin/env bash
# block-secret-access.sh
# Prevents the coding agent from calling Bitwarden CLI (bw).
# This ensures the agent can never fetch secrets even if it discovers
# the secret resolution code.

input=$(cat)
cmd=$(echo "$input" | python3 -c "import json,sys; print(json.load(sys.stdin).get('tool_input',{}).get('command',''))")

if echo "$cmd" | grep -qE '\bbw\b'; then
  echo "Blocked: calling Bitwarden CLI (bw) is not allowed for agents." >&2
  exit 2
fi
exit 0