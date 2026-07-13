#!/usr/bin/env bash
# block-main-writes.sh
input=$(cat)
cmd=$(echo "$input" | python -c "import json,sys; print(json.load(sys.stdin).get('tool_input',{}).get('command',''))")
branch=$(git branch --show-current 2>/dev/null)

if [[ "$branch" == "main" || "$branch" == "master" ]] && echo "$cmd" | grep -Eq '\bgit (commit|push)\b'; then
  echo "Blocked: can't commit/push while on $branch. Create a feature branch first." >&2
  exit 2
fi
exit 0