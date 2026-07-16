"""Print CloudWatch log event messages from a JSON file, one per line.

Usage: python3 format_log_events.py <json_file>

The input file should contain a JSON array of message strings
(as returned by ``aws logs filter-log-events --query 'events[*].message'``).
"""

import json
import sys


def main() -> None:
    with open(sys.argv[1]) as f:
        messages = json.load(f)
    for msg in messages:
        print(msg)


if __name__ == "__main__":
    main()
