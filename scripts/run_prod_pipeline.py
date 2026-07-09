#!/usr/bin/env python3
"""Manually trigger the production Step Functions orchestrator.

Usage:
    # Run daily connectors (IBKR + Trading 212) — same as the scheduled trigger
    python scripts/run_prod_pipeline.py

    # Run all connectors including XTB
    python scripts/run_prod_pipeline.py --with-xtb

    # Run with a specific XTB file
    python scripts/run_prod_pipeline.py --with-xtb --xtb-file s3://investment-portfolio-pipeline/staging/xtb/file.csv

    # Dry-run: print the input JSON without starting an execution
    python scripts/run_prod_pipeline.py --dry-run
"""

import argparse
import json
import sys
from datetime import datetime, timezone

import boto3

REGION = "eu-west-1"
STATE_MACHINE_ARN = (
    "arn:aws:states:eu-west-1:364947026340:stateMachine:portfolio-pipeline-orchestrator"
)

# Task definition ARNs (prod)
TASK_DEF_ARNS = {
    "ibkr": "arn:aws:ecs:eu-west-1:364947026340:task-definition/portfolio-pipeline-prod-ibkr:1",
    "trading212": "arn:aws:ecs:eu-west-1:364947026340:task-definition/portfolio-pipeline-prod-trading212:1",
    "xtb": "arn:aws:ecs:eu-west-1:364947026340:task-definition/portfolio-pipeline-prod-xtb:1",
    "consolidate_allocate": "arn:aws:ecs:eu-west-1:364947026340:task-definition/portfolio-pipeline-prod-consolidate-allocate:1",
}


def build_input(with_xtb: bool, xtb_file: str | None = None) -> dict:
    """Build the execution input matching the state machine's expected schema."""
    connectors = [
        {
            "name": "ibkr",
            "task_def_arn": TASK_DEF_ARNS["ibkr"],
            "command": ["run-connector", "ibkr", "--target-currency", "EUR"],
        },
        {
            "name": "trading212",
            "task_def_arn": TASK_DEF_ARNS["trading212"],
            "command": ["run-connector", "trading212", "--target-currency", "EUR"],
        },
    ]

    if with_xtb:
        cmd = ["run-connector", "xtb", "--target-currency", "EUR"]
        if xtb_file:
            cmd.extend(["--xtb-file", xtb_file])
        connectors.append(
            {
                "name": "xtb",
                "task_def_arn": TASK_DEF_ARNS["xtb"],
                "command": cmd,
            }
        )

    return {
        "connectors": connectors,
        "consolidate_allocate_task_def_arn": TASK_DEF_ARNS["consolidate_allocate"],
        "demo": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manually trigger the production pipeline orchestrator"
    )
    parser.add_argument(
        "--with-xtb",
        action="store_true",
        help="Include the XTB connector (default: IBKR + Trading 212 only)",
    )
    parser.add_argument(
        "--xtb-file",
        type=str,
        default=None,
        help="S3 URI for the XTB file (e.g. s3://bucket/staging/xtb/file.csv)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the input JSON without starting an execution",
    )
    args = parser.parse_args()

    execution_input = build_input(with_xtb=args.with_xtb, xtb_file=args.xtb_file)
    input_json = json.dumps(execution_input, indent=2)

    if args.dry_run:
        print("Dry-run — execution input:")
        print(input_json)
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    execution_name = f"manual-{timestamp}"

    sfn = boto3.client("stepfunctions", region_name=REGION)
    response = sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=execution_name,
        input=input_json,
    )

    print(f"Started execution: {execution_name}")
    print(f"ARN:              {response['executionArn']}")
    print(f"Start date:       {response['startDate']}")
    print()
    print("Monitor at:")
    print(f"  https://eu-west-1.console.aws.amazon.com/states/home?region=eu-west-1#/executions/details/{response['executionArn']}")


if __name__ == "__main__":
    main()