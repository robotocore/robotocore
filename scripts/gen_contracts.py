#!/usr/bin/env python3
"""Generate contract files from botocore service models.

Creates contracts/{service}.json for each specified service and operation,
derived entirely from botocore's service-2.json definitions.

Usage:
    uv run python scripts/gen_contracts.py                    # Generate all default contracts
    uv run python scripts/gen_contracts.py --service s3       # Generate one service
    uv run python scripts/gen_contracts.py --dry-run          # Preview without writing
"""

import argparse
import json
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from robotocore.testing.contract import AWSContract, save_contracts

# Default operations to generate contracts for — safe read-only operations
DEFAULT_CONTRACTS: dict[str, list[str]] = {
    "s3": ["ListBuckets", "GetBucketLocation", "HeadBucket"],
    "dynamodb": ["ListTables", "DescribeTable"],
    "sqs": ["ListQueues", "GetQueueUrl", "GetQueueAttributes"],
    "lambda": ["ListFunctions", "GetFunction"],
    "sns": ["ListTopics", "ListSubscriptions"],
    "iam": ["ListUsers", "ListRoles"],
    "sts": ["GetCallerIdentity"],
    "cloudformation": ["ListStacks", "DescribeStacks"],
    "logs": ["DescribeLogGroups"],
    "events": ["ListRules"],
}

CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"


def generate_contracts(
    service: str,
    operations: list[str],
    dry_run: bool = True,
) -> list[dict]:
    """Generate contracts for a service from botocore models."""
    contracts = []
    for op in operations:
        try:
            contract = AWSContract.from_botocore(service, op)
            contracts.append(contract)
            print(f"  {op}: {len(contract.response_keys)} keys, protocol={contract.protocol}")
        except Exception as e:
            print(f"  {op}: ERROR - {e}")

    if not dry_run and contracts:
        path = save_contracts(CONTRACTS_DIR, service, contracts)
        print(f"  -> Saved to {path}")

    return [c.to_dict() for c in contracts]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate AWS contract files from botocore models")
    parser.add_argument("--service", help="Generate for a single service")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Preview only")
    parser.add_argument("--write", action="store_true", default=False, help="Write files")
    parser.add_argument("--json", action="store_true", help="Output JSON")
    args = parser.parse_args()

    # Default to dry-run unless --write is specified
    dry_run = not args.write

    if args.service:
        services = {args.service: DEFAULT_CONTRACTS.get(args.service, [])}
        if not services[args.service]:
            print(f"No default operations for {args.service}. Use --operations to specify.")
            sys.exit(1)
    else:
        services = DEFAULT_CONTRACTS

    all_results = {}
    for svc, ops in sorted(services.items()):
        print(f"\n{svc}:")
        result = generate_contracts(svc, ops, dry_run=dry_run)
        all_results[svc] = result

    if args.json:
        print(json.dumps(all_results, indent=2))

    total_ops = sum(len(ops) for ops in all_results.values())
    mode = "generated" if not dry_run else "previewed (use --write to save)"
    print(f"\nTotal: {total_ops} contracts for {len(all_results)} services — {mode}")


if __name__ == "__main__":
    main()
