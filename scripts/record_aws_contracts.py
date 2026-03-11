#!/usr/bin/env python3
"""Record contracts from real AWS responses.

Runs safe read-only operations against real AWS and records response structure,
headers, and error formats as contract files in contracts/{service}.json.

SAFETY: Only runs GET/List/Describe operations — never creates or mutates resources.

Usage:
    uv run python scripts/record_aws_contracts.py --service s3       # Record S3 contracts
    uv run python scripts/record_aws_contracts.py --all              # Record all services
    uv run python scripts/record_aws_contracts.py --dry-run          # Preview operations
    uv run python scripts/record_aws_contracts.py --service s3 --write  # Actually record

Requires valid AWS credentials in the environment (AWS_PROFILE, AWS_ACCESS_KEY_ID, etc.).
"""

import argparse
import sys
from pathlib import Path

import boto3
import botocore.session

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from robotocore.testing.contract import AWSContract, Contract, save_contracts

CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"

# Safe read-only operations per service. Each entry is (operation, kwargs).
# These MUST be read-only — no creates, updates, or deletes.
SAFE_OPERATIONS: dict[str, list[tuple[str, dict]]] = {
    "s3": [
        ("list_buckets", {}),
    ],
    "dynamodb": [
        ("list_tables", {}),
    ],
    "sqs": [
        ("list_queues", {}),
    ],
    "lambda": [
        ("list_functions", {}),
    ],
    "sns": [
        ("list_topics", {}),
        ("list_subscriptions", {}),
    ],
    "iam": [
        ("list_users", {}),
        ("list_roles", {}),
    ],
    "sts": [
        ("get_caller_identity", {}),
    ],
    "cloudformation": [
        ("list_stacks", {}),
        ("describe_stacks", {}),
    ],
    "logs": [
        ("describe_log_groups", {}),
    ],
    "events": [
        ("list_rules", {}),
    ],
}

# Map from boto3 method names to botocore operation names
_session = botocore.session.get_session()


def _method_to_operation(service: str, method: str) -> str:
    """Convert a boto3 method name to a botocore operation name."""
    model = _session.get_service_model(service)
    for op_name in model.operation_names:
        # boto3 converts PascalCase to snake_case
        snake = "".join(f"_{c.lower()}" if c.isupper() else c for c in op_name).lstrip("_")
        if snake == method:
            return op_name
    return method


def record_service(service: str, dry_run: bool = True) -> list[Contract]:
    """Record contracts for a service by calling real AWS.

    Args:
        service: AWS service name.
        dry_run: If True, only print what would be done.

    Returns:
        List of recorded contracts.
    """
    ops = SAFE_OPERATIONS.get(service, [])
    if not ops:
        print(f"  No safe operations defined for {service}")
        return []

    contracts = []
    client = boto3.client(service, region_name="us-east-1")

    for method, kwargs in ops:
        op_name = _method_to_operation(service, method)
        if dry_run:
            print(f"  Would call {service}.{method}({kwargs})")
            continue

        try:
            # Use the raw HTTP response to capture headers
            http_response = None

            def capture_response(response, **_kwargs):
                nonlocal http_response
                http_response = response

            event_name = f"after-call.{service}.{op_name}"
            client.meta.events.register(event_name, capture_response)

            response = getattr(client, method)(**kwargs)

            # Extract headers from the HTTPHeaders in ResponseMetadata
            metadata = response.get("ResponseMetadata", {})
            headers = dict(metadata.get("HTTPHeaders", {}))

            model = _session.get_service_model(service)
            protocol = model.protocol

            contract = AWSContract.record(
                service=service,
                operation=op_name,
                response=response,
                headers=headers,
                protocol=protocol,
            )
            contracts.append(contract)
            print(f"  {op_name}: {len(contract.response_keys)} keys, status={contract.status_code}")

        except Exception as e:
            print(f"  {op_name}: ERROR - {type(e).__name__}: {e}")

    return contracts


def main() -> None:
    parser = argparse.ArgumentParser(description="Record AWS contracts from real AWS responses")
    parser.add_argument("--service", help="Record for a single service")
    parser.add_argument("--all", action="store_true", help="Record all services")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Preview only")
    parser.add_argument("--write", action="store_true", default=False, help="Write contract files")
    args = parser.parse_args()

    if not args.service and not args.all:
        parser.print_help()
        sys.exit(1)

    dry_run = not args.write

    services = [args.service] if args.service else sorted(SAFE_OPERATIONS.keys())

    for svc in services:
        print(f"\n{svc}:")
        contracts = record_service(svc, dry_run=dry_run)
        if contracts and not dry_run:
            path = save_contracts(CONTRACTS_DIR, svc, contracts)
            print(f"  -> Saved to {path}")

    mode = "recorded" if not dry_run else "previewed (use --write to record)"
    print(f"\nDone — {mode}")


if __name__ == "__main__":
    main()
