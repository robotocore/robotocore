#!/usr/bin/env python3
"""CI-friendly contract validation script.

Validates robotocore responses against recorded AWS contracts by running
operations against the local server and checking response structure.

Usage:
    uv run python scripts/validate_contracts.py --service s3   # Validate one service
    uv run python scripts/validate_contracts.py --all          # Validate all
    uv run python scripts/validate_contracts.py --all --strict # Fail on any mismatch
    uv run python scripts/validate_contracts.py --all --json   # JSON report
"""

import argparse
import json
import os
import sys
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from robotocore.testing.contract import AWSContract, Contract, load_contracts

CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"
ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")

# Operations that require pre-existing resources — skip them in validation
# unless the resource exists. Map from (service, operation) to a setup function.
OPERATIONS_NEEDING_SETUP: dict[tuple[str, str], dict] = {
    ("s3", "GetBucketLocation"): {"skip": True, "reason": "needs existing bucket"},
    ("s3", "HeadBucket"): {"skip": True, "reason": "needs existing bucket"},
    ("dynamodb", "DescribeTable"): {"skip": True, "reason": "needs existing table"},
    ("sqs", "GetQueueUrl"): {"skip": True, "reason": "needs existing queue"},
    ("sqs", "GetQueueAttributes"): {"skip": True, "reason": "needs existing queue"},
    ("lambda", "GetFunction"): {"skip": True, "reason": "needs existing function"},
}


def make_client(service: str):
    """Create a boto3 client pointing at robotocore."""
    config_kwargs = {}
    if service == "s3":
        config_kwargs["s3"] = {"addressing_style": "path"}
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=Config(**config_kwargs),
    )


def _operation_to_method(operation: str) -> str:
    """Convert PascalCase operation name to snake_case method name."""
    result = []
    for i, c in enumerate(operation):
        if c.isupper() and i > 0:
            result.append("_")
        result.append(c.lower())
    return "".join(result)


def validate_service(
    service: str,
    contracts: list[Contract],
    strict: bool = False,
) -> dict:
    """Validate robotocore responses against contracts for a service."""
    client = make_client(service)
    results = {}

    for contract in contracts:
        op = contract.operation
        setup_info = OPERATIONS_NEEDING_SETUP.get((service, op))
        if setup_info and setup_info.get("skip"):
            results[op] = {
                "status": "skipped",
                "reason": setup_info["reason"],
            }
            print(f"  {op}: SKIPPED ({setup_info['reason']})")
            continue

        method = _operation_to_method(op)
        try:
            response = getattr(client, method)()
            metadata = response.get("ResponseMetadata", {})
            headers = dict(metadata.get("HTTPHeaders", {}))

            result = AWSContract.validate(
                service=service,
                operation=op,
                response=response,
                headers=headers,
                contract=contract,
            )

            status = "PASS" if result.passed else ("FAIL" if strict else "WARN")
            results[op] = {
                "status": status,
                **result.to_dict(),
            }
            label = status
            if not result.passed:
                issues = []
                if result.missing_keys:
                    issues.append(f"missing={result.missing_keys}")
                if result.extra_keys:
                    issues.append(f"extra={result.extra_keys}")
                if result.wrong_types:
                    issues.append(f"types={result.wrong_types}")
                if result.missing_headers:
                    issues.append(f"headers={result.missing_headers}")
                if result.format_mismatches:
                    issues.append(f"format={result.format_mismatches}")
                label += f" ({', '.join(issues)})"
            print(f"  {op}: {label}")

        except Exception as e:
            results[op] = {
                "status": "ERROR",
                "error": f"{type(e).__name__}: {e}",
            }
            print(f"  {op}: ERROR - {type(e).__name__}: {e}")

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate robotocore against AWS contracts")
    parser.add_argument("--service", help="Validate one service")
    parser.add_argument("--all", action="store_true", help="Validate all services with contracts")
    parser.add_argument("--strict", action="store_true", help="Fail on any mismatch")
    parser.add_argument("--json", action="store_true", help="Output JSON report")
    args = parser.parse_args()

    if not args.service and not args.all:
        parser.print_help()
        sys.exit(1)

    all_contracts = load_contracts(CONTRACTS_DIR)

    if args.service:
        if args.service not in all_contracts:
            print(f"No contracts found for {args.service}")
            sys.exit(1)
        services = {args.service: all_contracts[args.service]}
    else:
        services = all_contracts

    all_results = {}
    total_pass = 0
    total_fail = 0
    total_skip = 0
    total_error = 0

    for svc, contracts in sorted(services.items()):
        print(f"\n{svc}:")
        results = validate_service(svc, contracts, strict=args.strict)
        all_results[svc] = results

        for op, r in results.items():
            if r["status"] == "PASS":
                total_pass += 1
            elif r["status"] in ("FAIL", "WARN"):
                total_fail += 1
            elif r["status"] == "skipped":
                total_skip += 1
            else:
                total_error += 1

    if args.json:
        print(json.dumps(all_results, indent=2))

    total = total_pass + total_fail + total_skip + total_error
    print(
        f"\nResults: {total_pass}/{total} passed, {total_fail} failed, "
        f"{total_skip} skipped, {total_error} errors"
    )

    if args.strict and total_fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
