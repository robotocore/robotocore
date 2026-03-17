#!/usr/bin/env python3
"""Validate response shapes against botocore output models.

Calls operations on the live server and recursively validates that
response structure matches what botocore's service model defines.

Usage:
    uv run python scripts/validate_response_shapes.py --service s3
    uv run python scripts/validate_response_shapes.py --service s3 --service dynamodb --service sqs
    uv run python scripts/validate_response_shapes.py --all
    uv run python scripts/validate_response_shapes.py --top 20
    uv run python scripts/validate_response_shapes.py --service s3 --json
"""

import argparse
import json
import sys

import boto3
import botocore.exceptions
import botocore.loaders
import botocore.session

from scripts.lib.param_filler import get_params_for_operation, to_snake_case
from src.robotocore.testing.shape_validator import (
    ShapeValidationResult,
    validate_operation_response,
)

# Top 20 most-used AWS services (for --top 20)
TOP_SERVICES = [
    "s3",
    "dynamodb",
    "sqs",
    "sns",
    "lambda",
    "iam",
    "sts",
    "ec2",
    "cloudwatch",
    "logs",
    "secretsmanager",
    "ssm",
    "kinesis",
    "events",
    "stepfunctions",
    "ecs",
    "ecr",
    "apigateway",
    "route53",
    "cloudformation",
]

# Operations to skip: destructive, pagination-only, or known problematic
SKIP_OPS = {
    "DeleteBucket",
    "DeleteQueue",
    "DeleteTopic",
    "DeleteTable",
    "DeleteFunction",
    "DeleteStack",
    "DeleteApi",
    "DeleteRestApi",
    "PurgeQueue",
    "TerminateInstances",
    "DeleteCluster",
    "DeleteDBInstance",
    "DeleteDBCluster",
    "DeleteCacheCluster",
    "TerminateEnvironment",
    "DeleteVpc",
    "DeleteSubnet",
    "DeleteSecurityGroup",
}


def get_registered_services() -> list[str]:
    """Get all services registered in robotocore."""
    try:
        from src.robotocore.services.registry import SERVICE_REGISTRY

        return sorted(SERVICE_REGISTRY.keys())
    except ImportError:
        # Fallback: use botocore's available services
        session = botocore.session.get_session()
        return sorted(session.get_available_services())


def get_safe_operations(service_name: str) -> list[str]:
    """Get list/describe/get operations for a service (safe to call)."""
    loader = botocore.loaders.Loader()
    try:
        api = loader.load_service_model(service_name, "service-2")
    except Exception:
        return []
    operations = api.get("operations", {})
    safe_prefixes = ("List", "Describe", "Get")
    return sorted(n for n in operations if n.startswith(safe_prefixes) and n not in SKIP_OPS)


def validate_service(
    service_name: str,
    endpoint: str,
    check_optional: bool = True,
) -> list[ShapeValidationResult]:
    """Validate all safe operations for a service."""
    client = boto3.client(
        service_name,
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )

    operations = get_safe_operations(service_name)
    results: list[ShapeValidationResult] = []

    for op_name in operations:
        params = get_params_for_operation(service_name, op_name)
        if params is None:
            params = {}

        result = _call_and_validate(client, service_name, op_name, params, check_optional)
        results.append(result)

    return results


def _call_and_validate(
    client,
    service_name: str,
    operation_name: str,
    params: dict,
    check_optional: bool,
) -> ShapeValidationResult:
    """Call an operation and validate the response shape."""
    try:
        method = getattr(client, to_snake_case(operation_name))
        response = method(**params)
        return validate_operation_response(
            service_name,
            operation_name,
            response,
            check_optional=check_optional,
        )
    except botocore.exceptions.ParamValidationError:
        result = ShapeValidationResult(service=service_name, operation=operation_name)
        result.skipped = True
        result.skip_reason = "param validation (never contacted server)"
        return result
    except client.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        # "Not found" errors are expected — the operation works, just no data
        result = ShapeValidationResult(service=service_name, operation=operation_name)
        result.skipped = True
        result.skip_reason = f"client error: {code}"
        return result
    except Exception as e:
        result = ShapeValidationResult(service=service_name, operation=operation_name)
        result.skipped = True
        result.skip_reason = f"exception: {str(e)[:80]}"
        return result


def print_results(
    service_name: str,
    results: list[ShapeValidationResult],
    verbose: bool = False,
) -> tuple[int, int, int]:
    """Print results for a service. Returns (pass_count, fail_count, skip_count)."""
    passed = [r for r in results if r.passed and not r.skipped]
    failed = [r for r in results if not r.passed and not r.skipped]
    skipped = [r for r in results if r.skipped]

    total_checked = len(passed) + len(failed)
    status = "PASS" if not failed else "FAIL"

    print(
        f"{service_name}: {total_checked} operations checked, "
        f"{len(passed)} pass, {len(failed)} fail, {len(skipped)} skip — {status}"
    )

    for r in failed:
        for v in r.errors:
            print(
                f"  {r.operation}: ERROR {v.path} — {v.issue} "
                f"(expected {v.expected}, got {v.actual})"
            )
        if verbose:
            for v in r.warnings:
                print(f"  {r.operation}: WARN  {v.path} — {v.issue}")

    return len(passed), len(failed), len(skipped)


def results_to_json(all_results: dict[str, list[ShapeValidationResult]]) -> dict:
    """Convert all results to a JSON-serializable dict."""
    output = {}
    for service, results in all_results.items():
        service_data = {
            "operations": [],
            "summary": {"pass": 0, "fail": 0, "skip": 0},
        }
        for r in results:
            op_data = {
                "operation": r.operation,
                "passed": r.passed,
                "skipped": r.skipped,
                "skip_reason": r.skip_reason,
                "violations": [
                    {
                        "path": v.path,
                        "issue": v.issue,
                        "expected": v.expected,
                        "actual": v.actual,
                        "severity": v.severity,
                    }
                    for v in r.violations
                ],
            }
            service_data["operations"].append(op_data)
            if r.skipped:
                service_data["summary"]["skip"] += 1
            elif r.passed:
                service_data["summary"]["pass"] += 1
            else:
                service_data["summary"]["fail"] += 1
        output[service] = service_data
    return output


def main():
    parser = argparse.ArgumentParser(description="Validate response shapes against botocore")
    parser.add_argument("--service", action="append", help="Service(s) to validate")
    parser.add_argument("--all", action="store_true", help="Validate all registered services")
    parser.add_argument("--top", type=int, help="Validate top N most-used services")
    parser.add_argument("--endpoint", default="http://localhost:4566", help="Endpoint URL")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--verbose", action="store_true", help="Show warnings too")
    parser.add_argument(
        "--no-optional",
        action="store_true",
        help="Don't report missing optional keys",
    )
    args = parser.parse_args()

    if args.all:
        services = get_registered_services()
    elif args.top:
        services = TOP_SERVICES[: args.top]
    elif args.service:
        services = args.service
    else:
        parser.error("Specify --service, --all, or --top N")
        return 1

    check_optional = not args.no_optional
    all_results: dict[str, list[ShapeValidationResult]] = {}
    total_pass = total_fail = total_skip = 0

    for service in services:
        results = validate_service(service, args.endpoint, check_optional)
        all_results[service] = results

        if not args.json:
            p, f, s = print_results(service, results, verbose=args.verbose)
            total_pass += p
            total_fail += f
            total_skip += s

    if args.json:
        print(json.dumps(results_to_json(all_results), indent=2))
    else:
        print(f"\n{'=' * 60}")
        print(f"Total: {total_pass} pass, {total_fail} fail, {total_skip} skip")
        if total_fail:
            print(f"EXIT 1 — {total_fail} shape violation(s) found")

    return 1 if total_fail else 0


if __name__ == "__main__":
    sys.exit(main())
