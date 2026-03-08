#!/usr/bin/env python3
"""Probe a service to discover which operations actually work.

Usage:
    uv run python scripts/probe_service.py --service sqs
    uv run python scripts/probe_service.py --service ec2 --endpoint http://localhost:4566
    uv run python scripts/probe_service.py --service s3 --json

Calls each operation with minimal valid parameters and reports which ones
return success vs error. Use this BEFORE writing compat tests to avoid
writing tests for operations that aren't implemented.

Output: list of working operations that are safe to write tests for.
"""

import argparse
import json
import sys

import boto3
import botocore.exceptions
import botocore.loaders

# Minimal valid parameters for common parameter types.
# These are enough to make the API call succeed syntactically,
# even if the referenced resources don't exist.
PARAM_DEFAULTS = {
    "string": "test-probe-value",
    "integer": 1,
    "long": 1,
    "boolean": False,
    "timestamp": "2024-01-01T00:00:00Z",
    "blob": b"test",
    "list": [],
    "map": {},
    "structure": {},
}

# Operations that are destructive or have side effects we don't want during probing.
SKIP_OPERATIONS = {
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
}

# Operations that require specific parameters we can provide.
KNOWN_PARAMS = {
    "sqs": {
        "CreateQueue": {"QueueName": "probe-test-queue"},
        "ListQueues": {},
        "GetQueueUrl": {"QueueName": "probe-test-queue"},
    },
    "s3": {
        "ListBuckets": {},
        "CreateBucket": {"Bucket": "probe-test-bucket"},
    },
    "dynamodb": {
        "ListTables": {},
    },
    "sns": {
        "ListTopics": {},
        "CreateTopic": {"Name": "probe-test-topic"},
    },
    "iam": {
        "ListRoles": {},
        "ListUsers": {},
        "ListPolicies": {},
    },
    "lambda": {
        "ListFunctions": {},
    },
    "sts": {
        "GetCallerIdentity": {},
    },
    "cloudformation": {
        "ListStacks": {},
    },
    "events": {
        "ListEventBuses": {},
        "ListRules": {},
    },
    "logs": {
        "DescribeLogGroups": {},
    },
    "kinesis": {
        "ListStreams": {},
    },
    "secretsmanager": {
        "ListSecrets": {},
    },
    "ssm": {
        "DescribeParameters": {},
    },
    "kms": {
        "ListKeys": {},
        "ListAliases": {},
    },
    "ec2": {
        "DescribeInstances": {},
        "DescribeVpcs": {},
        "DescribeSubnets": {},
        "DescribeSecurityGroups": {},
        "DescribeKeyPairs": {},
        "DescribeImages": {},
        "DescribeRegions": {},
        "DescribeAvailabilityZones": {},
    },
    "route53": {
        "ListHostedZones": {},
    },
    "acm": {
        "ListCertificates": {},
    },
    "ecr": {
        "DescribeRepositories": {},
    },
    "cloudwatch": {
        "ListMetrics": {},
        "ListDashboards": {},
        "DescribeAlarms": {},
    },
}


def get_list_operations(service_name: str) -> list[str]:
    """Get all List/Describe/Get operations for a service from botocore."""
    loader = botocore.loaders.Loader()
    try:
        api = loader.load_service_model(service_name, "service-2")
    except Exception:
        return []
    operations = api.get("operations", {})
    # Focus on read operations that are safe to call
    safe_prefixes = ("List", "Describe", "Get")
    return sorted(
        name
        for name in operations
        if name.startswith(safe_prefixes) and name not in SKIP_OPERATIONS
    )


def probe_operation(client, operation_name: str, params: dict) -> tuple[bool, str]:
    """Try to call an operation. Returns (success, message)."""
    try:
        method = getattr(client, _to_snake_case(operation_name))
        method(**params)
        return True, "OK"
    except client.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        msg = e.response["Error"]["Message"]
        # These error codes mean the operation IS implemented but we gave bad params
        implemented_errors = {
            "ResourceNotFoundException",
            "ValidationException",
            "NotFoundException",
            "NoSuchEntity",
            "InvalidParameterValue",
            "MissingParameter",
            "InvalidParameter",
            "MalformedPolicyDocument",
            "NoSuchBucket",
            "QueueDoesNotExist",
            "ResourceNotFoundFault",
            "HostedZoneNotFound",
            "InvalidInput",
            "FunctionNotFound",
            "RepositoryNotFoundException",
            "ResourceNotFoundException",
            "ParameterNotFound",
            "SecretNotFoundException",
        }
        if code in implemented_errors:
            return True, f"implemented ({code})"
        # 501 or "not implemented" message = not supported
        status_code = e.response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == 501:
            return False, f"not implemented (501: {code})"
        if "not implemented" in msg.lower() or "unknown" in msg.lower():
            return False, f"not implemented ({code}: {msg})"
        # Other errors likely mean it IS implemented
        return True, f"likely implemented ({code})"
    except botocore.exceptions.ParamValidationError:
        # Missing required params — can't probe without them, but the operation exists in botocore
        return True, "needs params (skipped)"
    except Exception as e:
        return False, f"error: {e}"


def _to_snake_case(name: str) -> str:
    """Convert PascalCase to snake_case."""
    import re

    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def main():
    parser = argparse.ArgumentParser(description="Probe AWS service operations")
    parser.add_argument("--service", required=True, help="AWS service name (e.g., sqs, s3)")
    parser.add_argument("--endpoint", default="http://localhost:4566", help="Endpoint URL")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument(
        "--all", action="store_true", help="Probe all operations, not just List/Describe/Get"
    )
    args = parser.parse_args()

    client = boto3.client(
        args.service,
        endpoint_url=args.endpoint,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )

    if args.all:
        loader = botocore.loaders.Loader()
        api = loader.load_service_model(args.service, "service-2")
        operations = sorted(
            name for name in api.get("operations", {}) if name not in SKIP_OPERATIONS
        )
    else:
        operations = get_list_operations(args.service)

    known = KNOWN_PARAMS.get(args.service, {})
    results = {"working": [], "broken": [], "service": args.service}

    for op in operations:
        params = known.get(op, {})
        ok, msg = probe_operation(client, op, params)
        if ok:
            results["working"].append({"operation": op, "message": msg})
        else:
            results["broken"].append({"operation": op, "message": msg})

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print(
            f"\n{args.service}: {len(results['working'])} working, {len(results['broken'])} broken"
        )
        print(f"\nWorking operations ({len(results['working'])}):")
        for r in results["working"]:
            print(f"  + {r['operation']}: {r['message']}")
        if results["broken"]:
            print(f"\nBroken operations ({len(results['broken'])}):")
            for r in results["broken"]:
                print(f"  - {r['operation']}: {r['message']}")
        print(f"\nSafe to write tests for: {', '.join(r['operation'] for r in results['working'])}")

    return 0 if not results["broken"] else 1


if __name__ == "__main__":
    sys.exit(main())
