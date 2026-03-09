#!/usr/bin/env python3
"""Probe a service to discover which operations actually work.

Usage:
    uv run python scripts/probe_service.py --service sqs
    uv run python scripts/probe_service.py --service ec2 --all
    uv run python scripts/probe_service.py --service s3 --json
    uv run python scripts/probe_service.py --service sns --all --json

Calls each operation with auto-filled parameters from botocore shapes.
Classifies operations as:
  - working: returned 200 or a "resource not found" error (proves implementation)
  - needs_params: couldn't auto-fill params, client-side validation blocked
  - not_implemented: server returned 501 or "not implemented"
  - 500_error: server crashed

Use this BEFORE writing compat tests. Only write tests for "working" ops.
"""

import argparse
import json
import re
import sys

import boto3
import botocore.exceptions
import botocore.loaders
import botocore.session

# Operations that are destructive or have side effects we don't want.
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
    "DeleteDBInstance",
    "DeleteDBCluster",
    "DeleteCacheCluster",
    "TerminateEnvironment",
    "DeleteVpc",
    "DeleteSubnet",
    "DeleteSecurityGroup",
}

# Error codes that prove the operation IS implemented (just bad params)
IMPLEMENTED_ERROR_CODES = {
    "AccessDeniedException",
    "CertificateNotFoundException",
    "ConflictException",
    "EntityAlreadyExistsException",
    "FunctionNotFound",
    "HostedZoneNotFound",
    "InvalidInput",
    "InvalidParameter",
    "InvalidParameterException",
    "InvalidParameterValue",
    "InvalidParameterValueException",
    "InvalidRequestException",
    "MalformedPolicyDocument",
    "MissingParameter",
    "NoSuchBucket",
    "NoSuchEntity",
    "NoSuchHostedZone",
    "NoSuchObjectLockConfiguration",
    "NotFoundException",
    "ParameterNotFound",
    "QueueDoesNotExist",
    "RepositoryNotFoundException",
    "ResourceNotFoundFault",
    "ResourceNotFoundException",
    "SecretNotFoundException",
    "ServiceNotFoundException",
    "StreamNotFoundException",
    "TableNotFoundException",
    "TopicNotFoundException",
    "ValidationError",
    "ValidationException",
}

# Hand-tuned params for operations where auto-fill isn't good enough.
# These override auto-filled params.
KNOWN_PARAMS = {
    "sqs": {
        "CreateQueue": {"QueueName": "probe-test-queue"},
        "GetQueueUrl": {"QueueName": "probe-test-queue"},
    },
    "s3": {
        "CreateBucket": {"Bucket": "probe-test-bucket"},
    },
    "sns": {
        "CreateTopic": {"Name": "probe-test-topic"},
    },
    "sts": {
        "GetCallerIdentity": {},
    },
}


def _to_snake_case(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _build_fake_arn(service: str, resource_type: str = "thing") -> str:
    """Build a syntactically valid fake ARN."""
    return f"arn:aws:{service}:us-east-1:123456789012:{resource_type}/probe-test"


def _auto_fill_params(service_name: str, operation_name: str) -> dict | None:
    """Auto-fill required params from botocore shapes.

    Returns a dict of params, or None if we can't fill them.
    """
    session = botocore.session.get_session()
    try:
        model = session.get_service_model(service_name)
        op = model.operation_model(operation_name)
    except Exception:
        return None

    if op.input_shape is None:
        return {}

    params = {}
    required = set(getattr(op.input_shape, "required_members", []))

    for name, shape in op.input_shape.members.items():
        # Skip optional params
        if name not in required:
            continue

        # Skip streaming body params
        if hasattr(shape, "serialization") and shape.serialization.get("streaming"):
            params[name] = b"probe-test-body"
            continue

        type_name = shape.type_name

        if type_name == "string":
            # Detect ARN-like params
            name_lower = name.lower()
            if "arn" in name_lower:
                params[name] = _build_fake_arn(service_name)
            elif name_lower in ("bucket", "bucketname"):
                params[name] = "probe-test-bucket"
            elif "name" in name_lower or "id" in name_lower:
                params[name] = "probe-test-value"
            elif "url" in name_lower:
                params[name] = "http://localhost:4566/probe"
            else:
                params[name] = "probe-test-value"
        elif type_name == "integer" or type_name == "long":
            params[name] = 1
        elif type_name == "boolean":
            params[name] = False
        elif type_name == "timestamp":
            params[name] = "2024-01-01T00:00:00Z"
        elif type_name == "blob":
            params[name] = b"probe-test"
        elif type_name == "list":
            params[name] = []
        elif type_name == "map":
            params[name] = {}
        elif type_name == "structure":
            # Try to fill nested required structure
            nested = _fill_structure(service_name, shape)
            if nested is not None:
                params[name] = nested
            else:
                return None  # Can't auto-fill this
        else:
            return None

    return params


def _fill_structure(service_name: str, shape) -> dict | None:
    """Recursively fill a structure shape with minimal valid values."""
    if not hasattr(shape, "members"):
        return {}

    result = {}
    required = set(getattr(shape, "required_members", []))

    for name, member in shape.members.items():
        if name not in required:
            continue

        type_name = member.type_name
        name_lower = name.lower()

        if type_name == "string":
            if "arn" in name_lower:
                result[name] = _build_fake_arn(service_name)
            else:
                result[name] = "probe-test-value"
        elif type_name in ("integer", "long"):
            result[name] = 1
        elif type_name == "boolean":
            result[name] = False
        elif type_name == "timestamp":
            result[name] = "2024-01-01T00:00:00Z"
        elif type_name == "blob":
            result[name] = b"test"
        elif type_name == "list":
            result[name] = []
        elif type_name == "map":
            result[name] = {}
        elif type_name == "structure":
            nested = _fill_structure(service_name, member)
            if nested is not None:
                result[name] = nested
            else:
                return None
        else:
            return None

    return result


def probe_operation(
    client, service_name: str, operation_name: str, params: dict
) -> tuple[str, str]:
    """Try to call an operation.

    Returns (status, message) where status is one of:
      working, needs_params, not_implemented, 500_error
    """
    try:
        method = getattr(client, _to_snake_case(operation_name))
        method(**params)
        return "working", "OK"
    except client.exceptions.ClientError as e:
        code = e.response["Error"]["Code"]
        msg = e.response["Error"]["Message"]
        status_code = e.response["ResponseMetadata"]["HTTPStatusCode"]

        if code in IMPLEMENTED_ERROR_CODES:
            return "working", f"implemented ({code})"

        # Check diagnostic header
        diag = (
            e.response.get("ResponseMetadata", {})
            .get("HTTPHeaders", {})
            .get("x-robotocore-diag", "")
        )

        if status_code == 500:
            if diag and "NotImplementedError" in diag:
                return "not_implemented", "not implemented (500+diag)"
            return "500_error", f"server crash ({code}: {msg[:60]})"

        if status_code == 501:
            return "not_implemented", "not implemented (501)"

        if msg and ("not implemented" in msg.lower() or "unknown" in msg.lower()):
            return "not_implemented", f"not implemented ({code})"

        # Other 4xx errors generally mean it IS implemented
        return "working", f"likely implemented ({code}: {(msg or '')[:60]})"

    except botocore.exceptions.ParamValidationError:
        return "needs_params", "client-side validation (never contacted server)"
    except Exception as e:
        return "500_error", f"exception: {str(e)[:60]}"


def get_operations(service_name: str, all_ops: bool) -> list[str]:
    """Get operation names to probe."""
    loader = botocore.loaders.Loader()
    try:
        api = loader.load_service_model(service_name, "service-2")
    except Exception:
        return []
    operations = api.get("operations", {})
    if all_ops:
        return sorted(n for n in operations if n not in SKIP_OPERATIONS)
    safe_prefixes = ("List", "Describe", "Get")
    return sorted(n for n in operations if n.startswith(safe_prefixes) and n not in SKIP_OPERATIONS)


def main():
    parser = argparse.ArgumentParser(description="Probe AWS service operations")
    parser.add_argument("--service", required=True, help="AWS service name")
    parser.add_argument(
        "--endpoint",
        default="http://localhost:4566",
        help="Endpoint URL",
    )
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Probe all ops, not just List/Describe/Get",
    )
    args = parser.parse_args()

    client = boto3.client(
        args.service,
        endpoint_url=args.endpoint,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )

    operations = get_operations(args.service, args.all)
    known = KNOWN_PARAMS.get(args.service, {})

    counts = {
        "working": 0,
        "needs_params": 0,
        "not_implemented": 0,
        "500_error": 0,
    }
    results = []

    for op in operations:
        # Priority: known params > auto-filled > empty
        if op in known:
            params = known[op]
        else:
            auto = _auto_fill_params(args.service, op)
            params = auto if auto is not None else {}

        status, msg = probe_operation(client, args.service, op, params)
        counts[status] += 1
        results.append({"operation": op, "status": status, "message": msg})

    if args.json:
        print(
            json.dumps(
                {
                    "service": args.service,
                    "counts": counts,
                    "operations": results,
                },
                indent=2,
            )
        )
    else:
        total = sum(counts.values())
        print(f"\n{args.service}: {total} operations probed")
        print(f"  working:         {counts['working']}")
        print(f"  needs_params:    {counts['needs_params']}")
        print(f"  not_implemented: {counts['not_implemented']}")
        print(f"  500_error:       {counts['500_error']}")

        if counts["working"]:
            print(f"\nWorking ({counts['working']}):")
            for r in results:
                if r["status"] == "working":
                    print(f"  + {r['operation']}: {r['message']}")

        if counts["needs_params"]:
            print(f"\nNeeds params ({counts['needs_params']}):")
            for r in results:
                if r["status"] == "needs_params":
                    print(f"  ? {r['operation']}: {r['message']}")

        if counts["not_implemented"]:
            print(f"\nNot implemented ({counts['not_implemented']}):")
            for r in results:
                if r["status"] == "not_implemented":
                    print(f"  - {r['operation']}: {r['message']}")

        if counts["500_error"]:
            print(f"\n500 errors ({counts['500_error']}):")
            for r in results:
                if r["status"] == "500_error":
                    print(f"  ! {r['operation']}: {r['message']}")

    return 0 if counts["500_error"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
