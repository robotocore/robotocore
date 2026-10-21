#!/usr/bin/env python3
"""Probe a service to discover which operations actually work.

Usage:
    uv run python -m scripts.probe_service --service sqs
    uv run python -m scripts.probe_service --service ec2 --all
    uv run python -m scripts.probe_service --service s3 --json
    uv run python -m scripts.probe_service --service sns --all --json
    uv run python -m scripts.probe_service --all-services

Probes the RUST twin of this repo (default endpoint http://localhost:4566).
Calls each operation with auto-filled parameters from botocore shapes.
Classifies operations as:
  - working: returned 200 or a "resource not found" error (proves implementation)
  - needs_params: couldn't auto-fill params, client-side validation blocked
  - not_implemented: server returned 501 or "not implemented"
  - 500_error: server crashed
  - timeout: the server never answered within the read timeout

Per AGENTS.md only HTTP 501 means "genuinely absent" — a ClientError with a
4xx code means the handler ran and the probe params were just wrong.

Use this BEFORE writing compat tests. Only write tests for "working" ops.
``--all-services`` walks /_robotocore/services on the live twin and writes
one probes/<service>.json artifact per service.
"""

import argparse
import json
import sys
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import boto3
import socket
import botocore.exceptions
import botocore.loaders
from botocore.config import Config

from scripts.lib.param_filler import (
    KNOWN_PARAMS,
    auto_fill_params,
    to_snake_case,
)

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


# Keep private aliases for backward compatibility with any callers
_to_snake_case = to_snake_case
_auto_fill_params = auto_fill_params


def probe_operation(
    client, service_name: str, operation_name: str, params: dict
) -> tuple[str, str]:
    """Try to call an operation.

    Returns (status, message) where status is one of:
      working, needs_params, not_implemented, 500_error, timeout
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

        # Only a literal "not implemented" (from a moto-modeled 501-style
        # message at a non-501 status) means absence. Modeled
        # ResourceNotFound/Unknown* faults say "unknown" and prove the
        # server answer came from a real handler — bucket them as working.
        if msg and "not implemented" in msg.lower():
            return "not_implemented", f"not implemented ({code})"

        # Other 4xx errors generally mean it IS implemented.
        return "working", f"likely implemented ({code}: {(msg or '')[:60]})"

    except botocore.exceptions.ParamValidationError:
        return "needs_params", "client-side validation (never contacted server)"
    except (botocore.exceptions.ReadTimeoutError,
            botocore.exceptions.ConnectReadTimeoutError,
            socket.timeout) as e:
        # A hanging/timing-out handler is a distinct artifact: the twin
        # never answered. Contract labels 500_error as "server crashed",
        # which a timeout is not — bucket it separately.
        return "timeout", f"read timeout ({type(e).__name__})"
    except botocore.exceptions.EndpointConnectionError as e:
        return "timeout", f"endpoint unreachable ({str(e)[:60]})"
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


def make_client(service_name: str, endpoint: str, read_timeout: float = 60.0):
    """Build a boto3 client pointed at the twin.

    total_max_attempts=1 keeps a genuine 500_error from being retried twice
    more by botocore before classification — one request, one verdict.
    read_timeout caps how long a hanging handler can block the probe run
    (a read timeout classifies as 500_error: the server never answered).
    """
    return boto3.client(
        service_name,
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=Config(
            retries={"total_max_attempts": 1},
            read_timeout=read_timeout,
            connect_timeout=min(read_timeout, 10.0),
        ),
    )


def _botocore_service_name(service_name: str) -> str:
    """Map the twin's registered name to botocore's model name when they differ.

    The twin registers compact names ("vpclattice", "rdsdata"); botocore
    hyphenates them ("vpc-lattice", "rds-data"). Falls back to the twin's name
    itself when no model matches.
    """
    loader = botocore.loaders.Loader()
    try:
        available = loader.list_available_services("service-2")
    except Exception:
        available = []
    compact = service_name.replace("-", "").replace("_", "").lower()
    for name in available:
        if name.replace("-", "").replace("_", "").lower() == compact:
            return name
    return service_name


def list_services(endpoint: str) -> list[dict]:
    """Read the live twin's service table. Returns [{name, protocol, ...}]."""
    with urllib.request.urlopen(f"{endpoint.rstrip('/')}/_robotocore/services", timeout=30) as r:
        return json.load(r)["services"]


def probe_service(
    service_name: str, all_ops: bool, endpoint: str, read_timeout: float = 60.0,
    budget: float = 0.0,
) -> dict:
    """Probe every operation of one service. Returns the artifact dict.

    budget: wall-clock seconds before the probe stops mid-service and records
    note "partial: time-boxed" (0 = no budget).
    """
    botocore_name = _botocore_service_name(service_name)
    client = make_client(botocore_name, endpoint, read_timeout)
    known = KNOWN_PARAMS.get(service_name, {})
    counts = {
        "working": 0,
        "needs_params": 0,
        "not_implemented": 0,
        "500_error": 0,
        "timeout": 0,
    }
    results = []
    deadline = time.monotonic() + budget if budget > 0 else None
    note = None

    for op in get_operations(botocore_name, all_ops):
        if deadline is not None and time.monotonic() > deadline:
            note = "partial: time-boxed"
            break
        # Priority: known params > auto-filled > empty
        if op in known:
            params = known[op]
        else:
            auto = _auto_fill_params(botocore_name, op)
            params = auto if auto is not None else {}

        status, msg = probe_operation(client, service_name, op, params)
        counts[status] += 1
        results.append({"operation": op, "status": status, "message": msg})

    artifact = {
        "service": service_name,
        "generated_at": datetime.now(UTC).isoformat(),
        "endpoint": endpoint,
        "counts": counts,
        "operations": results,
    }
    if note:
        artifact["note"] = note
    return artifact


def _run_all_services(args) -> int:
    """Probe every service the live twin registers and write probes/<svc>.json."""
    endpoints = list_services(args.endpoint)
    only = [s.strip() for s in args.only.split(",") if s.strip()]
    if only:
        wanted = set(only)
        endpoints = [e for e in endpoints if e["name"] in wanted]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    total_500 = 0
    written = 0
    print(f"Probing {len(endpoints)} services from {args.endpoint} …", flush=True)
    for i, entry in enumerate(endpoints, 1):
        svc = entry["name"]
        try:
            result = probe_service(
                svc, all_ops=True, endpoint=args.endpoint, read_timeout=args.timeout,
                budget=args.budget,
            )
        except Exception as exc:  # noqa: BLE001 — one bad service must not stop the fleet
            print(f"[{i}/{len(endpoints)}] {svc}: SKIPPED ({str(exc)[:80]})", flush=True)
            continue

        path = out_dir / f"{svc}.json"
        path.write_text(json.dumps(result, indent=2) + "\n")
        written += 1
        n_ops = sum(result["counts"].values())
        cases = ", ".join(f"{k}={v}" for k, v in result["counts"].items() if v)
        print(f"[{i}/{len(endpoints)}] {svc}: {n_ops} ops — {cases}", flush=True)
        total_500 += result["counts"]["500_error"] + result["counts"]["timeout"]

    print(
        f"\nWrote {written} probe files to {out_dir}; fleet-wide 500_errors+timeouts: {total_500}",
        flush=True,
    )
    return 0 if total_500 == 0 else 1


def main():
    parser = argparse.ArgumentParser(description="Probe AWS service operations")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--service", help="AWS service name")
    group.add_argument(
        "--all-services",
        action="store_true",
        help="Probe every service the live twin registers and write probes/*.json",
    )
    parser.add_argument(
        "--only",
        default="",
        help="Comma-separated service names to restrict --all-services to",
    )
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
    parser.add_argument(
        "--out-dir",
        default=str(Path(__file__).resolve().parent.parent / "probes"),
        help="Directory for --all-services artifacts (default: probes/)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Per-operation read timeout in seconds (default: 60)",
    )
    parser.add_argument(
        "--budget",
        type=float,
        default=0.0,
        help="Per-service wall-clock budget in seconds; on expiry the artifact "
        "is written partial with note 'partial: time-boxed' (default: none)",
    )
    args = parser.parse_args()

    if args.all_services:
        return _run_all_services(args)

    result = probe_service(args.service, all_ops=args.all, endpoint=args.endpoint,
                           read_timeout=args.timeout, budget=args.budget)
    counts = result["counts"]
    results = result["operations"]

    if args.json:
        print(json.dumps(result, indent=2))
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
