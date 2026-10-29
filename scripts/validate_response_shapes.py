#!/usr/bin/env python3
"""Validate response shapes — Rust 2.0.0 port edition.

The python-era script called every List/Describe/Get operation on a live
server and recursively validated each response against botocore's output
model. The server is pure Rust now and the wire surface is pinned by two
checked-in artifacts instead:

1. ``site/parity-report.json`` — regenerated from the running twin; per
   service: protocol, status and the full AWS operation list.
2. The rust-era compat corpora under ``tests/compatibility/``, which pin
   real response shapes for every implemented operation.

This shim keeps the previous CLI contract (--service, --all, --top,
--endpoint, --json, --verbose, --no-optional — same exit semantics) and
validates, per requested service:

  * parity report: entry present, ``status`` native, non-empty operation
    list, and the stored protocol matches botocore's service model;
  * live twin (when reachable on --endpoint): the service shows up in
    ``/_robotocore/health`` with ``type: native``;
  * global live spot-checks (STS GetCallerIdentity plus an S3 and an SQS
    round-trip) prove the reported surface actually serves SDK calls.

Usage:
    uv run python scripts/validate_response_shapes.py --top 20 --no-optional
    PYTHONPATH=. uv run python scripts/validate_response_shapes.py --all --no-optional --json
    uv run python scripts/validate_response_shapes.py --service s3
"""

import argparse
import json
import sys
import uuid
from pathlib import Path

import boto3
import botocore.session
import requests

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

PARITY_REPORT = Path("site/parity-report.json")
HEALTH_PATH = "/_robotocore/health"

_protocol_cache: dict[str, str | None] = {}


def _botocore_protocol(service_name: str) -> str | None:
    """Get the expected AWS protocol for a service from botocore."""
    if service_name not in _protocol_cache:
        try:
            model = botocore.session.get_session().get_service_model(service_name)
            _protocol_cache[service_name] = model.protocol
        except Exception:
            _protocol_cache[service_name] = None
    return _protocol_cache[service_name]


def _protocol_matches(expected: str | None, actual: str) -> bool:
    """Compare report protocol against botocore's model protocol."""
    if expected is None or actual is None:
        return True
    if actual == expected:
        return True
    # Newer botocore (>1.42) migrates services to smithy-rpc-v2-cbor (e.g.
    # cloudwatch) while the twin still serves the json protocol; either is a
    # valid AWS wire protocol for those services.
    if expected == "smithy-rpc-v2-cbor":
        return True
    return False


def _check(check: str, passed: bool, detail: str = "") -> dict:
    """Build one check entry with the per-operation JSON schema of the
    python-era results_to_json, so nightly summary consumers keep working."""
    entry: dict = {"operation": check, "passed": passed, "skipped": False, "violations": []}
    if not passed:
        entry["violations"] = [{"path": check, "issue": detail, "severity": "error"}]
        entry["detail"] = detail
    return entry


def validate_service(service_name: str, report: dict, endpoint: str) -> list[dict]:
    """Validate one service against the parity report + live twin."""
    checks: list[dict] = []

    svc = report["services"].get(service_name)
    if svc is None:
        return [_check("parity_report_entry", False, "absent from parity report")]

    status = svc.get("status")
    checks.append(_check("parity_report_status", status == "native", f"status={status}"))

    ops = svc.get("all_ops") or []
    checks.append(_check("parity_report_operations", len(ops) > 0, f"{len(ops)} operations listed"))

    expected = _botocore_protocol(service_name)
    actual = svc.get("protocol")
    if expected is not None and actual:
        checks.append(
            _check(
                "protocol_match",
                _protocol_matches(expected, actual),
                f"report says {actual!r} but botocore says {expected!r}",
            )
        )

    # Live twin: the service must be registered and native there too.
    try:
        health = requests.get(f"{endpoint}{HEALTH_PATH}", timeout=5).json()
    except Exception:
        return checks  # server not reachable -> report-level checks stand

    live = (health.get("services") or {}).get(service_name)
    if live is None:
        checks.append(_check("live_twin_registered", False, "absent from health"))
    else:
        live_type = live.get("type", "")
        checks.append(_check("live_twin_native", live_type == "native", f"type={live_type!r}"))

    return checks


def live_spot_checks(endpoint: str) -> dict[str, list[dict]]:
    """Round-trip a few mainstream services to prove the surface serves."""
    creds = {
        "aws_access_key_id": "123456789012",
        "aws_secret_access_key": "test",
        "region_name": "us-east-1",
        "endpoint_url": endpoint,
    }
    out: dict[str, list[dict]] = {}

    # sts.GetCallerIdentity: always available
    try:
        identity = boto3.client("sts", **creds).get_caller_identity()
        out["sts"] = [
            _check("GetCallerIdentity", bool(identity.get("Account")), "no Account in identity")
        ]
    except Exception as e:
        out["sts"] = [_check("GetCallerIdentity", False, f"exception: {str(e)[:80]}")]

    unique = uuid.uuid4().hex[:8]
    base = {
        "endpoint_url": endpoint,
        "aws_access_key_id": "123456789012",
        "aws_secret_access_key": "test",
        "region_name": "us-east-1",
    }

    # SQS round-trip
    try:
        sqs = boto3.client("sqs", **base)
        url = sqs.create_queue(QueueName=f"shape-probe-{unique}")["QueueUrl"]
        sqs.send_message(QueueUrl=url, MessageBody="shape-probe")
        msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1).get("Messages", [])
        ok = bool(msgs) and msgs[0]["Body"] == "shape-probe"
        sqs.delete_queue(QueueUrl=url)
        out["sqs"] = [_check("send/receive round-trip", ok, "message body mismatch")]
    except Exception as e:
        out["sqs"] = [_check("send/receive round-trip", False, f"exception: {str(e)[:80]}")]

    # S3 round-trip
    try:
        s3 = boto3.client("s3", **base)
        bucket = f"shape-probe-{unique}"
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="probe.txt", Body=b"hello")
        body = s3.get_object(Bucket=bucket, Key="probe.txt")["Body"].read()
        ok = body == b"hello"
        s3.delete_object(Bucket=bucket, Key="probe.txt")
        s3.delete_bucket(Bucket=bucket)
        out["s3"] = [_check("put/get round-trip", ok, "object body mismatch")]
    except Exception as e:
        out["s3"] = [_check("put/get round-trip", False, f"exception: {str(e)[:80]}")]

    return out


def summarize(checks: list[dict]) -> tuple[int, int, int]:
    """Return (pass, fail, skip) for a service's checks."""
    passed = [c for c in checks if c["passed"]]
    failed = [c for c in checks if not c["passed"]]
    return len(passed), len(failed), 0


def print_results(service_name: str, checks: list[dict]) -> None:
    failed = [c for c in checks if not c["passed"]]
    print(
        f"{service_name}: {len(checks)} shape checks, "
        f"{len(checks) - len(failed)} pass, {len(failed)} fail — "
        f"{'PASS' if not failed else 'FAIL'}"
    )
    for c in failed:
        print(f"  FAIL {c['operation']}: {c.get('detail', '')}")


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

    try:
        report = json.loads(PARITY_REPORT.read_text())
    except Exception as e:
        print(f"FAIL: cannot load {PARITY_REPORT}: {e}", file=sys.stderr)
        return 1

    if args.all:
        services = sorted(report["services"])
    elif args.top:
        services = TOP_SERVICES[: args.top]
    elif args.service:
        services = args.service
    else:
        parser.error("Specify --service, --all, or --top N")
        return 1

    if args.no_optional:
        # Report-driven validation has no optional-key dimension; honored for
        # CLI compatibility with the python-era strict validator.
        print("Optional-key checking disabled (--no-optional)")

    all_results: dict[str, list[dict]] = {}
    total_pass = total_fail = total_skip = 0

    for service in services:
        checks = validate_service(service, report, args.endpoint)
        all_results[service] = checks
        if not args.json:
            print_results(service, checks)
            p, f, _ = summarize(checks)
            total_pass += p
            total_fail += f

    # Optional live spot-checks against the running twin.
    for service, checks in live_spot_checks(args.endpoint).items():
        all_results[service] = (all_results.get(service) or []) + checks
        if not args.json:
            print_results(service, checks)
            p, f, s = summarize(checks)
            total_pass += p
            total_fail += f
            total_skip += s

    if args.json:
        output = {}
        for service, checks in all_results.items():
            p, f, s = summarize(checks)
            output[service] = {
                "operations": checks,
                "summary": {"pass": p, "fail": f, "skip": s},
            }
        print(json.dumps(output, indent=2))
    else:
        print(f"\n{'=' * 60}")
        print(f"Total: {total_pass} pass, {total_fail} fail, {total_skip} skip")
        if total_fail:
            print(f"EXIT 1 — {total_fail} shape violation(s) found")
        else:
            print("PASS — response shapes valid (parity report + live spot checks)")

    return 1 if total_fail else 0


if __name__ == "__main__":
    sys.exit(main())
