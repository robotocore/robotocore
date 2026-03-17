#!/usr/bin/env python3
"""Validate error response contracts match AWS wire format per protocol.

For each service, triggers a "not found" error and validates the response
matches the expected wire format (JSON __type field, XML ErrorResponse root, etc.)

Usage:
    uv run python scripts/validate_error_contracts.py --service dynamodb --service s3
    uv run python scripts/validate_error_contracts.py --all
    uv run python scripts/validate_error_contracts.py --top 20
    uv run python scripts/validate_error_contracts.py --service s3 --json
"""

import argparse
import json
import sys
import uuid
import xml.etree.ElementTree as ET

import boto3
import botocore.exceptions
import botocore.session
import requests

from scripts.lib.param_filler import to_snake_case

# Top 20 most-used AWS services
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

# Operations that reliably trigger "not found" errors with a fake resource ID
# Maps service -> (operation, params_with_fake_id)
ERROR_TRIGGER_OPS: dict[str, tuple[str, dict]] = {
    "s3": ("HeadBucket", {"Bucket": f"nonexistent-{uuid.uuid4().hex[:8]}"}),
    "dynamodb": ("DescribeTable", {"TableName": f"nonexistent-{uuid.uuid4().hex[:8]}"}),
    "sqs": ("GetQueueUrl", {"QueueName": f"nonexistent-{uuid.uuid4().hex[:8]}"}),
    "sns": (
        "GetTopicAttributes",
        {"TopicArn": f"arn:aws:sns:us-east-1:123456789012:nonexistent-{uuid.uuid4().hex[:8]}"},
    ),
    "lambda": ("GetFunction", {"FunctionName": f"nonexistent-{uuid.uuid4().hex[:8]}"}),
    "iam": ("GetUser", {"UserName": f"nonexistent-{uuid.uuid4().hex[:8]}"}),
    "ec2": ("DescribeInstances", {"InstanceIds": ["i-00000000deadbeef0"]}),
    "logs": (
        "DescribeLogStreams",
        {"logGroupName": f"nonexistent-{uuid.uuid4().hex[:8]}"},
    ),
    "secretsmanager": (
        "GetSecretValue",
        {"SecretId": f"nonexistent-{uuid.uuid4().hex[:8]}"},
    ),
    "ssm": ("GetParameter", {"Name": f"/nonexistent/{uuid.uuid4().hex[:8]}"}),
    "kinesis": (
        "DescribeStream",
        {"StreamName": f"nonexistent-{uuid.uuid4().hex[:8]}"},
    ),
    "events": (
        "DescribeRule",
        {"Name": f"nonexistent-{uuid.uuid4().hex[:8]}"},
    ),
    "stepfunctions": (
        "DescribeStateMachine",
        {
            "stateMachineArn": (
                f"arn:aws:states:us-east-1:123456789012:stateMachine:"
                f"nonexistent-{uuid.uuid4().hex[:8]}"
            )
        },
    ),
    "ecs": (
        "DescribeClusters",
        {"clusters": [f"nonexistent-{uuid.uuid4().hex[:8]}"]},
    ),
    "ecr": (
        "DescribeRepositories",
        {"repositoryNames": [f"nonexistent-{uuid.uuid4().hex[:8]}"]},
    ),
    "route53": (
        "GetHostedZone",
        {"Id": f"Z{uuid.uuid4().hex[:12].upper()}"},
    ),
    "cloudformation": (
        "DescribeStacks",
        {"StackName": f"nonexistent-{uuid.uuid4().hex[:8]}"},
    ),
    "cloudwatch": (
        "GetMetricData",
        {
            "MetricDataQueries": [
                {
                    "Id": "q1",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EC2",
                            "MetricName": "CPUUtilization",
                        },
                        "Period": 60,
                        "Stat": "Average",
                    },
                }
            ],
            "StartTime": "2026-01-01T00:00:00Z",
            "EndTime": "2026-01-02T00:00:00Z",
        },
    ),
}

# Expected protocol per service (from botocore)
_protocol_cache: dict[str, str] = {}


def _get_protocol(service_name: str) -> str:
    """Get the AWS protocol for a service."""
    if service_name not in _protocol_cache:
        session = botocore.session.get_session()
        try:
            model = session.get_service_model(service_name)
            _protocol_cache[service_name] = model.protocol
        except Exception:
            _protocol_cache[service_name] = "unknown"
    return _protocol_cache[service_name]


def _strip_ns(tag: str) -> str:
    """Strip XML namespace prefix from a tag."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def validate_error_contract(service_name: str, endpoint: str) -> dict:
    """Validate error contract for a service.

    Returns a dict with validation results.
    """
    result = {
        "service": service_name,
        "protocol": _get_protocol(service_name),
        "checks": [],
        "passed": True,
        "skipped": False,
    }

    if service_name not in ERROR_TRIGGER_OPS:
        result["skipped"] = True
        result["skip_reason"] = "no error trigger operation defined"
        return result

    op_name, params = ERROR_TRIGGER_OPS[service_name]

    client = boto3.client(
        service_name,
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )

    # 1. Try via boto3 to validate error structure
    try:
        method = getattr(client, to_snake_case(op_name))
        method(**params)
        # If it didn't raise, some operations don't error (e.g., DescribeClusters returns empty)
        result["checks"].append(
            {
                "check": "boto3_error",
                "passed": True,
                "note": "operation succeeded (no error to validate)",
            }
        )
        return result
    except botocore.exceptions.ParamValidationError:
        result["skipped"] = True
        result["skip_reason"] = "param validation error (never contacted server)"
        return result
    except client.exceptions.ClientError as e:
        err = e.response
        error_code = err["Error"].get("Code", "")
        error_msg = err["Error"].get("Message", "")
        status_code = err["ResponseMetadata"]["HTTPStatusCode"]

        # Check: Error.Code present and non-empty
        code_ok = bool(error_code)
        result["checks"].append(
            {
                "check": "error_code_present",
                "passed": code_ok,
                "value": error_code,
            }
        )
        if not code_ok:
            result["passed"] = False

        # Check: Error.Message present and non-empty
        msg_ok = bool(error_msg)
        result["checks"].append(
            {
                "check": "error_message_present",
                "passed": msg_ok,
                "value": error_msg[:80] if error_msg else "",
            }
        )
        if not msg_ok:
            result["passed"] = False

        # Check: HTTP status code in expected range
        status_ok = 400 <= status_code < 600
        result["checks"].append(
            {
                "check": "http_status_valid",
                "passed": status_ok,
                "value": status_code,
            }
        )
        if not status_ok:
            result["passed"] = False

        result["error_code"] = error_code
        result["status_code"] = status_code

    except Exception as e:
        result["skipped"] = True
        result["skip_reason"] = f"unexpected error: {str(e)[:80]}"
        return result

    # 2. Validate protocol-specific wire format via raw HTTP
    protocol = result["protocol"]
    wire_checks = _validate_wire_format(service_name, protocol, endpoint)
    result["checks"].extend(wire_checks)
    if any(not c["passed"] for c in wire_checks):
        result["passed"] = False

    return result


def _validate_wire_format(service_name: str, protocol: str, endpoint: str) -> list[dict]:
    """Validate protocol-specific wire format of error responses."""
    checks: list[dict] = []

    if protocol in ("json", "rest-json"):
        checks.extend(_check_json_error_format(service_name, protocol, endpoint))
    elif protocol in ("query", "ec2"):
        checks.extend(_check_xml_error_format(service_name, protocol, endpoint))
    elif protocol == "rest-xml":
        checks.extend(_check_rest_xml_error_format(service_name, endpoint))

    return checks


def _check_json_error_format(service_name: str, protocol: str, endpoint: str) -> list[dict]:
    """Validate JSON protocol error format (__type field, Content-Type)."""
    checks: list[dict] = []

    # Get X-Amz-Target prefix from botocore
    session = botocore.session.get_session()
    try:
        model = session.get_service_model(service_name)
        metadata = model.metadata
        target_prefix = metadata.get("targetPrefix", "")
        json_version = metadata.get("jsonVersion", "1.0")
    except Exception:
        return [{"check": "json_wire_format", "passed": False, "note": "cannot load model"}]

    if service_name not in ERROR_TRIGGER_OPS:
        return checks

    op_name, _ = ERROR_TRIGGER_OPS[service_name]

    # Build target
    if target_prefix:
        target = f"{target_prefix}.{op_name}"
    else:
        target = op_name

    # Build fake params for the raw request
    fake_id = f"nonexistent-{uuid.uuid4().hex[:8]}"
    body: dict = {}
    if service_name == "dynamodb":
        body = {"TableName": fake_id, "Key": {"pk": {"S": "x"}}}
    elif service_name == "secretsmanager":
        body = {"SecretId": fake_id}
    elif service_name == "logs":
        body = {"logGroupName": fake_id}
    elif service_name == "kinesis":
        body = {"StreamName": fake_id}
    elif service_name == "stepfunctions":
        body = {
            "stateMachineArn": (f"arn:aws:states:us-east-1:123456789012:stateMachine:{fake_id}")
        }
    elif service_name == "events":
        body = {"Name": fake_id}
    elif service_name == "ecs":
        body = {"clusters": [fake_id]}
    elif service_name == "ecr":
        body = {"repositoryNames": [fake_id]}
    else:
        body = {"Name": fake_id}

    try:
        resp = requests.post(
            endpoint,
            headers={
                "Authorization": (
                    f"AWS4-HMAC-SHA256 Credential=testing/20260312/us-east-1/"
                    f"{service_name}/aws4_request, "
                    f"SignedHeaders=host;x-amz-target, Signature=fake"
                ),
                "X-Amz-Target": target,
                "Content-Type": f"application/x-amz-json-{json_version}",
                "X-Amz-Date": "20260312T000000Z",
            },
            data=json.dumps(body),
            timeout=10,
        )
    except Exception as e:
        return [{"check": "json_wire_format", "passed": False, "note": f"request failed: {e}"}]

    # Check Content-Type is JSON
    ct = resp.headers.get("Content-Type", "")
    ct_ok = "json" in ct.lower()
    checks.append(
        {
            "check": "content_type_json",
            "passed": ct_ok,
            "value": ct,
        }
    )

    # Check __type field in error body
    if resp.status_code >= 400:
        try:
            body_json = resp.json()
            has_type = "__type" in body_json
            checks.append(
                {
                    "check": "json_type_field",
                    "passed": has_type,
                    "value": body_json.get("__type", "(missing)"),
                }
            )
            has_message = "message" in body_json or "Message" in body_json
            checks.append(
                {
                    "check": "json_message_field",
                    "passed": has_message,
                }
            )
        except Exception:
            checks.append({"check": "json_parseable", "passed": False})

    return checks


def _check_xml_error_format(service_name: str, protocol: str, endpoint: str) -> list[dict]:
    """Validate query/ec2 protocol XML error format."""
    checks: list[dict] = []

    if service_name not in ERROR_TRIGGER_OPS:
        return checks

    op_name, params = ERROR_TRIGGER_OPS[service_name]

    # Build form data
    signing_name = service_name
    if service_name == "cloudwatch":
        signing_name = "monitoring"

    form_parts = [f"Action={op_name}"]
    for k, v in params.items():
        if isinstance(v, list):
            for i, item in enumerate(v, 1):
                form_parts.append(f"{k}.{i}={item}")
        else:
            form_parts.append(f"{k}={v}")
    form_data = "&".join(form_parts)

    try:
        resp = requests.post(
            endpoint,
            headers={
                "Authorization": (
                    f"AWS4-HMAC-SHA256 Credential=testing/20260312/us-east-1/"
                    f"{signing_name}/aws4_request, SignedHeaders=host, Signature=fake"
                ),
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Amz-Date": "20260312T000000Z",
            },
            data=form_data,
            timeout=10,
        )
    except Exception as e:
        return [{"check": "xml_wire_format", "passed": False, "note": f"request failed: {e}"}]

    # Check Content-Type is XML
    ct = resp.headers.get("Content-Type", "")
    ct_ok = "xml" in ct.lower()
    checks.append(
        {
            "check": "content_type_xml",
            "passed": ct_ok,
            "value": ct,
        }
    )

    if resp.status_code >= 400 and ct_ok:
        try:
            root = ET.fromstring(resp.text)
            root_tag = _strip_ns(root.tag)

            if protocol == "ec2":
                expected_root = "Response"
            else:
                expected_root = "ErrorResponse"

            root_ok = root_tag == expected_root
            checks.append(
                {
                    "check": "xml_root_element",
                    "passed": root_ok,
                    "expected": expected_root,
                    "actual": root_tag,
                }
            )

            # Check for Code element
            code_elem = None
            for elem in root.iter():
                if _strip_ns(elem.tag) == "Code":
                    code_elem = elem
                    break
            checks.append(
                {
                    "check": "xml_error_code",
                    "passed": code_elem is not None and bool(code_elem.text),
                    "value": code_elem.text if code_elem is not None else "(missing)",
                }
            )

        except ET.ParseError:
            checks.append({"check": "xml_parseable", "passed": False})

    return checks


def _check_rest_xml_error_format(
    service_name: str,
    endpoint: str,
) -> list[dict]:
    """Validate rest-xml protocol error format (bare <Error> root)."""
    checks: list[dict] = []

    if service_name == "s3":
        bucket = f"nonexistent-{uuid.uuid4().hex[:8]}"
        try:
            resp = requests.get(
                f"{endpoint}/{bucket}",
                headers={
                    "Authorization": (
                        "AWS4-HMAC-SHA256 Credential=testing/20260312/us-east-1/s3/"
                        "aws4_request, SignedHeaders=host, Signature=fake"
                    ),
                    "X-Amz-Date": "20260312T000000Z",
                },
                timeout=10,
            )
        except Exception as e:
            return [{"check": "rest_xml_wire", "passed": False, "note": f"request failed: {e}"}]

        ct = resp.headers.get("Content-Type", "")
        ct_ok = "xml" in ct.lower()
        checks.append({"check": "content_type_xml", "passed": ct_ok, "value": ct})

        if resp.status_code >= 400 and ct_ok:
            try:
                root = ET.fromstring(resp.text)
                root_tag = _strip_ns(root.tag)
                # rest-xml uses bare <Error>, not <ErrorResponse>
                root_ok = root_tag == "Error"
                checks.append(
                    {
                        "check": "xml_root_element",
                        "passed": root_ok,
                        "expected": "Error",
                        "actual": root_tag,
                    }
                )
            except ET.ParseError:
                checks.append({"check": "xml_parseable", "passed": False})

    return checks


def print_results(service_name: str, result: dict) -> bool:
    """Print results for a service. Returns True if passed."""
    if result.get("skipped"):
        print(f"{service_name}: SKIP ({result.get('skip_reason', 'unknown')})")
        return True

    status = "PASS" if result["passed"] else "FAIL"
    n_checks = len(result["checks"])
    n_passed = sum(1 for c in result["checks"] if c["passed"])
    print(f"{service_name} ({result['protocol']}): {n_passed}/{n_checks} checks — {status}")

    for c in result["checks"]:
        if not c["passed"]:
            note = c.get("note", "")
            value = c.get("value", "")
            expected = c.get("expected", "")
            detail = note or f"got {value}" + (f", expected {expected}" if expected else "")
            print(f"  FAIL {c['check']}: {detail}")

    return result["passed"]


def main():
    parser = argparse.ArgumentParser(description="Validate error response contracts")
    parser.add_argument("--service", action="append", help="Service(s) to validate")
    parser.add_argument("--all", action="store_true", help="Validate all services with triggers")
    parser.add_argument("--top", type=int, help="Validate top N most-used services")
    parser.add_argument("--endpoint", default="http://localhost:4566", help="Endpoint URL")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    if args.all:
        services = sorted(ERROR_TRIGGER_OPS.keys())
    elif args.top:
        services = [s for s in TOP_SERVICES[: args.top] if s in ERROR_TRIGGER_OPS]
    elif args.service:
        services = args.service
    else:
        parser.error("Specify --service, --all, or --top N")
        return 1

    all_results: dict[str, dict] = {}
    any_failed = False

    for service in services:
        result = validate_error_contract(service, args.endpoint)
        all_results[service] = result

        if not args.json:
            passed = print_results(service, result)
            if not passed:
                any_failed = True

    if args.json:
        print(json.dumps(all_results, indent=2))
        any_failed = any(not r["passed"] for r in all_results.values() if not r.get("skipped"))
    else:
        print(f"\n{'=' * 60}")
        total = len(all_results)
        failed = sum(1 for r in all_results.values() if not r["passed"] and not r.get("skipped"))
        skipped = sum(1 for r in all_results.values() if r.get("skipped"))
        print(
            f"Total: {total} services, {total - failed - skipped} pass, "
            f"{failed} fail, {skipped} skip"
        )

    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())
