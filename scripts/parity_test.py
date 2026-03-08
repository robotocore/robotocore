#!/usr/bin/env python3
"""Automated parity test harness — compare robotocore vs LocalStack responses.

Runs the same boto3 operations against both robotocore (port 4566) and
LocalStack (port 4567), compares response structure (keys, types, status codes),
and reports differences.

Usage:
    # Both servers must be running
    uv run python scripts/parity_test.py
    uv run python scripts/parity_test.py --service s3
    uv run python scripts/parity_test.py --json
    uv run python scripts/parity_test.py --robotocore-port 4566 --localstack-port 4567
"""

import argparse
import json
import sys
import time

import boto3
from botocore.config import Config


def _client(service: str, port: int):
    """Create a boto3 client pointing at a local endpoint."""
    return boto3.client(
        service,
        endpoint_url=f"http://localhost:{port}",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=Config(
            retries={"max_attempts": 0},
            connect_timeout=5,
            read_timeout=10,
        ),
    )


def _compare_structure(a, b, path="") -> list[str]:
    """Compare response structures recursively. Returns list of differences."""
    diffs = []
    if type(a) is not type(b):
        diffs.append(f"{path}: type mismatch: {type(a).__name__} vs {type(b).__name__}")
        return diffs

    if isinstance(a, dict):
        a_keys = set(a.keys()) - {"ResponseMetadata"}
        b_keys = set(b.keys()) - {"ResponseMetadata"}
        for key in a_keys - b_keys:
            diffs.append(f"{path}.{key}: missing in LocalStack response")
        for key in b_keys - a_keys:
            diffs.append(f"{path}.{key}: missing in Robotocore response")
        for key in a_keys & b_keys:
            diffs.extend(_compare_structure(a[key], b[key], f"{path}.{key}"))
    elif isinstance(a, list):
        if len(a) != len(b):
            diffs.append(f"{path}: list length {len(a)} vs {len(b)}")
        for i, (ai, bi) in enumerate(zip(a, b)):
            diffs.extend(_compare_structure(ai, bi, f"{path}[{i}]"))

    return diffs


def _run_test(name: str, fn_roboto, fn_local) -> dict:
    """Run a single parity test. Returns result dict."""
    result = {"name": name, "status": "unknown", "diffs": [], "error": None}

    try:
        r_result = fn_roboto()
        r_status = r_result.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
    except Exception as e:
        result["status"] = "robotocore_error"
        result["error"] = f"Robotocore: {type(e).__name__}: {e}"
        return result

    try:
        l_result = fn_local()
        l_status = l_result.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
    except Exception as e:
        result["status"] = "localstack_error"
        result["error"] = f"LocalStack: {type(e).__name__}: {e}"
        return result

    if r_status != l_status:
        result["diffs"].append(f"HTTP status: {r_status} vs {l_status}")

    diffs = _compare_structure(r_result, l_result)
    result["diffs"].extend(diffs)
    result["status"] = "match" if not result["diffs"] else "diverged"
    return result


# ---------------------------------------------------------------------------
# Test definitions per service
# ---------------------------------------------------------------------------


def _sts_tests(rp: int, lp: int) -> list[dict]:
    rc = _client("sts", rp)
    lc = _client("sts", lp)
    return [
        _run_test(
            "sts.GetCallerIdentity",
            rc.get_caller_identity,
            lc.get_caller_identity,
        ),
    ]


def _s3_tests(rp: int, lp: int) -> list[dict]:
    rc = _client("s3", rp)
    lc = _client("s3", lp)
    bucket = f"parity-test-{int(time.time())}"

    results = []

    # CreateBucket
    results.append(
        _run_test(
            "s3.CreateBucket",
            lambda: rc.create_bucket(Bucket=bucket),
            lambda: lc.create_bucket(Bucket=bucket),
        )
    )

    # ListBuckets
    results.append(_run_test("s3.ListBuckets", rc.list_buckets, lc.list_buckets))

    # PutObject
    results.append(
        _run_test(
            "s3.PutObject",
            lambda: rc.put_object(Bucket=bucket, Key="test.txt", Body=b"hello"),
            lambda: lc.put_object(Bucket=bucket, Key="test.txt", Body=b"hello"),
        )
    )

    # GetObject
    def _get_r():
        resp = rc.get_object(Bucket=bucket, Key="test.txt")
        resp["Body"] = resp["Body"].read()
        return resp

    def _get_l():
        resp = lc.get_object(Bucket=bucket, Key="test.txt")
        resp["Body"] = resp["Body"].read()
        return resp

    results.append(_run_test("s3.GetObject", _get_r, _get_l))

    # Cleanup
    try:
        rc.delete_object(Bucket=bucket, Key="test.txt")
        rc.delete_bucket(Bucket=bucket)
    except Exception:
        pass
    try:
        lc.delete_object(Bucket=bucket, Key="test.txt")
        lc.delete_bucket(Bucket=bucket)
    except Exception:
        pass

    return results


def _sqs_tests(rp: int, lp: int) -> list[dict]:
    rc = _client("sqs", rp)
    lc = _client("sqs", lp)
    queue_name = f"parity-test-{int(time.time())}"

    results = []

    # CreateQueue
    r_url = l_url = None

    def _create_r():
        nonlocal r_url
        resp = rc.create_queue(QueueName=queue_name)
        r_url = resp["QueueUrl"]
        return resp

    def _create_l():
        nonlocal l_url
        resp = lc.create_queue(QueueName=queue_name)
        l_url = resp["QueueUrl"]
        return resp

    results.append(_run_test("sqs.CreateQueue", _create_r, _create_l))

    # ListQueues
    results.append(_run_test("sqs.ListQueues", rc.list_queues, lc.list_queues))

    # SendMessage + ReceiveMessage
    if r_url and l_url:
        results.append(
            _run_test(
                "sqs.SendMessage",
                lambda: rc.send_message(QueueUrl=r_url, MessageBody="parity test"),
                lambda: lc.send_message(QueueUrl=l_url, MessageBody="parity test"),
            )
        )

    # Cleanup
    try:
        rc.delete_queue(QueueUrl=r_url)
    except Exception:
        pass
    try:
        lc.delete_queue(QueueUrl=l_url)
    except Exception:
        pass

    return results


def _sns_tests(rp: int, lp: int) -> list[dict]:
    rc = _client("sns", rp)
    lc = _client("sns", lp)

    results = []
    topic_name = f"parity-test-{int(time.time())}"

    r_arn = l_arn = None

    def _create_r():
        nonlocal r_arn
        resp = rc.create_topic(Name=topic_name)
        r_arn = resp["TopicArn"]
        return resp

    def _create_l():
        nonlocal l_arn
        resp = lc.create_topic(Name=topic_name)
        l_arn = resp["TopicArn"]
        return resp

    results.append(_run_test("sns.CreateTopic", _create_r, _create_l))
    results.append(_run_test("sns.ListTopics", rc.list_topics, lc.list_topics))

    # Cleanup
    try:
        rc.delete_topic(TopicArn=r_arn)
    except Exception:
        pass
    try:
        lc.delete_topic(TopicArn=l_arn)
    except Exception:
        pass

    return results


def _dynamodb_tests(rp: int, lp: int) -> list[dict]:
    rc = _client("dynamodb", rp)
    lc = _client("dynamodb", lp)
    table = f"parity-test-{int(time.time())}"

    results = []

    results.append(
        _run_test(
            "dynamodb.CreateTable",
            lambda: rc.create_table(
                TableName=table,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            ),
            lambda: lc.create_table(
                TableName=table,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            ),
        )
    )

    results.append(_run_test("dynamodb.ListTables", rc.list_tables, lc.list_tables))

    results.append(
        _run_test(
            "dynamodb.PutItem",
            lambda: rc.put_item(TableName=table, Item={"pk": {"S": "test"}}),
            lambda: lc.put_item(TableName=table, Item={"pk": {"S": "test"}}),
        )
    )

    results.append(
        _run_test(
            "dynamodb.GetItem",
            lambda: rc.get_item(TableName=table, Key={"pk": {"S": "test"}}),
            lambda: lc.get_item(TableName=table, Key={"pk": {"S": "test"}}),
        )
    )

    # Cleanup
    try:
        rc.delete_table(TableName=table)
    except Exception:
        pass
    try:
        lc.delete_table(TableName=table)
    except Exception:
        pass

    return results


def _iam_tests(rp: int, lp: int) -> list[dict]:
    rc = _client("iam", rp)
    lc = _client("iam", lp)

    results = []
    results.append(_run_test("iam.ListUsers", rc.list_users, lc.list_users))
    results.append(_run_test("iam.ListRoles", rc.list_roles, lc.list_roles))
    return results


def _lambda_tests(rp: int, lp: int) -> list[dict]:
    rc = _client("lambda", rp)
    lc = _client("lambda", lp)

    results = []
    results.append(_run_test("lambda.ListFunctions", rc.list_functions, lc.list_functions))
    return results


def _events_tests(rp: int, lp: int) -> list[dict]:
    rc = _client("events", rp)
    lc = _client("events", lp)

    results = []
    results.append(_run_test("events.ListRules", rc.list_rules, lc.list_rules))
    results.append(_run_test("events.ListEventBuses", rc.list_event_buses, lc.list_event_buses))
    return results


SERVICE_TESTS = {
    "sts": _sts_tests,
    "s3": _s3_tests,
    "sqs": _sqs_tests,
    "sns": _sns_tests,
    "dynamodb": _dynamodb_tests,
    "iam": _iam_tests,
    "lambda": _lambda_tests,
    "events": _events_tests,
}


def main():
    parser = argparse.ArgumentParser(description="Parity test: robotocore vs LocalStack")
    parser.add_argument("--service", help="Test a single service")
    parser.add_argument("--robotocore-port", type=int, default=4566)
    parser.add_argument("--localstack-port", type=int, default=4567)
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    rp = args.robotocore_port
    lp = args.localstack_port

    services = [args.service] if args.service else list(SERVICE_TESTS.keys())
    all_results = {}
    total_match = total_diverged = total_error = 0

    for svc in services:
        test_fn = SERVICE_TESTS.get(svc)
        if not test_fn:
            print(f"No parity tests for service: {svc}", file=sys.stderr)
            continue

        try:
            results = test_fn(rp, lp)
        except Exception as e:
            results = [{"name": f"{svc}.*", "status": "error", "error": str(e), "diffs": []}]

        all_results[svc] = results
        for r in results:
            if r["status"] == "match":
                total_match += 1
            elif r["status"] == "diverged":
                total_diverged += 1
            else:
                total_error += 1

    if args.json:
        print(json.dumps(all_results, indent=2))
        return

    # Text output
    print("=== Parity Test Results ===\n")

    for svc, results in all_results.items():
        print(f"  {svc}:")
        for r in results:
            icon = "✓" if r["status"] == "match" else "✗" if r["status"] == "diverged" else "!"
            print(f"    {icon} {r['name']}: {r['status']}")
            if r["diffs"]:
                for d in r["diffs"][:5]:
                    print(f"      - {d}")
            if r["error"]:
                print(f"      Error: {r['error']}")
        print()

    total = total_match + total_diverged + total_error
    print(
        f"Summary: {total_match}/{total} matched, {total_diverged} diverged, {total_error} errors"
    )  # noqa: E501

    if total_diverged > 0 or total_error > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
