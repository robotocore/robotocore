#!/usr/bin/env python3
"""Exercise AWS APIs against a running robotocore instance.

This script runs through basic CRUD operations for each supported service
to verify the gateway and moto bridge are working correctly.

Usage:
    # Start robotocore first, then:
    uv run python scripts/run_aws_tests.py
    uv run python scripts/run_aws_tests.py --service s3
    uv run python scripts/run_aws_tests.py --endpoint http://localhost:4566
"""

import argparse
import sys
import traceback

import boto3


def make_client(service: str, endpoint: str, region: str = "us-east-1"):
    return boto3.client(
        service,
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def test_s3(endpoint: str) -> bool:
    s3 = make_client("s3", endpoint)
    bucket = "robotocore-smoke-test"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="test.txt", Body=b"hello")
    obj = s3.get_object(Bucket=bucket, Key="test.txt")
    assert obj["Body"].read() == b"hello"
    s3.delete_object(Bucket=bucket, Key="test.txt")
    s3.delete_bucket(Bucket=bucket)
    return True


def test_sqs(endpoint: str) -> bool:
    sqs = make_client("sqs", endpoint)
    q = sqs.create_queue(QueueName="robotocore-smoke-test")
    url = q["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="ping")
    msgs = sqs.receive_message(QueueUrl=url)
    assert msgs["Messages"][0]["Body"] == "ping"
    sqs.delete_queue(QueueUrl=url)
    return True


def test_dynamodb(endpoint: str) -> bool:
    ddb = make_client("dynamodb", endpoint)
    table = "robotocore-smoke-test"
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName=table, Item={"id": {"S": "1"}, "data": {"S": "hello"}})
    item = ddb.get_item(TableName=table, Key={"id": {"S": "1"}})
    assert item["Item"]["data"]["S"] == "hello"
    ddb.delete_table(TableName=table)
    return True


def test_sns(endpoint: str) -> bool:
    sns = make_client("sns", endpoint)
    topic = sns.create_topic(Name="robotocore-smoke-test")
    arn = topic["TopicArn"]
    sns.list_topics()
    sns.delete_topic(TopicArn=arn)
    return True


def test_iam(endpoint: str) -> bool:
    iam = make_client("iam", endpoint)
    iam.list_users()
    return True


def test_sts(endpoint: str) -> bool:
    sts = make_client("sts", endpoint)
    identity = sts.get_caller_identity()
    assert "Account" in identity
    return True


TESTS = {
    "s3": test_s3,
    "sqs": test_sqs,
    "dynamodb": test_dynamodb,
    "sns": test_sns,
    "iam": test_iam,
    "sts": test_sts,
}


def main():
    parser = argparse.ArgumentParser(description="Smoke test AWS APIs")
    parser.add_argument("--endpoint", default="http://localhost:4566")
    parser.add_argument("--service", help="Test a specific service")
    args = parser.parse_args()

    tests = {args.service: TESTS[args.service]} if args.service else TESTS
    passed = 0
    failed = 0

    for name, test_fn in tests.items():
        try:
            test_fn(args.endpoint)
            print(f"  PASS  {name}")
            passed += 1
        except Exception:
            print(f"  FAIL  {name}")
            traceback.print_exc()
            failed += 1

    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
