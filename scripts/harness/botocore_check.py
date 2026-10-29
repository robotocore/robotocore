#!/usr/bin/env python3
"""End-to-end smoke test for a robotocore endpoint using boto3.

Exercises the full SQS lifecycle plus STS and S3 against the endpoint and
prints PASS/FAIL per step.  Exit 0 iff every step passes.

Usage:
    botocore_check.py [--endpoint URL]

Point it at the Rust server once it serves SQS/S3/STS; it is the
acceptance gate for those services.
"""

from __future__ import annotations

import argparse
import sys
import uuid

import boto3
from botocore.exceptions import ClientError, BotoCoreError

ACCOUNT = "123456789012"
REGION = "us-east-1"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--endpoint", default="http://localhost:4566")
    args = ap.parse_args()

    session = dict(endpoint_url=args.endpoint,
                   aws_access_key_id=ACCOUNT,
                   aws_secret_access_key="test",
                   region_name=REGION)
    sqs = boto3.client("sqs", **session)
    s3 = boto3.client("s3", **session)
    sts = boto3.client("sts", **session)

    queue_name = f"smoke-q-{uuid.uuid4().hex[:4]}"
    bucket = f"smoke-b-{uuid.uuid4().hex[:4]}"
    results = []

    def step(name: str, fn):
        try:
            fn()
            results.append((name, True, ""))
            print(f"PASS  {name}")
        except (ClientError, BotoCoreError) as e:
            detail = str(e).replace("\n", " ")[:200]
            results.append((name, False, detail))
            print(f"FAIL  {name}: {detail}")
        except Exception as e:
            results.append((name, False, f"{type(e).__name__}: {e}"[:200]))
            print(f"FAIL  {name}: {type(e).__name__}: {e}")

    # --- SQS lifecycle ------------------------------------------------------
    def create_queue():
        sqs.create_queue(QueueName=queue_name)

    def queue_url():
        return sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]

    url_holder = {}

    def get_url():
        if "url" not in url_holder:
            url_holder["url"] = queue_url()
        return url_holder["url"]

    def send_messages():
        for i in range(3):
            sqs.send_message(QueueUrl=get_url(), MessageBody=f"msg-{i}")

    received = {}

    def receive():
        msgs = sqs.receive_message(QueueUrl=get_url(),
                                   MaxNumberOfMessages=10,
                                   WaitTimeSeconds=1).get("Messages", [])
        assert len(msgs) == 3, f"expected 3 messages, got {len(msgs)}"
        received["first"] = msgs[0]

    def change_visibility():
        sqs.change_message_visibility(QueueUrl=get_url(),
                                      ReceiptHandle=received["first"]["ReceiptHandle"],
                                      VisibilityTimeout=30)

    def delete_message():
        sqs.delete_message(QueueUrl=get_url(),
                           ReceiptHandle=received["first"]["ReceiptHandle"])

    def get_attributes():
        attrs = sqs.get_queue_attributes(QueueUrl=get_url(),
                                         AttributeNames=["QueueArn",
                                                         "ApproximateNumberOfMessages"])
        assert "QueueArn" in attrs["Attributes"]

    def purge():
        # purge only succeeds after all messages are deleted; delete the
        # remaining two first.
        msgs = sqs.receive_message(QueueUrl=get_url(),
                                   MaxNumberOfMessages=10).get("Messages", [])
        for m in msgs:
            sqs.delete_message(QueueUrl=get_url(),
                               ReceiptHandle=m["ReceiptHandle"])
        sqs.purge_queue(QueueUrl=get_url())

    def list_queues():
        queues = sqs.list_queues().get("QueueUrls", [])
        assert any(queue_name in q for q in queues), f"{queue_name} not in {queues}"

    def delete_queue():
        sqs.delete_queue(QueueUrl=get_url())

    step("sqs.create_queue", create_queue)
    step("sqs.get_queue_url", get_url)
    step("sqs.send_message x3", send_messages)
    step("sqs.receive_message", receive)
    step("sqs.change_message_visibility", change_visibility)
    step("sqs.delete_message", delete_message)
    step("sqs.get_queue_attributes", get_attributes)
    step("sqs.purge_queue", purge)
    step("sqs.list_queues", list_queues)
    step("sqs.delete_queue", delete_queue)

    # --- STS ----------------------------------------------------------------
    def sts_identity():
        r = sts.get_caller_identity()
        assert r["Account"] == ACCOUNT, f"account {r['Account']} != {ACCOUNT}"

    step("sts.get_caller_identity", sts_identity)

    # --- S3 -----------------------------------------------------------------
    def create_bucket():
        s3.create_bucket(Bucket=bucket)

    def put_object():
        s3.put_object(Bucket=bucket, Key="a/b.txt", Body=b"hello golden")

    def get_object():
        body = s3.get_object(Bucket=bucket, Key="a/b.txt")["Body"].read()
        assert body == b"hello golden", f"body mismatch: {body!r}"

    def list_objects():
        r = s3.list_objects_v2(Bucket=bucket)
        assert r.get("KeyCount") == 1, f"KeyCount={r.get('KeyCount')}"

    def delete_object():
        s3.delete_object(Bucket=bucket, Key="a/b.txt")

    def delete_bucket():
        s3.delete_bucket(Bucket=bucket)

    step("s3.create_bucket", create_bucket)
    step("s3.put_object", put_object)
    step("s3.get_object", get_object)
    step("s3.list_objects_v2", list_objects)
    step("s3.delete_object", delete_object)
    step("s3.delete_bucket", delete_bucket)

    passed = sum(1 for _, ok, _ in results if ok)
    total = len(results)
    print(f"\n{'PASS' if passed == total else 'FAIL'}: {passed}/{total} steps")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
