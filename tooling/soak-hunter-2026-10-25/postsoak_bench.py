#!/usr/bin/env python3
"""Post-soak serial latency mini-bench — same op family as the
perf-ab-2026-10-24 baseline (client-side timing, single thread, rust-side
serial p50s in that lane: sts 0.469ms, s3_put 0.676ms, s3_get 0.598ms,
s3_list/head 0.627ms, sqs_send 0.44ms, sqs_receive 0.456ms, sqs_delete
0.439ms, ddb_put 0.449ms, ddb_get 0.44ms, ddb_scan-as-range 1.084ms,
ec2_describe_vpcs 0.487ms, ec2_describe_instances 0.441ms,
iam_list_users 0.461ms) — measured post-soak in a dedicated account."""
from __future__ import annotations

import json
import os
import statistics
import time
import uuid

import boto3
import botocore

ENDPOINT = os.environ.get("SOAK_ENDPOINT") or "http://localhost:4802"
REG = "us-east-1"
ACCT = "888200000001"

N_SMALL = 300
N_MED = 150


def client(svc):
    cfg = botocore.config.Config(
        retries={"max_attempts": 1},
        s3={"addressing_style": "path"} if svc == "s3" else {})
    return boto3.client(svc, endpoint_url=ENDPOINT, region_name=REG,
                        aws_access_key_id=ACCT, aws_secret_access_key="test",
                        config=cfg)


def stats(xs):
    xs.sort()
    return {"n": len(xs), "p50": round(xs[len(xs) // 2], 3),
            "p95": round(xs[min(int(len(xs) * .95), len(xs) - 1)], 3)}


out = {}
c_sts = client("sts"); c_s3 = client("s3"); c_sq = client("sqs")
c_dd = client("dynamodb"); c_ec2 = client("ec2"); c_iam = client("iam")


def timed(fn, n, us=None):
    res = {"n": 0}
    xs = []
    for _ in range(n):
        t = time.perf_counter_ns()
        fn()
        if us:
            us()
        xs.append((time.perf_counter_ns() - t) / 1e6)
    return stats(xs)


res = {}
res["sts_get_caller_identity"] = timed(lambda: c_sts.get_caller_identity(), N_SMALL)

rng = uuid.uuid4().hex[:8]
bucket = f"post-soak-{rng}"
c_s3.create_bucket(Bucket=bucket)
body = os.urandom(16384)
res["s3_put_16k"] = timed(lambda: c_s3.put_object(Bucket=bucket, Key="o", Body=body), N_MED)
res["s3_get_16k"] = timed(lambda: c_s3.get_object(Bucket=bucket, Key="o")["Body"].read(), N_MED)
res["s3_list_head"] = timed(lambda: c_s3.list_objects_v2(Bucket=bucket, MaxKeys=5), N_SMALL)
for i in range(N_MED):  # fill for list realism
    c_s3.put_object(Bucket=bucket, Key=f"p/{i}", Body=b"y" * 1024)
res["s3_list_filled"] = timed(lambda: c_s3.list_objects_v2(Bucket=bucket, Prefix="p/", MaxKeys=5), N_SMALL)

q = c_sq.create_queue(QueueName=f"post-soak-{rng}")["QueueUrl"]
res["sqs_send"] = timed(lambda: c_sq.send_message(QueueUrl=q, MessageBody="ping"), N_SMALL)
rec, dele = [], []
for _ in range(N_SMALL):
    t = time.perf_counter_ns()
    got = c_sq.receive_message(QueueUrl=q, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    rec.append((time.perf_counter_ns() - t) / 1e6)
    if got.get("Messages"):
        t = time.perf_counter_ns()
        c_sq.delete_message(QueueUrl=q, ReceiptHandle=got["Messages"][0]["ReceiptHandle"])
        dele.append((time.perf_counter_ns() - t) / 1e6)
res["sqs_receive"] = stats(rec)
res["sqs_delete"] = stats(dele)
c_sq.delete_queue(QueueUrl=q)

table = f"post-soak-{rng}"
c_dd.create_table(TableName=table,
                  KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                  AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                  BillingMode="PAY_PER_REQUEST")
for i in range(N_MED):
    c_dd.put_item(TableName=table, Item={"pk": {"S": f"k{i}"}, "v": {"S": "x"}})
res["ddb_put"] = timed(lambda: c_dd.put_item(
    TableName=table, Item={"pk": {"S": f"t{time.perf_counter_ns()}"}, "v": {"S": "y"}}), N_MED)
res["ddb_get"] = timed(lambda: c_dd.get_item(TableName=table, Key={"pk": {"S": "k7"}}), N_SMALL)
res["ddb_query_range"] = timed(lambda: c_dd.scan(TableName=table, Limit=10), N_SMALL)
c_dd.delete_table(TableName=table)

c_s3.delete_objects(Bucket=bucket, Delete={"Objects": [
    {"Key": f"p/{i}"} for i in range(N_MED)]})
try:
    c_s3.delete_bucket(Bucket=bucket)
except Exception:
    pass

res["ec2_describe_vpcs"] = timed(lambda: c_ec2.describe_vpcs(), N_SMALL)
res["ec2_describe_instances"] = timed(lambda: c_ec2.describe_instances(), N_SMALL)
res["iam_list_users"] = timed(lambda: c_iam.list_users(MaxItems=5), N_SMALL)

open("/tmp/hunter-soak/logs/postsoak_bench.json", "w").write(json.dumps(res, indent=2))
print(json.dumps(res, indent=2))
