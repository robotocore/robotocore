#!/usr/bin/env python3
"""HUNTER-SOAK teardown — delete everything the soak created in its accounts
(SQS queues, S3 buckets/objects, DDB tables, Lambda functions, EventBridge
rules/buses, SNS topics/subscriptions). Best-effort per resource; ends by
printing a per-account summary of leftovers."""
from __future__ import annotations

import json
import os

import boto3
import botocore

ENDPOINT = os.environ.get("SOAK_ENDPOINT") or "http://localhost:4802"
REG = "us-east-1"
ACCOUNTS = ["773100000001", "773100000002", "773100000003",
            "663200000001", "663300000001", "663400000001"]
MARKERS = ["hunter-soak", "soak-sched", "soak-bus-", "chaos-", "stray-",
           "chain-", "sched-sink-", "chain-hop", "chain-sink"]


def client(svc, acct):
    cfg = botocore.config.Config(
        retries={"max_attempts": 1},
        s3={"addressing_style": "path"} if svc == "s3" else {})
    return boto3.client(svc, endpoint_url=ENDPOINT, region_name=REG,
                        aws_access_key_id=acct, aws_secret_access_key="test",
                        config=cfg)


def ours(name):
    return any(name.startswith(m) or m in name for m in MARKERS)


for acct in ACCOUNTS:
    got, left = 0, []
    try:
        sq = client("sqs", acct)
        for q in sq.list_queues().get("QueueUrls", []):
            name = q.rsplit("/", 1)[-1]
            if ours(name):
                sq.delete_queue(QueueUrl=q); got += 1
            else:
                left.append(f"queue {name}")
    except Exception as e:
        left.append(f"sqs {e!r}"[:120])
    try:
        s3 = client("s3", acct)
        for b in s3.list_buckets().get("Buckets", []):
            name = b["Name"]
            if ours(name):
                keys = [{"Key": o["Key"]}
                        for o in s3.list_objects_v2(Bucket=name).get("Contents", [])]
                if keys:
                    s3.delete_objects(Bucket=name, Delete={"Objects": keys})
                s3.delete_bucket(Bucket=name); got += 1
            else:
                left.append(f"bucket {name}")
    except Exception as e:
        left.append(f"s3 {e!r}"[:120])
    try:
        dd = client("dynamodb", acct)
        for t in dd.list_tables().get("TableNames", []):
            if ours(t):
                dd.delete_table(TableName=t); got += 1
            else:
                left.append(f"table {t}")
    except Exception as e:
        left.append(f"ddb {e!r}"[:120])
    try:
        lam = client("lambda", acct)
        for f in lam.list_functions().get("Functions", []):
            if ours(f["FunctionName"]):
                lam.delete_function(FunctionName=f["FunctionName"]); got += 1
            else:
                left.append(f"fn {f['FunctionName']}")
    except Exception as e:
        left.append(f"lambda {e!r}"[:120])
    try:
        e = client("events", acct)
        for bus in e.list_event_buses().get("EventBuses", []):
            bn = bus["Name"]
            rules = e.list_rules(EventBusName=bn).get("Rules", [])
            for r in rules:
                rn = r["Name"]
                e.remove_targets(Rule=rn, EventBusName=bn, Ids=["t1", "lf", "tp",
                                                               "final", "next", "x", "y", "kick"], Force=True)
                e.delete_rule(Name=rn, EventBusName=bn); got += 1
            if bn != "default" and (ours(bn) or bn.startswith("chain")
                                    or bn.startswith("cycle") or bn.startswith("soak")):
                e.delete_event_bus(Name=bn); got += 1
    except Exception as e:
        left.append(f"events {e!r}"[:120])
    try:
        sns = client("sns", acct)
        for topic in sns.list_topics().get("Topics", []):
            arn = topic["TopicArn"]
            if ours(arn.rsplit(":", 1)[-1]):
                sns.delete_topic(TopicArn=arn); got += 1
    except Exception as e:
        left.append(f"sns {e!r}"[:120])
    print(acct, "deleted:", got, "left:", left)
try:
    print("lambda containers left:",
          sum(1 for _ in json.loads(json.dumps(0)) * [0]) or
          len(os.popen("docker ps -a --filter name=robotocore-lambda- -q").read().split()))
except Exception:
    pass
