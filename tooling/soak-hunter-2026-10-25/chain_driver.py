#!/usr/bin/env python3
"""Chain-hop driver — complements soak.py which (bug) never fires its
chain_hop block on a 300s sweep cadence. Runs beside the soak on the twin:
every 90s direct chain hops into the 5-deep bus chain + a 2-bus cycle kick;
drains the chain sink every 4th pass. Runs until STOP file appears."""
from __future__ import annotations

import json
import os
import time

import boto3
import botocore

ENDPOINT = os.environ.get("SOAK_ENDPOINT") or "http://localhost:4802"
REG = "us-east-1"
CHAIN = "663400000001"
LOGD = "/tmp/hunter-soak/logs"
T0 = time.time()


def client(svc, acct):
    cfg = botocore.config.Config(
        retries={"max_attempts": 1},
        s3={"addressing_style": "path"} if svc == "s3" else {})
    return boto3.client(svc, endpoint_url=ENDPOINT, region_name=REG,
                        aws_access_key_id=acct, aws_secret_access_key="test",
                        config=cfg)


def ev(kind, **kw):
    with open(f"{LOGD}/chaindriver.jsonl", "a") as f:
        f.write(json.dumps({"t": round(time.time() - T0, 1), "kind": kind, **kw}) + "\n")


ce = client("events", CHAIN)
csq = client("sqs", CHAIN)
chain_q = csq.get_queue_url(QueueName="chain-sink")["QueueUrl"]
n = 0
hops_drained = 0
while not os.path.exists(f"{LOGD}/STOP"):
    n += 1
    try:
        t = time.time()
        ce.put_events(Entries=[{
            "Source": "chain-hop", "DetailType": "hop",
            "Detail": json.dumps({"hop": n}), "EventBusName": "chain-bus-0"}])
        ev("chain_hop", n=n, ms=round((time.time() - t) * 1000))
        if n % 3 == 0:
            t = time.time()
            ce.put_events(Entries=[{
                "Source": "cycle-entry", "DetailType": "kick",
                "Detail": "{}", "EventBusName": "cycle-a"}])
            ev("cycle_kick", n=n, ms=round((time.time() - t) * 1000))
    except Exception as e:
        ev("hop_fail", err=repr(e)[:160])
    if n % 4 == 0:
        got_all = []
        for _ in range(100):
            got = csq.receive_message(QueueUrl=chain_q, MaxNumberOfMessages=10,
                                      WaitTimeSeconds=0)
            ms = got.get("Messages", [])
            if not ms:
                break
            got_all.extend(ms)
            csq.delete_message_batch(QueueUrl=chain_q, Entries=[
                {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]}
                for i, m in enumerate(ms)])
        hops_drained += len(got_all)
        ev("chain_drain", drained=len(got_all), cumulative=hops_drained)
    time.sleep(90)
print("chain driver done", flush=True)
