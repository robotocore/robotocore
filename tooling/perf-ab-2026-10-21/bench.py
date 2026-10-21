#!/usr/bin/env python3
"""robotocore (Rust twin) vs moto 5.2.4 (Python) A/B performance profile.

Identical boto3 workloads against both backends. Serial latency percentiles
+ 4-thread concurrency. Client-side timing (same harness process for both).
"""
import json, os, statistics, threading, time, uuid, sys
import boto3, botocore
from concurrent.futures import ThreadPoolExecutor

EP_RUST = "http://127.0.0.1:4566"
EP_MOTO = "http://127.0.0.1:4799"
BODY = os.urandom(16384)
AK_R = "100000000001"   # rust: access key IS the account id
AK_M = "200000000002"   # moto: separate account id

def client(svc, ep, ak):
    return boto3.client(svc, endpoint_url=ep, region_name="us-east-1",
                        aws_access_key_id=ak, aws_secret_access_key="test",
                        config=botocore.config.Config(
                            retries={"max_attempts": 1},
                            s3={"addressing_style": "path"} if svc == "s3" else {}))

def timed(fn, n, p=lambda: True):
    """Time n calls of fn; return list of per-call ms."""
    out = []
    for _ in range(n):
        if not p():   # precondition failed -> stop
            return None
        t0 = time.perf_counter_ns()
        fn()
        if p():
            out.append((time.perf_counter_ns() - t0) / 1e6)
    return out

def pct(xs, q):
    if not xs: return None
    s = sorted(xs)
    return round(s[int(q * (len(s) - 1))], 3)

def stats(xs):
    if not xs: return {"n": 0}
    return {"n": len(xs), "p50": pct(xs, .5), "p95": pct(xs, .95),
            "mean": round(statistics.mean(xs), 3)}

N_SMALL = 400     # cheap ops per backend
N_MED = 200       # medium ops (s3/ddb)

R = {}

def measure(name, ep, ak):
    c = {}
    for svc in ("sts","sqs","s3","dynamodb","ec2","iam"):
        c[svc] = client(svc, ep, ak)
    res = {}
    rng = uuid.uuid4().hex[:8]

    # --- sts.get_caller_identity ---
    res["sts_get_caller_identity"] = stats(timed(
        lambda: c["sts"].get_caller_identity(), N_SMALL))

    # --- s3 lifecycle (create bucket once; put/get/head/delete; list) ---
    bucket = f"perf-ab-{rng}"
    c["s3"].create_bucket(Bucket=bucket)
    obj = "perf/obj"
    body = os.urandom(16384)
    res["s3_put_16k"] = stats(timed(
        lambda: c["s3"].put_object(Bucket=bucket, Key=obj, Body=body), N_MED))
    res["s3_get_16k"] = stats(timed(
        lambda: c["s3"].get_object(Bucket=bucket, Key=obj)["Body"].read(), N_MED))
    res["s3_list_head"] = stats(timed(
        lambda: c["s3"].list_objects_v2(Bucket=bucket, MaxKeys=5), N_SMALL))
    c["s3"].delete_object(Bucket=bucket, Key=obj)
    c["s3"].delete_bucket(Bucket=bucket)

    # --- sqs cycle: send / receive / delete ---
    q = c["sqs"].create_queue(QueueName=f"perf-ab-{rng}")["QueueUrl"]
    body_json = json.dumps({"ping": rng})
    def send_one():
        c["sqs"].send_message(QueueUrl=q, MessageBody=body_json)
    received = []
    res["sqs_send"] = stats(timed(send_one, N_SMALL))
    try:
        res["sqs_receive_delete"] = stats(timed(lambda: None, 0) or [])
        msgs = timed(lambda: None, 0) or []
        # receive+delete N times
        rec = []
        dele = []
        for i in range(N_SMALL):
            t0 = time.perf_counter_ns()
            got = c["sqs"].receive_message(QueueUrl=q, MaxNumberOfMessages=1)
            rec.append((time.perf_counter_ns() - t0) / 1e6)
            if got.get("Messages"):
                msg = got["Messages"][0]
                t0 = time.perf_counter_ns()
                c["sqs"].delete_message(QueueUrl=q, ReceiptHandle=msg["ReceiptHandle"])
                dele.append((time.perf_counter_ns() - t0) / 1e6)
        res["sqs_receive"] = stats(rec) if rec else {"n": 0}
        res["sqs_delete"] = stats(dele) if dele else {"n": 0}
    finally:
        c["sqs"].delete_queue(QueueUrl=q)

    # --- dynamodb: table create once; put/get/delete ---
    table = f"perf-ab-{rng}"
    c["dynamodb"].create_table(TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST")
    for i in range(N_MED):
        c["dynamodb"].put_item(TableName=table,
            Item={"pk": {"S": f"k{i}"}, "v": {"S": "x"}})
    res["ddb_put"] = stats(timed(
        lambda: c["dynamodb"].put_item(TableName=table,
                                       Item={"pk": {"S": f"t{time.perf_counter_ns()}"}, "v": {"S": "y"}}),
        N_MED))
    res["ddb_get"] = stats(timed(
        lambda: c["dynamodb"].get_item(TableName=table, Key={"pk": {"S": f"k{(int(time.time()*1000)%N_MED)+N_MED}"}}), N_SMALL))
    res["ddb_query_range"] = stats(timed(
        lambda: c["dynamodb"].scan(TableName=table, Limit=10), N_SMALL))
    c["dynamodb"].delete_table(TableName=table)

    # --- ec2: describe internal (no params varies) ---
    res["ec2_describe_vpcs"] = stats(timed(
        lambda: c["ec2"].describe_vpcs(), N_SMALL))
    res["ec2_describe_instances"] = stats(timed(
        lambda: c["ec2"].describe_instances(), N_SMALL))

    # --- iam: wide list ---
    res["iam_list_users"] = stats(timed(
        lambda: c["iam"].list_users(MaxItems=5), N_SMALL))

    return res

def measure_concurrent(name, ep, ak, threads=4):
    """4 threads x (sqs send/receive/delete + ddb put/get + s3 put/get)."""
    c_main = {}
    for svc in ("sqs","dynamodb","s3"):
        c_main[svc] = client(svc, ep, ak)
    rng = uuid.uuid4().hex[:8]
    q = c_main["sqs"].create_queue(QueueName=f"perf-ab-c-{rng}")["QueueUrl"]
    table = f"perf-ab-c-{rng}"
    c_main["dynamodb"].create_table(TableName=table,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST")
    bucket = f"perf-ab-c-{rng.lower()[:8]}"
    c_main["s3"].create_bucket(Bucket=bucket)
    per_thread = 100
    count = {"cyc": 0}
    lock = threading.Lock()
    def worker(tid):
        cs = client("sqs", ep, ak); cd = client("dynamodb", ep, ak); c3 = client("s3", ep, ak)
        for i in range(per_thread):
            cs.send_message(QueueUrl=q, MessageBody=f"t{tid}-{i}")
            got = cs.receive_message(QueueUrl=q, MaxNumberOfMessages=1, WaitTimeSeconds=0)
            if got.get("Messages"):
                cs.delete_message(QueueUrl=q, ReceiptHandle=got["Messages"][0]["ReceiptHandle"])
            t0 = time.perf_counter_ns()
            cd.put_item(TableName=table, Item={"pk": {"S": f"{tid}-{i}"}, "v": {"S": "v"}})
            cd.get_item(TableName=table, Key={"pk": {"S": f"{tid}-{i}"}})
            c3.put_object(Bucket=bucket, Key=f"w/{tid}-{i}", Body=b"y" * 8192)
            c3.get_object(Bucket=bucket, Key=f"w/{tid}-{i}")
            with lock:
                count["cyc"] += 1
    t0 = time.perf_counter()
    ts = [threading.Thread(target=worker, args=(t,)) for t in range(threads)]
    for t in ts: t.start()
    for t in ts: t.join()
    wall = time.perf_counter() - t0
    for svc in ("sqs","dynamodb","s3"):
        c_main[svc].close() if hasattr(c_main[svc], "close") else None
    try: c_main["dynamodb"].delete_table(TableName=table)
    except Exception: pass
    n = threads * per_thread
    return {"wall_s": round(wall, 2), "threads": threads,
            "cycles_per_thread": per_thread,
            "mixed_ops_per_sec": round(n * 4 / wall, 1)}   # 4 wire ops per cycle

def rss_mb(pid):
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS"):
                    return round(int(line.split()[1]) / 1024, 1)
    except Exception:
        return None
    return None

def rust_pid():
    out = os.popen("pgrep -f 'robotocore-rust.*4566'").read().split()
    return int(out[0]) if out else None

def moto_pid():
    out = os.popen("pgrep -f 'moto_server.*4799'").read().split()
    return int(out[0]) if out else None

def run(backend):
    ep, ak = (EP_RUST, AK_R) if backend == "rust" else (EP_MOTO, AK_M)
    print(f"--- {backend} server profiling…", flush=True)
    t0 = time.perf_counter()
    out = measure(backend, ep, ak)
    print(f"    serial measured in {time.perf_counter()-t0:.1f}s", flush=True)
    t0 = time.perf_counter()
    cc = measure_concurrent(backend, ep, ak)
    print(f"    concurrent: {cc}", flush=True)
    return {"serial": out, "concurrent": cc,
            "rss_mb": rss_mb(backend == "rust" and rust_pid() or moto_pid())}

if __name__ == "__main__":
    only = sys.argv[1] if len(sys.argv) > 1 else "both"
    out = {}
    if only in ("both", "rust"):  out["rust"] = run("rust")
    if only in ("both", "moto"):  out["moto"] = run("moto")
    with open("/tmp/perf-ab/results.json", "w") as f:
        json.dump(out, f, indent=2)
    print(json.dumps(out, indent=2))
