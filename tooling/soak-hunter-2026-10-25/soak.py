#!/usr/bin/env python3
"""HUNTER-SOAK wave-3 lane — 30-minute mixed soak + chaos on a scratch twin.

Endpoint from SOAK_ENDPOINT (default http://localhost:4802). The twin process
must be running with LAMBDA_EXECUTOR=docker. All resources live in dedicated
accounts so compat lanes on any shared twin stay untouched:

  soak   773100000001   steady-state SQS/S3/DDB inventory + metric lambda
  sched  773100000002   scheduler-edge rules (rate/cron wildcards/annual/monthly)
  sched2 773100000003   second scheduler scope (account-scoped isolation proof)
  chaos  663200000001   rule churn + 50-target no-op fan-out rules
  stray  663300000001   stray rules cross-account targeting soak inventory
  chain  663400000001   5-deep bus chain + 2-bus cycle + chain->Lambda/SNS hops

Sampler (60s cadence): /proc VmRSS+HWM, threads, open fds, lambda container
count, /tmp/robotocore-lambda-* entry counts. Leak verdict: RSS slope
> 10 MB/hour is a leak finding.
"""
from __future__ import annotations

import concurrent.futures
import io
import json
import os
import subprocess
import threading
import time
import zipfile

import boto3
import botocore

ENDPOINT = os.environ.get("SOAK_ENDPOINT") or "http://localhost:4802"
REG = os.environ.get("SOAK_REGION") or "us-east-1"
RUN_SECONDS = int(os.environ.get("SOAK_SECONDS", 1800))

SOAK, SCHED_A, SCHED_B = "773100000001", "773100000002", "773100000003"
CHAOS, STRAY, CHAIN = "663200000001", "663300000001", "663400000001"

T0 = time.time()
LOGD = "/tmp/hunter-soak/logs"
os.makedirs(LOGD, exist_ok=True)
LOG = open(f"{LOGD}/events.jsonl", "a", buffering=1)
LAT: dict[str, list] = {}
LOCK = threading.Lock()
STOP = threading.Event()
TWIN_PID = int(open("/tmp/hunter-soak/twin.pid").read().strip())


def ev(kind, **kw):
    LOG.write(json.dumps({"t": round(time.time() - T0, 1), "kind": kind, **kw}) + "\n")


def client(svc, acct):
    cfg = botocore.config.Config(
        retries={"max_attempts": 1},
        s3={"addressing_style": "path"} if svc == "s3" else {},
    )
    return boto3.client(svc, endpoint_url=ENDPOINT, region_name=REG,
                        aws_access_key_id=acct, aws_secret_access_key="test",
                        config=cfg)


def timed(family, fn):
    t = time.perf_counter()
    out = fn()
    ms = (time.perf_counter() - t) * 1000
    with LOCK:
        LAT.setdefault(family, []).append(((time.time() - T0) / 60.0, ms))
    return ms, out


def rolled(family, lo_min, hi_min):
    """latency stats within [lo, hi) minutes elapsed."""
    with LOCK:
        xs = [m for (t, m) in LAT.get(family, []) if lo_min <= t < hi_min]
    if not xs:
        return None
    xs.sort()
    return {"n": len(xs), "p50": round(xs[len(xs) // 2], 1),
            "p95": round(xs[min(int(len(xs) * 0.95), len(xs) - 1)], 1)}


# ---------------------------------------------------------------- inventory
print("provisioning soak inventory...", flush=True)
s = client("s3", SOAK)
sq = client("sqs", SOAK)
dd = client("dynamodb", SOAK)

QUEUE = sq.create_queue(QueueName="hunter-soak-sink")["QueueUrl"]
SINK_ARN = sq.get_queue_attributes(QueueUrl=QUEUE, AttributeNames=["QueueArn"])[
    "Attributes"]["QueueArn"]
BUCKET = f"hunter-soak-{SOAK}"
s.create_bucket(Bucket=BUCKET)
TBL = "hunter-soak-tbl"
dd.create_table(TableName=TBL,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST")
dd.get_waiter("table_exists").wait(TableName=TBL)
BODY = os.urandom(16384)
ev("inventory_ready")
print("inventory ready", flush=True)

# --------------------------------------------- docker-metric lambda (soak) --
lam = client("lambda", SOAK)


def zpy(code):
    b = io.BytesIO()
    zipfile.ZipFile(b, "w", zipfile.ZIP_DEFLATED).writestr("index.py", code)
    return b.getvalue()


lam.create_function(
    FunctionName="soak-metric", Runtime="python3.12",
    Role=f"arn:aws:iam::{SOAK}:role/r", Handler="index.handler",
    MemorySize=256, Timeout=30,
    Code={"ZipFile": zpy(
        "import hashlib, json\n\n"
        "def handler(e, c):\n"
        "    d = hashlib.sha256(json.dumps(e, default=str).encode()).hexdigest()\n"
        "    buf = [('x' * 4096) for _ in range(64)]\n"
        "    return {'ok': True, 'strike': d[:8], 'evt': e.get('seq', 0), 'n': len(buf)}\n")})
lam.get_waiter("function_active").wait(FunctionName="soak-metric")
ev("metric_lambda_ready")

WAVES = {"n": 0, "ok": 0, "ms": []}


def one_invoke(seq):
    r = lam.invoke(FunctionName="soak-metric",
                   Payload=json.dumps({"seq": seq, "chaos": os.urandom(512).hex()}))
    return ("FunctionError" not in r
            and json.loads(r["Payload"].read()).get("ok") is True)


def invoke_metrics():
    WAVES["n"] += 1
    seq = WAVES["n"]
    # every 5th wave bursts 5 concurrent docker invocations
    wave = range(5) if seq % 5 == 0 else range(1)
    t0 = time.time()
    try:
        with concurrent.futures.ThreadPoolExecutor(5) as px:
            oks = list(px.map(lambda _: one_invoke(seq), wave))
        ms = round((time.time() - t0) * 1000)
        WAVES["ok"] += sum(1 for o in oks if o)
        WAVES["ms"].append(ms)
        ev("lambda_wave", seq=seq, ok=sum(1 for o in oks if o), n=len(wave), ms=ms)
    except Exception as e:
        WAVES["fail"] = WAVES.get("fail", 0) + 1
        ev("lambda_wave_fail", seq=seq, err=repr(e)[:200])


def lambda_loop():
    while not STOP.is_set():
        invoke_metrics()
        STOP.wait(60.0)


# ------------------------------------------------------------- steady state -
def sqs_worker():
    n = 0
    while not STOP.is_set():
        n += 1
        ms, _ = timed("sqs_send", lambda: sq.send_message(
            QueueUrl=QUEUE, MessageBody=f"m{n}"))
        ms2, got = timed("sqs_receive", lambda: sq.receive_message(
            QueueUrl=QUEUE, MaxNumberOfMessages=1, WaitTimeSeconds=0))
        for m in got.get("Messages", []):
            ms3, _ = timed("sqs_delete", lambda mm=m: sq.delete_message(
                QueueUrl=QUEUE, ReceiptHandle=mm["ReceiptHandle"]))
        STOP.wait(0.35)
        if any(x > 3000 for x in (ms, ms2)):
            ev("ss_error", fam="sqs", err="slow", ms=ms, ms2=ms2)


def s3_worker():
    n = 0
    while not STOP.is_set():
        n += 1
        k = f"steady/{n % 50}"
        ms1, _ = timed("s3_put", lambda: s.put_object(Bucket=BUCKET, Key=k, Body=BODY))
        ms2, _ = timed("s3_get", lambda: s.get_object(Bucket=BUCKET, Key=k))
        if n % 20 == 0:
            ms3, _ = timed("s3_list", lambda: s.list_objects_v2(
                Bucket=BUCKET, Prefix="steady/", MaxKeys=5))
        else:
            ms3 = 0.0
        STOP.wait(0.35)
        if any(x > 3000 for x in (ms1, ms2, ms3)):
            ev("ss_error", fam="s3", err="slow", ms=ms1, ms2=ms2, ms3=ms3)


def ddb_worker():
    n = 0
    while not STOP.is_set():
        n += 1
        pk = f"k{n % 50}"
        ms1, _ = timed("ddb_put", lambda: dd.put_item(
            TableName=TBL, Item={"pk": {"S": pk}, "v": {"S": f"v{n}"}}))
        ms2, _ = timed("ddb_get", lambda: dd.get_item(TableName=TBL, Key={"pk": {"S": pk}}))
        if n % 20 == 0:
            ms3, _ = timed("ddb_scan", lambda: dd.scan(TableName=TBL, Limit=10))
        else:
            ms3 = 0.0
        STOP.wait(0.35)
        if any(x > 3000 for x in (ms1, ms2, ms3)):
            ev("ss_error", fam="ddb", err="slow", ms=ms1, ms2=ms2)


# ---------------------------------------------------- sampler (60s cadence) -
def read_proc(pid):
    rss = hwm = threads = fds = None
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS"):
                    rss = int(line.split()[1]) / 1024.0
                elif line.startswith("VmHWM"):
                    hwm = int(line.split()[1]) / 1024.0
                elif line.startswith("Threads:"):
                    threads = int(line.split()[1])
        fds = len(os.listdir(f"/proc/{pid}/fd"))
    except Exception as e:
        ev("sampler_proc_fail", err=repr(e)[:120])
    return rss, hwm, threads, fds


def tmp_counts():
    base = "/tmp/robotocore-lambda-"
    out = {}
    for d in ("docker", "exec", "build", "node", "bridges"):
        try:
            out[d] = len(os.listdir(base + d))
        except OSError:
            out[d] = None
    return out


def ctr_count():
    out = subprocess.run(
        ["docker", "ps", "-a", "--filter", "name=robotocore-lambda-",
         "--format", "{{.Names}}"], capture_output=True, text=True)
    return len(out.stdout.splitlines())


SAMPLER_ROWS: list[dict] = []


def sampler():
    while not STOP.is_set():
        rss, hwm, thr, fds = read_proc(TWIN_PID)
        rec = {"t": round(time.time() - T0, 1), "rss_mb": rss, "hwm_mb": hwm,
               "threads": thr, "fds": fds, "lambda_ctrs": ctr_count()}
        for k, v in tmp_counts().items():
            rec[f"tmp_{k}"] = v
        SAMPLER_ROWS.append(rec)
        ev("sample", **rec)
        STOP.wait(60.0)


# ---------------------------------------------------- scheduler-edge rules --
EXPRS = {
    "rate-1m": "rate(1 minute)",
    "cron-0/2": "cron(0/2 * * * ? *)",
    "cron-wildmin": "cron(* * * * ? *)",
    "cron-annual": "cron(15 10 1 1 ? *)",
    "cron-monthly": "cron(30 12 1 * ? *)",
    "cron-hourly": "cron(45 * * * ? *)",
}


def setup_sched_rules(acct, suffix):
    """Scheduled rules in one account, all targeting that account's own sink
    view of the soak queue — SQS ARN dispatch resolves the queue by the ARN's
    own (account, region), so rules fire only into the rules' account scope."""
    # per-account sink queues: SCHED_A/B get their own sinks so rule identity
    # stays unambiguous.
    q = client("sqs", acct)
    qu = q.create_queue(QueueName=f"sched-sink-{suffix}")["QueueUrl"]
    arn = q.get_queue_attributes(QueueUrl=qu, AttributeNames=["QueueArn"])[
        "Attributes"]["QueueArn"]
    e = client("events", acct)
    created = {}
    for name, expr in EXPRS.items():
        rn = f"soak-sched-{name}-{suffix}"
        e.put_rule(Name=rn, ScheduleExpression=expr, State="ENABLED",
                   Description="hunter-soak wave-3 edge")
        e.put_targets(Rule=rn, Targets=[
            {"Id": "t1", "Arn": arn, "Input": json.dumps({"rule": rn})}])
        created[rn] = expr
    # stray-shape probe: scheduled rule on a CUSTOM bus. moto/real Events pin
    # ScheduleExpression to the default bus; the era twin stores it — the
    # soak records whichever behavior lands.
    try:
        e.create_event_bus(Name=f"soak-bus-{suffix}")
        brn = f"soak-sched-busbill-{suffix}"
        e.put_rule(EventBusName=f"soak-bus-{suffix}", Name=brn,
                   ScheduleExpression="rate(1 minute)", State="ENABLED",
                   Description="custom-bus scheduled rule")
        e.put_targets(EventBusName=f"soak-bus-{suffix}", Rule=brn, Targets=[
            {"Id": "t1", "Arn": arn, "Input": json.dumps({"rule": brn})}])
        created[brn] = "custom-bus-rate-1m"
    except Exception as ex:
        ev("custom_bus_rule_fail", acct=acct, err=repr(ex)[:160])
    # disabled rule must never fire
    drn = f"soak-sched-disabled-{suffix}"
    e.put_rule(Name=drn, ScheduleExpression="rate(1 minute)", State="DISABLED")
    e.put_targets(Rule=drn, Targets=[{"Id": "t1", "Arn": arn}])
    created[drn] = "disabled"
    return created, qu


def drain_all(sqcli, qurl, nmax=600):
    got_all = []
    for _ in range(nmax):
        got = sqcli.receive_message(QueueUrl=qurl, MaxNumberOfMessages=10,
                                    WaitTimeSeconds=0)
        ms = got.get("Messages", [])
        if not ms:
            break
        got_all.extend(ms)
        sqcli.delete_message_batch(QueueUrl=qurl, Entries=[
            {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]}
            for i, m in enumerate(ms)])
    return got_all


def sched_sweep(tag):
    """Drain each scheduler sink once; count Scheduled Event deliveries per
    rule marker. Scheduled deliveries arrive either as the raw envelope
    (target without Input) or as the target Input `{"rule": <name>}`."""
    total = {"scheduled": 0, "per_rule": {}, "other": 0}
    for acct, suffix in ((SCHED_A, "a"), (SCHED_B, "b"), (SOAK, "main")):
        sqcli = client("sqs", acct)
        qurl = sched_queues[suffix]
        msgs = drain_all(sqcli, qurl)
        for m in msgs:
            try:
                j = json.loads(m.get("Body", ""))
            except Exception:
                j = {}
            rule = None
            if (j.get("source") == "aws.events"
                    and j.get("detail-type") == "Scheduled Event"):
                rule = ((j.get("resources") or [""])[0]).rsplit("/")[-1]
            elif isinstance(j.get("rule"), str):
                rule = j["rule"]
            if rule:
                total["scheduled"] += 1
                total["per_rule"][rule] = total["per_rule"].get(rule, 0) + 1
            else:
                total["other"] += 1
    ev("sched_sweep", tag=tag, **total)


sched_queues: dict[str, str] = {}

# ------------------------------------------------------------- chaos duty ---
def chaos_rule_churn():
    e = client("events", CHAOS)
    for i in range(20):
        rn = f"chaos-churn-{i}"
        e.put_rule(Name=rn, ScheduleExpression="rate(1 minute)")
        e.put_targets(Rule=rn, Targets=[{"Id": "t", "Arn": SINK_ARN,
                                         "Input": json.dumps({"churn": i})}])
        e.delete_rule(Name=rn)
    ev("chaos_churn_done", cycles=20)


def chaos_burst_rule():
    """50-target rule targeting unhooked codebuild ARNs — worst-case fan-out
    per scheduler tick, every target a hooked-None silent drop."""
    e = client("events", CHAOS)
    e.put_rule(Name="chaos-burst-50", ScheduleExpression="rate(1 minute)")
    tgts = [{"Id": f"t{i}", "Arn": f"arn:aws:codebuild:{REG}:{CHAOS}:project/x{i}"}
            for i in range(50)]
    e.put_targets(Rule="chaos-burst-50", Targets=tgts)
    ev("chaos_burst_rule_up", targets=50)


def chain_setup():
    """CHAIN acct: 5-deep bus chain -> chained sink queue; bounded 2-bus
    cycle; Scheduled->Lambda (docker-mode); Scheduled->SNS->SQS fan-out."""
    chain_sq = client("sqs", CHAIN)
    chain_q = chain_sq.create_queue(QueueName="chain-sink")["QueueUrl"]
    chain_arn = chain_sq.get_queue_attributes(
        QueueUrl=chain_q, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sched_queues["chain"] = chain_q
    ce = client("events", CHAIN)
    for i in range(5):
        ce.create_event_bus(Name=f"chain-bus-{i}")
    for i in range(4):
        ce.put_rule(EventBusName=f"chain-bus-{i}", Name="hop",
                    EventPattern=json.dumps({"source": ["chain-hop"]}))
        ce.put_targets(EventBusName=f"chain-bus-{i}", Rule="hop", Targets=[{
            "Id": "next",
            "Arn": f"arn:aws:events:{REG}:{CHAIN}:event-bus/chain-bus-{i + 1}"}])
    ce.put_rule(EventBusName="chain-bus-4", Name="sink",
                EventPattern=json.dumps({"source": ["chain-hop"]}))
    ce.put_targets(EventBusName="chain-bus-4", Rule="sink", Targets=[{
        "Id": "final", "Arn": chain_arn, "Input": json.dumps({"rule": "chain-sink"})}])
    # bounded 2-bus cycle fed by its own entry event
    ce.create_event_bus(Name="cycle-a")
    ce.create_event_bus(Name="cycle-b")
    for bus, tgt_bus in (("cycle-a", "cycle-b"), ("cycle-b", "cycle-a")):
        ce.put_rule(EventBusName=bus, Name="hop",
                    EventPattern=json.dumps({"source": ["cycle-entry"]}))
        ce.put_targets(EventBusName=bus, Rule="hop", Targets=[{
            "Id": "x", "Arn": f"arn:aws:events:{REG}:{CHAIN}:event-bus/{tgt_bus}"}])
    # Scheduled -> Lambda target (live docker invocation per fire)
    lamc = client("lambda", CHAIN)
    lamc.create_function(FunctionName="chain-hop", Runtime="python3.12",
                         Role=f"arn:aws:iam::{CHAIN}:role/r",
                         Handler="index.handler", Timeout=30,
                         Code={"ZipFile": zpy("def handler(e, c):\n    return {'hopped': 1}\n")})
    lamc.get_waiter("function_active").wait(FunctionName="chain-hop")
    fn_arn = lamc.get_function_configuration(FunctionName="chain-hop")["FunctionArn"]
    ce.put_rule(Name="chain-sched-lambda", ScheduleExpression="rate(1 minute)")
    ce.put_targets(Rule="chain-sched-lambda", Targets=[{
        "Id": "lf", "Arn": fn_arn}])
    # Scheduled -> SNS -> SQS fanout into the chained sink
    snsc = client("sns", CHAIN)
    topic = snsc.create_topic(Name="chain-hop-topic")["TopicArn"]
    snsc.subscribe(TopicArn=topic, Protocol="sqs", Endpoint=chain_arn)
    ce.put_rule(Name="chain-sched-sns", ScheduleExpression="rate(1 minute)")
    ce.put_targets(Rule="chain-sched-sns", Targets=[{"Id": "tp", "Arn": topic}])
    ev("chain_setup_done")
    return chain_q


def chain_hop(shots):
    ce = client("events", CHAIN)
    for i in range(shots):
        t0 = time.time()
        ce.put_events(Entries=[{
            "Source": "chain-hop", "DetailType": "hop",
            "Detail": json.dumps({"hop": i}), "EventBusName": "chain-bus-0"}])
        ev("chain_hop_latency", i=i, ms=round((time.time() - t0) * 1000))
    ce.put_events(Entries=[{
        "Source": "cycle-entry", "DetailType": "kick",
        "Detail": "{}", "EventBusName": "cycle-a"}])
    ev("cycle_entry_posted")


def stray_setup_and_fire():
    se = client("events", STRAY)
    se.put_rule(Name="stray-wild",
                EventPattern=json.dumps({"source": [{"prefix": "hunter"}]}))
    se.put_targets(Rule="stray-wild", Targets=[
        {"Id": "a", "Arn": SINK_ARN},                                  # cross-account sqs by ARN
        {"Id": "b", "Arn": f"arn:aws:dynamodb:{REG}:{SOAK}:table/{TBL}"},   # hooked-None kind
        {"Id": "c", "Arn": f"arn:aws:codebuild:{REG}:{SOAK}:project/c"},    # unknown service
        {"Id": "d", "Arn": f"arn:aws:events:{REG}:{SOAK}:bus/no-such-bus"}, # foreign bus
    ])
    se.put_rule(Name="stray-sched", ScheduleExpression="rate(1 minute)")
    se.put_targets(Rule="stray-sched", Targets=[{"Id": "a", "Arn": SINK_ARN}])
    r = se.put_events(Entries=[{
        "Source": "hunter-chaos", "DetailType": "stray",
        "Detail": json.dumps({"stray": i})} for i in range(3)])
    ev("stray_fired", n=r.get("ResponseMetadata", {}).get("HTTPStatusCode"))


def chaos_loop():
    while not STOP.is_set():
        try:
            chaos_rule_churn()
        except Exception as e:
            ev("chaos_churn_fail", err=repr(e)[:160])
        try:
            stray_setup_and_fire()
        except Exception as e:
            ev("stray_fire_fail", err=repr(e)[:160])
        STOP.wait(180.0)


# ------------------------------------------------------------------- main ---
def main():
    ev("start", pid=TWIN_PID, endpoint=ENDPOINT)
    for w in (sqs_worker, s3_worker, ddb_worker):
        threading.Thread(target=w, daemon=True).start()
    threading.Thread(target=lambda_loop, daemon=True).start()
    threading.Thread(target=sampler, daemon=True).start()

    sched_a, qa = setup_sched_rules(SCHED_A, "a")
    sched_b, qb = setup_sched_rules(SCHED_B, "b")
    sched_queues["a"], sched_queues["b"], sched_queues["main"] = qa, qb, QUEUE
    ev("sched_rules_up", a=len(sched_a), b=len(sched_b))

    threading.Thread(target=chaos_loop, daemon=True).start()
    try:
        chain_setup()
    except Exception as e:
        ev("chain_setup_fail", err=repr(e)[:200])
    try:
        chaos_burst_rule()
    except Exception as e:
        ev("chaos_burst_fail", err=repr(e)[:160])
    invoke_metrics()

    interval = float(os.environ.get("SOAK_SWEEP_INTERVAL", 300))
    next_sweep = time.time() + interval
    while time.time() - T0 < RUN_SECONDS:
        time.sleep(min(10, max(1, next_sweep - time.time())))
        now = time.time()
        if now >= next_sweep and now - T0 < RUN_SECONDS:
            elapsed_min = (now - T0) / 60
            sched_sweep(f"m{int(elapsed_min)}")
            # chain-hop probe every other sweep (non-scheduled direct hops)
            if int(elapsed_min) % 10 == 4:
                try:
                    chain_hop(3)
                except Exception as e:
                    ev("chain_hop_fail", err=repr(e)[:160])
                time.sleep(3)
                drained = drain_all(client("sqs", CHAIN), sched_queues["chain"])
                ev("chain_sink_drain", msgs=len(drained))
            fams = {f: rolled(f, 0, elapsed_min) for f in
                    ("sqs_send", "sqs_receive", "s3_put", "s3_get",
                     "ddb_put", "ddb_get")}
            ev("latency_checkpoint", minute=round(elapsed_min, 1), **fams)
            try:
                out = subprocess.run(["curl", "-s", f"{ENDPOINT}/_robotocore/health"],
                                     capture_output=True, text=True)
                h = json.loads(out.stdout or "{}")
                err = subprocess.run(["curl", "-s", f"{ENDPOINT}/_robotocore/usage/errors"],
                                     capture_output=True, text=True)
                erj = json.loads(err.stdout or "{}")
                ev("health_ckpt", uptime=h.get("uptime"), status=h.get("status"),
                   errors=erj.get("total_errors", sum(
                       (erj.get("totals", {}) or {}).values())
                   ) or 0)
            except Exception as e:
                ev("health_ckpt_fail", err=repr(e)[:160])
            next_sweep = now + interval

    ev("soak_window_done", seconds=round(time.time() - T0, 1))
    STOP.set()
    time.sleep(45)  # settle: --rm containers + extractions flush
    with LOCK:
        lat = {k: [m for (_t, m) in v] for k, v in LAT.items()}
    final = {
        "samples": SAMPLER_ROWS,
        "waves": WAVES,
        "final_ctr_count": ctr_count(),
        "final_tmp": tmp_counts(),
        "families": {f: rolled(f, 0, 1e9) for f in LAT},
    }
    open(f"{LOGD}/final.json", "w").write(json.dumps(final, indent=2))
    ev("final_written", ctrs=final["final_ctr_count"], tmp=final["final_tmp"])
    print("SOAK DONE", flush=True)


if __name__ == "__main__":
    main()
