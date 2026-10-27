#!/usr/bin/env python3
"""Analyze the HUNTER-SOAK run: RSS slope, container/tmp drift, scheduler
cadence evidence, family latency drift, worst-case captures."""
from __future__ import annotations

import json
import os
import statistics
import subprocess

import boto3
import botocore

ENZ = "SOAK_ENDPOINT"
ENDPOINT = os.environ.get(ENZ) or "http://localhost:4802"
REG = "us-east-1"
LOGD = "/tmp/hunter-soak/logs"

EVENTS_RAW = [json.loads(l) for l in open(f"{LOGD}/events.jsonl") if l.strip()]
# keep only the final (30-minute) soak invocation: the file also holds the
# short dry run with its own T0 and the pre-fix binary
starts = [i for i, e in enumerate(EVENTS_RAW) if e.get("kind") == "start"]
EVENTS = EVENTS_RAW[starts[-1:] and starts[-1] or 0:] if starts else EVENTS_RAW
FINAL = json.loads(open(f"{LOGD}/final.json").read())

# ---- RSS slope ------------------------------------------------------------
samples = [s for s in FINAL.get("samples", []) if s.get("rss_mb") is not None] if False else [
    s for s in EVENTS if s["kind"] == "sample" and s.get("rss_mb") is not None]
n = len(samples)
if n >= 2:
    xs = [s["t"] for s in samples]
    ys = [s["rss_mb"] for s in samples]
    tbar = sum(xs) / n
    ybar = sum(ys) / n
    slope_mb_per_h = sum((x - tbar) * (y - ybar) for x, y in zip(xs, ys)) / (
        sum((x - tbar) ** 2 for x in xs) * 3600) * 3600 / 3600
    # redo properly: slope per second -> per hour
    denom = sum((x - tbar) ** 2 for x in xs)
    slope_per_s = sum((x - tbar) * (y - ybar) for x, y in zip(xs, ys)) / denom
    slope_mb_per_h = slope_per_s * 3600
else:
    slope_mb_per_h = None

# ---- scheduler cadence ----------------------------------------------------
sweeps = [e for e in EVENTS if e["kind"] == "sched_sweep"]
per_rule_total: dict[str, int] = {}
sched_total = other_total = 0
for w in sweeps:
    sched_total += w.get("scheduled", 0)
    other_total += w.get("other", 0)
    for rule, c in (w.get("per_rule") or {}).items():
        per_rule_total[rule] = per_rule_total.get(rule, 0) + c

# ---- waves ----------------------------------------------------------------
waves = [e for e in EVENTS if e["kind"] == "lambda_wave"]
wave_fail = [e for e in EVENTS if e["kind"] == "lambda_wave_fail"]
ss_errors = [e for e in EVENTS if e["kind"] == "ss_error"]
chain = [e for e in EVENTS if e["kind"].startswith("chain_") or e["kind"].startswith("cycle_")]
chains_fail = [e for e in chain if "fail" in e["kind"]]

# ---- final live checks ----------------------------------------------------
out = subprocess.run(["docker", "ps", "-a", "--filter", "name=robotocore-lambda-",
                      "--format", "{{.Names}}"], capture_output=True, text=True)
leftover_ctrs = out.stdout.split()
tmp = {}
for d in ("docker", "exec", "build", "node", "bridges"):
    base = f"/tmp/robotocore-lambda-{d}"
    try:
        tmp[d] = len(os.listdir(base))
    except OSError:
        tmp[d] = None

def err_totals():
    out = subprocess.run(["curl", "-s", f"{ENDPOINT}/_robotocore/usage/errors"],
                         capture_output=True, text=True)
    return json.loads(out.stdout or "{}")

def audit_501s(limit=200):
    out = subprocess.run(
        ["curl", "-s", f"{ENDPOINT}/_robotocore/audit?limit={limit}"],
        capture_output=True, text=True)
    d = json.loads(out.stdout or "{}")
    return [(e.get("service"), e.get("operation"), e.get("status_code"))
            for e in d.get("entries", []) if e.get("status_code") >= 500]

errors = err_totals()
bad_audit = audit_501s()

# ---- latency drift: earliest checkpoint vs final (cumulative p50/p95) -----
checkpoints = sorted([e for e in EVENTS if e["kind"] == "latency_checkpoint"],
                     key=lambda e: e.get("minute", 0))
lat_first = checkpoints[0] if checkpoints else None
lat_last = checkpoints[-1] if checkpoints else None

report = {
    "rss": {"n_samples": n,
            "start_mb": samples[0]["rss_mb"] if n else None,
            "end_mb": samples[-1]["rss_mb"] if n else None,
            "peak_mb": max((s["hwm_mb"] or 0) for s in samples) if n else None,
            "slope_mb_per_hour": round(slope_mb_per_h, 3) if slope_mb_per_h is not None else None,
            "leak_verdict": "LEAK (>10MB/h)" if slope_mb_per_h and slope_mb_per_h > 10 else "within-budget"},
    "threads_fds": {"start": {k: samples[0][k] for k in ("threads", "fds")} if n else None,
                    "end": {k: samples[-1][k] for k in ("threads", "fds")} if n else None},
    "containers": {"live_leftover": leftover_ctrs},
    "tmp_dirs": tmp,
    "scheduler": {"sweeps": len(sweeps), "scheduled_deliveries": sched_total,
                  "per_rule": per_rule_total, "other": other_total},
    "lambda": {"waves": len(waves), "fail_waves": len(wave_fail),
               "total_invokes_ok": sum(w.get("ok", 0) for w in waves),
               "wave_ms_first": waves[0].get("ms") if waves else None,
               "wave_ms_last": waves[-1].get("ms") if waves else None},
    "workload": {"ss_errors": len(ss_errors), "chain_events": len(chain),
                 "chain_failures": len(chains_fail)},
    "live_errors_endpoint": errors,
    "audit_5xx": bad_audit,
    "latency_first_checkpoint": lat_first,
    "latency_last_checkpoint": lat_last,
}
open(f"{LOGD}/analysis.json", "w").write(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
