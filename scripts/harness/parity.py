#!/usr/bin/env python3
"""Fidelity parity runner for the robotocore Rust port.

Runs the existing Python compat test suites against BOTH the Python server
(ground truth, :4566) and the Rust server (:4567), classifies every test
pass/fail in a 4-way diff, cross-references with botocore spec op counts,
and emits a state file + the single most important next work item.

Usage:
    parity.py                    # full run, all services
    parity.py --services sqs,s3  # subset
    parity.py --next             # just re-derive the next work item from state
    parity.py --state FILE       # where to write/read state (default: .parity/state.json)

The headline metric is *fidelity %* per service = rust_pass / py_pass.
A value < 100% means Rust is losing behavior the Python (moto) server has.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
RUST_REPO = Path(os.environ.get("RUST_REPO", REPO))
# When invoked via `uv run python`, sys.executable IS the project venv python.
PYV = os.environ.get("ROBOTOCORE_PYTHON", sys.executable)
PY_PORT = os.environ.get("PY_PORT", "4566")
RUST_PORT = os.environ.get("RUST_PORT", "4567")
PY_URL = f"http://127.0.0.1:{PY_PORT}"
RUST_URL = f"http://127.0.0.1:{RUST_PORT}"
RUST_BIN = str(RUST_REPO / "target/release/robotocore-rust")
COMPAT_DIR = REPO / "tests/compatibility"

# Native crate -> botocore service name (for spec op counts)
NATIVE = {
    "sts": "sts", "sqs": "sqs", "s3": "s3", "dynamodb": "dynamodb",
    "sns": "sns", "secretsmanager": "secretsmanager", "kms": "kms",
    "ssm": "ssm", "iam": "iam", "lambda": "lambda",
    "logs": "logs", "events": "events", "kinesis": "kinesis",
    "firehose": "firehose", "cloudwatch": "cloudwatch", "ecr": "ecr",
    "ecs": "ecs", "stepfunctions": "stepfunctions",
}
# Auto-detect the compat suite for a service by scanning the compat dir.
# Prefers an exact test_{svc}_compat.py; falls back to any test file whose
# name contains the service token.
def find_suite(svc):
    token = svc.replace("-", "")
    files = list(COMPAT_DIR.glob("test_*_compat.py"))
    exact = COMPAT_DIR / f"test_{svc}_compat.py"
    if exact.exists():
        return exact
    # exact token match in filename
    for f in files:
        stem = f.name.replace("test_", "").replace("_compat.py", "")
        if stem == token:
            return f
    # substring match (e.g. "state" for stepfunctions handled by exact above)
    for f in files:
        stem = f.name.replace("test_", "").replace("_compat.py", "")
        if token in stem:
            return f
    return None


# Services to always include beyond the 18 native (bridge spot-checks)
BRIDGE_SPOTCHECK = ["ec2", "rds", "cloudformation", "lambda"]



def log(msg):
    print(msg, flush=True)


def sh(cmd, **kw):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, **kw)


def ensure_python_server():
    r = sh(f"curl -sf --max-time 3 {PY_URL}/_robotocore/health", timeout=10)
    if r.returncode == 0:
        log(f"[ok] Python server up at {PY_URL}")
        return True
    log("[..] Starting Python server (local uvicorn)...")
    log_file = open("/tmp/parity_py.log", "w")
    subprocess.Popen(
        [PYV, "-m", "robotocore.main"],
        stdout=log_file, stderr=subprocess.STDOUT, cwd=str(REPO),
        env=dict(os.environ, ROBOTOCORE_PORT=PY_PORT),
        start_new_session=True)
    for _ in range(30):
        time.sleep(2)
        r = sh(f"curl -sf --max-time 3 {PY_URL}/_robotocore/health", timeout=10)
        if r.returncode == 0:
            log(f"[ok] Python server up at {PY_URL}")
            return True
    log("[!!] Python server failed to start (see /tmp/parity_py.log)")
    return False


SIDECAR_PORT = os.environ.get("SIDECAR_PORT", "4568")
SIDECAR_URL = f"http://127.0.0.1:{SIDECAR_PORT}"
SIDECAR_HEALTH = f"{SIDECAR_URL}/_sidecar/health"
SIDECAR_SCRIPT = str(RUST_REPO / "scripts/moto_sidecar.py")
state_dir_global = Path("/tmp/parity")


def ensure_sidecar():
    """Verify the moto sidecar daemon is up."""
    r = sh(f"curl -sf --max-time 3 {SIDECAR_HEALTH}", timeout=10)
    if r.returncode == 0:
        log(f"[ok] moto sidecar up at {SIDECAR_URL}")
        return True
    log("[!!] moto sidecar not running. Start it with: bash scripts/start_parity_servers.sh")
    return False


def _bridge_ready():
    """Probe the bridge with a real EC2 call. Returns True when it works."""
    r = sh(
        f"curl -s -X POST {RUST_URL}/ --max-time 10 "
        f"-H 'Content-Type: application/x-www-form-urlencoded' "
        f"-H 'Authorization: AWS4-HMAC-SHA256 Credential=123456789012/20240101/us-east-1/ec2/aws4_request' "
        f"-H 'X-Amz-Date: 20240101T000000Z' "
        f"-d 'Action=DescribeInstances&Version=2016-11-15' 2>/dev/null",
        timeout=15,
    )
    return r.returncode == 0 and "<DescribeInstancesResponse" in r.stdout


def ensure_rust_server():
    """Ensure the Rust server is up WITH a WORKING moto bridge, using PID files.

    Only touches processes belonging to THIS parity instance (LOG_PREFIX +
    ports), never servers owned by other worktrees/agents.
    """
    ensure_sidecar()
    log("[..] (Re)starting Rust server with moto bridge...")
    sh(f"cd {RUST_REPO} && cargo build --release", timeout=600)
    pid_file = f"/tmp/{os.environ.get('LOG_PREFIX', 'parity')}_rust.pid"
    old_pid = None
    if os.path.exists(pid_file):
        try:
            old_pid = int(open(pid_file).read().strip())
        except ValueError:
            old_pid = None
    if old_pid:
        sh(f"kill -0 {old_pid} 2>/dev/null && kill {old_pid} 2>/dev/null; true", timeout=10)
        time.sleep(1)
    # Belt-and-braces: any process visibly bound to OUR port only
    sh(f"fuser -k {RUST_PORT}/tcp 2>/dev/null; true", timeout=10)
    time.sleep(1)
    proc = subprocess.Popen([RUST_BIN, "--port", RUST_PORT, "--moto-url", SIDECAR_URL],
                            stdout=open(f"/tmp/{os.environ.get('LOG_PREFIX', 'parity')}_rust.log", "w"),
                            stderr=subprocess.STDOUT, start_new_session=True)
    with open(pid_file, "w") as f:
        f.write(str(proc.pid))
    # Wait for the server to be up
    for _ in range(20):
        time.sleep(1)
        r = sh(f"curl -sf --max-time 3 {RUST_URL}/_robotocore/health", timeout=10)
        if r.returncode == 0:
            break
    # Wait for the BRIDGE to actually work (sidecar may still be loading backends)
    for attempt in range(30):
        # Check sidecar is still alive
        sc = sh(f"curl -sf --max-time 3 {SIDECAR_HEALTH}", timeout=10)
        if sc.returncode != 0:
            log(f"[!!] sidecar died during bridge wait (attempt {attempt})")
            # Restart sidecar
            _restart_sidecar()
            continue
        if _bridge_ready():
            log(f"[ok] Rust server (with working bridge) up at {RUST_URL}")
            return True
        time.sleep(2)
    log("[!!] Rust server up but bridge not working")
    return False


def _restart_sidecar():
    # Kill only OUR sidecar (by PID file), not other worktrees' sidecars.
    pid_file = f"/tmp/{os.environ.get('LOG_PREFIX', 'parity')}_sidecar.pid"
    if os.path.exists(pid_file):
        try:
            old_pid = int(open(pid_file).read().strip())
            sh(f"kill -0 {old_pid} 2>/dev/null && kill -9 {old_pid} 2>/dev/null; true", timeout=10)
        except ValueError:
            pass
    sh(f"fuser -k {SIDECAR_PORT}/tcp 2>/dev/null; true", timeout=10)
    time.sleep(1)
    script = str(REPO / "scripts/moto_sidecar.py")
    side_log = f"/tmp/{os.environ.get('LOG_PREFIX', 'parity')}_sidecar.log"
    log_cmd = f"cd {REPO} && nohup {PYV} {script} --port {SIDECAR_PORT} > {side_log} 2>&1 & echo $! > {pid_file}"
    subprocess.Popen(log_cmd, shell=True, cwd=str(REPO), start_new_session=True, executable="/bin/bash")
    time.sleep(5)


def spec_op_count(svc):
    import glob, gzip, importlib.util
    spec = importlib.util.find_spec("botocore")
    base = str(Path(spec.origin).parent / "data")
    pat = os.path.join(base, svc, "*", "service-2.json.gz")
    paths = glob.glob(pat)
    if not paths:
        return None
    with gzip.open(paths[0]) as f:
        return len(json.load(f).get("operations", {}))


def parse_junit(path):
    """Return dict: test_name -> 'pass' | 'fail' | 'skip'."""
    if not os.path.exists(path):
        return {}
    tree = ET.parse(path)
    root = tree.getroot()
    result = {}
    # testsuite may be nested or top-level
    for ts in root.iter("testsuite"):
        for tc in ts.findall("testcase"):
            name = tc.get("name")
            cls = tc.get("classname", "")
            key = f"{cls}::{name}" if cls else name
            if tc.find("failure") is not None or tc.find("error") is not None:
                result[key] = "fail"
            elif tc.find("skipped") is not None:
                result[key] = "skip"
            else:
                result[key] = "pass"
    return result


PER_SUITE_TIMEOUT = int(os.environ.get("PARITY_SUITE_TIMEOUT", "600"))  # seconds per service per endpoint


def run_suite(suite_name, endpoint, out_xml):
    """Run one compat suite against an endpoint, return parsed results.

    Uses a hard timeout to catch runaway tests. If the suite times out,
    returns partial results from whatever XML was written.
    """
    env = dict(os.environ, ENDPOINT_URL=endpoint)
    # Never let ambient AWS_* endpoint vars redirect the suite to the wrong server
    for var in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL_STS",
                "AWS_DEFAULT_PROFILE", "AWS_PROFILE"):
        env.pop(var, None)
    cmd = (f"cd {REPO} && ENDPOINT_URL={endpoint} {PYV} -m pytest "
           f"{COMPAT_DIR}/{suite_name} -q -p no:cacheprovider "
           f"--junitxml={out_xml}")
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                           env=env, timeout=PER_SUITE_TIMEOUT)
        timed_out = False
    except subprocess.TimeoutExpired:
        timed_out = True
        # The XML file may have partial results
        r = None
    results = parse_junit(out_xml)
    if timed_out:
        log(f"     [WARN] {suite_name} vs {endpoint} timed out after {PER_SUITE_TIMEOUT}s "
            f"({len(results)} partial results)")
    return results, r


def classify(py_res, rust_res):
    """4-way classification with counts + failing test lists.

    Trust rule: when a run is truncated (suite timeout / collection crash),
    the two sides see different test sets, which cannot be compared honestly;
    the caller then omits the fidelity figure instead of publishing fiction.
    """
    py_names = set(py_res)
    rust_names = set(rust_res)
    names = py_names | rust_names
    truncated = bool(py_names and rust_names and py_names != rust_names)
    both_pass = [n for n in names if py_res.get(n) == "pass" and rust_res.get(n) == "pass"]
    rust_gap = [n for n in names if py_res.get(n) == "pass" and rust_res.get(n) == "fail"]  # fidelity loss
    py_only_fail = [n for n in names if py_res.get(n) == "fail" and rust_res.get(n) != "pass"]  # env issue
    both_fail = [n for n in names if py_res.get(n) == "fail" and rust_res.get(n) == "fail"]
    skipped = [n for n in names if "skip" in (py_res.get(n), rust_res.get(n))]
    return {
        "both_pass": len(both_pass),
        "rust_gap": len(rust_gap),
        "py_only_fail": len(py_only_fail),
        "both_fail": len(both_fail),
        "skipped": len(skipped),
        "total": len(names),
        "rust_gap_tests": sorted(rust_gap),
        "truncated": truncated,
    }


def run_all(services, state_dir):
    if not ensure_python_server():
        log("[!!] aborting: Python ground-truth server unavailable")
        return {"services": {}, "generated": time.strftime("%Y-%m-%dT%H:%M:%S"), "aborted": True}
    if not ensure_rust_server():
        log("[!!] aborting: Rust server / bridge unavailable")
        return {"services": {}, "generated": time.strftime("%Y-%m-%dT%H:%M:%S"), "aborted": True}
    xml_dir = Path(state_dir) / "xml"
    xml_dir.mkdir(parents=True, exist_ok=True)
    report = {"services": {}, "generated": time.strftime("%Y-%m-%dT%H:%M:%S")}
    for svc in services:
        suite = find_suite(svc)
        if not suite:
            report["services"][svc] = {"status": "no_suite", "spec_ops": spec_op_count(svc),
                                       "crate": "native" if svc in NATIVE else "bridge"}
            log(f"[skip] {svc}: no compat suite")
            continue
        log(f"[..] {svc}: running {suite.name} vs both endpoints...")
        py_res, _ = run_suite(suite.name, PY_URL, str(xml_dir / f"{svc}_py.xml"))
        rust_res, _ = run_suite(suite.name, RUST_URL, str(xml_dir / f"{svc}_rust.xml"))
        c = classify(py_res, rust_res)
        py_pass = sum(1 for v in py_res.values() if v == "pass")
        rust_pass = sum(1 for v in rust_res.values() if v == "pass")
        if c["truncated"]:
            # Test sets differ between runs — a fidelity% would be fiction.
            fidelity = None
            log(f"[WARN] {svc}: truncated run (py={len(py_res)} vs rust={len(rust_res)} tests); fidelity omitted")
        else:
            fidelity = round(100.0 * rust_pass / py_pass, 1) if py_pass else None
        report["services"][svc] = {
            "status": "ok",
            "crate": "native" if svc in NATIVE else "bridge",
            "spec_ops": spec_op_count(svc),
            "py_pass": py_pass, "rust_pass": rust_pass,
            "fidelity_pct": fidelity,
            **c,
        }
        log(f"     {svc}: fidelity={fidelity}%  py={py_pass} rust={rust_pass} gap={c['rust_gap']}")
    return report


def derive_next_work(report):
    """Pick the single most impactful next work item.

    Priority order:
      1. Infra gap: bridge service at 0% (sidecar not routing) — fix plumbing first.
      2. Test blind spot: native service with no compat suite.
      3. Largest native fidelity gap (the real porting work).
    """
    if not report.get("services"):
        return "No data. Run a full parity run first."

    infra = []
    blind = []
    gaps = []
    for svc, d in report["services"].items():
        if d.get("status") != "ok":
            if d.get("status") == "no_suite" and d.get("crate") == "native":
                blind.append((9999, f"write compat suite for native {svc} (zero coverage)"))
            continue
        if d.get("crate") == "bridge":
            if d.get("rust_pass", 0) == 0 and svc not in {"rds"}:
                infra.append((d["rust_gap"], f"FIX BRIDGE: {svc} has 0 passing tests — moto sidecar not routing (infra, not porting)"))
            elif d.get("fidelity_pct") is not None and d["fidelity_pct"] < 80:
                gaps.append((d["rust_gap"], f"port {svc} to native (bridge fidelity {d['fidelity_pct']}%)"))
            continue
        gap = d.get("rust_gap", 0)
        fid = d.get("fidelity_pct")
        if gap > 0:
            gaps.append((gap, f"fix {svc}: {gap} fidelity gap(s) at {fid}% fidelity"))

    if infra:
        infra.sort(reverse=True)
        return infra[0][1]
    if blind:
        blind.sort(reverse=True)
        return blind[0][1]
    if gaps:
        gaps.sort(reverse=True)
        return gaps[0][1]
    return "All native services at 100% fidelity and covered. Next: expand bridge coverage or port a new service."


def save_state(report, next_work, state_path):
    state = {
        "generated": report.get("generated"),
        "services": report.get("services", {}),
        "next_work": next_work,
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2))
    log(f"[ok] state written to {state_path}")


def print_summary(report, next_work):
    log("")
    log("=" * 72)
    log("FIDELITY MAP (rust_pass / py_pass, gap = fidelity loss)")
    log("=" * 72)
    log(f"{'service':15} {'type':7} {'spec':>5} {'py':>5} {'rust':>5} {'fid%':>6} {'gap':>4}")
    for svc, d in sorted(report["services"].items()):
        if d.get("status") != "ok":
            log(f"{svc:15} {d.get('status','?'):15}")
            continue
        fid = d.get("fidelity_pct")
        log(f"{svc:15} {d['crate']:7} {str(d.get('spec_ops') or '?'):>5} "
            f"{d['py_pass']:>5} {d['rust_pass']:>5} {str(fid or '-'):>6} {d['rust_gap']:>4}")
    log("=" * 72)
    log(f"NEXT WORK: {next_work}")
    log("=" * 72)


TOTAL_TIMEOUT = 1800  # 30 minutes max for a full run


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--services", default=",".join(sorted(NATIVE.keys())))
    ap.add_argument("--state", default=str(RUST_REPO / ".parity/state.json"))
    ap.add_argument("--next", action="store_true", help="just re-derive next work from state")
    ap.add_argument("--timeout", type=int, default=TOTAL_TIMEOUT,
                    help="max total seconds for the full run (default 1800)")
    args = ap.parse_args()
    state_path = Path(args.state)

    if args.next:
        if state_path.exists():
            state = json.loads(state_path.read_text())
            log(f"NEXT WORK: {state.get('next_work')}")
            return
        log("No state file. Run a full parity run first.")
        return

    services = [s.strip() for s in args.services.split(",") if s.strip()]
    # include bridge spot-checks if not explicitly limited
    if args.services == ",".join(sorted(NATIVE.keys())) or not args.services:
        for bsvc in BRIDGE_SPOTCHECK:
            if bsvc not in services and find_suite(bsvc):
                services.append(bsvc)

    # Overall timeout guard
    import signal
    def _timeout_handler(signum, frame):
        log(f"[!!] Total run exceeded {args.timeout}s — saving partial state")
        raise SystemExit(1)
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(args.timeout)

    report = run_all(services, state_path.parent)
    signal.alarm(0)  # cancel the alarm
    next_work = derive_next_work(report)
    save_state(report, next_work, state_path)
    print_summary(report, next_work)


if __name__ == "__main__":
    main()
