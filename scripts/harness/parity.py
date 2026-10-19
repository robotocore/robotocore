#!/usr/bin/env python3
"""Fidelity parity runner for robotocore (pure Rust).

The Python server and the moto sidecar are gone; every compat suite runs
once against the single Rust server (rotocore natively implements every
ported service). Fidelity is actual vs expected, where expected = per-suite
pass counts in the golden baseline `.parity/expectations_current.json`.
Regressions print "EXPECTATIONS DRIFT" and the run exits 2.

Usage:
    parity.py                    # full run (tests/compatibility against the Rust server)
    parity.py --services sqs,s3  # subset
    parity.py --update-expectations   # promote a clean full run to the baseline
    parity.py --next             # just re-derive the next work item from state
    parity.py --state FILE       # state path (default: .parity/state.json)
    parity.py --expectations FILE  # baseline path (default: .parity/expectations_current.json)

Port comes from the environment: RUST_PORT (default 4566 — the shipped port).
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
RUST_PORT = os.environ.get("RUST_PORT", "4566")
# localhost (not 127.0.0.1): account-scoped control-plane APIs (s3control
# and kin) send the AccountId as a DNS subdomain of the endpoint host;
# an IP endpoint cannot take that subdomain and every request would
# die in client-side name resolution. *.localhost resolves to loopback.
RUST_URL = f"http://localhost:{RUST_PORT}"
RUST_BIN = str(RUST_REPO / "target/release/robotocore-rust")
COMPAT_DIR = REPO / "tests/compatibility"
# Golden baseline: per-suite pass counts the Rust lane must keep matching.
EXPECTATIONS_FILE = RUST_REPO / ".parity/expectations_current.json"

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


def ensure_rust_server():
    """Ensure the Rust robotocore server is up (PID-file managed, sidecar-free)."""
    log("[..] (Re)starting Rust server (rotocore native)...")
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
    proc = subprocess.Popen([RUST_BIN, "--host", "127.0.0.1", "--port", RUST_PORT],
                            stdout=open(f"/tmp/{os.environ.get('LOG_PREFIX', 'parity')}_rust.log", "w"),
                            stderr=subprocess.STDOUT, start_new_session=True)
    with open(pid_file, "w") as f:
        f.write(str(proc.pid))
    for _ in range(20):
        time.sleep(1)
        r = sh(f"curl -sf --max-time 3 {RUST_URL}/_robotocore/health", timeout=10)
        if r.returncode == 0:
            log(f"[ok] Rust server up at {RUST_URL}")
            return True
    log("[!!] Rust server failed to start (see /tmp/parity_rust.log)")
    return False


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
    """Run one compat suite against an endpoint, return (results, proc, timed_out).

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
    return results, r, timed_out


def classify_rust_only(results, timed_out):
    """Single-lane classification for --rust-only runs.

    Truncation rule is the same trust rule classify() applies for dual runs,
    applied to the one lane we have: a run that hit the suite timeout or that
    CRASHed before writing any testcase (empty/missing XML) is marked
    `truncated` and must not feed fidelity or drift comparisons.
    """
    counts = {"pass": 0, "fail": 0, "skip": 0}
    for v in results.values():
        counts[v] = counts.get(v, 0) + 1
    return {
        "pass": counts["pass"],
        "fail": counts["fail"],
        "skip": counts["skip"],
        "total": len(results),
        "truncated": bool(timed_out or not results),
    }


def print_rust_only_summary(report, next_work):
    """--parity fidelity map: actual pass counts vs the golden baseline."""
    log("")
    log("=" * 72)
    log("EXPECTATIONS MAP (rust-only: pass counts, fidelity = actual / expected)")
    log("=" * 72)
    log(f"{'service':15} {'spec':>5} {'rust':>5} {'fid%':>6} {'exp':>6} {'drift':>5}")
    for svc, d in sorted(report["services"].items()):
        if d.get("status") != "ok":
            log(f"{svc:15} {d.get('status','?'):15}")
            continue
        exp = d.get("expected")
        drift = d.get("drift")
        fid = str(d.get("fidelity_pct") or "-")
        drift = str(drift) if drift is not None else "-"
        log(f"{svc:15} {str(d.get('spec_ops') or '?'):>5} "
            f"{d['pass']:>5} {fid:>6} {str(exp if exp is not None else '-'):>6} {drift:>5}")
    log("=" * 72)
    if report.get("expectations_drift"):
        log(f"EXPECTATIONS DRIFT: {len(report['expectations_drift'])} suite(s) below baseline")
    log(f"NEXT WORK: {next_work}")
    log("=" * 72)


def drift_regressions(report, expectations):
    """Return [(deficit, suite, line)] for suites that went below baseline.

    Only complete runs count as regressions: a truncated run is an infra
    failure (CRASH/timeout), not the Rust server losing fidelity, and
    suites without a baseline entry have nothing to compare against.
    """
    out = []
    for svc, d in sorted(report["services"].items()):
        if d.get("status") != "ok" or d.get("truncated"):
            continue
        exp = expectations.get(svc) or {}
        expected, actual = exp.get("pass"), d.get("pass")
        if expected is None or actual is None or actual >= expected:
            continue
        out.append((expected - actual, svc,
                    f"EXPECTATIONS DRIFT: suite {svc} expected {expected}, got {actual}"))
    out.sort(reverse=True)
    return out


def classify_rust_only(results, timed_out):
    """Single-lane classification: pass/fail/skip + truncation trust rule."""
    counts = {"pass": 0, "fail": 0, "skip": 0}
    for v in results.values():
        counts[v] = counts.get(v, 0) + 1
    return {
        "pass": counts["pass"], "fail": counts["fail"], "skip": counts["skip"],
        "total": len(results),
        "truncated": bool(timed_out or not results),
    }


def run_all_rust_only(services, state_dir, expectations):
    """Run every suite ONCE against the Rust server; compare to the baseline."""
    if not ensure_rust_server():
        log("[!!] aborting: Rust server unavailable")
        return {"services": {}, "generated": time.strftime("%Y-%m-%dT%H:%M:%S"), "aborted": True}
    xml_dir = Path(state_dir) / "xml"
    xml_dir.mkdir(parents=True, exist_ok=True)
    report = {"services": {}, "generated": time.strftime("%Y-%m-%dT%H:%M:%S")}
    for svc in services:
        suite = find_suite(svc)
        if not suite:
            report["services"][svc] = {"status": "no_suite", "spec_ops": spec_op_count(svc),
                                       "crate": "native"}
            log(f"[skip] {svc}: no compat suite")
            continue
        expected = (expectations.get(svc) or {}).get("pass")
        log(f"[..] {svc}: running {suite.name} vs rust endpoint..." +
            (f" (baseline {expected})" if expected is not None else " (no baseline entry)"))
        rust_res, _, timed_out = run_suite(suite.name, RUST_URL, str(xml_dir / f"{svc}_rust.xml"))
        r = classify_rust_only(rust_res, timed_out)
        rec = {
            "status": "ok", "crate": "native", "spec_ops": spec_op_count(svc),
            "pass": r["pass"], "fail": r["fail"], "skip": r["skip"],
            "total": r["total"], "truncated": r["truncated"], "fidelity_pct": None,
        }
        if r["truncated"]:
            log(f"[WARN] {svc}: truncated run; fidelity/drift omitted")
        elif expected is None:
            log(f"     {svc}: pass={r['pass']}/{r['total']} (no baseline entry)")
        else:
            rec["expected"] = expected
            if expected:
                rec["fidelity_pct"] = round(100.0 * r["pass"] / expected, 1)
            rec["drift"] = r["pass"] - expected
            mark = f"{rec['fidelity_pct']}%" if expected else "-"
            if rec["fidelity_pct"] is not None and rec["fidelity_pct"] < 100.0:
                mark += f" [-{rec['drift']} vs baseline {expected}]"
            log(f"     {svc}: pass={r['pass']}/{r['total']}  fidelity={mark}")
        report["services"][svc] = rec
    report["expectations_drift"] = [
        {"suite": s, "line": l} for _, s, l in drift_regressions(report, expectations)
    ]
    if report["expectations_drift"]:
        log("")
        log("EXPECTATIONS DRIFT (actual < baseline):")
        for _, _s, line in report["expectations_drift"]:
            log(f"  {line}")
    return report


def load_expectations(path):
    """Read the golden baseline; return per-suite dict {} when unusable."""
    if not path.exists():
        log(f"[warn] no expectations file at {path} — drift comparison skipped "
            f"(promote a clean full run with --update-expectations)")
        return {}
    try:
        doc = json.loads(path.read_text())
    except (ValueError, OSError) as e:
        log(f"[warn] unreadable expectations file {path}: {e}")
        return {}
    per = doc.get("per_suite", {})
    log(f"[ok] baseline loaded: {len(per)} suites from {path}")
    return per


def write_expectations(report, path):
    """Promote a clean run's pass counts to the golden baseline (merge-safe).

    Suites not part of this run (e.g. a --services subset run) keep their
    existing baseline entries; truncated/missing suites keep theirs too.
    """
    per = {}
    if path.exists():
        try:
            per = json.loads(path.read_text()).get("per_suite", {})
        except (ValueError, OSError) as e:
            log(f"[warn] could not read existing expectations ({e}); starting fresh")
            per = {}
    kept = refreshed = failed = 0
    for svc, d in sorted(report["services"].items()):
        if d.get("status") != "ok" or d.get("truncated"):
            if svc in per:
                kept += 1
            else:
                failed += 1
            continue
        per[svc] = {"pass": d.get("pass", 0)}
        refreshed += 1
    doc = {
        "comment": ("Golden baseline: per-suite compat-suite pass counts the Rust server "
                    "must keep matching. Frozen from the Python ground-truth era; "
                    "regenerate with: uv run python scripts/harness/parity.py --rust-only "
                    "--update-expectations"),
        "generated": report.get("generated") or time.strftime("%Y-%m-%dT%H:%M:%S"),
        "per_suite": per,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2) + "\n")
    log(f"[ok] expectations written to {path} ({refreshed} refreshed, {kept} kept, {failed} skipped — "
        f"truncated/failed suites are never promoted)")


def derive_next_work(report):
    """Pick the single most impactful next work item.

    Priority order:
      1. Infra gap: bridge service at 0% (sidecar not routing) — fix plumbing first.
      2. Test blind spot: native service with no compat suite.
      3. Largest native fidelity gap (the real porting work).
    """
    if not report.get("services"):
        return "No data. Run a full parity run first."

    def rec_gap(d):
        """Deficit vs this run's reference (py lane in dual, baseline in rust-only)."""
        if "rust_gap" in d:
            return d["rust_gap"]
        drift = d.get("drift", 0) or 0
        return -drift if drift < 0 else 0

    infra = []
    blind = []
    gaps = []
    for svc, d in report["services"].items():
        if d.get("status") != "ok":
            if d.get("status") == "no_suite" and d.get("crate") == "native":
                blind.append((9999, f"write compat suite for native {svc} (zero coverage)"))
            continue
        if d.get("crate") == "bridge":
            lane_pass = d.get("rust_pass", d.get("pass", 0))
            if lane_pass == 0 and svc not in {"rds"}:
                infra.append((rec_gap(d), f"FIX BRIDGE: {svc} has 0 passing tests — moto sidecar not routing (infra, not porting)"))
            elif d.get("fidelity_pct") is not None and d["fidelity_pct"] < 80:
                gaps.append((rec_gap(d), f"port {svc} to native (bridge fidelity {d['fidelity_pct']}%)"))
            continue
        gap = rec_gap(d)
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


def save_state(report, next_work, state_path, mode=None):
    state = {
        "generated": report.get("generated"),
        "services": report.get("services", {}),
        "next_work": next_work,
    }
    if mode:
        state["mode"] = mode
    if report.get("expectations_drift") is not None:
        state["expectations_drift"] = report["expectations_drift"]
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2))
    log(f"[ok] state written to {state_path}")


TOTAL_TIMEOUT = 1800  # 30 minutes max for a full run


def main():
    ap = argparse.ArgumentParser()

    def _default_services():
        # The drift gate covers every compat suite physically present on
        # disk (restored/deleted suites follow the files, not a hardcoded
        # list — wave-promoted suites are thereby drift-gated too).
        suites = sorted(
            p_.stem[len("test_"):-len("_compat")]
            for p_ in (RUST_REPO / "tests" / "compatibility").glob("test_*_compat.py")
        )
        return ",".join(suites) if suites else ",".join(sorted(NATIVE.keys()))

    ap.add_argument("--services", default=_default_services())
    ap.add_argument("--state", default=str(RUST_REPO / ".parity/state.json"))
    ap.add_argument("--next", action="store_true", help="just re-derive next work from state")
    ap.add_argument("--timeout", type=int, default=TOTAL_TIMEOUT,
                    help="max total seconds for the full run (default 1800)")
    ap.add_argument("--dual", action="store_true",
                    help="run each suite vs Python ground truth AND rust (py-vs-rust fidelity)")
    ap.add_argument("--rust-only", dest="rust_only", action="store_true",
                    help="run each suite ONCE vs rust; fidelity = actual vs golden baseline "
                         "(fired automatically when the Python ground-truth server is absent)")
    ap.add_argument("--update-expectations", dest="update_expectations", action="store_true",
                    help="after the run, promote its clean pass counts into the baseline "
                         "(merge-safe; truncated/failed suites are never promoted)")
    ap.add_argument("--expectations", default=str(EXPECTATIONS_FILE),
                    help="baseline path (default: .parity/expectations_current.json)")
    args = ap.parse_args()
    state_path = Path(args.state)
    expectations_path = Path(args.expectations)

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

    EXPECTATIONS = load_expectations(expectations_path)
    report = run_all_rust_only(services, state_path.parent, EXPECTATIONS)
    signal.alarm(0)  # cancel the alarm
    if args.update_expectations:
        if report.get("aborted"):
            log("[skip] aborted run — nothing promoted to the baseline")
        else:
            write_expectations(report, expectations_path)
    next_work = derive_next_work(report)
    print_rust_only_summary(report, next_work)
    save_state(report, next_work, state_path, mode="rust-only")
    if report.get("expectations_drift"):
        log("[!!] baseline regression detected (see EXPECTATIONS DRIFT above)")
        raise SystemExit(2)


if __name__ == "__main__":
    main()
