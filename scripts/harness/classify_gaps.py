#!/usr/bin/env python3
"""Stage 1: classify every rust_gap test into a fixable bucket.

Usage:
    python scripts/harness/classify_gaps.py --service ssm
    python scripts/harness/classify_gaps.py --all          # all services in .parity
    python scripts/harness/classify_gaps.py --all --out .parity/gap_report.json

Input  (all read-only):
    .parity/xml/{svc}_{py,rust}.xml     junit from the last parity run
    tests/compatibility/test_*_compat.py  ground-truth test sources
    crates/{crate}/src/handler.rs        Rust dispatch tables
    botocore/data/{svc}/*/service-2.json.gz  specs
Output:
    .parity/gap_report.json  (see DESIGN.md section 3, stage 1)

Exit code 0 on success. No network, no server.
"""
from __future__ import annotations

import argparse
import glob
import gzip
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

REPO = Path("/Users/jackdanger/www/robotocore")
RUST_REPO = Path("/Users/jackdanger/www/robotocore-rust")
XML_DIR = RUST_REPO / ".parity" / "xml"
COMPAT_DIR = REPO / "tests" / "compatibility"
BOTOCORE_DATA = REPO / ".venv/lib/python3.12/site-packages/botocore/data"
MOTO_DIR = REPO / ".venv/lib/python3.12/site-packages/moto"

# crate dir name for each service (differs for logs)
CRATE_NAME = {"logs": "cloudwatch-logs", "sts": None, "secretsmanager": "secretsmanager"}


def load_junit(path: Path) -> dict:
    """test name -> None (pass) | 'SKIP' | failure message text."""
    out = {}
    if not path.exists():
        return out
    for ts in ET.parse(path).getroot().iter("testsuite"):
        for tc in ts.findall("testcase"):
            n = tc.get("name")
            f = tc.find("failure")
            e = tc.find("error")
            if f is not None:
                out[n] = (f.get("message") or "") + "\n" + (f.get("text") or "")
            elif e is not None:
                out[n] = (e.get("message") or "") + "\n" + (e.get("text") or "")
            elif tc.find("skipped") is not None:
                out[n] = "SKIP"
            else:
                out[n] = None
    return out


def pascalize(snake: str) -> str:
    return "".join(w.capitalize() for w in snake.split("_"))


def spec_op_map(svc: str) -> dict:
    """snake boto method -> Pascal op name, from the botocore spec."""
    pat = str(BOTOCORE_DATA / svc / "*" / "service-2.json.gz")
    paths = glob.glob(pat)
    if not paths:
        return {}
    spec = json.load(gzip.open(paths[0]))
    ops = list(spec.get("operations", {}).keys())
    return {re.sub(r"(?<!^)(?=[A-Z])", "_", o).lower(): o for o in ops}


def load_test_bodies(path: Path) -> dict:
    """test name -> source body (args + function)."""
    out = {}
    if not path.exists():
        return out
    src = path.read_text()
    for m in re.finditer(r"def (test_\w+)\(([^)]*)\):(.*?)(?=\n    def |\n    @|\nclass |\Z)", src, re.S):
        out[m.group(1)] = (m.group(2), m.group(3))
    return out


def load_stub_ops(crate: str) -> set:
    p = RUST_REPO / "crates" / crate / "src" / "handler.rs"
    if not p.exists():
        return set()
    return set(re.findall(r'"(\w+)" => self\.json_stub', p.read_text()))


def stub_line_numbers(crate: str) -> dict:
    p = RUST_REPO / "crates" / crate / "src" / "handler.rs"
    if not p.exists():
        return {}
    out = {}
    for i, line in enumerate(p.read_text().splitlines(), 1):
        m = re.search(r'"(\w+)" => self\.json_stub', line)
        if m:
            out[m.group(1)] = i
    return out


def moto_ref(svc: str, op: str):
    """(file, 'def name') for the moto backend implementation, or None."""
    moto_svc = {"logs": "logs"}.get(svc, svc)
    d = MOTO_DIR / moto_svc
    if not d.exists():
        return None
    m = re.sub(r"(?<!^)(?=[A-Z])", "_", op).lower()
    for fname in ("models.py", "models"):
        f = d / fname
        if f.is_file():
            src = f.read_text()
            if re.search(r"def " + re.escape(m) + r"\(", src):
                return f"{moto_svc}/{fname}:def {m}"
    return None


# ---------------------------------------------------------------------------
# Classifier: failure text -> bucket
# ---------------------------------------------------------------------------
def classify_failure(msg: str):
    """Return (bucket, details) for one failing test's junit text."""
    first = msg.split("\n")[0]
    ce = re.search(r"An error occurred \(([\w.]+)\) when calling (\w+) operation", msg)
    code, op_in_err = (ce.group(1), ce.group(2)) if ce else (None, None)

    # A1 missing field
    m = re.search(r"KeyError: '(\w+)'", first)
    if m:
        return "A1", {"missing_field": m.group(1)}
    m = re.search(r"assert '(\w+)' in \{", first)
    if m:
        return "A1", {"missing_field": m.group(1)}
    # A3 wrong shape
    if "AttributeError" in first:
        return "A3", {"shape_error": first[:120]}
    # A2 empty list item expected
    if "IndexError" in first:
        return "A2", {}
    # C1 wrong status
    if ce and code and code.isdigit():
        return "C1", {"status": int(code), "op": op_in_err,
                      "not_implemented": "not implemented" in msg}
    # C2/C3 wrong error code
    if ce and code:
        if code == "NotImplemented":
            return "C5", {"op": op_in_err}
        if code == "ValidationException":
            # test likely expects a specific not-found code
            exp = re.search(r"== '(\w*NotFound\w*)'|in \(([^)]*NotFound[^)]*)\)", msg)
            return "C2", {"expected": exp.group(1) or (exp.group(2).strip("' ") if exp else None),
                          "op": op_in_err}
        if code in ("InvalidAction", "InvalidParameterValue", "InvalidParameterException",
                    "MissingParameter", "InvalidParameterCombination"):
            return "C3", {"code": code, "op": op_in_err}
        return "C6", {"code": code, "op": op_in_err}
    # D did not raise
    if "DID NOT RAISE" in msg:
        return "D", {}
    # B empty results
    if (re.search(r"assert 0 == \d+", first) or re.search(r"assert \d+ == 0", first)
            or "in []" in msg or "len([])" in msg):
        return "B", {}
    # K filter/projection
    if re.search(r"assert '\w+' not in \{", first):
        return "K", {}
    # G stub ids
    if re.search(r"startswith of str.*stub-|'stub-'\.startswith", msg):
        return "G", {}
    # count mismatches -> B
    if re.search(r"assert \d+ == \d+", first):
        return "B", {}
    # assertion on error code string
    if re.search(r"assert '\w+' == '\w+NotFound\w*'|assert '\w+' in \('\w*NotFound", first):
        return "C2", {}
    if first.startswith("AssertionError") or first.startswith("assert "):
        return "B_assertion_other", {"first": first[:120]}
    if "Timeout" in msg:
        return "I", {}
    return "Z", {"first": first[:120]}


BUCKET_TO_WORKSTREAM = {
    "A1": "WS1", "A3": "WS1", "A2": "WS1", "C1": "WS1", "C5": "WS1",
    "B": "WS2", "B_assertion_other": "WS2", "K": "WS2", "D": "WS2", "C6": "WS2", "G": "WS2",
    "C2": "WS3", "C3": "WS3",
    "I": "WS4", "Z": "WS4",
}


def test_ops(args: str, body: str, opmap: dict):
    """boto ops referenced by a test body."""
    methods = set(re.findall(r"\.\b([a-z][a-z0-9_]{3,})\(", body))
    ops = {opmap[m] for m in methods if m in opmap}
    return ops


def process_service(svc: str) -> dict:
    crate = CRATE_NAME.get(svc, svc)
    py = load_junit(XML_DIR / f"{svc}_py.xml")
    rust = load_junit(XML_DIR / f"{svc}_rust.xml")
    gap = {n: rust[n] for n in set(py) & set(rust)
           if py[n] is None and rust[n] not in (None, "SKIP")}
    opmap = spec_op_map(svc)
    testfile = COMPAT_DIR / f"test_{svc}_compat.py"
    bodies = load_test_bodies(testfile)
    stubs = load_stub_ops(crate) if crate else set()
    stub_lines = stub_line_numbers(crate) if crate else {}

    tests = []
    for name, msg in sorted(gap.items()):
        bucket, details = classify_failure(msg)
        base = name.split("::")[-1]
        args, body = bodies.get(base, ("", ""))
        ops = test_ops(args, body, opmap)
        if ops and ops <= stubs:
            coverage = "all_stubbed"
        elif ops & stubs:
            coverage = "partial_stub"
        else:
            coverage = "no_stub"
        confidence = "high" if coverage in ("all_stubbed",) or bucket in ("A1", "C1", "C2", "C3", "C5") else "medium"
        rec = {
            "test": name, "bucket": bucket,
            "workstream": BUCKET_TO_WORKSTREAM.get(bucket, "WS4"),
            "ops": sorted(ops), "op_coverage": coverage,
            "details": details,
            "rust_file": f"crates/{crate}/src/handler.rs" if crate else None,
            "rust_stub_lines": {o: stub_lines[o] for o in ops if o in stub_lines},
            "moto_refs": {o: moto_ref(svc, o) for o in sorted(ops) if moto_ref(svc, o)},
            "confidence": confidence,
        }
        tests.append(rec)
    return {"service": svc, "crate": crate, "tests": tests,
            "counts": dict(Counter(t["bucket"] for t in tests)),
            "workstreams": dict(Counter(t["workstream"] for t in tests))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--service", help="single service name")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--out", default=str(RUST_REPO / ".parity/gap_report.json"))
    args = ap.parse_args()

    if args.service:
        svcs = [args.service]
    else:
        svcs = sorted({p.name.split("_py.xml")[0] for p in XML_DIR.glob("*_py.xml")})

    report = {"services": []}
    for svc in svcs:
        if not (XML_DIR / f"{svc}_py.xml").exists():
            continue
        r = process_service(svc)
        report["services"].append(r)
        print(f"{svc:15} gap={len(r['tests']):4}  {r['workstreams']}")

    Path(args.out).write_text(json.dumps(report, indent=2))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
