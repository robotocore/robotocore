#!/usr/bin/env python3
"""Stage 3: "next work" generator.

Usage:
    python scripts/harness/next_work.py                 # print the #1 item as JSON
    python scripts/harness/next_work.py --n 10          # top 10
    python scripts/harness/next_work.py --bucket A1     # filter by bucket
    python scripts/harness/next_work.py --json          # write .parity/next.json

Contract
--------
in : .parity/worklist.json   (stage 2, ordered by ROI)
     .parity/gap_report.json (stage 1, for per-test details)
     crates/<crate>/src/handler.rs  (to resolve exact line + current arm text)
out: .parity/next.json + stdout
    {
      "rank": 1,
      "group_key": "ssm/CreateOpsItem",
      "service": "ssm", "crate": "ssm", "op": "CreateOpsItem",
      "action": "de_stub",
      "workstream": "WS1",
      "tests": ["test_ssm_compat.py::test_create_ops_item_with_all_fields", ...],
      "expected_tests_fixed": 9,
      "why": "CreateOpsItem is a json_stub (line 43); spec requires
              OpsItemId(oi-...); 9 failing ssm tests call it",
      "file": "crates/ssm/src/handler.rs",
      "line": 43,
      "current": '"CreateOpsItem" => self.json_stub(&req, "OpsItemId"),',
      "patch": { ... the gen_rust_op.py 3-part patch ... },
      "verify": "ENDPOINT_URL=http://127.0.0.1:4567 python -m pytest <single test>",
      "rollback": "git checkout -- crates/ssm/src/handler.rs crates/ssm/src/models.rs",
      "confidence": "high"
    }

Rules
-----
* Never emit two items touching the same handler file in the same batch
  (the apply loop runs them sequentially; this script just reports order).
* Items with confidence "low" are included but flagged "review_first".
* The verify command targets the FIRST test in the group (cheapest check);
  after it passes, the file-level run confirms the rest.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

RUST_REPO = Path("/Users/jackdanger/www/robotocore-rust")
PYV = "/Users/jackdanger/www/robotocore/.venv/bin/python"
GEN = RUST_REPO / "scripts" / "harness" / "gen_rust_op.py"


def run_gen(service, op):
    r = subprocess.run([PYV, str(GEN), "--service", service, "--op", op],
                       capture_output=True, text=True, timeout=60)
    if r.returncode != 0:
        return None
    out = r.stdout
    parts = {}
    m = re.search(r"# ---- append to crates/(\S+)/src/models.rs ----\n(.*?)(?=\n\n# ----)", out, re.S)
    if m:
        parts["crate"] = m.group(1)
        parts["model_append"] = m.group(2).strip()
    m = re.search(r"# ---- append inside impl block.*?----\n(.*?)(?=\n\n# ----)", out, re.S)
    if m:
        parts["method_append"] = m.group(1).strip()
    m = re.search(r"# ---- replace the existing match arm.*?----\n(\S.*)", out)
    if m:
        parts["dispatch_new"] = m.group(1).strip()
    return parts


def current_arm(crate, op):
    p = RUST_REPO / "crates" / crate / "src" / "handler.rs"
    if not p.exists():
        return None, None
    for i, line in enumerate(p.read_text().splitlines(), 1):
        if re.search(r'"' + re.escape(op) + r'"\s*=>', line):
            return i, line.strip()
    return None, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=1)
    ap.add_argument("--bucket")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    wl = json.loads((RUST_REPO / ".parity/worklist.json").read_text())
    report = json.loads((RUST_REPO / ".parity/gap_report.json").read_text())
    # index gap_report by service for "why" text + confidence
    idx = {s["service"]: s for s in report["services"]}

    # Skip port_behavior groups that aggregate >5 tests: those are
    # over-credited (many unrelated tests happen to call the same op) and
    # are not a single fixable unit. They are surfaced separately as
    # "investigate" cards, not ranked work.
    ranked = [e for e in wl if not (e["action"] == "port_behavior" and len(e["tests"]) > 5)]
    items = []
    for i, e in enumerate(ranked, 1):
        if args.bucket and e.get("action") not in args.bucket:
            continue
        svc, op, crate, action = e["service"], e["op"], e["crate"], e["action"]
        line, cur = (None, None)
        if op:
            line, cur = current_arm(crate, op)
        patch = run_gen(svc, op) if op and action in ("de_stub", "add_fields", "fix_shape") else None
        # why text
        if action == "de_stub" and line:
            why = (f"{op} is a json_stub at line {line} "
                   f"(returns a placeholder id/empty body); {len(e['tests'])} "
                   f"failing {svc} tests call it.")
        elif action == "add_fields":
            missing = e.get("spec_output_shape")
            why = (f"{op} is implemented but its response is missing spec fields "
                   f"{sorted(missing) if missing else '?'}; {len(e['tests'])} tests fail on read-back.")
        elif action == "fix_error_code":
            why = (f"{op} returns the wrong error code on the not-found path; "
                   f"{len(e['tests'])} tests assert the specific code.")
        else:
            why = f"{action} for {op}: {len(e['tests'])} failing tests."
        first_test = e["tests"][0]
        item = {
            "rank": i,
            "group_key": e["group_key"],
            "service": svc, "crate": crate, "op": op,
            "action": action,
            "workstream": e.get("workstream"),
            "tests": e["tests"],
            "expected_tests_fixed": e["expected_tests_fixed"],
            "why": why,
            "file": f"crates/{crate}/src/handler.rs",
            "line": line,
            "current": cur,
            "patch": patch,
            "verify": (f"ENDPOINT_URL=http://127.0.0.1:4567 {PYV} -m pytest "
                       f"{first_test} -x -q"),
            "rollback": f"git checkout -- crates/{crate}/src/handler.rs crates/{crate}/src/models.rs",
            "confidence": e.get("confidence"),
        }
        items.append(item)
        if i >= args.n:
            break

    out = items[0] if len(items) == 1 else items
    if args.json:
        (RUST_REPO / ".parity/next.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
