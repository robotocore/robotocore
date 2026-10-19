#!/usr/bin/env python3
"""Gate and status for the moto-parity port goal (docs/moto-parity-goal.md).

Exit code 0  -> G1+G2 satisfied (every old-served service native or
                ledger-CLOSED). Run with --gates to also execute G3.
Exit code 1  -> port work remains (prints the queue).
Exit code 2  -> could not reach the live twin (start it: make start).
"""
import json
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ANCHOR_SHA = "1e9a1c37"
LEDGER = REPO / "docs" / "moto-parity-progress.md"
TWIN = "http://localhost:4566/_robotocore/services"
UNCERTAIN = {
    "applicationautoscaling",
    "dynamodbstreams",
    "kinesis-video-archived-media",
    "sagemaker-runtime",
    "scheduler",
    "servicecatalogappregistry",
    "servicediscovery",
    "neptune",
}


def old_registry():
    """name -> status at the pre-rotocore anchor SHA."""
    out = subprocess.run(
        ["git", "-C", str(REPO), "show", f"{ANCHOR_SHA}:src/core/service_registry.rs"],
        capture_output=True, text=True,
    )
    if out.returncode != 0:
        print(f"FATAL: cannot read anchor registry at {ANCHOR_SHA}: {out.stderr.strip()}")
        sys.exit(2)
    return dict(re.findall(r'\(\s*"([a-z0-9-]+)",\s*"(?:native|moto)"', out.stdout))


def twin_inventory():
    try:
        with urllib.request.urlopen(f"{TWIN}", timeout=10) as r:
            payload = json.load(r)
    except Exception as e:  # noqa: BLE001
        print(f"FATAL: cannot reach the twin at {TWIN}: {e}")
        print("start it first: make start")
        sys.exit(2)
    return {s["name"]: s["status"] for s in payload["services"]}


def ledger_sets():
    closed, parked = set(), set()
    if LEDGER.exists():
        for line in LEDGER.read_text().splitlines():
            for kind, bucket in (("CLOSED", closed), ("PARKED", parked)):
                m = re.match(rf"{kind}\s+([a-z0-9-]+)", line.strip(), re.IGNORECASE)
                if m:
                    bucket.add(m.group(1))
    return closed, parked


def probe_rank(name):
    """(working, not_implemented) probe counts, for queue ordering hints."""
    path = REPO / "probes" / f"{name}.json"
    if not path.exists():
        return None
    try:
        counts = json.loads(path.read_text()).get("counts", {})
        return counts.get("working", 0), counts.get("not_implemented", 0)
    except Exception:  # noqa: BLE001
        return None


def main() -> int:
    gates_only = "--gates" in sys.argv[1:]
    old = old_registry()
    live = twin_inventory()
    closed, parked = ledger_sets()

    served_old = {n for n, st in old.items() if st in ("native", "moto")}
    remaining = sorted(
        n for n in served_old
        if live.get(n) != "native" and n not in closed and n not in parked
    )
    closed_list = sorted(closed & served_old)
    parked_list = sorted(parked & served_old)

    print(f"old served surface: {len(served_old)} services "
          f"({ANCHOR_SHA})")
    print(f"native now: {sum(1 for n in served_old if live.get(n) == 'native')}")

    if remaining:
        print(f"REMAINING ({len(remaining)}):")
        for n in remaining:
            pr = probe_rank(n)
            hint = f"  probe working={pr[0]:3d} not_implemented={pr[1]:3d}" if pr else "  no probe file"
            tag = " *UNCERTAIN*" if n in UNCERTAIN else ""
            print(f"  - {n}{tag}{hint}")
    else:
        print("REMAINING: none")
    for label, bucket in (("CLOSED", closed_list), ("PARKED", parked_list)):
        if bucket:
            print(f"{label} ({len(bucket)}): " + ", ".join(bucket))

    uncertain_open = [n for n in UNCERTAIN if live.get(n) != "native"
                      and n not in closed and n not in parked]
    if uncertain_open:
        print(f"UNCERTAIN unresolved (G2): {', '.join(sorted(uncertain_open))}")

    done = not remaining and not uncertain_open
    print("GATE:", "PASS" if done else "OPEN")
    if not done or gates_only:
        print("\nStill to run before declaring done:")
        print("  cargo fmt --all --check")
        print("  cargo clippy --workspace --all-targets -- -D warnings")
        print("  cargo test --workspace --lib")
        print("  make parity")
        print("  uv run pytest tests/compatibility -q   (pass count >= ledger start row)")

    if gates_only:
        import shlex
        for cmd in (
            ["cargo", "fmt", "--all", "--check"],
            ["cargo", "clippy", "--workspace", "--all-targets", "--", "-D", "warnings"],
            ["cargo", "test", "--workspace", "--lib"],
            ["make", "parity"],
        ):
            print(f"\n$ {shlex.join(cmd)}")
            rc = subprocess.run(cmd, cwd=REPO).returncode
            print(f"  -> exit {rc}")
            if rc != 0:
                print(f"GATE FAILED: {shlex.join(cmd)}")
                return 1

    return 0 if done else 1


if __name__ == "__main__":
    sys.exit(main())
