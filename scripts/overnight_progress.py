#!/usr/bin/env python3
"""Progress tracking and reporting for overnight v2.

Reads/writes logs/overnight/progress.json. Tracks per-service status,
coverage deltas, timing, and graduation.

Usage:
    uv run python scripts/overnight_progress.py --init
    uv run python scripts/overnight_progress.py --start-service sqs
    uv run python scripts/overnight_progress.py --complete-service sqs \
        --after-covered 18 --graduated
    uv run python scripts/overnight_progress.py --fail-service sqs --reason "3 consecutive failures"
    uv run python scripts/overnight_progress.py --report
    uv run python scripts/overnight_progress.py --report --json
    uv run python scripts/overnight_progress.py --graduated
"""

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

PROGRESS_DIR = Path("logs/overnight")
PROGRESS_FILE = PROGRESS_DIR / "progress.json"


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def load_progress() -> dict:
    """Load progress file, or return empty structure."""
    try:
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_progress(data: dict):
    PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def get_baseline_coverage() -> dict:
    """Snapshot current coverage from compat_coverage.py."""
    result = subprocess.run(
        ["uv", "run", "python", "scripts/compat_coverage.py", "--all", "--json"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return {}
    services = json.loads(result.stdout)
    total_ops = sum(s["total_ops"] for s in services)
    total_covered = sum(s["covered"] for s in services)
    return {
        "total_ops": total_ops,
        "total_covered": total_covered,
        "total_pct": round(total_covered / total_ops * 100, 1) if total_ops else 0,
        "per_service": {
            s["service"]: {"covered": s["covered"], "total": s["total_ops"]} for s in services
        },
    }


def cmd_init():
    """Initialize progress tracking with baseline snapshot."""
    baseline = get_baseline_coverage()
    data = {
        "version": 2,
        "started_at": now_iso(),
        "baseline": baseline,
        "services": [],
        "summary": {
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "graduated": 0,
            "tests_added": 0,
        },
    }
    save_progress(data)
    print("Initialized progress tracking")
    bc = baseline
    print(f"Baseline: {bc['total_covered']}/{bc['total_ops']} ({bc['total_pct']}%)")


def cmd_start_service(service: str):
    """Mark a service as in-progress."""
    data = load_progress()
    if not data:
        print("No progress file. Run --init first.", file=sys.stderr)
        sys.exit(1)

    # Check if already exists
    for s in data["services"]:
        if s["service"] == service:
            s["status"] = "in_progress"
            s["started_at"] = now_iso()
            save_progress(data)
            return

    data["services"].append(
        {
            "service": service,
            "status": "in_progress",
            "started_at": now_iso(),
            "before_covered": data["baseline"]["per_service"].get(service, {}).get("covered", 0),
        }
    )
    save_progress(data)


def cmd_complete_service(service: str, after_covered: int, graduated: bool):
    """Mark a service as completed."""
    data = load_progress()
    if not data:
        print("No progress file.", file=sys.stderr)
        sys.exit(1)

    for s in data["services"]:
        if s["service"] == service:
            before = s.get("before_covered", 0)
            s["status"] = "completed"
            s["completed_at"] = now_iso()
            s["after_covered"] = after_covered
            s["delta"] = after_covered - before
            s["graduated"] = graduated
            data["summary"]["completed"] += 1
            data["summary"]["tests_added"] += s["delta"]
            if graduated:
                data["summary"]["graduated"] += 1
            save_progress(data)
            print(
                f"{service}: {before} -> {after_covered} (+{s['delta']})"
                + (" GRADUATED!" if graduated else "")
            )
            return

    print(f"Service {service} not found in progress", file=sys.stderr)
    sys.exit(1)


def cmd_fail_service(service: str, reason: str):
    """Mark a service as failed."""
    data = load_progress()
    if not data:
        print("No progress file.", file=sys.stderr)
        sys.exit(1)

    for s in data["services"]:
        if s["service"] == service:
            s["status"] = "failed"
            s["failed_at"] = now_iso()
            s["reason"] = reason
            data["summary"]["failed"] += 1
            save_progress(data)
            return

    print(f"Service {service} not found in progress", file=sys.stderr)
    sys.exit(1)


def cmd_report(as_json: bool):
    """Print progress report."""
    data = load_progress()
    if not data:
        print("No progress file. Run --init first.", file=sys.stderr)
        sys.exit(1)

    if as_json:
        print(json.dumps(data, indent=2))
        return

    baseline = data.get("baseline", {})
    summary = data.get("summary", {})
    services = data.get("services", [])

    print("Overnight v2 Progress Report")
    print("=" * 60)
    print(f"Started:  {data.get('started_at', 'unknown')}")
    print(
        f"Baseline: {baseline.get('total_covered', '?')}/{baseline.get('total_ops', '?')} "
        f"({baseline.get('total_pct', '?')}%)"
    )
    print()
    print(f"Services completed: {summary.get('completed', 0)}")
    print(f"Services failed:    {summary.get('failed', 0)}")
    print(f"Tests added:        {summary.get('tests_added', 0)}")
    print(f"Graduated to 100%:  {summary.get('graduated', 0)}")

    completed = [s for s in services if s.get("status") == "completed"]
    if completed:
        print()
        print("Completed services:")
        print("-" * 60)
        for s in completed:
            grad = " \u2605 GRADUATED" if s.get("graduated") else ""
            delta = s.get("delta", 0)
            print(f"  {s['service']:<28s} +{delta:<4d} -> {s.get('after_covered', '?')}{grad}")

    failed = [s for s in services if s.get("status") == "failed"]
    if failed:
        print()
        print("Failed services:")
        for s in failed:
            print(f"  {s['service']:<28s} {s.get('reason', 'unknown')}")

    in_progress = [s for s in services if s.get("status") == "in_progress"]
    if in_progress:
        print()
        print("In progress:")
        for s in in_progress:
            print(f"  {s['service']}")


def cmd_graduated():
    """List services that graduated to 100%."""
    data = load_progress()
    for s in data.get("services", []):
        if s.get("graduated"):
            print(s["service"])


def main():
    parser = argparse.ArgumentParser(description="Overnight v2 progress tracking")
    parser.add_argument("--init", action="store_true", help="Initialize with baseline snapshot")
    parser.add_argument("--start-service", metavar="NAME", help="Mark service as in-progress")
    parser.add_argument("--complete-service", metavar="NAME", help="Mark service as completed")
    parser.add_argument("--fail-service", metavar="NAME", help="Mark service as failed")
    parser.add_argument("--after-covered", type=int, help="Coverage count after completion")
    parser.add_argument("--graduated", action="store_true", help="Service reached 100%")
    parser.add_argument("--reason", default="", help="Failure reason")
    parser.add_argument("--report", action="store_true", help="Print progress report")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    if args.init:
        cmd_init()
    elif args.start_service:
        cmd_start_service(args.start_service)
    elif args.complete_service:
        if args.after_covered is None:
            print("--after-covered required with --complete-service", file=sys.stderr)
            sys.exit(1)
        cmd_complete_service(args.complete_service, args.after_covered, args.graduated)
    elif args.fail_service:
        cmd_fail_service(args.fail_service, args.reason)
    elif args.report:
        cmd_report(args.json)
    elif args.graduated and not args.complete_service:
        cmd_graduated()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
