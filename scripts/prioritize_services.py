#!/usr/bin/env python3
"""Tiered service prioritization for overnight test expansion.

Replaces the simple gap-sort in next_service.py with a multi-tier strategy:
  Tier 1 (Graduation): 90-99% coverage — fewest ops to 100%
  Tier 2 (Medium):     40-89%, ≤200 ops — best working-untested ratio
  Tier 3 (Large):      <40%, ≤300 ops — same sort
  Tier 4 (Skip):       >300 ops — too big for overnight

Usage:
    uv run python scripts/prioritize_services.py --all
    uv run python scripts/prioritize_services.py --json
    uv run python scripts/prioritize_services.py --resume logs/overnight/progress.json
"""

import argparse
import json
import subprocess
import sys


def get_coverage_data() -> list[dict]:
    """Get coverage data from compat_coverage.py --json."""
    result = subprocess.run(
        ["uv", "run", "python", "scripts/compat_coverage.py", "--all", "--json"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error running compat_coverage.py: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout)


def load_progress(path: str) -> set[str]:
    """Load completed services from progress.json."""
    try:
        with open(path) as f:
            data = json.load(f)
        return {s["service"] for s in data.get("services", []) if s.get("status") == "completed"}
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def classify_services(
    coverage: list[dict],
    completed: set[str],
    max_total: int = 300,
) -> dict[str, list[dict]]:
    """Classify services into tiers."""
    tiers = {"graduation": [], "medium": [], "large": [], "skip": []}

    for svc in coverage:
        name = svc["service"]
        total = svc["total_ops"]
        covered = svc["covered"]
        pct = svc["coverage_pct"]

        if name in completed:
            continue
        if total == 0:
            continue
        if pct >= 100.0:
            continue

        entry = {
            "service": name,
            "total_ops": total,
            "covered": covered,
            "coverage_pct": pct,
            "gap": total - covered,
            "tier": "",
        }

        if pct >= 90.0:
            entry["tier"] = "graduation"
            tiers["graduation"].append(entry)
        elif total > max_total:
            entry["tier"] = "skip"
            tiers["skip"].append(entry)
        elif pct >= 40.0:
            entry["tier"] = "medium"
            tiers["medium"].append(entry)
        else:
            entry["tier"] = "large"
            tiers["large"].append(entry)

    # Sort each tier
    # Graduation: fewest ops needed to reach 100%
    tiers["graduation"].sort(key=lambda s: s["gap"])
    # Medium/Large: highest coverage % first (closest to done)
    tiers["medium"].sort(key=lambda s: -s["coverage_pct"])
    tiers["large"].sort(key=lambda s: -s["coverage_pct"])
    tiers["skip"].sort(key=lambda s: -s["coverage_pct"])

    return tiers


def flatten_tiers(tiers: dict[str, list[dict]]) -> list[dict]:
    """Flatten tiers into priority-ordered list."""
    return tiers["graduation"] + tiers["medium"] + tiers["large"]


def print_human(tiers: dict[str, list[dict]], show_skip: bool = False):
    """Print human-readable output."""
    tier_labels = {
        "graduation": "Tier 1 — Graduation (90-99%, fewest ops to 100%)",
        "medium": "Tier 2 — Medium (40-89%, ≤200 ops)",
        "large": "Tier 3 — Large (<40%, ≤300 ops)",
        "skip": "Tier 4 — Skip (>300 ops, separate runs)",
    }

    total_gap = 0
    for tier_name in ["graduation", "medium", "large"]:
        services = tiers[tier_name]
        if not services:
            continue
        print(f"\n{tier_labels[tier_name]}")
        print("-" * 70)
        for s in services:
            bar_len = int(s["coverage_pct"] / 5)
            bar = "\u2588" * bar_len + "\u2591" * (20 - bar_len)
            print(
                f"  {s['service']:<30s} {s['covered']:>4d}/{s['total_ops']:<4d} "
                f"{s['coverage_pct']:5.1f}%  {bar}  gap={s['gap']}"
            )
            total_gap += s["gap"]

    if show_skip and tiers["skip"]:
        print(f"\n{tier_labels['skip']}")
        print("-" * 70)
        for s in tiers["skip"]:
            print(
                f"  {s['service']:<30s} {s['covered']:>4d}/{s['total_ops']:<4d} "
                f"{s['coverage_pct']:5.1f}%  gap={s['gap']}"
            )

    actionable = sum(len(tiers[t]) for t in ["graduation", "medium", "large"])
    print(f"\n{actionable} actionable services, {total_gap} total ops to cover")
    if tiers["skip"]:
        print(f"{len(tiers['skip'])} services skipped (>300 ops)")


def main():
    parser = argparse.ArgumentParser(description="Tiered service prioritization")
    parser.add_argument("--all", action="store_true", help="Show all tiers including skip")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--resume", metavar="PATH", help="Skip services completed in progress.json")
    parser.add_argument("--max-total", type=int, default=300, help="Max ops for non-skip tier")
    args = parser.parse_args()

    coverage = get_coverage_data()
    completed = load_progress(args.resume) if args.resume else set()
    tiers = classify_services(coverage, completed, max_total=args.max_total)

    if args.json:
        output = {
            "tiers": tiers,
            "ordered": flatten_tiers(tiers),
            "completed_count": len(completed),
        }
        print(json.dumps(output, indent=2))
    else:
        if completed:
            print(f"({len(completed)} services already completed, skipped)")
        print_human(tiers, show_skip=args.all)


if __name__ == "__main__":
    main()
