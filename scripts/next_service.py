#!/usr/bin/env python3
"""Pick the next service to improve compat test coverage for.

Prioritizes by: most untested operations that are likely working (needs_params + working).
Skips services already at >95% coverage.

Usage:
    uv run python scripts/next_service.py              # print next service name
    uv run python scripts/next_service.py --all        # print full ranked queue
    uv run python scripts/next_service.py --skip ec2   # skip specific services
"""

import argparse
import re
import subprocess
import sys


def get_coverage():
    """Get per-service coverage from compat_coverage.py."""
    result = subprocess.run(
        ["uv", "run", "python", "scripts/compat_coverage.py"], capture_output=True, text=True
    )
    services = {}
    for line in result.stdout.splitlines():
        # Parse lines like: "ec2                      296     756     39.2%  ███..."
        m = re.match(r"\s*(\S+)\s+(\d+)\s+(\d+)\s+([\d.]+)%", line)
        if m and m.group(1) != "TOTAL":
            svc = m.group(1)
            tested, total, pct = int(m.group(2)), int(m.group(3)), float(m.group(4))
            services[svc] = {"tested": tested, "total": total, "pct": pct, "gap": total - tested}
    return services


def main():
    parser = argparse.ArgumentParser(description="Pick next service for coverage expansion")
    parser.add_argument("--all", action="store_true", help="Show full ranked queue")
    parser.add_argument("--skip", nargs="*", default=[], help="Services to skip")
    parser.add_argument("--min-gap", type=int, default=2, help="Min gap to include")
    args = parser.parse_args()

    coverage = get_coverage()

    # Sort by: gap size (descending), but skip services at >95%
    ranked = []
    for svc, info in coverage.items():
        if svc in args.skip:
            continue
        if info["pct"] >= 95.0:
            continue
        if info["gap"] < args.min_gap:
            continue
        ranked.append((svc, info))

    # Sort: biggest gap first, but deprioritize EC2 (too massive for a single session)
    ranked.sort(key=lambda x: (-x[1]["gap"] if x[0] != "ec2" else -1, x[0]))

    if not ranked:
        print("ALL_DONE", file=sys.stderr)
        sys.exit(1)

    if args.all:
        for svc, info in ranked:
            t, tot, p, g = info["tested"], info["total"], info["pct"], info["gap"]
            print(f"{svc:25s} {t:4d}/{tot:4d} ({p:5.1f}%)  gap={g}")
    else:
        print(ranked[0][0])


if __name__ == "__main__":
    main()
