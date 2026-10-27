#!/usr/bin/env python3
"""Probe-corpus staleness gate.

Mechanism (process lesson from the 2026-10-24 review wave): a service's
compat/probe corpus may not lag its implementation. For every probe file we
compare `generated_at` against the newest commit date among the files that
implement that service under rotocore/src/services/ (and, when a probe row is
newer, it counts as fresh). A probe that predates its implementation's last
arm/fix merge is a stale claim about what the twin serves.

Exit 1 with a report when at least one service is stale, so `make parity`
fails loudly. `--refresh-commands` prints the exact per-service re-probe
invocations to run next.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROBES = ROOT / "probes"


def git_time(path: str) -> datetime | None:
    """Last commit date (committer, UTC) that touched path, or None."""
    out = subprocess.run(
        ["git", "log", "-1", "--format=%cI", "--", path],
        cwd=ROOT,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not out:
        return None
    return datetime.fromisoformat(out)


def parse_generated_at(path: Path) -> datetime | None:
    try:
        data = json.loads(path.read_text())
    except Exception:
        return None
    raw = data.get("generated_at")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def probe_name_from_file(path: Path) -> str:
    return path.stem


# Probe/service names whose implementation lives under a different stem
# or outside rotocore/src/services (caught by the wave-4 skip-audit — a
# gate that permanently skips corpora can never see their staleness).
IMPL_PATH_OVERRIDES: dict[str, list[str]] = {
    "cognitoidentity": ["rotocore/src/services/cognito_identity.rs"],
    "iotdata": ["rotocore/src/services/iot_data.rs"],
    "apigatewaymanagementapi": ["rotocore/src/services/apigw_management.rs"],
    "service-quotas": ["rotocore/src/services/servicequotas/"],
    "es": ["rotocore/src/services/opensearch.rs", "rotocore/src/services/opensearch/"],
    "neptune": ["rotocore/src/services/rds.rs"],
    "logs": ["crates/cloudwatch-logs/src/", "rotocore/src/services/cloudwatch_logs.rs"],
    "sts": ["src/core/services/sts.rs"],
    # bedrock-agentcore-control shares bedrockagentcore's module dir: both
    # botocore models deliver on the `bedrock-agentcore` signing scope and the
    # union backend lives in one directory (issue #371).
    "bedrock-agentcore-control": ["rotocore/src/services/bedrockagentcore/"],
}


def service_dirs_for(name: str) -> list[str]:
    """Candidate implementation paths for a probe/service name.

    Probe names use AWS hyphenation (service-quotas, cognito-idp) while the
    engine uses snake case mod files (servicequotas/mod.rs, cognito_idp.rs,
    directconnect/mod.rs, lambda/ dir...). Try both spellings per location,
    plus the explicit override table for corpus names whose implementation
    lives under a different file stem.
    """
    overrides = IMPL_PATH_OVERRIDES.get(name)
    if overrides:
        return list(overrides)
    variants = {name, name.replace("-", "_"), name.replace("_", "-")}
    # deslashed variant too (service-quotas dir is servicequotas/)
    variants.add(name.replace("-", "").replace("_", ""))
    patterns: list[str] = []
    for v in sorted(variants):
        patterns.append(f"rotocore/src/services/{v}.rs")
        patterns.append(f"rotocore/src/services/{v}/")
        patterns.append(f"crates/{v}/src/")
    return patterns


def latest_impl_time(name: str) -> tuple[datetime | None, str | None]:
    best: datetime | None = None
    best_path: str | None = None
    for pattern in service_dirs_for(name):
        out = subprocess.run(
            ["git", "log", "-1", "--format=%cI", "--", pattern],
            cwd=ROOT,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if not out:
            continue
        t = datetime.fromisoformat(out)
        if best is None or t > best:
            best, best_path = t, pattern
    return best, best_path


def main() -> int:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--refresh-commands", action="store_true", help="also print the re-probe invocations"
    )
    args = arg_parser.parse_args()

    stale: list[tuple[str, datetime, datetime]] = []
    fresh_count = 0
    skipped: list[str] = []

    for probe_file in sorted(PROBES.glob("*.json")):
        name = probe_name_from_file(probe_file)
        probe_time = parse_generated_at(probe_file)
        if probe_time is None:
            # Wave-4 finding 2: a truncated/corrupt artifact must NOT
            # silently pass the gate — treat it as stale.
            stale.append((name, probe_time, None))
            continue
        impl_time, _ = latest_impl_time(name)
        if impl_time is None:
            skipped.append(name)
            continue
        if impl_time > probe_time:
            stale.append((name, probe_time, impl_time))
        else:
            fresh_count += 1

    for name, probe_time, impl_time in stale:
        print(
            f"STALE {name}: probes generated {probe_time:%Y-%m-%d %H:%M}, "
            f"implementation merged {impl_time:%Y-%m-%d %H:%M} (arms newer)"
        )
    if skipped:
        print(f"skipped (no timestamp or no impl mapping): {len(skipped)}")
    print(f"fresh: {fresh_count}  stale: {len(stale)}")

    if args.refresh_commands and stale:
        for name, _t, _i in stale:
            # --all is mandatory: without it probe_service writes a
            # List/Describe/Get-only artifact whose fresh generated_at
            # silently shrank the corpus (wave-4 finding 1). The writer
            # is not atomic (finding 2): refresh runs should still be
            # followed by `make error-census` and a `git diff` review.
            print(
                f"  ENDPOINT_URL=http://localhost:4566 "
                f"uv run python -m scripts.probe_service --service {name} --all"
            )

    return 1 if stale else 0


if __name__ == "__main__":
    sys.exit(main())
