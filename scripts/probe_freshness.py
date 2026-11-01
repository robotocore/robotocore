#!/usr/bin/env python3
"""Probe-corpus staleness gate.

Mechanism (process lesson from the 2026-10-24 review wave): a service's
compat/probe corpus may not lag its implementation. Waves 4-6 proved the
first mechanic a bad fit: commit-date comparisons re-stale every touched
arm after each squash merge, so corpora got re-bumped on every merge
without a probe actually running, and the GH-Actions merge-ref checkout
needed its own collapse detector (`history_is_attributable`) to survive
graph shapes where path attribution lies.

The gate is content-based now:

  - each `probes/<svc>.json` carries an `impl_fingerprint` map
    (impl-path pattern -> sha256 over the resolved file/dir contents)
    from the tree the corpus was genuinely generated against;
  - the gate recomputes that fingerprint from the checkout's working
    tree. A match means "this corpus documents exactly this
    implementation"; a mismatch — or a missing fingerprint — is STALE.

No git history is consulted, so squash merges, rebases and synthetic
merge refs cannot churn the verdict; only real implementation-content
change does.

`--backfill` stamps today's fingerprint into every probe file. That is
bookkeeping, not regeneration: for arms whose implementation actually
changed, run the per-service re-probe commands printed by
`--refresh-commands` first (and follow regens with `make error-census`
and a `git diff` review — wave-4 findings 1 and 2).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROBES = ROOT / "probes"


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


def existing_impl_paths(name: str) -> list[Path]:
    paths = []
    for pattern in service_dirs_for(name):
        path = ROOT / pattern
        if path.is_file() or path.is_dir():
            paths.append(path)
    return paths


def _hash_files(files: list[tuple[str, bytes]]) -> str:
    h = hashlib.sha256()
    for rel, data in files:
        h.update(rel.encode())
        h.update(b"\0")
        h.update(data)
    return h.hexdigest()


def impl_fingerprint(name: str) -> dict[str, str]:
    """sha256 per implementation path, over this checkout's working tree.

    File patterns hash the file bytes; directory patterns hash every file
    under them sorted by path, as (path, NUL, bytes) tuples. A pattern that
    resolves to nothing hashes the constant b"absent" so a directory
    appearing or disappearing flips the fingerprint.
    """
    out: dict[str, str] = {}
    for pattern in service_dirs_for(name):
        path = ROOT / pattern
        if path.is_file():
            out[pattern] = _hash_files([(pattern, path.read_bytes())])
        elif path.is_dir():
            files = [(str(p.relative_to(ROOT)), p.read_bytes())
                     for p in sorted(path.rglob("*")) if p.is_file()]
            out[pattern] = _hash_files(files) if files else "absent"
        else:
            out[pattern] = "absent"
    return out


def fingerprint_matches(name: str, probe: dict) -> bool:
    """False on ANY doubt: missing/corrupt fingerprint or a read failure
    is stale (fail loud); only an exact content match passes."""
    stamp = probe.get("impl_fingerprint")
    if not isinstance(stamp, dict):
        return False
    return impl_fingerprint(name) == stamp


def parse_generated_at(path: Path) -> datetime | None:
    """Legacy field, kept for report context only — not a gate input."""
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


def main() -> int:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--refresh-commands", action="store_true", help="also print the re-probe invocations"
    )
    arg_parser.add_argument(
        "--backfill",
        action="store_true",
        help="stamp today's impl_fingerprint into every probe file (bookkeeping, NOT regeneration)",
    )
    args = arg_parser.parse_args()

    if args.backfill:
        stamped = 0
        for probe_file in sorted(PROBES.glob("*.json")):
            name = probe_file.stem
            try:
                data = json.loads(probe_file.read_text())
            except Exception as exc:  # noqa: BLE001
                print(f"could not parse {probe_file.name}: {exc}", file=sys.stderr)
                continue
            data["impl_fingerprint"] = impl_fingerprint(name)
            probe_file.write_text(json.dumps(data, indent=2) + "\n")
            stamped += 1
        print(f"backfilled impl_fingerprint into {stamped} probe files")
        return 0

    stale: list[str] = []
    fresh_count = 0
    skipped: list[str] = []

    for probe_file in sorted(PROBES.glob("*.json")):
        name = probe_name_from_file(probe_file)
        try:
            probe = json.loads(probe_file.read_text())
        except Exception:
            # Wave-4 finding 2: a truncated/corrupt artifact must NOT
            # silently pass the gate — treat it as stale.
            stale.append(name)
            continue
        if not existing_impl_paths(name):
            skipped.append(name)
            continue
        if fingerprint_matches(name, probe):
            fresh_count += 1
        else:
            stale.append(name)

    for name in stale:
        print(
            f"STALE {name}: implementation content differs from the probe's "
            f"impl_fingerprint (the corpus documents a different tree)"
        )
    if skipped:
        print(f"skipped (no impl mapping): {len(skipped)}")
    print(f"fresh: {fresh_count}  stale: {len(stale)}")

    if args.refresh_commands and stale:
        for name in stale:
            # --all is mandatory: without it probe_service writes a
            # List/Describe/Get-only artifact whose fresh fingerprint
            # silently shrank the corpus (wave-4 finding 1). The writer
            # is not atomic: refresh runs should still be followed by
            # `make error-census` and a `git diff` review.
            print(
                f"  ENDPOINT_URL=http://localhost:4566 "
                f"uv run python -m scripts.probe_service --service {name} --all"
            )

    return 1 if stale else 0


if __name__ == "__main__":
    sys.exit(main())
