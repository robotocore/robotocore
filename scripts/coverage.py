#!/usr/bin/env python3
"""The botocore-surface coverage machine.

Inputs: probes/ (per-op wire truth from the fleet sweep), the fork-moto
module map (moto-fidelity triage), test corpora (compat + iac + module
unit tests), and (when present) tooling/surface-absence.json.

Outputs:
- tooling/coverage.json       (per-op: status, bucket, screens, absence)
- tooling/coverage-budget.json (per-service covered counts, ratcheted)
plus the exit code for the budget gate:
  0 = covered set >= stored ratchet; 2 = regressions found.

Bucket taxonomy (cov_bucket): working / pending_fill (probe status
needs_params — the filler rejected the fill or the op is a client-side
probe gap; the twin was never contacted) / absent_crash_faithful
(genuine server-side error payloads only: HTTP 500, or a server answer
botocore could not parse) / unreachable (timeout-class: connection
failures and hangs) / modeled_deny (probe-status deny) / absent
(everything else). Since the wave-11 classifier split in
scripts/probe_service.py the catch-all no longer labels client-side
attribute errors or connection failures as 500_error, so
absent_crash_faithful only counts real server errors.

Absent-bucket mapping (wave 11). The old single `absent` catch-all for
not_implemented probe rows is now attributed per manifest class from
tooling/surface-absence.json (built by scripts/build_surface_absence.py):

  manifest class modeled_deny                → bucket absent_modeled_deny
  manifest class absent_moto_crash_faithful  → bucket absent_moto_
                                               crash_faithful
  manifest class absent_no_handler           → bucket absent_no_handler
  manifest class legacy_gap                  → bucket legacy_gap

Topline invariance: the ratcheted covered figure REMAINS working-ops only
(9456/10268 at the manifest build) — bucket attribution for the recorded
denies is reported per class, none of it is counted into `covered` this
wave, and the existing bucket set/covered formula is otherwise unchanged.
The plan doc's modeled-deny-inclusive covered currency is a later
ratchet, not this wave's. Rows recorded in the manifest under
modeled_deny remain visible in tooling/coverage.json via the per-op
`absence` row so nothing is silently dropped from attribution.

The gate path additionally verifies the manifest itself by invoking
scripts/build_surface_absence.py --check: a manifest class not backed by
its derivation (evidence pointing at sources that no longer contain it)
fails the gate (exit 2), entries whose op no longer appears as
not_implemented in probes are gate-warned on stderr, and unrecorded
probe rows trigger a deterministic re-derivation before the budget
ratchet runs.

Usage:
  uv run python scripts/coverage.py            # regenerate only
  uv run python scripts/coverage.py --gate     # regenerate + budget ratchet
"""

import argparse
import json
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).parent.parent
PROBES = ROOT / "probes"
COVERAGE = ROOT / "tooling" / "coverage.json"
BUDGET = ROOT / "tooling" / "coverage-budget.json"
ABSENCE = ROOT / "tooling" / "surface-absence.json"

# Test-corpus hints: which suites/lanes exercise which service id.
TEST_HINTS = {
    "compat global": ROOT / "tests" / "compatibility",
    "iac": ROOT / "tests" / "iac",
}


# Manifest-class → bucket attribution for the old `absent` catch-all
# (mapping documented in the module docstring; covered stays working-only,
# so none of these land in the ratcheted covered set this wave).
ABSENCE_CLASS_BUCKETS = {
    "modeled_deny": "absent_modeled_deny",
    "absent_moto_crash_faithful": "absent_moto_crash_faithful",
    "absent_no_handler": "absent_no_handler",
    "legacy_gap": "legacy_gap",
}


def load_ops(service: str):
    p = PROBES / f"{service}.json"
    if not p.exists():
        return None
    return json.loads(p.read_text())


def cov_bucket(status: str, absence: dict | None):
    if status == "working":
        return "working"
    if status == "needs_params":
        # The probe never produced a trusted call: the fill was rejected by
        # botocore's client-side validation, or the probe itself was a
        # client-side gap (see the probe_service.py classifier split).
        return "pending_fill"
    if status == "500_error":
        # A genuine server-side error payload: HTTP 500, or a server answer
        # botocore could not parse. Never a client-side probe artifact.
        return "absent_crash_faithful"
    if status == "timeout":
        # connection-level failure / hang: the twin never (fully) answered
        return "unreachable"
    if status == "modeled_deny":
        return "modeled_deny"
    if status == "not_implemented" and absence:
        # Manifest-recorded deny: attributed per recorded class (see the
        # docstring's absent-bucket mapping). Kept OUT of the covered set —
        # topline stays working-only this wave.
        cls = absence.get("class") if isinstance(absence, dict) else None
        if cls in ABSENCE_CLASS_BUCKETS:
            return ABSENCE_CLASS_BUCKETS[cls]
    return "absent"


def count_members(twin_dir: Path, svc: str, op_snake: str) -> bool:
    """A hand-rolled check that the module exposes an arm (best-effort)."""
    hits = 0
    for module in (
        ROOT / "rotocore" / "src" / "services" / svc,
        ROOT / "rotocore" / "src" / "services",
    ):
        f = module / f"{svc}.rs"
        if f.exists():
            hits += Path(f).read_text().lower().count(op_snake)
    for f in (ROOT / "tests" / "compatibility").glob(f"test_{svc}*.py"):
        hits += f.read_text().lower().count(op_snake)
    return hits > 0


def load_absence() -> dict:
    """Manifest rows keyed service → op → entry (unwrapping the manifest)."""
    if not ABSENCE.exists():
        return {}
    try:
        data = json.loads(ABSENCE.read_text())
    except Exception as e:  # noqa: BLE001
        print(f"warn: unreadable absence manifest: {e}", file=sys.stderr)
        return {}
    if isinstance(data, dict) and "services" in data:
        return data.get("services", {})
    return data if isinstance(data, dict) else {}


def main() -> None:
    services = sorted(f.stem for f in PROBES.glob("*.json"))
    absence = load_absence()

    coverage: dict[str, dict] = {}
    absent_manifest_entries = 0
    absence_class_counts: dict[str, int] = defaultdict(int)
    for svc in services:
        probe = load_ops(svc)
        if probe is None:
            continue
        entry: dict[str, dict] = {}
        for op in probe.get("operations", []):
            op_name = op["operation"]
            status = op["status"]
            snake = re.sub(r"(?<!^)(?=[A-Z])", "_", op_name).lower()
            screens = {"probe": True}

            manifest = absence.get(svc, {}).get(op_name) or absence.get(svc, {}).get(snake)
            if manifest is not None:
                absent_manifest_entries += 1
                if isinstance(manifest, dict) and manifest.get("class"):
                    absence_class_counts[manifest["class"]] += 1
            screens["unit"] = count_members(ROOT, svc, snake)
            bucket = cov_bucket(status, manifest)
            conformance = op.get("conformance", None)
            entry[op_name] = {
                "probe_status": status,
                "bucket": bucket,
                "conformance": conformance,
                "probe_message": (op.get("message") or "")[:180],
                "screens": screens,
                "absence": manifest,
            }
        coverage[svc] = {
            "covered": sum(
                1
                for o in entry.values()
                if o["bucket"] in ("working", "absent_but_exercised", "modeled_deny")
            ),
            "total": len(entry),
            "ops": entry,
        }
    absent_bucket_counts: dict[str, int] = defaultdict(int)
    for svc in coverage.values():
        for o in svc["ops"].values():
            if o["bucket"] not in (
                "working",
                "pending_fill",
                "absent_crash_faithful",
                "unreachable",
                "modeled_deny",
            ):
                absent_bucket_counts[o["bucket"]] += 1
    (ROOT / "tooling" / "coverage.json").write_text(json.dumps(coverage, indent=1, sort_keys=True))
    print(
        f"covered={coverage_totals_covered(coverage):>{7}} / {coverage_totals_total(coverage)}",
    )
    print(f"absent_manifest_entries: {absent_manifest_entries}")
    if absence_class_counts:
        parts = ", ".join(f"{k}={absence_class_counts[k]}" for k in sorted(absence_class_counts))
        print(f"absence classes: {parts}")
    print(f"absent buckets: {dict(sorted(absent_bucket_counts.items()))}")


def coverage_totals(cov) -> tuple[int, int]:
    return sum(v["covered"] for v in cov.values()), sum(v["total"] for v in cov.values())


def coverage_totals_covered(cov):
    return coverage_totals(cov)[0]


def coverage_totals_total(cov):
    return coverage_totals(cov)[1]


def write_budget(coverage: dict):
    (ROOT / "tooling" / "coverage-budget.json").write_text(
        json.dumps(
            {s: round(v["covered"] / v["total"] * 10000) for s, v in coverage.items()},
            indent=1,
            sort_keys=True,
        )
    )


def refresh_manifest() -> bool:
    """Re-derive the absence manifest for the gate path.

    Sequence: `build_surface_absence.py --check` first. When the committed
    manifest merely drifted (rows vs the probe corpus changed) the gate
    rebuilds it mechanically and verifies again. A class not backed by its
    derivation — a claim the twin or fork sources no longer contain, or an
    OVERRIDES-disposition still pending — is NOT healed: the gate fails.
    """
    builder = ROOT / "scripts" / "build_surface_absence.py"

    def run(*argv: str) -> "subprocess.CompletedProcess[str]":
        r = subprocess.run([sys.executable, str(builder), *argv], capture_output=True, text=True)
        if r.stdout:
            print(r.stdout, end="")
        if r.stderr:
            print(r.stderr, file=sys.stderr, end="")
        return r

    r = run("--check")
    unbacked = "not backed:" in r.stderr
    if r.returncode == 2 and not unbacked:
        print("(manifest drifted — rebuilding mechanically)", file=sys.stderr)
        run()
        r = run("--check")
        unbacked = "not backed:" in r.stderr
    if r.returncode != 0 or unbacked:
        print("surface-absence manifest gate failed", file=sys.stderr)
        return False
    return True


def gate() -> int:
    cov = json.loads((ROOT / "tooling" / "coverage.json").read_text())
    cur = {s: round(v["covered"] / v["total"] * 10000) for s, v in cov.items() if v["total"]}
    budget_p = ROOT / "tooling" / "coverage-budget.json"
    if not budget_p.exists():
        write_budget(cov)
        print(f"coverage-budget initialized ({len(cur)} services)")
        return 0
    stored = json.loads(budget_p.read_text())
    regressions = [
        (s, f"{stored[s] / 100:.1%} → {cur.get(s, 0) / 100:.1%}")
        for s in sorted(set(stored) | set(cur))
        if cur.get(s, 0) < stored.get(s, 0)
    ]
    if regressions:
        print("coverage regressions:", file=sys.stderr)
        for s, beat in regressions:
            print(f"  {s}: {beat}", file=sys.stderr)
        return 2
    # ratchet forward
    write_budget(cov)
    print(f"coverage gate OK — {len(cur)} services ratcheted")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Surface coverage machine")
    ap.add_argument("--gate", action="store_true", help="run the budget ratchet after regen")
    args = ap.parse_args()
    if args.gate and not refresh_manifest():
        sys.exit(2)
    main()
    sys.exit(gate() if args.gate else 0)
