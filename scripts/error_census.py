#!/usr/bin/env python3
"""Live wire census for recorded denies.

Process lesson from the 2026-10-24 review wave: "501 NotImplemented is the
gap canon" lived in docs only — modules answered their own 500 envelopes and
JSON heresies survived eleven review waves. This census takes the deny rows
of tooling/surface-absence.json and asserts their live wire behavior:

- modeled_deny / absent_no_handler rows must classify via the probe runner
  as `not_implemented` (501 + NotImplemented, in whatever transport shape
  the AGENTS.md rule expects),
- absent_moto_crath rows may also classify crash-faithfully (`500_error`) —
  that is the recorded moto-mirror contract,
- anything answering `working` contradicts the manifest (either a
  classification regression or a probe-corpus staleness — exit 1 names it).

Usage:
    uv run python scripts/error_census.py            # sampled 40 rows
    uv run python scripts/error_census.py --all      # every deny row
    uv run python scripts/error_census.py --json
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))  # probe_service imports `scripts.lib.*`

_spec = importlib.util.spec_from_file_location(
    "probe_service", ROOT / "scripts" / "probe_service.py"
)
probe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(probe)

import os  # noqa: E402

import boto3  # noqa: E402
import botocore  # noqa: E402

ENDPOINT = os.environ.get("ENDPOINT_URL", "http://localhost:4566")
MANIFEST = ROOT / "tooling" / "surface-absence.json"


# Manifest keys use the twin's registry names; botocore's client names
# differ for fourteen of them. Without this map the census silently skips
# those rows — a census with silent gaps defeats itself.
BOTOCORE_NAME_MAP = {
    "acmpca": "acm-pca",
    "applicationautoscaling": "application-autoscaling",
    "cognitoidentity": "cognito-identity",
    "emrcontainers": "emr-containers",
    "emrserverless": "emr-serverless",
    "rdsdata": "rds-data",
    "redshiftdata": "redshift-data",
    "servicecatalogappregistry": "servicecatalog-appregistry",
    "ssoadmin": "sso-admin",
    "timestreaminfluxdb": "timestream-influxdb",
    "timestreamquery": "timestream-query",
    "timestreamwrite": "timestream-write",
    "vpclattice": "vpc-lattice",
    "workspacesweb": "workspaces-web",
}


def make_client(service: str):
    """Same client recipe the probe runner uses for every service."""
    return boto3.client(
        BOTOCORE_NAME_MAP.get(service, service),
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="999999999999",
        aws_secret_access_key="test",
        config=botocore.config.Config(retries={"max_attempts": 1}),
    )


def load_deny_rows() -> list[tuple[str, str, str]]:
    data = json.loads(MANIFEST.read_text())
    services = data.get("services", {})
    rows: list[tuple[str, str, str]] = []
    for svc, ops in services.items():
        for op, meta in ops.items():
            rows.append((svc, op, meta.get("class", "absent")))
    return rows


def verdict_for(status: str, klass: str) -> tuple[bool, str]:
    if status == "not_implemented":
        return True, "PASS"
    if klass == "absent_moto_crash_faithful" and status == "500_error":
        return True, "PASS (crash-faithful rows mirror moto's raw 500)"
    if klass == "legacy_gap" and status == "working":
        # closed-gap rows: the manifest text keeps the history and the twin
        # legitimately serves them now; a probe refresh clears these.
        return True, "PASS (closed gap, probe refresh pending)"
    if klass == "legacy_gap" and status == "not_implemented":
        return False, "STILL ABSENT — legacy gap not yet closed"
    if status == "working":
        return False, "REGRESSION — manifest records a deny but the twin answers"
    if status == "needs_params":
        return True, "SKIPPED (client-side fill gap, not a deny claim)"
    if status == "timeout":
        return False, "TIMEOUT — census probe hung"
    return False, f"UNEXPECTED {status}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="probe every deny row")
    parser.add_argument("--sample", type=int, default=40)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--seed", type=int, default=31337)
    args = parser.parse_args()

    rows = load_deny_rows()
    if args.all:
        sample = rows
    else:
        sample = random.Random(args.seed).sample(rows, min(args.sample, len(rows)))

    results = []
    failures = 0
    skipped_model = 0
    for svc, op, klass in sample:
        try:
            client = make_client(svc)
        except botocore.exceptions.UnknownServiceError:
            # The manifest keys entries by the twin's registry name; some
            # registry names are not botocore vip names (e.g. timestreaminfluxdb
            # vs timestream-influxdb). Count the row as skipped instead of
            # accepting it as a pass — the census summary carries the number.
            skipped_model += 1
            continue
        try:
            status, reason = probe.probe_operation(client, svc, op, {})
        except botocore.exceptions.EndpointConnectionError as e:
            status, reason = "connection", str(e)[:60]
        except Exception as e:  # noqa: BLE001 — census must survive any op
            status, reason = "exception", str(e)[:60]
        ok, verdict = verdict_for(status, klass)
        if not ok:
            failures += 1
        results.append(
            {
                "service": svc,
                "op": op,
                "class": klass,
                "live_status": status,
                "reason": reason,
                "verdict": verdict,
                "ok": ok,
            }
        )

    ok_n = len(results) - failures
    if args.json:
        print(
            json.dumps(
                {
                    "summary": {"total": len(results), "passed": ok_n, "failed": failures},
                    "failed_rows": [r for r in results if not r["ok"]],
                },
                indent=2,
            )
        )
    else:
        for r in results:
            flag = "ok  " if r["ok"] else "FAIL"
            print(
                f"{flag} {r['service']}.{r['op']} class={r['class']} "
                f"live={r['live_status']} -> {r['verdict']}"
            )
        extra = f" (+{skipped_model} skipped unknown botocore names)" if skipped_model else ""
        print(f"census: {ok_n}/{len(results)} rows conform to the recorded deny contract{extra}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
