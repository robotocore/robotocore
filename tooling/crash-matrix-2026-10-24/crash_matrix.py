#!/usr/bin/env python3
"""Probe-vs-moto crash matrix for the fleet's committed 500_error rows.

For every (service, operation) row found as status "500_error" in probes/*.json
this script:

  1. builds the same auto-filled params the probe runner uses
     (scripts.lib.param_filler),
  2. calls the op against a scratch moto server (moto fork 5.1.23.dev, the
     same checkout the port probes against) and against a scratch
     robotocore twin,
  3. records, for each side: the HTTP status, the botocore failure mode
     (ClientError code / ParseError reason / param-validation), and the raw
     response first line(s) captured via a botocore `needs-retry` hook so the
     actual wire envelope is visible,

and prints a side-by-side matrix plus a verdict suggestion:

  "moto-crash"    moto fails the same way             -> keep crash-faithful
  "moto-clean"    moto answers a modeled error/200    -> twin must be fixed
  ...anything ambiguous prints the raw envelopes for manual judgment.

Usage:
  uv run python tooling/crash-matrix-2026-10-24/crash_matrix.py \
      --service ses,elb,resource-groups \
      --moto http://127.0.0.1:4795 --twin http://127.0.0.1:4794

Only HTTP status/kinds are compared; no artifacts are rewritten.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import botocore.exceptions
import botocore.loaders

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from scripts.lib.param_filler import auto_fill_params, get_required_params, to_snake_case  # noqa: E402
from scripts.probe_service import get_operations, SKIP_OPERATIONS, KNOWN_PARAMS  # noqa: E402

import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402


class RawCapture:
    """Grab the last raw HTTP response text via a botocore retry hook."""

    def __init__(self, session):
        self.last = None
        session.register("needs-retry", self._capture)

    def _capture(self, response, **kwargs):
        try:
            self.last = {
                "status": response.status_code,
                "headers": dict(response.headers),
                "body": response.text,
            }
        except Exception:  # noqa: BLE001 — hook must never break the call
            self.last = None
        return None  # normal retry decision


def classify(client, method, params, pending_capture):
    """One call; returns a dict describing the wire outcome."""
    pending_capture.last = None
    try:
        resp = method(**params) or {}
        return {"kind": "OK", "code": "", "message": "", "raw": pending_capture.last}
    except client.exceptions.ClientError as e:
        rm = e.response.get("ResponseMetadata", {})
        return {
            "kind": "ClientError",
            "code": e.response["Error"]["Code"],
            "message": e.response["Error"]["Message"],
            "status": rm.get("HTTPStatusCode"),
            "raw": pending_capture.last,
        }
    except botocore.exceptions.ParamValidationError as e:
        return {"kind": "ParamValidation", "code": "", "message": str(e)[:80], "raw": None}
    except botocore.exceptions.EndpointConnectionError as e:
        return {"kind": "EndpointConnection", "code": "", "message": str(e)[:80], "raw": None}
    except Exception as e:  # noqa: BLE001
        return {
            "kind": type(e).__name__,
            "code": "",
            "message": str(e)[:110],
            "raw": pending_capture.last,
        }


def smallest_body(wat: str, n: int = 200) -> str:
    s = re.sub(r"\s+", " ", wat or "")
    return s[:n]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--service", required=True, help="comma-separated twin service names")
    ap.add_argument("--moto", default="http://127.0.0.1:4795")
    ap.add_argument("--twin", default="http://127.0.0.1:4794")
    ap.add_argument("--ops", default="", help="restrict to these op names (comma-separated)")
    args = ap.parse_args()

    loader = botocore.loaders.Loader()

    def botocore_name(svc: str) -> str:
        compact = svc.replace("-", "").replace("_", "").lower()
        available = loader.list_available_services("service-2")
        for name in available:
            if name.replace("-", "").replace("_", "").lower() == compact:
                return name
        return svc

    for svc in [s.strip() for s in args.service.split(",") if s.strip()]:
        bn = botocore_name(svc)
        ops_all = get_operations(bn, all_ops=True)
        wanted = (
            [o.strip() for o in args.ops.split(",") if o.strip()]
            if args.ops
            else None
        )

        rows_committed = json.load(open(REPO / "probes" / f"{svc}.json"))["operations"]
        crashed = [r for r in rows_committed if r.get("status") == "500_error"]
        targets = crashed if wanted is None else [r for r in crashed if r["operation"] in wanted]
        if not targets:
            print(f"== {svc}: no matching crashed rows\n")
            continue

        session_m = botocore.session.get_session()
        session_t = botocore.session.get_session()
        cap_m, cap_t = RawCapture(session_m), RawCapture(session_t)
        moto_raw = boto3.Session(botocore_session=session_m)
        twin_raw = boto3.Session(botocore_session=session_t)
        cfg = Config(retries={"total_max_attempts": 1}, read_timeout=60)
        cm = moto_raw.client(bn, endpoint_url=args.moto, region_name="us-east-1",
                             aws_access_key_id="testing", aws_secret_access_key="testing",
                             config=cfg)
        ct = twin_raw.client(bn, endpoint_url=args.twin, region_name="us-east-1",
                             aws_access_key_id="testing", aws_secret_access_key="testing",
                             config=cfg)

        print("=" * 100)
        print(f"== {svc} ({bn})")
        for row in targets:
            op = row["operation"]
            if op in SKIP_OPERATIONS or op not in ops_all:
                continue
            params = KNOWN_PARAMS.get(svc, {}).get(op) or auto_fill_params(bn, op) or {}
            if not params:
                params = get_required_params(svc, op) or {}
            method_name = to_snake_case(op)
            m = getattr(cm, method_name, None)
            t = getattr(ct, method_name, None)
            if m is None or t is None:
                print(f"- {op}: probe-method missing client-side; skip")
                continue
            pm = params.copy()
            pt = params.copy()
            rm = classify(cm, m, pm, cap_m)
            rt = classify(ct, t, pt, cap_t)

            def short(r):
                if r["kind"] == "OK":
                    return "OK"
                if r["kind"] == "ClientError":
                    return f"{r['code']} @ {r['status']}: {str(r['message'])[:60]}"
                return f"{r['kind']}: {str(r['message'])[:80]}"

            same_kind = rm["kind"] == rt["kind"] and (
                rm["kind"] != "ClientError" or rm["code"] == rt["code"]
            )
            verdict = "SAME" if same_kind else "DIFF"
            print(f"- {op}")
            print(f"    moto : {short(rm)}")
            print(f"    twin : {short(rt)}")
            if rm.get("raw") and rm["raw"].get("body"):
                print(f"    moto body  [{rm['raw']['status']} {rm['raw']['headers'].get('content-type','')}]: "
                      f"{smallest_body(rm['raw']['body'])}")
            if rt.get("raw") and rt["raw"].get("body"):
                print(f"    twin body  [{rt['raw']['status']} {rt['raw']['headers'].get('content-type','')}]: "
                      f"{smallest_body(rt['raw']['body'])}")
            print(f"    verdict: {verdict}")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
