#!/usr/bin/env python3
"""Dual-oracle triage of the EC2 500_error cluster.

For every operation recorded as ``500_error`` in probes/ec2.json, replay the
exact same auto-filled request (same param_filler the prober used) against
BOTH the live robotocore twin and a moto 5.2.4 oracle, capture raw
(status, code, message) from both, and classify:

  - faithful-crash   moto also 500s (any shape) — the twin is faithful
  - ours-to-fix      moto answers 200 or a modeled 4xx while the twin crashes
  - fixed-since-probe the twin already answers now (probe row is stale)

Writes tooling/ec2-crash-triage.json. Progress goes to a resumable ndjson
companion (--resume skips ops already recorded there).

Usage:
    python tooling/ec2-crash-triage-runner.py            # full sweep
    python tooling/ec2-crash-triage-runner.py --limit 5  # smoke
    python tooling/ec2-crash-triage-runner.py --twin http://127.0.0.1:4796
"""

import argparse
import json
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import boto3  # noqa: E402
import botocore.exceptions  # noqa: E402

from botocore.config import Config  # noqa: E402

from scripts.lib.param_filler import auto_fill_params, to_snake_case  # noqa: E402

DEFAULT_MOTO = "http://127.0.0.1:4799"
DEFAULT_TWIN = "http://127.0.0.1:4566"
READ_TIMEOUT = 6.0  # hangs are the bug; nothing legit takes >6s here

RAW = {}


def _hook(response_dict, operation_model=None, customized_response_dict=None, **kw):
    RAW["d"] = {
        "status": response_dict.get("status_code"),
        "body": bytes(response_dict.get("body") or b""),
    }


def make_client(endpoint: str, op_names: list[str]):
    client = boto3.client(
        "ec2",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="testing",  # same creds the prober used
        aws_secret_access_key="testing",
        config=Config(
            retries={"total_max_attempts": 1},
            read_timeout=READ_TIMEOUT,
            connect_timeout=5,
        ),
    )
    for op in op_names:
        client.meta.events.register(f"before-parse.ec2.{op}", _hook)
    return client


_CODE_RE = re.compile(rb"<Code>([^<]+)</Code>")
_MSG_RE = re.compile(rb"<Message>([^<]*)</Message>")


def extract_raw(raw_status, body: bytes):
    """Pull (code, message) out of a parse-failed body (XML error shape)."""
    code = msg = None
    c = _CODE_RE.search(body)
    if c:
        code = c.group(1).decode("utf-8", "replace")
    m = _MSG_RE.search(body)
    if m:
        msg = m.group(1).decode("utf-8", "replace")
    return raw_status, code, msg


def call(client, op: str, params: dict) -> dict:
    RAW["d"] = None
    m = getattr(client, to_snake_case(op))
    t0 = time.monotonic()
    out: dict = {}
    try:
        m(**params)
        out.update(status=200, code=None, message=None)
    except botocore.exceptions.ParamValidationError:
        out.update(status="needs_params", code=None, message="client-side validation")
    except botocore.exceptions.ClientError as e:
        sc = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        out.update(
            status=sc,
            code=e.response["Error"].get("Code"),
            message=(e.response["Error"].get("Message") or "")[:160],
        )
    except botocore.exceptions.ReadTimeoutError:
        out.update(status="read-timeout", code=None, message=None)
    except botocore.exceptions.EndpointConnectionError as e:
        out.update(status="conn-refused", code=None, message=str(e)[:80])
    except Exception as e:  # noqa: BLE001 — parse failures and the like
        out.update(status="exception", exc=type(e).__name__, message=str(e)[:120])
    out["elapsed"] = round(time.monotonic() - t0, 2)
    if RAW["d"]:
        raw_status, raw_code, raw_msg = extract_raw(
            RAW["d"]["status"], RAW["d"]["body"][:4000]
        )
        out["wire_status"] = raw_status
        if raw_code:
            out["code"] = raw_code
            if raw_msg:
                out["message"] = raw_msg[:160]
        elif out["status"] != 200:
            # non-XML wire body (HTML crash page etc.) — show it verbatim
            body = RAW["d"]["body"][:200].decode("utf-8", "replace").strip()
            if body:
                out["message"] = "raw body: " + re.sub(r"\s+", " ", body)[:150]
    return out


def classify(moto: dict, twin: dict) -> tuple[str, str]:
    ms = moto.get("wire_status", moto.get("status"))
    ts = twin.get("wire_status", twin.get("status"))
    moto_crashed = ms == 500 or ms == "read-timeout" or ms == "conn-refused"
    twin_crashed = ts == 500 or ts == "read-timeout" or ts == "exception" or ts == "conn-refused"

    if moto_crashed:
        if twin_crashed:
            return "faithful-crash", f"both crashed (moto wire {ms}, twin {ts})"
        return (
            "moto-crash-faithful",
            f"moto crashed (wire {ms}, python-shape), twin answered {ts} — out of scope",
        )

    if twin_crashed:
        if ms == 200:
            return "ours-to-fix", f"moto 200, twin {ts}"
        if isinstance(ms, int) and 400 <= ms < 500:
            return "ours-to-fix", f"moto 400 {moto.get('code')}, twin {ts}"
        return "ours-to-fix", f"moto {ms}, twin {ts}"

    # twin answered with a real response now — probe row is stale
    if ts == 200 and ms == 200:
        return "fixed-since-probe", "twin 200 now (was recorded 500)"
    return "fixed-since-probe", f"twin {ts} now {(twin.get('code') or '')}, moto {ms} {(moto.get('code') or '')}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--moto", default=DEFAULT_MOTO)
    ap.add_argument("--twin", default=DEFAULT_TWIN)
    ap.add_argument("--limit", type=int, default=0, help="only first N ops (smoke)")
    ap.add_argument("--ops", default="", help="comma-separated op filter")
    ap.add_argument("--read-timeout", type=float, default=READ_TIMEOUT)
    args = ap.parse_args()

    probe = json.loads((ROOT / "probes" / "ec2.json").read_text())
    rows = [o for o in probe["operations"] if o["status"] == "500_error"]
    if args.ops:
        wanted = {o.strip() for o in args.ops.split(",") if o.strip()}
        rows = [o for o in rows if o["operation"] in wanted]
    if args.limit:
        rows = rows[: args.limit]
    ops = [r["operation"] for r in rows]
    print(f"{len(ops)} 500_error ops to triage (moto={args.moto} twin={args.twin})", flush=True)

    params_for = {}
    for op in ops:
        params_for[op] = auto_fill_params("ec2", op)
        if params_for[op] is None:
            params_for[op] = {}
            print(f"  warn: {op}: param fill failed, calling with no params", flush=True)

    twin = make_client(args.twin, ops)
    moto = make_client(args.moto, ops)

    partial = Path(__file__).with_suffix(".ndjson.part")
    done: dict[str, dict] = {}
    if "--resume" in sys.argv and partial.exists():
        for line in partial.read_text().splitlines():
            if line.strip():
                d = json.loads(line)
                done[d["op"]] = d
        print(f"resume: {len(done)} ops already recorded", flush=True)
    else:
        partial.write_text("")

    out_path = ROOT / "tooling" / "ec2-crash-triage.json"
    entries = []
    with partial.open("a") as sink:
        for i, op in enumerate(ops, 1):
            if op in done:
                entries.append(done[op])
                continue
            params = params_for[op]
            tc = call(twin, op, params)
            mc = call(moto, op, params)
            cls, note = classify(mc, tc)
            entry = {
                "op": op,
                "moto_status": mc.get("wire_status", mc.get("status")),
                "moto_code": mc.get("code"),
                "moto_message": (mc.get("message") or "")[:160],
                "twin_status": tc.get("wire_status", tc.get("status")),
                "twin_code": tc.get("code"),
                "twin_note": note,
                "classification": cls,
                "twin_elapsed": tc.get("elapsed"),
            }
            entries.append(entry)
            sink.write(json.dumps(entry) + "\n")
            sink.flush()
            print(
                f"[{i}/{len(ops)}] {op:40} twin={str(tc.get('wire_status') or tc.get('status')):14} "
                f"moto={str(mc.get('wire_status') or mc.get('status')):14} {cls}",
                flush=True,
            )

    counts: dict[str, int] = {}
    for e in entries:
        counts[e["classification"]] = counts.get(e["classification"], 0) + 1

    artifact = {
        "service": "ec2",
        "generated_at": datetime.now(UTC).isoformat(),
        "method": "dual-oracle replay of probes/ec2.json 500_error rows; identical auto-filled params via scripts/lib/param_filler.py; moto 5.2.4 oracle",
        "moto_endpoint": args.moto,
        "twin_endpoint": args.twin,
        "total_500_rows": len(entries),
        "counts": counts,
        "operations": entries,
    }
    out_path.write_text(json.dumps(artifact, indent=2) + "\n")
    print(f"\ncounts: {counts}")
    print(f"wrote {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
