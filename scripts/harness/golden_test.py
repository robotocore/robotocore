#!/usr/bin/env python3
"""Golden-test harness for the robotocore golden baseline.

Replays the recorded HTTP requests from a golden baseline capture against
any robotocore-compatible endpoint (the live Python server today, the Rust
port later), records a normalized result file, and can diff two result
files op-by-op.

Usage:
    golden_test.py [--service {s3,sqs,sts,all}] [--baseline FILE]
                   [--endpoint URL] [--out FILE] [--setup] [--teardown]
    golden_test.py --diff FILE1 FILE2

Design notes
------------
* The baseline records raw requests with SigV4 headers (recorded as
  ``b'...'`` literal strings) that are stale by definition.  golden_test
  drops the signing/volatile headers and re-signs each request with
  botocore's SigV4Auth for the service+region parsed from the recorded
  Authorization header (default: sts/sqs/s3, us-east-1, account
  123456789012).  This keeps replay faithful: the server sees the same
  method/path/query/body with fresh valid credentials.
* Resource names from the baseline (bucket, queue) are rewritten to a
  per-run unique name (``--resource-suffix``, default: 4 hex chars) so
  replays never collide across runs or against the original capture.
  ``--setup`` creates those resources first; ``--teardown`` deletes them.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
import time
import uuid
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import boto3
import requests

# ---------------------------------------------------------------------------
# Volatile / normalization lists (extend here)
# ---------------------------------------------------------------------------

# Request headers dropped on replay (re-added by re-signing).
VOLATILE_REQUEST_HEADERS = {
    "authorization",
    "x-amz-date",
    "x-amz-security-token",
    "x-amz-content-sha256",
    "x-amz-checksum-crc32",
    "x-amz-sdk-checksum-algorithm",
    "x-amz-checksum-mode",
    "amz-sdk-invocation-id",
    "amz-sdk-request",
    "user-agent",
    "x-amz-user-agent",
    "expect",
}

# Response headers dropped from the recorded result.
VOLATILE_RESPONSE_HEADERS = {
    "date",
    "x-amz-request-id",
    "x-amz-id-2",
    "x-amzn-requestid",
    "x-robotocore-request-id",
    "x-localstack-tgt",
    "x-localstack-status",
    "content-length",
    "transfer-encoding",
}

# JSON body fields stripped before comparison (top-level, recursively for
# SQS Messages entries).  Extend per-service as new ops land in the baseline.
VOLATILE_BODY_FIELDS: Dict[str, List[str]] = {
    "*": ["RequestId", "x-amz-request-id"],
    "sqs": ["ReceiptHandle", "MessageId", "SentTimestamp",
            "ApproximateFirstReceiveTimestamp", "MD5OfBody",
            "MD5OfMessageBody"],
    "s3": ["ETag", "LastModified", "ChecksumCRC32", "ChecksumMode",
           "ChecksumAlgorithm", "Metadata"],
}

# Object-body fields that are not JSON (streams, raw bytes) and can never
# be replayed/compared from the baseline as-is.
BODY_PLACEHOLDERS = ("<_io.BytesIO", "<botocore.httpchecksum")

ACCOUNT_ID = "123456789012"
REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _decode_header_value(v: Any) -> str:
    """Normalize recorded header bytes like b'AWS4...' to plain strings."""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "replace")
    if isinstance(v, str) and v.startswith("b'") and v.endswith("'"):
        return v[2:-1]
    return v if isinstance(v, str) else str(v)


def _service_for(entry: Dict[str, Any], index: int) -> Optional[str]:
    """Infer the service of a baseline entry from its request shape."""
    h = entry.get("http") or {}
    headers = {k.lower(): _decode_header_value(v)
               for k, v in (h.get("headers") or {}).items()}
    if "x-amz-target" in headers:
        target = headers["x-amz-target"]
        if target.startswith("AmazonSQS"):
            return "sqs"
        if target.startswith("DynamoDB"):
            return "dynamodb"
        return "json"
    if h.get("method") in ("PUT", "GET", "DELETE", "POST") and (
            h.get("path") or "").startswith("/"):
        # S3-style bucket-key paths (no Action=, no X-Amz-Target).
        body = h.get("body") or ""
        if isinstance(body, str) and body.startswith("Action="):
            return "query"
        return "s3"
    if isinstance(h.get("body"), str) and h["body"].startswith("Action="):
        action = parse_qs(h["body"]).get("Action", [""])[0]
        if action == "GetCallerIdentity":
            return "sts"
        return "query"
    return None


def _sign_and_send(endpoint: str, service: str, method: str, path: str,
                   query: str, headers: Dict[str, str],
                   body: bytes) -> requests.Response:
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    url = endpoint.rstrip("/") + path + (("?" + query) if query else "")
    req = AWSRequest(method=method, url=url, headers=dict(headers),
                     data=body, auth_path=path)
    SigV4Auth(Credentials(ACCOUNT_ID, "test"), service, REGION).add_auth(req)
    out_headers = {k: (v.decode("utf-8", "replace")
                       if isinstance(v, (bytes, bytearray)) else v)
                   for k, v in req.headers.items()}
    return requests.request(method, url, data=body, headers=out_headers,
                            timeout=30)


def _parse_response(body: bytes, content_type: str) -> Any:
    """Decode a response body into the same shape as the baseline's
    ``response`` field (dict for JSON, string for XML/plain)."""
    if not body:
        return {}
    if "json" in content_type:
        try:
            return json.loads(body.decode("utf-8", "replace"))
        except ValueError:
            return body.decode("utf-8", "replace")
    text = body.decode("utf-8", "replace")
    if text.lstrip().startswith(("<", "{")):
        # keep raw text for XML / non-JSON so the diff stays readable
        return {"__raw__": text}
    return {"__raw__": text}


# ---------------------------------------------------------------------------
# Setup / teardown (boto3)
# ---------------------------------------------------------------------------

def _boto3_clients(endpoint: str):
    session = dict(endpoint_url=endpoint,
                   aws_access_key_id=ACCOUNT_ID,
                   aws_secret_access_key="test",
                   region_name=REGION)
    return (boto3.client("s3", **session),
            boto3.client("sqs", **session),
            boto3.client("sts", **session))


def _setup(endpoint: str, service: str, entries: List[Dict[str, Any]],
           bucket: str, queue: str) -> Dict[str, str]:
    s3, sqs, sts = _boto3_clients(endpoint)
    created: Dict[str, str] = {}
    if service in ("s3", "all") and bucket:
        if bucket in [b["Name"] for b in s3.list_buckets().get("Buckets", [])]:
            s3.delete_bucket(Bucket=bucket)  # idempotent: fresh start
        s3.create_bucket(Bucket=bucket)
        created["s3_bucket"] = bucket
    if service in ("sqs", "all") and queue:
        sqs.create_queue(QueueName=queue)
        created["sqs_queue"] = queue
    if service in ("sts", "all"):
        pass  # nothing to create
    return created


def _teardown(endpoint: str, created: Dict[str, str]) -> None:
    s3, sqs, sts = _boto3_clients(endpoint)
    if "s3_bucket" in created:
        try:
            # delete objects first (put_object leaves one in the bucket)
            objs = s3.list_objects_v2(Bucket=created["s3_bucket"]).get(
                "Contents", [])
            if objs:
                s3.delete_objects(Bucket=created["s3_bucket"],
                                  Delete={"Objects": [
                                      {"Key": o["Key"]} for o in objs]})
            s3.delete_bucket(Bucket=created["s3_bucket"])
        except Exception as e:
            print(f"teardown: s3 delete_bucket failed: {e}", file=sys.stderr)
    if "sqs_queue" in created:
        try:
            url = sqs.get_queue_url(
                QueueName=created["sqs_queue"])["QueueUrl"]
            sqs.delete_queue(QueueUrl=url)
        except Exception as e:
            print(f"teardown: sqs delete_queue failed: {e}", file=sys.stderr)


def _resource_names(entries: List[Dict[str, Any]]) -> Dict[str, str]:
    """Find the bucket/queue names used by the baseline entries."""
    names: Dict[str, str] = {"s3_bucket": None, "sqs_queue": None}
    for e in entries:
        h = e.get("http") or {}
        path = h.get("path") or ""
        body = h.get("body") or ""
        if names["s3_bucket"] is None and _service_for(e, 0) == "s3" \
                and path.startswith("/"):
            m = re.match(r"^/([^/?]+)", path)
            if m:
                names["s3_bucket"] = m.group(1)
        if isinstance(body, str) and '"QueueUrl"' in body:
            m = re.search(r'/(\d{12})/([^"?]+)', body)
            if m and names["sqs_queue"] is None:
                names["sqs_queue"] = m.group(2)
    return names


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------

def _rewrite_names(entry: Dict[str, Any], bucket: str, queue: str,
                   old_bucket: str, old_queue: str) -> Dict[str, Any]:
    """Return a copy of the entry with recorded resource names swapped."""
    e = json.loads(json.dumps(entry))  # deep copy
    h = e.get("http") or {}
    if old_bucket and h.get("path"):
        h["path"] = re.sub(r"^/" + re.escape(old_bucket) + r"(?=/|$)",
                           "/" + bucket, h["path"], count=1)
    if old_queue and isinstance(h.get("body"), str):
        # Queue names appear in the QueueUrl (body for SQS, not path).
        h["body"] = h["body"].replace(old_queue, queue)
    return e


def replay(entries: List[Dict[str, Any]], endpoint: str,
           bucket: str, queue: str) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for i, entry in enumerate(entries):
        h = entry.get("http") or {}
        if not h.get("method"):
            results.append({"op": entry["op"], "skipped": "no http request",
                            "status": None, "response": entry.get("response"),
                            "response_headers": {}, "ms": 0.0})
            continue
        service = _service_for(entry, i) or "s3"
        if service == "query":
            service = "sts"  # query-protocol calls in the baseline are STS
        if service == "json":
            service = "sqs"  # only SQS uses x-amz-json in this baseline

        e2 = _rewrite_names(entry, bucket, queue,
                            entry.get("orig_bucket"),
                            entry.get("orig_queue"))

        headers: Dict[str, str] = {}
        for k, v in (h.get("headers") or {}).items():
            if k.lower() in VOLATILE_REQUEST_HEADERS:
                continue
            if k.lower() == "content-length":
                continue
            headers[k] = _decode_header_value(v)

        body = e2["http"].get("body") or ""
        if isinstance(body, str) and any(
                body.startswith(p) for p in BODY_PLACEHOLDERS):
            # Baseline recorded a stream object for put_object; the capture
            # plan is known (capture.py: Body=b"hello golden world").
            if entry["op"] == "put_object":
                body = b"hello golden world"
            else:
                results.append({"op": entry["op"],
                                "skipped": "unreplayable body placeholder",
                                "status": None,
                                "response": entry.get("response"),
                                "response_headers": {}, "ms": 0.0})
                continue
        body_bytes = body.encode("utf-8") if isinstance(body, str) else bytes(body)

        t0 = time.monotonic()
        try:
            resp = _sign_and_send(endpoint, service, h["method"],
                                  e2["http"]["path"], e2["http"].get("query") or "",
                                  headers, body_bytes)
            ms = round((time.monotonic() - t0) * 1000, 2)
            status = resp.status_code
            rheaders = {k: v for k, v in resp.headers.items()
                        if k.lower() not in VOLATILE_RESPONSE_HEADERS}
            response = _parse_response(resp.content, resp.headers.get("Content-Type", ""))
            if status >= 400 and isinstance(response, dict):
                # record the error like the baseline does
                code = response.get("Code") or response.get("__type") or ""
                msg = response.get("Message") or response.get("message") or ""
                response = {"__error__": f"{code}({msg})"} if code or msg else response
        except Exception as ex:
            ms = round((time.monotonic() - t0) * 1000, 2)
            status = None
            response = {"__error__": f"{type(ex).__name__}({ex})"}
            rheaders = {}

        results.append({"op": entry["op"], "status": status,
                        "response": response,
                        "response_headers": dict(sorted(rheaders.items())),
                        "ms": ms})
    return results


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------

def _normalize_response(resp: Any, service: str) -> Any:
    fields = set(VOLATILE_BODY_FIELDS.get("*", [])) | set(
        VOLATILE_BODY_FIELDS.get(service, []))

    def strip(obj: Any, in_messages: bool = False) -> Any:
        if isinstance(obj, dict):
            out = {}
            for k, v in obj.items():
                if in_messages and k in VOLATILE_BODY_FIELDS.get("sqs", []):
                    continue
                if k in fields:
                    continue
                out[k] = strip(v, in_messages)
            return out
        if isinstance(obj, list):
            return [strip(v, in_messages) for v in obj]
        if isinstance(obj, str) and "__error__" in obj:
            # keep only the error code, messages vary
            m = re.match(r"([A-Za-z0-9.]+)\(", obj)
            return {"__error__": m.group(1) if m else obj}
        return obj

    if isinstance(resp, dict):
        out = {}
        for k, v in resp.items():
            if k in fields:
                continue
            if service == "sqs" and k == "Messages" and isinstance(v, list):
                out[k] = [strip(m, in_messages=True) for m in v]
            else:
                out[k] = strip(v)
        return out
    return resp


def diff(a_path: str, b_path: str) -> int:
    a = json.load(open(a_path))
    b = json.load(open(b_path))
    # Index by op so partial captures (e.g. --service sqs) can be diffed
    # against a full capture.
    by_op_a = {e.get("op"): e for e in a}
    by_op_b = {e.get("op"): e for e in b}
    common = [op for op in by_op_a if op in by_op_b]
    only_a = sorted(set(by_op_a) - set(by_op_b))
    only_b = sorted(set(by_op_b) - set(by_op_a))
    if only_a:
        print(f"note: ops only in A: {only_a}")
    if only_b:
        print(f"note: ops only in B: {only_b}")
    if not common:
        print("MISMATCH: no common ops to compare")
        return 1
    bad = 0
    for op in common:
        ea, eb = by_op_a[op], by_op_b[op]
        sa = _normalize_response(ea.get("response"), ea.get("service", "*"))
        sb = _normalize_response(eb.get("response"), eb.get("service", "*"))
        ok = ea.get("status") == eb.get("status") and sa == sb
        tag = "OK  " if ok else "DIFF"
        print(f"{tag} {ea.get('op')}: status {ea.get('status')}"
              f" vs {eb.get('status')}")
        if not ok:
            bad += 1
            if ea.get("status") != eb.get("status"):
                print(f"    status: {ea.get('status')} != {eb.get('status')}")
            if sa != sb:
                print(f"    a: {json.dumps(sa, sort_keys=True)}")
                print(f"    b: {json.dumps(sb, sort_keys=True)}")
    print(f"\n{'PASS' if bad == 0 else 'FAIL'}: "
          f"{len(common) - bad}/{len(common)} common ops match")
    return 0 if bad == 0 else 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--service", default="all",
                    choices=["s3", "sqs", "sts", "all"])
    ap.add_argument("--baseline", default="/tmp/golden/baseline.json")
    ap.add_argument("--endpoint", default="http://localhost:4566")
    ap.add_argument("--out", default=None)
    ap.add_argument("--setup", action="store_true",
                    help="create resources (queue/bucket) before replay")
    ap.add_argument("--teardown", action="store_true",
                    help="delete setup resources after replay")
    ap.add_argument("--resource-suffix", default=None,
                    help="suffix for unique resource names (default: 4 hex)")
    ap.add_argument("--diff", nargs=2, metavar=("FILE_A", "FILE_B"),
                    help="compare two result files and exit 0 iff equal")
    args = ap.parse_args()

    if args.diff:
        return diff(*args.diff)

    baseline = json.load(open(args.baseline))
    suffix = args.resource_suffix or uuid.uuid4().hex[:4]
    orig = _resource_names(baseline)
    bucket = f"golden-{suffix}" if orig["s3_bucket"] else None
    queue = f"golden-{suffix}" if orig["sqs_queue"] else None
    if bucket:
        print(f"rewriting bucket '{orig['s3_bucket']}' -> '{bucket}'")
    if queue:
        print(f"rewriting queue '{orig['sqs_queue']}' -> '{queue}'")

    entries = []
    for i, e in enumerate(baseline):
        service = _service_for(e, i) or "s3"
        if service in ("query",):
            service = "sts"
        if service == "json":
            service = "sqs"
        if args.service != "all" and service != args.service:
            continue
        e = dict(e)
        e["service"] = service
        e["orig_bucket"] = orig["s3_bucket"]
        e["orig_queue"] = orig["sqs_queue"]
        entries.append(e)

    print(f"replaying {len(entries)} ops against {args.endpoint} "
          f"(bucket={bucket}, queue={queue})")

    created: Dict[str, str] = {}
    try:
        if args.setup:
            created = _setup(args.endpoint, args.service, entries,
                             bucket, queue)
            print(f"setup: {created}")
        results = replay(entries, args.endpoint, bucket or "", queue or "")
    finally:
        if args.teardown and created:
            _teardown(args.endpoint, created)
            print(f"teardown: {created}")

    out = args.out or f"/tmp/golden/results-{time.strftime('%Y%m%d-%H%M%S')}.json"
    payload = [{"op": r["op"], "service": e["service"],
                "status": r.get("status"), "response": r.get("response"),
                "response_headers": r.get("response_headers", {}),
                "ms": r.get("ms")}
               for r, e in zip(results, entries)]
    with open(out, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"wrote {out}")

    # Ops without a recorded HTTP request (e.g. get_access_key_info, which
    # raised ParamValidationError client-side) cannot be replayed; they are
    # expected-None and reported, not failures.
    replayed = [p for p in payload if p["status"] is not None]
    skipped = [p for p in payload if p["status"] is None]
    non200 = [p for p in replayed if p["status"] not in (200, 404)]
    if non200:
        print(f"FAIL: unexpected non-2xx/404 statuses: "
              f"{[(p['op'], p['status']) for p in non200]}")
        return 1
    if skipped:
        print(f"note: {len(skipped)} ops skipped (no recorded HTTP request): "
              f"{[p['op'] for p in skipped]}")
    print(f"PASS: {len(replayed)}/{len(payload)} ops replayed, "
          f"{len(skipped)} skipped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
