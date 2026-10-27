#!/usr/bin/env python3
"""Golden capture tool for the robotocore golden harness.

Runs a curated list of boto3 operations against a running robotocore
endpoint and records the raw HTTP request + response for each call.

Usage:
    capture.py --services sts,s3,sqs [--extra svc=OperationName]...
               [--endpoint URL] --out FILE

Each captured entry (in stable execution order) has the shape:

    {
      "service": "s3", "op": "create_bucket", "seq": 0, "note": "happy-path",
      "http": {"method": "PUT", "path": "/b", "query": "",
               "headers": {...}, "body": "<base64>", "body_encoding": "base64"},
      "status": 200,
      "response_body": "<base64 of raw bytes>",
      "response_headers": {...},
      "volatile_response_paths": ["response.ETag", ...],
      "error": "Code", "error_message": "...",   # when the call raised
    }

Normalization applied at capture time:
  * SigV4 signing headers (Authorization, X-Amz-Date, content checksums)
    -> literal "SigV4: present"
  * SDK-volatile request headers (User-Agent, amz-sdk-*) -> dropped
  * Volatile response headers (Date, x-amz-request-id, ...) -> dropped
  * Entries carry volatile_response_paths so the differ can ignore
    inherently non-deterministic body fields (MessageId, ETag, ...).

The output file is deterministic apart from the volatile fields the differ
is told to ignore, so two captures of the same server diff clean.
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
import time
import uuid
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from botocore.httpsession import URLLib3Session

# ---------------------------------------------------------------------------
# Request/response interception
# ---------------------------------------------------------------------------

_SIGV4_VOLATILE_HEADERS = {
    "authorization", "x-amz-date", "x-amz-security-token",
    "x-amz-content-sha256", "x-amz-checksum-crc32",
    "x-amz-sdk-checksum-algorithm", "x-amz-checksum-mode",
    "x-amz-checksum-sha256",
}

_SDK_VOLATILE_HEADERS = {
    "amz-sdk-invocation-id", "amz-sdk-request",
    "user-agent", "x-amz-user-agent",
}

_VOLATILE_RESPONSE_HEADERS = {
    "date", "x-amz-request-id", "x-amz-id-2",
    "x-amzn-requestid", "x-robotocore-request-id",
    "x-localstack-tgt", "x-localstack-status",
}

OPERATION_VOLATILE_RESPONSE_PATHS: Dict[str, List[str]] = {
    "s3.PutObject": ["response.ETag", "response.ChecksumCRC32",
                     "response.ChecksumMode"],
    "s3.CopyObject": ["response.ETag"],
    "s3.GetObject": ["response.LastModified", "response.ETag",
                     "response.ChecksumCRC32", "response.ChecksumMode"],
    "sqs.SendMessage": ["response.MessageId", "response.MD5OfMessageBody"],
    "sqs.ReceiveMessage": ["response.Messages[*].MessageId",
                           "response.Messages[*].MD5OfBody",
                           "response.Messages[*].ReceiptHandle"],
}


class Interceptor:
    """Monkey-patches URLLib3Session.send to record raw HTTP traffic."""

    def __init__(self) -> None:
        self.current: Optional[Dict[str, Any]] = None
        self._orig_send = URLLib3Session.send

    def install(self) -> None:
        interceptor = self

        def send(self_session, request):
            entry = interceptor.current
            if entry is None:
                return self_session._send_orig(request)

            url = urlparse(request.url)
            body = request.body
            if hasattr(body, "read"):
                body = body.read()
            if isinstance(body, str):
                body = body.encode("utf-8")
            body = body or b""

            req_headers: Dict[str, str] = {}
            for k, v in request.headers.items():
                if isinstance(v, (bytes, bytearray)):
                    v = v.decode("utf-8", "replace")
                lk = k.lower()
                if lk in _SIGV4_VOLATILE_HEADERS:
                    req_headers[k] = "SigV4: present"
                elif lk in _SDK_VOLATILE_HEADERS:
                    continue
                else:
                    req_headers[k] = v

            entry["http"] = {
                "method": request.method,
                "path": url.path,
                "query": url.query,
                "headers": dict(sorted(req_headers.items())),
                "body": base64.b64encode(body).decode("ascii"),
                "body_encoding": "base64",
            }

            t0 = time.monotonic()
            response = self_session._send_orig(request)
            # botocore's non-streaming path already drained raw into
            # response._content before we see it; stream_output responses
            # (S3 GetObject etc.) fall back to draining raw.
            raw = getattr(response, "_content", None)
            if raw is None:
                raw = response.raw.read()
                response._content = raw
            entry["ms"] = round((time.monotonic() - t0) * 1000, 2)
            entry["status"] = response.status_code
            entry["response_body"] = base64.b64encode(raw).decode("ascii")

            resp_headers: Dict[str, str] = {}
            for k, v in response.headers.items():
                if k.lower() not in _VOLATILE_RESPONSE_HEADERS:
                    resp_headers[k] = v
            entry["response_headers"] = dict(sorted(resp_headers.items()))
            return response

        URLLib3Session._send_orig = self._orig_send
        URLLib3Session.send = send

    def uninstall(self) -> None:
        URLLib3Session.send = self._orig_send
        try:
            del URLLib3Session._send_orig
        except AttributeError:
            pass


# ---------------------------------------------------------------------------
# Operation plans
# ---------------------------------------------------------------------------

def _expect_error(call: Callable[[], Any],
                  expect_param_validation: bool = False) -> Any:
    try:
        call()
    except ClientError as e:
        return {"__error__": e.error_response.get("Code"),
                "__message__": e.error_response.get("Message")}
    except Exception as e:  # botocore ParamValidationError is a plain Exception
        if expect_param_validation:
            return {"__error__": type(e).__name__,
                    "__message__": str(e)}
        raise
    raise RuntimeError("expected an error but the call succeeded")


class _State:
    """Mutable state shared by the steps of one service plan."""
    def __init__(self) -> None:
        self.s3_bucket: Optional[str] = None
        self.sqs_url: Optional[str] = None


def _plan_s3(client: Any, state: _State) -> List[Dict[str, Any]]:
    bucket = "harness-%s" % uuid.uuid4().hex[:12]
    state.s3_bucket = bucket
    steps = [
        ("CreateBucket", "happy-path",
         lambda: client.create_bucket(Bucket=bucket), []),
        ("PutObject", "happy-path",
         lambda: client.put_object(Bucket=bucket, Key="a/b.txt",
                                   Body=b"hello golden world",
                                   ContentType="text/plain"),
         OPERATION_VOLATILE_RESPONSE_PATHS["s3.PutObject"]),
        ("GetObject", "happy-path",
         lambda: client.get_object(Bucket=bucket, Key="a/b.txt"),
         OPERATION_VOLATILE_RESPONSE_PATHS["s3.GetObject"]),
        ("ListObjectsV2", "happy-path",
         lambda: client.list_objects_v2(Bucket=bucket), []),
        ("GetBucketLocation", "happy-path",
         lambda: client.get_bucket_location(Bucket=bucket), []),
        ("GetObject", "error-not-found",
         lambda: _expect_error(
             lambda: client.get_object(Bucket=bucket, Key="missing.txt")),
         OPERATION_VOLATILE_RESPONSE_PATHS["s3.GetObject"]),
        ("CreateBucket", "error-validation",
         lambda: _expect_error(
             lambda: client.create_bucket(Bucket="bad bucket name!")), []),
        ("DeleteObject", "cleanup",
         lambda: client.delete_object(Bucket=bucket, Key="a/b.txt"), []),
        ("DeleteBucket", "cleanup",
         lambda: client.delete_bucket(Bucket=bucket), []),
    ]
    return _make_steps(steps)


def _plan_sqs(client: Any, state: _State) -> List[Dict[str, Any]]:
    name = "harness-%s" % uuid.uuid4().hex[:12]

    def send_message():
        return client.send_message(QueueUrl=state.sqs_url,
                                   MessageBody="hi golden")

    def receive_message():
        return client.receive_message(QueueUrl=state.sqs_url,
                                      MaxNumberOfMessages=1)

    def delete_queue():
        return client.delete_queue(QueueUrl=state.sqs_url)

    steps = [
        ("CreateQueue", "happy-path",
         lambda: _create_queue(client, name, state), []),
        ("SendMessage", "happy-path", send_message,
         OPERATION_VOLATILE_RESPONSE_PATHS["sqs.SendMessage"]),
        ("ReceiveMessage", "happy-path", receive_message,
         OPERATION_VOLATILE_RESPONSE_PATHS["sqs.ReceiveMessage"]),
        ("DeleteQueue", "cleanup", delete_queue, []),
        ("SendMessage", "error-not-found",
         lambda: _expect_error(
             lambda: client.send_message(
                 QueueUrl=("http://sqs.us-east-1.localhost.robotocore.cloud"
                           ":4566/123456789012/harness-does-not-exist"),
                 MessageBody="x")), []),
    ]
    return _make_steps(steps)


def _create_queue(client: Any, name: str, state: _State) -> Any:
    q = client.create_queue(QueueName=name)
    state.sqs_url = q["QueueUrl"]
    return q


def _plan_sts(client: Any, state: _State) -> List[Dict[str, Any]]:
    steps = [
        ("GetCallerIdentity", "happy-path",
         lambda: client.get_caller_identity(), []),
        ("GetCallerIdentity", "error-param-validation",
         lambda: _expect_error(
             lambda: client.get_caller_identity(Serial="bad"),
             expect_param_validation=True), []),
    ]
    return _make_steps(steps)


def _plan_ssm(client: Any, state: _State) -> List[Dict[str, Any]]:
    name = "/harness-%s" % uuid.uuid4().hex[:12]
    steps = [
        ("PutParameter", "happy-path",
         lambda: client.put_parameter(Name=name, Value="v1", Type="String"), []),
        ("GetParameter", "happy-path",
         lambda: client.get_parameter(Name=name), []),
        ("GetParameter", "error-not-found",
         lambda: _expect_error(
             lambda: client.get_parameter(Name="/harness-no-such-param")), []),
        ("DeleteParameter", "cleanup",
         lambda: client.delete_parameter(Name=name), []),
    ]
    return _make_steps(steps)


PLANS = {
    "sts": _plan_sts,
    "s3": _plan_s3,
    "sqs": _plan_sqs,
    "ssm": _plan_ssm,
}


def _make_steps(steps: List[tuple]) -> List[Dict[str, Any]]:
    return [{"op": op, "note": note, "call": call, "volatile": vol}
            for op, note, call, vol in steps]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def capture_service(client: Any, service: str, extra_ops: List[str],
                    interceptor: Interceptor) -> List[Dict[str, Any]]:
    state = _State()
    steps = PLANS[service](client, state)
    for op in extra_ops:
        method = getattr(client, op, None)
        if method is None:
            print("warning: %s has no operation %s; skipping" %
                  (service, op), file=sys.stderr)
            continue
        steps.append({"op": op, "note": "extra", "call": method,
                      "volatile": []})

    entries: List[Dict[str, Any]] = []
    for seq, step in enumerate(steps):
        entry: Dict[str, Any] = {"service": service, "op": step["op"],
                                 "seq": seq, "note": step["note"]}
        if step["volatile"]:
            entry["volatile_response_paths"] = step["volatile"]
        interceptor.current = entry
        try:
            result = step["call"]()
            if isinstance(result, dict) and "__error__" in result:
                entry["error"] = result["__error__"]
                entry["error_message"] = result.get("__message__")
        except ClientError as e:
            entry["error"] = e.error_response.get("Code")
            entry["error_message"] = e.error_response.get("Message")
        finally:
            interceptor.current = None
        for key, default in (("http", {"method": None, "path": None,
                                        "query": None, "headers": {},
                                        "body": "",
                                        "body_encoding": "base64"}),
                             ("status", None), ("response_body", ""),
                             ("response_headers", {})):
            entry.setdefault(key, default)
        entries.append(entry)
    return entries


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Capture golden HTTP traffic from a robotocore endpoint.")
    ap.add_argument("--services", required=True,
                    help="comma-separated boto3 service names (sts,s3,sqs,ssm)")
    ap.add_argument("--extra", action="append", default=[],
                    help="extra raw operation, format service=OperationName "
                         "(repeatable); the call is made with no arguments")
    ap.add_argument("--endpoint", default="http://localhost:4566")
    ap.add_argument("--out", required=True, help="output JSON file")
    args = ap.parse_args(argv)

    services = [s.strip() for s in args.services.split(",") if s.strip()]
    extras: Dict[str, List[str]] = {s: [] for s in services}
    for spec in args.extra:
        if "=" not in spec:
            print("error: --extra expects service=OperationName",
                  file=sys.stderr)
            return 2
        svc, op = spec.split("=", 1)
        svc, op = svc.strip(), op.strip()
        if svc not in PLANS and svc not in services:
            print("error: no plan for service %r (supported: %s)" %
                  (svc, ",".join(sorted(PLANS))), file=sys.stderr)
            return 2
        extras.setdefault(svc, []).append(op)

    session = boto3.session.Session(
        aws_access_key_id="123456789012",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    interceptor = Interceptor()
    interceptor.install()
    all_entries: List[Dict[str, Any]] = []
    try:
        for svc in services:
            client = session.client(svc, endpoint_url=args.endpoint)
            t0 = time.monotonic()
            entries = capture_service(client, svc, extras.get(svc, []),
                                      interceptor)
            print("%-12s %2d entries (%.0f ms)" %
                  (svc, len(entries), (time.monotonic() - t0) * 1000))
            all_entries.extend(entries)
    finally:
        interceptor.uninstall()

    out = {
        "format": "robotocore-golden-v1",
        "endpoint": args.endpoint,
        "services": services,
        "entries": all_entries,
    }
    with open(args.out, "w") as f:
        json.dump(out, f, indent=1)
        f.write("\n")
    print("wrote %s (%d entries)" % (args.out, len(all_entries)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
