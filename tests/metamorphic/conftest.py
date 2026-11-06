"""Metamorphic tests: relations that must hold for ANY correct AWS twin.

Unlike the compat suites, nothing here records an expected value — each
test states a relation between responses (put→get round-trip, ranged GET
= slice of full GET, paginated = unpaginated, v1/v2/versions listing
agreement, cross-account replay equivalence) and fails when the twin
violates it. That needs no oracle for WHAT the value should be, so it
keeps paying off as services grow without maintaining expectations.

Runs inside the normal compat shard (marker `metamorphic`); every test
uses its own account id, so xdist workers can't interfere.
"""

import json
import os
import random
import re
import uuid

import boto3
import pytest
from botocore.config import Config
from hypothesis import settings as _settings

# The machines do real HTTP per step: the default 200ms-per-example
# deadline throws flaky DeadlineExceeded on a loaded CI box. Bounded
# examples keep cost flat; derandomize keeps CI failures reproducible.
_settings.register_profile(
    "metamorphic",
    _settings(deadline=None, max_examples=25, derandomize=os.environ.get("CI") == "true"),
)
_settings.load_profile("metamorphic")

ENDPOINT_URL = "http://localhost:4566"
REGION = "us-east-1"

_session_cache: dict[str, object] = {}


def client(service: str, account: str = "123456789012"):
    kwargs = {
        "endpoint_url": ENDPOINT_URL,
        "region_name": REGION,
        "aws_access_key_id": account,
        "aws_secret_access_key": "metamorphic",
    }
    if service == "s3":
        kwargs["config"] = Config(s3={"addressing_style": "path"})
    return boto3.client(service, **kwargs)


def fresh_account() -> str:
    """A unique 12-digit account id — the twin's per-test isolation unit."""
    return str(random.randint(10**11, 10**12 - 1))


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


# --- canonicalization -------------------------------------------------------

# Response members whose PRESENCE is semantically meaningful but whose
# value is run-local state (nonces, tokens minted per call).
OPAQUE_VALUE_KEYS = {"ReceiptHandle"}

_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?")
_EPOCH_RE = re.compile(r"\b1[0-9]{12}(?:\.\d+)?\b")


def canonical(obj, account: str | None = None):
    """Normalize the parts of a response that legitimately differ between
    two runs of the same script: response metadata, request ids, UUIDs,
    timestamps, epoch millis, and (optionally) the account id."""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k in ("ResponseMetadata", "ETag", "Marker", "NextMarker"):
                continue
            if k in OPAQUE_VALUE_KEYS:
                # Presence is checked; the value is run-local state (a
                # receipt handle embeds message ids, queue URL and epoch).
                out[k] = "{OPAQUE}"
                continue
            out[k] = canonical(v, account)
        return out
    if isinstance(obj, list):
        # Order-bearing for message/listing payloads; the relations that
        # need set semantics sort their own projections explicitly.
        return [canonical(v, account) for v in obj]
    if isinstance(obj, str):
        s = obj
        if account:
            s = s.replace(account, "{ACCOUNT}")
        s = _UUID_RE.sub("{UUID}", s)
        s = _TS_RE.sub("{TS}", s)
        s = _EPOCH_RE.sub("{EPOCH}", s)
        return s
    return obj


def assert_same(a, b, what: str):
    import json as _json

    ca, cb = canonical(a), canonical(b)
    if ca != cb:
        da = _json.dumps(ca, sort_keys=True, default=str)
        db = _json.dumps(cb, sort_keys=True, default=str)
        # Show the first divergence — print nothing when identical.
        for i, (x, y) in enumerate(zip(da, db)):
            if x != y:
                pytest.fail(
                    f"{what} diverges at byte {i} of the canonical dump:\n"
                    f"  A: ...{da[max(0, i - 60) : i + 60]}\n  B: ...{db[max(0, i - 60) : i + 60]}"
                )
        pytest.fail(
            f"{what} diverges (length {len(da)} vs {len(db)}):\n  A: {da[:200]}\n  B: {db[:200]}"
        )


# --- S3 body corpus ---------------------------------------------------------


def s3_bodies() -> list[bytes]:
    rng = random.Random(1234)
    return [
        b"",
        b"a",
        "unicode ✓ ☃ 𝕊".encode(),
        bytes(range(256)),
        bytes(rng.getrandbits(8) for _ in range(256 * 1024)),
        b"\x00" * 1024,
        json.dumps({"nested": {"list": [1, 2.5, None, True]}}).encode(),
    ]
