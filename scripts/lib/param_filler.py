"""Shape-driven probe-parameter synthesis from botocore models.

gen-2 (2026-10-22) closes the needs_params bucket: recursive required-member
synthesis — enums from the model, date-time floors, base64 blobs, ARN/queue /
bucket/token conventions, list/map members synthesized from element shapes.

When the filler can also walk an op's whole (non-required) member tree, the
prober gains a "full-shape invocation" mode.

All content is value synthesis utilities only — no I/O.
"""

# Hand-tuned params for operations where auto-fill isn't good enough.
# Per-lane hand-written fills live as data files: scripts/lib/fills/<any>.json,
# each {"Service": {"Op": {param json}}}. Loaded at import so parallel fix
# lanes can contribute fills without editing this file (merge-free).
import pathlib as _pl
import re

import botocore.session


def _load_fill_files(base: dict) -> dict:
    fills_dir = _pl.Path(__file__).resolve().parent / "fills"
    if not fills_dir.is_dir():
        return base
    merged = dict(base)
    import json as _json

    for path in sorted(fills_dir.glob("*.json")):
        try:
            data = _json.loads(path.read_text())
        except Exception:
            continue
        for svc, ops in data.items():
            merged.setdefault(svc, {})
            for op, params in ops.items():
                if isinstance(params, dict):
                    merged[svc].setdefault(op, params)
    return merged


KNOWN_PARAMS: dict[str, dict[str, dict]] = _load_fill_files(
    {
        "sqs": {
            "CreateQueue": {"QueueName": "probe-test-queue"},
            "GetQueueUrl": {"QueueName": "probe-test-queue"},
        },
        "s3": {
            "CreateBucket": {"Bucket": "probe-test-bucket"},
        },
        "sns": {
            "CreateTopic": {"Name": "probe-test-topic"},
        },
        "sts": {
            "GetCallerIdentity": {},
        },
        # connect's evaluation-form ops: the full-shape walk hits the tagged
        # union + nested question tree botocore validates strictly, so seed the
        # smallest structurally valid fill (as if auto-filled: first enum,
        # filler-style "probe-*-id" ids).
        "connect": {
            "CreateEvaluationForm": {
                "InstanceId": "probe-instance-id-id",
                "Title": "probe-test",
                "Items": [
                    {
                        "Question": {
                            "Title": "probe-test",
                            "RefId": "probe-test",
                            "QuestionType": "TEXT",
                        },
                    }
                ],
            },
            "UpdateEvaluationForm": {
                "InstanceId": "probe-instance-id-id",
                "EvaluationFormId": "probe-evaluation-form-id-id",
                "EvaluationFormVersion": 1,
                "Title": "probe-test",
                "Items": [
                    {
                        "Question": {
                            "Title": "probe-test",
                            "RefId": "probe-test",
                            "QuestionType": "TEXT",
                        },
                    }
                ],
            },
        },
        # fsx ids are length/pattern-strict (fsvol-0123456789abcdef0 etc.);
        # auto-fill's generic probe ids fail botocore's client-side validation,
        # so these twin-501 ops never reached the server and were bucketed
        # needs_params. Seed pattern-valid ids so the probe contacts the twin
        # and records the actual NotImplemented 501 (verified 2026-10-25).
        "fsx": {
            "RestoreVolumeFromSnapshot": {
                "VolumeId": "fsvol-0123456789abcdef0",
                "SnapshotId": "fssnap-0123456789abcdef0",
            },
            "UpdateVolume": {"VolumeId": "fsvol-0123456789abcdef0"},
            "CopySnapshotAndUpdateVolume": {
                "VolumeId": "fsvol-0123456789abcdef0",
                "SourceSnapshotARN": (
                    "arn:aws:fsx:us-east-1:123456789012:snapshot/fssnap-0123456789abcdef0"
                ),
            },
            "CreateSnapshot": {
                "Name": "probe-snapshot-name",
                "VolumeId": "fsvol-0123456789abcdef0",
            },
            "DeleteVolume": {"VolumeId": "fsvol-0123456789abcdef0"},
        },
    }
)

_ENUM_FALLBACK = "probe-test"
_FAKE_ACCOUNT = "123456789012"
_DATE_FLOOR = "2024-01-01T00:00:00Z"
_MAX_DEPTH = 8


def to_snake_case(name: str) -> str:
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def build_fake_arn(service: str, resource_type: str = "thing") -> str:
    return f"arn:aws:{service}:us-east-1:{_FAKE_ACCOUNT}:{resource_type}/probe-test"


def build_fake_bucket(root_name: str = "bucket") -> str:
    root = re.sub(r"[^a-z0-9]", "", root_name)[:20] or "bucket"
    return f"probe-{root}-bucket"


def arn_resource_kind(shape_name: str, member_name: str) -> str:
    sn = shape_name.lower()
    lo = member_name.lower()
    for token, kind in (
        ("certificate", "certificate"),
        ("lambda", "function"),
        ("function", "function"),
        ("table", "table"),
        ("queue", "queue"),
        ("vault", "vault"),
        ("secret", "secret"),
        ("policy", "policy"),
        ("instance", "instance"),
        ("vpc", "vpc"),
        ("key", "key"),
    ):
        if token in sn or token in lo:
            return kind
    return "thing"


def _pattern_string(pattern: str, member_name: str) -> str | None:
    """Generate a string satisfying common botocore `pattern` regexes:
    prefixed ids, uuids, short alnum tokens, versioned names."""
    if not pattern:
        return None
    p = pattern.strip().lstrip("^").rstrip("$")
    # <prefix>-<alnum-id> like s-0123abcd
    m = re.fullmatch(r"([A-Za-z]+)-\\?[0-9a-zA-Z\\{\}\[\]\-|\+\.]+\$?", p)
    if m and re.search(r"[\\\[\]\{\}\-]", p[len(m.group(1)) + 1 :]):
        return f"{m.group(1)}-0123456789abcdef"
    # uuid-like
    if re.search(r"8-0-4-89", p.replace("[", "{")) and False:
        pass
    if re.search(r"[0-9a-fA-F]+\}-[0-9a-f]\{4", p) or re.search(r"\}\{", p):
        # uuid-ish alternation
        h = "0123456789abcdef"
        return f"{h[:8]}-{h[0:4]}-4{h[:3]}-a{h[:3]}-{h[:12]}"
    if re.search(r"/\?arn:aws", p) or "arn:" in p:
        return None
    if re.fullmatch(r"[A-Za-z0-9_.:/-]+", p):
        return p[:24]
    # simple [0-9a-f]{n} blocks
    m2 = re.fullmatch(r"[0-9a-fA-F]{32}", p)
    if m2:
        return "0123456789abcdef" * 2
    return None


def _string_value(member_shape, member_name: str, service: str) -> str:
    enum = getattr(member_shape, "enum", None)
    if enum:
        return enum[0]
    pattern = getattr(member_shape, "pattern", None)
    if pattern:
        got = _pattern_string(pattern, member_name)
        if got is not None:
            return got
    shape_name = (getattr(member_shape, "name", "") or "").lower()
    low = (member_name or "").lower()

    if "tablename" in low:
        return "probe-table-name"
    if "arn" in low or "arn" in shape_name:
        return build_fake_arn(service, arn_resource_kind(shape_name, low))
    if "bucket" in low or shape_name.endswith("bucketname") or shape_name == "bucket":
        return build_fake_bucket(low.replace("bucket", "") or "bucket")
    if "queueurl" in low.replace("-", ""):
        return f"http://localhost:4566/{_FAKE_ACCOUNT}/probe-queue"
    if low.endswith("url") or "url" in low:
        return "http://localhost:4566/probe"
    if low.replace("-", "").endswith("keyid") or shape_name.endswith("KeyId"):
        return f"arn:aws:kms:us-east-1:{_FAKE_ACCOUNT}:key/00000000-0000-0000-0000-000000000000"
    if "token" in low or shape_name.endswith("token"):
        return f"probe-token-{abs(hash(member_name)) % 10000:04d}"
    if low.replace("-", "").endswith("clienttoken"):
        return "probe-test-client-token"
    if "email" in low:
        return "probe@robotocore.test"
    if "ipaddress" in low.replace("-", "") or low.endswith("ip"):
        return "203.0.113.7"
    if "cidr" in low.replace("-", "") or low.endswith("cidr"):
        return "10.0.0.0/16"
    if "role" in low and ("arn" in low or "arn" in shape_name or "role" in shape_name):
        return f"arn:aws:iam::{_FAKE_ACCOUNT}:role/probe-test-role"
    if "domainname" in low:
        return "probe-test-domain"
    if "timestamp" in low or shape_name.endswith("timestamp"):
        return _DATE_FLOOR
    if low.endswith("id"):
        return f"probe-{to_snake_case(member_name or 'id').replace('_', '-')}-id"
    v = "probe-test-value"
    # Respect declared length constraints (AWS ids are usually longer
    # than the phrase token).
    mn = getattr(member_shape, "min", None)
    mx = getattr(member_shape, "max", None)
    if mn and len(v) < mn:
        body = "0" * (int(mn) - len(v))
        v = f"probe-test-value{body}"[: int(mn)] if mx is None or mx >= int(mn) else v
    return v


def _fill_member(shape, service: str, depth: int, member_name: str = ""):
    if depth > _MAX_DEPTH:
        return None
    tn = getattr(shape, "type_name", None)

    if tn in ("integer", "long"):
        mn = shape.metadata.get("min")
        mx = shape.metadata.get("max")
        val = 1
        if mn is not None:
            val = max(val, int(mn))
        if mx is not None:
            val = min(val, int(mx))
        return val
    if tn == "boolean":
        return False
    if tn in ("double", "float"):
        return 1.0
    if tn == "timestamp":
        return _DATE_FLOOR
    if tn == "blob":
        return b"probe-test-blob"
    if tn == "string":
        v = _string_value(shape, member_name, service)
        mn = getattr(shape, "min", None)
        mx = getattr(shape, "max", None)
        if mn is not None and len(v) < int(mn):
            v = (v + "0" * int(mn))[: int(mn)] if mx is None or mx >= int(mn) else v
        if mx is not None and len(v) > int(mx):
            v = v[: int(mx)]
        return v
    if tn == "list":
        inner = getattr(shape, "member", None)
        if inner is None:
            return []
        one = _fill_member(inner, service, depth + 1, member_name)
        return None if one is None else [one]
    if tn == "map":
        val_shape = getattr(shape, "value", None)
        if val_shape is None:
            return {}
        one = _fill_member(val_shape, service, depth + 1, member_name)
        return None if one is None else {"probe-key": one}
    if tn == "structure":
        nested: dict = {}
        for n, m in shape.members.items():
            v = _fill_member(m, service, depth + 1, n)
            if v is None:
                return None
            nested[n] = v
        return nested
    return None


def fill_structure(gen_shape, service: str, depth: int = 0):
    """Shape-driven member fill on an arbitrary botocore shape."""
    return _fill_member(gen_shape, service, depth)


def auto_fill_params(service_name: str, operation_name: str) -> dict | None:
    """Shape-driven synthesis of the op's full input (all members).

    gen-2 semantics: every member is filled (not required-only) — shape
    conformance is the probe's goal; 501 / absence decisions live on the
    server, not in the filler.
    """
    session = botocore.session.get_session()
    try:
        try:
            model = session.get_service_model(service_name)
        except botocore.exceptions.UnknownServiceError:
            model = session.get_service_model(_botocore_service_name(service_name))
        op = model.operation_model(operation_name)
    except Exception:
        return None

    if op.input_shape is None:
        return {}

    return _fill_member(op.input_shape, service_name, 0, "")


def _botocore_service_name(service_name: str) -> str:
    """Map the twin's compact id to botocore's (hyphenated) name."""
    session = botocore.session.get_session()
    names = set(session.get_available_services())
    if service_name in names:
        return service_name
    compact = service_name.replace("-", "").replace("_", "").lower()
    for n in names:
        if n.replace("-", "").replace("_", "") == compact:
            return n
    return service_name


def get_params_for_operation(service_name: str, operation_name: str) -> dict | None:
    """Get params for an operation, KNOWN_PARAMS first, then shape synthesis."""
    known = KNOWN_PARAMS.get(service_name, {})
    if operation_name in known:
        return known[operation_name]
    return auto_fill_params(service_name, operation_name)


def _fill_required(shape, service: str, depth: int, member_name: str = ""):
    """Shape-driven synthesis, required members only (with enums)."""
    if depth > _MAX_DEPTH:
        return None
    tn = getattr(shape, "type_name", None)
    if tn in ("integer", "long"):
        mn = shape.metadata.get("min")
        mx = shape.metadata.get("max")
        val = 1
        if mn is not None:
            val = max(val, int(mn))
        if mx is not None:
            val = min(val, int(mx))
        return val
    if tn == "boolean":
        return False
    if tn in ("double", "float"):
        return 1.0
    if tn == "timestamp":
        return _DATE_FLOOR
    if tn == "blob":
        return b"probe-test-blob"
    if tn == "string":
        enum = getattr(shape, "enum", None)
        if enum:
            return enum[0]
        return _string_value(shape, member_name, service)
    if tn == "list":
        inner = getattr(shape, "member", None)
        if inner is None:
            return []
        one = _fill_required(inner, service, depth + 1, member_name)
        return None if one is None else [one]
    if tn == "map":
        val_shape = getattr(shape, "value", None)
        if val_shape is None:
            return {}
        one = _fill_required(val_shape, service, depth + 1, member_name)
        return None if one is None else {"probe-key": one}
    if tn == "structure":
        req = set(getattr(shape, "required_members", []))
        nested = {}
        for n, m in shape.members.items():
            if n not in req:
                continue
            v = _fill_required(m, service, depth + 1, n)
            if v is None:
                return None
            nested[n] = v
        return nested
    return None


def auto_fill_params_required_only(service_name: str, operation_name: str) -> dict | None:
    """Required-member-only synthesis, for stage-2 probe retries."""
    session = botocore.session.get_session()
    try:
        model = session.get_service_model(_botocore_service_name(service_name))
        op = model.operation_model(operation_name)
    except Exception:
        return None
    if op.input_shape is None:
        return {}
    return _fill_required(op.input_shape, service_name, 0, "")


def get_required_params(service_name: str, operation_name: str) -> dict | None:
    """Required-member-only synthesis (stage-2 probe retries)."""
    known = KNOWN_PARAMS.get(service_name, {})
    if operation_name in known:
        return known[operation_name]
    return auto_fill_params_required_only(service_name, operation_name)
