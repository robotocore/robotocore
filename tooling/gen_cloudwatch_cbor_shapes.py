#!/usr/bin/env python3
"""Generate `crates/cloudwatch/src/cbor_shapes.json` from botocore's
cloudwatch service model.

Smithy RPC v2 CBOR responses carry timestamps as tag-1 epoch values and blobs
as byte strings, and SDKs decode doubles and integers by CBOR major type. The
handler answers in AWS JSON 1.0 (RFC 3339 timestamp strings, base64 blobs,
whatever number type serde_json picked), so the CBOR encoder walks each
operation's output shape to restore the wire types.

Emits `{"ops": {Operation: node}}` where a node is `{"t": type}` plus
`"m": {member: node}` (structure), `"e": node` (list member) or `"v": node`
(map value). Leaves that encode the same in both protocols (string, boolean)
are omitted, and containers left with nothing to convert are dropped, so an
absent member means "encode generically".

Also emits the error shapes. CloudWatch is `awsQueryCompatible`: an RPC v2
error names the error *shape* in `__type` and carries the query error code in
`x-amzn-query-error: <code>;<Sender|Receiver>`; SDKs pick the exception class
from the shape and report the header's code. The handler raises query codes,
and one code can belong to several shapes (`ResourceNotFound` is both
`DashboardNotFoundError` and `ResourceNotFound`), so each operation lists the
shapes it can raise: `"errors": {Shape: {"code", "sender"}}` and
`"op_errors": {Operation: [Shape, ...]}`.

Usage:
    uv run python tooling/gen_cloudwatch_cbor_shapes.py
"""

from __future__ import annotations

import json
from pathlib import Path

import botocore.session

OUT = Path(__file__).resolve().parents[1] / "crates/cloudwatch/src/cbor_shapes.json"

# Leaf types whose CBOR encoding depends on the model.
TYPED_LEAVES = {"timestamp", "blob", "double", "float", "integer", "long"}


def node(shape, seen: frozenset[str]) -> dict | None:
    t = shape.type_name
    if t in TYPED_LEAVES:
        return {"t": t}
    if shape.name in seen:
        return None
    seen = seen | {shape.name}
    if t == "structure":
        members = {}
        for name, member in shape.members.items():
            n = node(member, seen)
            if n is not None:
                members[name] = n
        return {"t": t, "m": members} if members else None
    if t == "list":
        n = node(shape.member, seen)
        return {"t": t, "e": n} if n is not None else None
    if t == "map":
        n = node(shape.value, seen)
        return {"t": t, "v": n} if n is not None else None
    return None


def main() -> None:
    model = botocore.session.get_session().get_service_model("cloudwatch")
    ops = {}
    op_errors = {}
    for name in sorted(model.operation_names):
        op = model.operation_model(name)
        out = op.output_shape
        n = node(out, frozenset()) if out is not None else None
        if n is not None:
            ops[name] = n
        if op.error_shapes:
            op_errors[name] = sorted(e.name for e in op.error_shapes)
    errors = {}
    for shape in model.error_shapes:
        meta = shape.metadata.get("error", {})
        errors[shape.name] = {
            "code": meta.get("code", shape.name),
            "sender": bool(meta.get("senderFault", False)),
        }
    doc = {"ops": ops, "errors": errors, "op_errors": op_errors}
    OUT.write_text(json.dumps(doc, indent=1, sort_keys=True) + "\n")
    print(f"wrote {OUT} ({len(ops)} operations)")


if __name__ == "__main__":
    main()
