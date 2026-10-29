#!/usr/bin/env python3
"""Generate `rotocore/src/services/redshift/shapes.json` from botocore's
redshift service model (the wire spec the moto sidecar serializes against).

Emits, per operation:
  - `in`:  input members as ordered `[name, wireName, node]` triples
  - `out`: output members in the same encoding
with nodes as compact dicts `{"t": type, "m": [...members...]}`. Order is
botocore's model order — serde_json's default alphabetical maps would lose
it on the Rust side, hence triples instead of objects.

Also embeds `error_codes`: AWS error code -> HTTP status (moto honors the
status codes in the model's error shapes; most are 400, notable faults 404).
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_MAIN_REPO = Path("/home/termag/termag/projects/robotocore")
_VENV_REL = ".venv/lib/python3.13/site-packages/botocore/data/redshift/2012-12-01"
BOTO = _ROOT / _VENV_REL
if not BOTO.exists():
    BOTO = _MAIN_REPO / _VENV_REL
OUT = Path(__file__).resolve().parents[1] / "rotocore/src/services/redshift/shapes.json"

d = json.loads(gzip.open(BOTO / "service-2.json.gz", "rb").read())
NS = d["metadata"]["xmlNamespace"]
ops, shapes = d["operations"], d["shapes"]

SCALARS = {"string", "long", "integer", "boolean", "timestamp", "double", "blob"}


def wire_name(ref: dict, default: str) -> str:
    return ref.get("locationName") or ref.get("queryName") or default


def node(shape_ref: dict) -> dict:
    s = shapes[shape_ref["shape"]]
    out = {"t": s["type"]}
    if s.get("flattened"):
        out["f"] = True
    if s["type"] == "structure":
        out["m"] = [
            [k, wire_name(ref, k), node(ref)] for k, ref in s.get("members", {}).items()
        ]
    elif s["type"] == "list":
        out["i"] = {"n": wire_name(s["member"], "member"), **node(s["member"])}
    elif s["type"] == "map":
        out["k"] = {"n": wire_name(s["key"], "key"), **node(s["key"])}
        out["v"] = {"n": wire_name(s["value"], "value"), **node(s["value"])}
    return out


def to_snake(name: str) -> str:
    out = []
    for i, c in enumerate(name):
        if c.isupper() and i > 0:
            out.append("_")
        out.append(c.lower())
    return "".join(out)


result: dict = {"ns": NS, "ops": {}, "error_codes": {}}

for op_name, spec in sorted(ops.items()):
    entry: dict = {"snake": to_snake(spec["name"])}
    if "input" in spec:
        entry["in"] = node(spec["input"])["m"]
    if "output" in spec:
        entry["out"] = node(spec["output"])["m"]
    result["ops"][spec["name"]] = entry

for name, s in shapes.items():
    err = s.get("error")
    if err:
        code = err.get("code", name[:-5] if name.endswith("Fault") else name)
        result["error_codes"][code] = err.get("httpStatusCode", 400)

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(result, separators=(",", ":")))
print(f"wrote {OUT} ({OUT.stat().st_size} bytes, {len(result['ops'])} ops)")
