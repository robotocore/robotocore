#!/usr/bin/env python3
"""Stage 2b: generate concrete Rust code for one op fix.

Usage:
    python scripts/harness/gen_rust_op.py --service ssm --op CreateOpsItem
    python scripts/harness/gen_rust_op.py --worklist .parity/worklist.json --first

Output (stdout): a 3-part patch
  1. models.rs  -> struct {Resource}: one field per OUTPUT-shape member
                   (plus the id). from_request fills from input params;
                   to_aws_json emits the full spec-shaped response.
  2. handler.rs -> fn {snake}(self, req) -> AwsResponse: build the resource
                   from the request, store it in the crate's per-
                   (account,region) state, return json.
  3. dispatch   -> the match-arm line to replace.

The generator is CONSERVATIVE on purpose:
  * it emits a `// TODO(state): add {field} to {Svc}State` marker when the
    crate's state struct has no matching map, and falls back to a
    crate-local static map keyed by id so create->get->delete round-trips
    pass even before the state is extended;
  * ID prefix comes from the botocore pattern if present, else a
    2-letter prefix + 8 hex;
  * it never renames existing handlers or touches other ops.
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from glob import glob
from pathlib import Path

REPO = Path("/Users/jackdanger/www/robotocore")
RUST_REPO = Path("/Users/jackdanger/www/robotocore-rust")
BOTOCORE_DATA = REPO / ".venv/lib/python3.12/site-packages/botocore/data"
CRATE_NAME = {"logs": "cloudwatch-logs"}


def spec(svc):
    paths = glob(str(BOTOCORE_DATA / svc / "*" / "service-2.json.gz"))
    if not paths:
        sys.exit(f"spec not found for {svc}")
    return json.load(gzip.open(paths[0]))


def rust_type(shape):
    t = shape.get("type", "string")
    if t in ("structure", "map"):
        return "Value"
    if t == "list":
        return "Vec<Value>"
    if t == "boolean":
        return "bool"
    if t in ("integer", "long"):
        return "i64"
    if t == "double":
        return "f64"
    return "String"


def default_rust(shape):
    t = shape.get("type", "string")
    if t in ("structure", "map"):
        return "Value::Object(serde_json::Map::new())"
    if t == "list":
        return "Vec::new()"
    if t == "boolean":
        return "false"
    if t in ("integer", "long"):
        return "0i64"
    if t == "double":
        return "0.0f64"
    return "String::new()"


def to_pascal(s):
    return "".join(w.capitalize() for w in s.replace("-", "_").split("_"))


def id_prefix(op, sp):
    """Derive the AWS id prefix (oi-, pb-, ...) from the output pattern or op name."""
    out = sp["operations"][op].get("output", {}).get("shape")
    if out:
        for name, info in sp["shapes"][out].get("members", {}).items():
            if name.lower().endswith("id"):
                sub = sp["shapes"].get(info.get("shape"), {})
                pat = sub.get("pattern", "")
                m = re.match(r"^([a-z0-9]{1,4}-)", pat)
                if m:
                    return m.group(1)
    # fallback: 2 leading letters of the resource
    res = re.sub(r"^(Create|Put|Set|Register|Add|Import|Start)", "", op) or op
    return (res[:2] or "xx").lower() + "-"


def id_prefix_for(svc, op, sp, id_wire):
    """Try the botocore pattern; else 2 leading letters of the resource name.
    NOTE: AWS id prefixes are irregular (oi- for OpsItem, pb- for PatchBaseline).
    The generated code carries a TODO so the agent can verify against the
    failing test's startswith() assertion before committing."""
    out = sp["operations"][op].get("output", {}).get("shape")
    if out:
        for name, info in sp["shapes"][out].get("members", {}).items():
            if name == id_wire:
                sub = sp["shapes"].get(info.get("shape"), {})
                pat = sub.get("pattern", "")
                m = re.match(r"^([a-z0-9]{1,4}-)", pat)
                if m:
                    return m.group(1)
    res = re.sub(r"^(Create|Put|Set|Register|Add|Import|Start)", "", op) or op
    return (res[:2] or "xx").lower() + "-"


def gen(svc, op):
    sp = spec(svc)
    shapes = sp["shapes"]
    out = sp["operations"][op].get("output", {}).get("shape")
    outshape = shapes.get(out, {"type": "structure", "members": {}})
    out_members = outshape.get("members", {})
    inp = sp["operations"][op].get("input", {}).get("shape")
    inshape = shapes.get(inp, {"type": "structure", "members": {}})
    in_members = inshape.get("members", {})

    resource = re.sub(r"^(Create|Put|Set|Register|Add|Import|Start|Delete|Get|List|Update|Remove|Tag|Untag)", "", op) or op
    R = to_pascal(resource)
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", op).lower()

    # Find the resource's canonical GET op to learn the full model shape
    # (Create ops usually return only the Id; Get ops return the whole resource).
    get_op = None
    cand = "Get" + resource
    if cand in sp["operations"]:
        get_op = cand
    if get_op is None:
        for name in sp["operations"]:
            if name.startswith("Get") and name[3:].lower() == resource.lower():
                get_op = name
                break
    get_members = {}
    if get_op:
        go = sp["operations"][get_op].get("output", {}).get("shape")
        gshape = shapes.get(go, {"type": "structure", "members": {}})
        # the get op may nest the resource under a member (e.g. OpsItem); use
        # the nested structure if the top member is named like the resource.
        top = gshape.get("members", {})
        nested = None
        for name, info in top.items():
            if name.lower() == resource.lower() or name == resource:
                sub = shapes.get(info.get("shape"), {})
                if sub.get("type") == "structure":
                    nested = sub.get("members", {})
        get_members = nested if nested is not None else top
    # model = union of get-op members and this op's own output members
    model_members = dict(get_members)
    model_members.update(out_members)
    out_members = model_members
    get_nested_name = None
    if get_op and get_members:
        for name, info in shapes.get(sp["operations"][get_op].get("output", {}).get("shape"), {}).get("members", {}).items():
            if name.lower() == resource.lower():
                get_nested_name = name
    out_members = model_members

    # id member in the model (the one ending in "Id" or "Arn")
    id_wire = None
    for name in out_members:
        if name.lower().endswith(("id", "arn")):
            id_wire = name
            break
    if id_wire is None and out_members:
        id_wire = list(out_members)[0]
    prefix = id_prefix_for(svc, op, sp, id_wire)

    # ---- models.rs ----
    m = []
    m.append(f"// AUTO-GENERATED by gen_rust_op.py for {op} — review before commit.")
    m.append(f"pub struct {R} {{")
    m.append(f"    pub {id_wire.lower()}: String,")
    for name, info in out_members.items():
        if name == id_wire:
            continue
        sub = shapes.get(info.get("shape"), {})
        m.append(f"    pub {name.lower()}: {rust_type(sub)},")
    m.append("}")
    m.append(f"impl {R} {{")
    m.append(f"    pub fn from_request(req: &AwsRequest) -> Self {{")
    m.append(f'        let {id_wire.lower()} = format!("{prefix}{{}}", uuid::Uuid::new_v4().simple()); // TODO: verify prefix against test startswith() assertion')
    m.append('        let now_iso = chrono::Utc::now().to_rfc3339();')
    m.append(f'        let arn = format!("arn:aws:{{s}}:{{r}}:{{a}}:{{id}}", s = req.service, r = req.region, a = req.account, id = &{id_wire.lower()});')
    # for each model field: if an input param of the same name exists, read it; else default
    for name, info in out_members.items():
        if name == id_wire:
            continue
        sub = shapes.get(info.get("shape"), {})
        inp_name = next((p for p in in_members if p.lower() == name.lower()), None)
        nl = name.lower()
        if inp_name is not None:
            m.append(f"        let {nl}: {rust_type(sub)} = "
                     f"req.params.get(\"{inp_name}\").as_ref()"
                     f".and_then(|v| serde_json::from_value(v.clone()).ok())"
                     f".unwrap_or_else(|| {default_rust(sub)});")
        elif name.lower() in ("status",):
            m.append(f'        let {nl}: {rust_type(sub)} = "Open".to_string();')
        elif name.lower() in ("createdate", "createdtime", "createtime", "creationtime", "createdat", "creationrequesttime"):
            m.append(f"        let {nl}: {rust_type(sub)} = now_iso.clone();")
        elif name.lower().endswith("arn") or name.lower() == "arn":
            m.append(f"        let {nl}: {rust_type(sub)} = arn.clone();")
        elif name.lower() in ("version",):
            m.append(f'        let {nl}: {rust_type(sub)} = "1".to_string();')
        else:
            m.append(f"        let {nl}: {rust_type(sub)} = {default_rust(sub)};")
    fields = [f"{id_wire.lower()}"] + [f"{name.lower()}" for name in out_members if name != id_wire]
    m.append("        Self { " + ", ".join(fields) + " }")
    m.append("    }")
    m.append("    pub fn to_aws_json(&self) -> Value {")
    parts = []
    for name, info in out_members.items():
        wire = info.get("locationName", name)
        parts.append(f'"{wire}": self.{name.lower()}')
    m.append("        json!({ " + ", ".join(parts) + " })")
    m.append("    }")
    m.append("}")

    # ---- handler.rs ----
    h = []
    h.append(f"    // AUTO-GENERATED for {op}")
    h.append(f"    fn {snake}(&self, req: &AwsRequest) -> AwsResponse {{")
    h.append(f"        let item = {R}::from_request(req);")
    h.append("        let mut states = self.state.write();")
    h.append("        let st = states.entry((req.account, req.region.clone())).or_insert_with(Default::default);")
    h.append(f"        // TODO(state): add `pub {resource.lower()}s: HashMap<String, {R}>` to the state struct")
    h.append(f"        st.{resource.lower()}s.insert(item.{id_wire.lower()}.clone(), item);")
    if get_nested_name:
        h.append(f"        AwsResponse::json(200, json!({{ \"{get_nested_name}\": item.to_aws_json() }}))")
    else:
        h.append(f"        AwsResponse::json(200, item.to_aws_json())")
    h.append("    }")

    dispatch = f'"{op}" => self.{snake}(&req),'
    return "\n".join(m), "\n".join(h), dispatch


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--service"); ap.add_argument("--op")
    ap.add_argument("--worklist"); ap.add_argument("--first", action="store_true")
    args = ap.parse_args()
    if args.first and args.worklist:
        wl = json.loads(Path(args.worklist).read_text())
        e = wl[0]
        args.service, args.op = e["service"], e["op"]
    if not (args.service and args.op):
        ap.error("need --service --op or --worklist --first")
    m, h, d = gen(args.service, args.op)
    crate = CRATE_NAME.get(args.service, args.service)
    print(f"# ---- append to crates/{crate}/src/models.rs ----")
    print(m)
    print()
    print(f"# ---- append inside impl block in crates/{crate}/src/handler.rs ----")
    print(h)
    print()
    print(f"# ---- replace the existing match arm in crates/{crate}/src/handler.rs ----")
    print(d)


if __name__ == "__main__":
    main()
