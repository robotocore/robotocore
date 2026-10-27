#!/usr/bin/env python3
"""Generate spec-correct operation stubs from botocore specs.

Usage:
    python scripts/harness/gen_ops.py --service ssm
    python scripts/harness/gen_ops.py --service iam --ops ListUsers,GetRole
    python scripts/harness/gen_ops.py --service ssm --protocol json

For each operation, reads the botocore output shape and generates
a Rust handler method that returns a spec-conforming response
with default values for all fields.
"""

import argparse
import gzip
import json
import os
import re
import sys

import botocore

def find_spec(service: str) -> dict:
    """Load the botocore service spec."""
    data_dir = os.path.join(os.path.dirname(botocore.__file__), "data")
    svc_dir = os.path.join(data_dir, service)
    if not os.path.exists(svc_dir):
        print(f"Error: service '{service}' not found in botocore data", file=sys.stderr)
        sys.exit(1)
    # Find the version directory
    versions = os.listdir(svc_dir)
    version = sorted(versions)[-1]  # latest version
    spec_path = os.path.join(svc_dir, version, "service-2.json.gz")
    with gzip.open(spec_path, "rt") as f:
        return json.load(f)

def default_value_for_shape(shape: dict, shapes: dict) -> str:
    """Generate a default Rust value for a shape."""
    stype = shape.get("type", "string")

    if stype == "structure":
        members = shape.get("members", {})
        if not members:
            return "json!({})"
        entries = []
        for name, info in members.items():
            sub_shape = shapes.get(info.get("shape", ""), {})
            # Use the wire name (locationName for query protocol, else member name)
            wire_name = info.get("locationName", name)
            val = default_value_for_shape(sub_shape, shapes)
            entries.append(f'"{wire_name}": {val}')
        if entries:
            return "json!({" + ", ".join(entries) + "})"
        return "json!({})"

    elif stype == "list":
        return "json!([])"

    elif stype == "map":
        return "json!({})"

    elif stype == "timestamp":
        return "Value::Null"

    elif stype == "boolean":
        return "false"

    elif stype == "integer" or stype == "long":
        return "0"

    elif stype == "double":
        return "0.0"

    elif stype == "blob":
        return "Value::Null"

    else:  # string
        return '""'

def generate_stub(op_name: str, spec: dict, protocol: str) -> str:
    """Generate a Rust stub for an operation."""
    op = spec["operations"].get(op_name)
    if not op:
        return f"// {op_name}: no spec found"

    output_shape_name = op.get("output", {}).get("shape")
    if not output_shape_name:
        # No output shape - return empty
        if protocol in ("json", "rest-json"):
            return f'AwsResponse::json(200, json!({{}}))'
        else:
            return f'AwsResponse::xml(200, String::new())'

    output_shape = spec["shapes"].get(output_shape_name, {})

    if protocol in ("json", "rest-json"):
        # JSON protocol: return the output shape directly
        body = default_value_for_shape(output_shape, spec["shapes"])
        return f"AwsResponse::json(200, {body})"

    elif protocol == "query":
        # Query protocol: wrap in <OpResponse><OpResult>...</OpResult></OpResponse>
        members = output_shape.get("members", {})
        if not members:
            return f'AwsResponse::query_success("{op_name}", String::new())'

        body_parts = []
        for name, info in members.items():
            wire_name = info.get("locationName", name)
            sub_shape = spec["shapes"].get(info.get("shape", ""), {})
            stype = sub_shape.get("type", "string")
            if stype == "list":
                member_shape_name = sub_shape.get("member", {}).get("shape", "")
                member_shape = spec["shapes"].get(member_shape_name, {})
                member_members = member_shape.get("members", {})
                if member_members:
                    inner_parts = []
                    for mn, mi in member_members.items():
                        mi_shape = spec["shapes"].get(mi.get("shape", ""), {})
                        mi_stype = mi_shape.get("type", "string")
                        if mi_stype in ("integer", "long"):
                            inner_parts.append("<" + mn + ">0</" + mn + ">")
                        elif mi_stype == "boolean":
                            inner_parts.append("<" + mn + ">false</" + mn + ">")
                        else:
                            inner_parts.append("<" + mn + "/>")
                    body_parts.append("<" + wire_name + "><member>" + " ".join(inner_parts) + "</member></" + wire_name + ">")
                else:
                    body_parts.append("<" + wire_name + "><member/></" + wire_name + ">")
            elif stype == "structure":
                body_parts.append("<" + wire_name + "/>")
            elif stype == "boolean":
                body_parts.append("<" + wire_name + ">false</" + wire_name + ">")
            elif stype in ("integer", "long", "double"):
                body_parts.append("<" + wire_name + ">0</" + wire_name + ">")
            else:
                body_parts.append("<" + wire_name + "/>")

        body_xml = " ".join(body_parts)
        return f'AwsResponse::query_success("{op_name}", "{body_xml}")'

    else:
        # Fallback: empty JSON
        return f"AwsResponse::json(200, json!({{}}))"

def main():
    parser = argparse.ArgumentParser(description="Generate spec-correct operation stubs")
    parser.add_argument("--service", required=True, help="AWS service name (e.g., ssm, iam)")
    parser.add_argument("--ops", help="Comma-separated list of operations (default: all)")
    parser.add_argument("--output", help="Output file (default: stdout)")
    parser.add_argument("--rust", action="store_true", help="Generate Rust match arms")
    args = parser.parse_args()

    spec = find_spec(args.service)
    protocol = spec["metadata"]["protocol"]
    print(f"# Service: {args.service} (protocol: {protocol})", file=sys.stderr)

    ops = list(spec["operations"].keys())
    if args.ops:
        ops = [o.strip() for o in args.ops.split(",")]

    lines = []
    for op_name in sorted(ops):
        stub = generate_stub(op_name, spec, protocol)
        if args.rust:
            lines.append(f'    "{op_name}" => {{ // {protocol}')
            lines.append(f'        {stub}')
            lines.append('    },')
        else:
            lines.append(f"# {op_name} ({protocol})")
            lines.append(stub)
            lines.append("")

    output = "\n".join(lines)
    if args.output:
        with open(args.output, "w") as f:
            f.write(output + "\n")
        print(f"Written {len(ops)} stubs to {args.output}", file=sys.stderr)
    else:
        print(output)

if __name__ == "__main__":
    main()
