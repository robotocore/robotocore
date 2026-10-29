#!/usr/bin/env python3
"""
gen_missing_stubs.py - Generate spec-shaped stubs for missing operations.
"""

import argparse
import gzip
import json
import os
import re
import sys

import botocore

def find_spec(service):
    data_dir = os.path.join(os.path.dirname(botocore.__file__), "data")
    svc_dir = os.path.join(data_dir, service)
    if not os.path.exists(svc_dir):
        print("Error: service '%s' not found" % service, file=sys.stderr)
        sys.exit(1)
    versions = os.listdir(svc_dir)
    version = sorted(versions)[-1]
    spec_path = os.path.join(svc_dir, version, "service-2.json.gz")
    with gzip.open(spec_path, "rt") as f:
        return json.load(f)

def get_output_shape(spec, op_name):
    op = spec["operations"].get(op_name, {})
    output_name = op.get("output", {}).get("shape", "")
    if not output_name:
        return {}
    return spec["shapes"].get(output_name, {})

def gen_default(shape, shapes, depth=0):
    if depth > 5:
        return 'json!({})'
    stype = shape.get("type", "string")
    if stype == "string":
        return '""'
    elif stype == "integer":
        return "0"
    elif stype == "long":
        return "0"
    elif stype == "double":
        return "0.0"
    elif stype == "boolean":
        return "false"
    elif stype == "timestamp":
        return '"1970-01-01T00:00:00Z"'
    elif stype == "list":
        return "json!([])"
    elif stype == "map":
        return "json!({})"
    elif stype == "structure":
        members = shape.get("members", {})
        if not members:
            return "json!({})"
        entries = []
        for name, info in members.items():
            sub = shapes.get(info.get("shape", ""), {})
            wire = info.get("locationName", name)
            val = gen_default(sub, shapes, depth + 1)
            entries.append('"%s": %s' % (wire, val))
        return "json!({" + ", ".join(entries) + "})"
    elif stype == "blob":
        return '""'
    else:
        return "json!(null)"

def camel_to_snake(name):
    s = re.sub('([a-z])([A-Z])', r'\1_\2', name)
    return s.lower()

def gen_stub(op_name, spec, protocol):
    shapes = spec.get("shapes", {})
    output = get_output_shape(spec, op_name)
    fn_name = camel_to_snake(op_name)

    if protocol == "query":
        members = output.get("members", {})
        xml_parts = []
        for name, info in members.items():
            sub = shapes.get(info.get("shape", ""), {})
            wire = info.get("locationName", name)
            stype = sub.get("type", "string")
            if stype in ("string",):
                xml_parts.append("<%s></%s>" % (wire, wire))
            elif stype in ("integer", "long"):
                xml_parts.append("<%s>0</%s>" % (wire, wire))
            elif stype == "boolean":
                xml_parts.append("<%s>false</%s>" % (wire, wire))
            elif stype == "timestamp":
                xml_parts.append("<%s>1970-01-01T00:00:00Z</%s>" % (wire, wire))
            elif stype == "list":
                xml_parts.append("<%s>" % wire)
                xml_parts.append("</%s>" % wire)
            elif stype == "structure":
                xml_parts.append("<%s>" % wire)
                for sname, sinfo in sub.get("members", {}).items():
                    swire = sinfo.get("locationName", sname)
                    xml_parts.append("<%s></%s>" % (swire, swire))
                xml_parts.append("</%s>" % wire)
        body = "".join(xml_parts)
        body_esc = body.replace("\\", "\\\\").replace('"', '\\"')
        return '    fn %s(&self, req: &AwsRequest) -> AwsResponse {\n        let body = "%s";\n        AwsResponse::xml(200, "%s", body)\n    }' % (fn_name, body_esc, op_name)
    else:
        val = gen_default(output, shapes)
        return '    fn %s(&self, req: &AwsRequest) -> AwsResponse {\n        AwsResponse::json(200, %s)\n    }' % (fn_name, val)

def extract_existing_ops(handler_path):
    content = open(handler_path).read()
    ops = set()
    for m in re.finditer(r'"([A-Z][A-Za-z0-9]+)"\s*=>', content):
        ops.add(m.group(1))
    return ops

def find_insert_pos(handler_path):
    content = open(handler_path).read()
    matches = list(re.finditer(r'"([A-Z][A-Za-z0-9]+)"\s*=>\s*self\.', content))
    if matches:
        return matches[-1].end()
    fb = re.search(r'(?:other|_)\s*=>', content)
    if fb:
        return fb.start()
    return -1

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--max-ops", type=int, default=50)
    args = parser.parse_args()

    spec = find_spec(args.service)
    protocol = spec.get("metadata", {}).get("protocol", "json")
    operations = spec.get("operations", {})

    handler_path = "/Users/jackdanger/www/robotocore-rust/crates/%s/src/handler.rs" % args.service
    if not os.path.exists(handler_path):
        print("Handler not found: %s" % handler_path, file=sys.stderr)
        sys.exit(1)

    existing = extract_existing_ops(handler_path)
    missing = [op for op in operations if op not in existing]

    print("Service: %s (%s)" % (args.service, protocol))
    print("Spec ops: %d, Existing: %d, Missing: %d" % (len(operations), len(existing), len(missing)))

    if not missing:
        print("No missing ops!")
        return

    stubs = []
    arms = []
    for op in missing[:args.max_ops]:
        fn = camel_to_snake(op)
        stubs.append(gen_stub(op, spec, protocol))
        arms.append('            "%s" => self.%s(&req),\n' % (op, fn))

    if args.dry_run:
        print("\nMatch arms (%d):" % len(arms))
        for a in arms[:10]:
            print(a, end="")
        if len(arms) > 10:
            print("  ... +%d more" % (len(arms) - 10))
        print("\nFirst stub:")
        print(stubs[0])
    elif args.apply:
        content = open(handler_path).read()
        pos = find_insert_pos(handler_path)
        if pos < 0:
            print("No insert position found", file=sys.stderr)
            sys.exit(1)
        new_content = content[:pos] + "".join(arms) + content[pos:]
        impl_end = new_content.rfind("\n}")
        if impl_end < 0:
            impl_end = len(new_content)
        stubs_text = "\n" + "\n".join(stubs) + "\n"
        new_content = new_content[:impl_end] + stubs_text + new_content[impl_end:]
        open(handler_path, "w").write(new_content)
        print("Applied %d arms, %d stubs" % (len(arms), len(stubs)))

if __name__ == "__main__":
    main()
