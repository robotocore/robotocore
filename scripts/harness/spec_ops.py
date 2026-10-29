#!/usr/bin/env python3
"""Spec-driven operation analysis for the Rust port.

Reads botocore service-2.json specs and generates:
1. Operation list with HTTP method, path, protocol
2. Coverage report (which ops are implemented in Rust)
3. Handler stubs for unimplemented ops

Usage:
    spec_ops.py --service s3 [--json] [--stubs DIR]
    spec_ops.py --service sqs --implemented CreateQueue,SendMessage
"""

import argparse
import gzip
import json
import sys
from pathlib import Path

BOTOCORE_DATA = Path(__file__).parent.parent.parent / ".venv/lib/python3.14/site-packages/botocore/data"

def load_spec(service):
    svc_dir = BOTOCORE_DATA / service
    if not svc_dir.exists():
        print(f"Error: service {service} not found", file=sys.stderr)
        sys.exit(1)
    versions = sorted([d for d in svc_dir.iterdir() if d.is_dir()], reverse=True)
    if not versions:
        print(f"Error: no versions for {service}", file=sys.stderr)
        sys.exit(1)
    spec_file = versions[0] / "service-2.json.gz"
    if not spec_file.exists():
        spec_file = versions[0] / "service-2.json"
    if spec_file.suffix == ".gz":
        with gzip.open(spec_file) as f:
            return json.load(f)
    return json.load(open(spec_file))

def get_operations(spec):
    ops = {}
    for name, op in spec.get("operations", {}).items():
        http = op.get("http", {})
        ops[name] = {
            "name": name,
            "method": http.get("method", "?"),
            "path": http.get("requestUri", "/"),
            "protocol": spec.get("metadata", {}).get("protocol", "?"),
            "errors": [e.get("code", "?") for e in op.get("errors", [])],
            "deprecated": op.get("deprecated", False),
        }
    return ops

def name_to_fn(name):
    result = []
    for i, c in enumerate(name):
        if c.isupper() and i > 0:
            result.append("_")
        result.append(c.lower())
    return "".join(result)

def coverage_report(service, implemented_ops):
    spec = load_spec(service)
    ops = get_operations(spec)
    total = len(ops)
    implemented = [n for n in ops if n in implemented_ops]
    not_impl = [n for n in ops if n not in implemented_ops]
    print(f"\n{'='*60}")
    print(f"Service: {service}")
    print(f"Protocol: {spec['metadata'].get('protocol', '?')}")
    print(f"Total: {total}  Implemented: {len(implemented)}  Missing: {len(not_impl)}")
    print(f"{'='*60}")
    if implemented:
        print(f"\nImplemented:")
        for n in sorted(implemented):
            o = ops[n]
            print(f"  OK  {n:40s} {o['method']:6s} {o['path']}")
    if not_impl:
        print(f"\nNot implemented:")
        for n in sorted(not_impl):
            o = ops[n]
            dep = " [DEPRECATED]" if o["deprecated"] else ""
            print(f"  ... {n:40s} {o['method']:6s} {o['path']}{dep}")
    return ops

def generate_stubs(service, implemented_ops, output_dir):
    spec = load_spec(service)
    ops = get_operations(spec)
    protocol = spec["metadata"].get("protocol", "query")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stubs = []
    for name, op in sorted(ops.items()):
        if name in implemented_ops or op["deprecated"]:
            continue
        fn = name_to_fn(name)
        stubs.append((name, op, fn))
    stub_file = output_dir / f"{service}_stubs.rs"
    with open(stub_file, "w") as f:
        f.write(f"// Auto-generated stubs for {service} ({protocol} protocol)\n\n")
        for name, op, fn in stubs:
            f.write(f"// {name}: {op['method']} {op['path']}\n")
            f.write(f"//   errors: {', '.join(op['errors'][:5])}\n")
            f.write(f"//   fn {fn}(&self, req: &AwsRequest) -> AwsResponse {{\n")
            f.write(f"//       AwsResponse::error(400, \"NotImplemented\", \"{name} not implemented\")\n")
            f.write(f"//   }}\n\n")
    print(f"\nWrote {len(stubs)} stubs to {stub_file}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", required=True)
    parser.add_argument("--implemented", default="")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--stubs", default=None)
    args = parser.parse_args()
    implemented = [s.strip() for s in args.implemented.split(",") if s.strip()]
    if args.json:
        spec = load_spec(args.service)
        print(json.dumps(get_operations(spec), indent=2))
        return
    coverage_report(args.service, implemented)
    if args.stubs:
        generate_stubs(args.service, implemented, args.stubs)

if __name__ == "__main__":
    main()
