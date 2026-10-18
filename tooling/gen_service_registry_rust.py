#!/usr/bin/env python3
"""Regenerate src/core/service_registry.rs from the Python service registry.

The Rust server's /_robotocore/services and /_robotocore/health endpoints
report the same catalog as the Python server. Their entries must stay in
lockstep: run this any time src/robotocore/services/registry.py changes.

    uv run python tooling/gen_service_registry_rust.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
REGISTRY = REPO / "src/robotocore/services/registry.py"
OUT = REPO / "src/core/service_registry.rs"


def rust_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')


def extract() -> list[tuple[str, str, str, str]]:
    src = REGISTRY.read_text()
    services = []
    for m in re.finditer(r'"([a-z0-9\-]+)":\s*ServiceInfo\(', src):
        name = m.group(1)
        start = m.end()
        depth, i = 1, start
        while depth > 0 and i < len(src):
            if src[i] == "(":
                depth += 1
            elif src[i] == ")":
                depth -= 1
            i += 1
        body = src[start:i]
        status = re.search(r"ServiceStatus\.([A-Z_]+)", body)
        strings = re.findall(r'"((?:[^"\\]|\\.)*)"', body)
        if not status or len(strings) < 3:
            print(f"skipping {name}: parse miss", file=sys.stderr)
            continue
        stype = "native" if status.group(1) == "NATIVE" else "moto"
        services.append((name, stype, strings[1], strings[2].replace('\\"', '"')))
    return sorted(services)


def main() -> None:
    services = extract()
    if not services:
        raise SystemExit("no services extracted; aborting")
    rows = "\n".join(
        f'    ("{n}", "{t}", "{p}", "{rust_escape(d)}"),' for n, t, p, d in services
    )
    OUT.replace().write_text if False else None
    OUT.write_text(
        "//! Service registry mirroring `src/robotocore/services/registry.py`.\n"
        "//! Generated to match the Python table: name, type (native|moto), protocol,\n"
        "//! description. Regenerate with tooling/gen_service_registry_rust.py.\n\n"
        "/// (name, type, protocol, description), sorted by name.\n"
        f"pub const SERVICE_REGISTRY: &[(&str, &str, &str, &str)] = &[\n{rows}\n];\n"
    )
    print(f"wrote {OUT} ({len(services)} services)")


if __name__ == "__main__":
    main()
