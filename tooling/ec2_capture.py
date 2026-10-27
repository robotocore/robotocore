"""Capture moto's EC2 responses for every op the compat suite calls.

Offline dev tool (not part of CI): parses tests/compatibility/test_ec2_compat.py,
extracts ec2 client calls with their statically-derivable kwargs, and replays
them in file order against the moto sidecar. Writes per-call JSON records so
the Rust port can (1) mirror exact response shapes for stateless ops and
(2) diff its own responses against moto's.

Usage:
    uv run python tooling/ec2_capture.py --sidecar http://127.0.0.1:4802 \
        --out tooling/ec2_capture.json.gz
"""

from __future__ import annotations

import argparse
import ast
import gzip
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

TEST_FILE = Path("tests/compatibility/test_ec2_compat.py")

# Fixture values we substitute so calls still make sense against a live sidecar.
FIXTURE_FILL = {
    "vpc_and_subnet": {},
    "snapshot": {},
    "owned_ami": {},
    "instance_id": "i-0e580ebfb7e6767f0",
}
UNIQUE_RE = re.compile(r'_unique\("([a-z]+)"\)')


def fill_uuid(_a, _k):
    return f"i-{uuid.uuid4().hex[:17]}"


def _static_value(node: ast.expr, dynamic: dict[str, object]) -> object | None:
    """Best-effort constant extraction from a test source expression."""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.List):
        items = [_static_value(e, dynamic) for e in node.elts]
        return None if any(i is None for i in items) else items
    if isinstance(node, ast.Set):
        return None
    if isinstance(node, ast.Dict):
        keys = [_static_value(k, dynamic) for k in node.keys]
        vals = [_static_value(v, dynamic) for v in node.values]
        if any(v is None for v in vals) or any(k is None for k in keys):
            return None
        return dict(zip(keys, vals))
    if isinstance(node, ast.Call):
        func = node.func
        name = getattr(func, "id", None) or getattr(func, "attr", None)
        if name == "_unique" and len(node.args) == 1:
            prefix = getattr(node.args[0], "value", "")
            if prefix:
                return dynamic.get(f"_unique:{prefix}", f"{prefix}-{uuid.uuid4().hex[:8]}")
        if name == "make_zip":  # lambda helper in other suites; skip
            return None
        fill = dynamic.get(f"call:{name}")
        return fill
    if isinstance(node, ast.Name):
        return dynamic.get(node.id)
    if isinstance(node, ast.Attribute):
        return dynamic.get(getattr(node, "attr", ""))
    return None


def collect_calls() -> list[dict]:
    """Return the ordered ec2 calls in the test file (static kwargs only)."""
    source = TEST_FILE.read_text()
    tree = ast.parse(source)
    calls: list[dict] = []
    current = {"class": "", "test": ""}

    dynamic: dict[str, object] = {}
    unique_names: dict[str, str] = {}

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            current["class"] = node.name
            continue

    # Walk in line order so args referencing earlier assignments in the same
    # test can be resolved from a tiny scope.
    def visit_stmt(body, cls, test):
        for stmt in body:
            # Track `x = "y"` / `x = _unique("k")` style locals.
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(
                stmt.targets[0], ast.Name
            ):
                val = _static_value(stmt.value, dynamic)
                if val is not None:
                    dynamic[stmt.targets[0].id] = val
            if isinstance(stmt, ast.If):
                visit_stmt(stmt.body, cls, test)
                continue
            if isinstance(stmt, ast.Try):
                for b in stmt.body + [i for h in stmt.handlers for i in h.body]:
                    visit_stmt([b], cls, test)
                continue
            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                visit_stmt(stmt.body, cls, stmt.name)
                continue
            for sub in ast.walk(stmt):
                if isinstance(sub, ast.ClassDef):
                    continue
                if isinstance(sub, ast.Call):
                    func = sub.func
                    recv = getattr(func, "value", None)
                    recv_name = getattr(recv, "id", None) or getattr(recv, "attr", None)
                    method = getattr(func, "attr", None)
                    if recv_name in ("ec2", "client") and method:
                        kwargs = {}
                        ok = True
                        for kw in sub.keywords:
                            if kw.arg is None:
                                ok = False
                                break
                            val = _static_value(kw.value, dynamic)
                            if val is None:
                                ok = False
                                break
                            kwargs[kw.arg] = val
                        if ok and sub.args:
                            ok = False
                        if ok:
                            key = f"_unique:{kwargs.get('prefix', '')}"
                            calls.append(
                                {
                                    "class": cls,
                                    "test": test,
                                    "op": method,
                                    "params": kwargs,
                                }
                            )

    for item in tree.body:
        if isinstance(item, ast.ClassDef):
            dynamic = dict(FIXTURE_FILL)
            unique_names = {}
            classifier = item.name
            for fn in item.body:
                if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    call_idx = len(calls)
                    visit_stmt(fn.body, classifier, fn.name)
                    if len(calls) == call_idx:
                        dynamic = dict(FIXTURE_FILL)
                    # Reset per-test bindings, keep the _unique vocabulary.
                    for k in list(dynamic):
                        if not k.startswith("_unique") and k not in FIXTURE_FILL:
                            dynamic.pop(k, None)
    return calls


def apply_param_typing(params: dict) -> dict:
    """botocore serializes lists as `.1` indices; keep one realistic value."""
    out = {}
    for k, v in params.items():
        if isinstance(v, list):
            if v:
                out[f"{k}.1"] = v[0]
        elif isinstance(v, dict):
            # Flat dotted form for one-level dicts (TagSpecifications needs
            # nested lists; those tests are covered by dedicated fixtures).
            for ik, iv in v.items():
                out[f"{k}.{ik}"] = iv
        else:
            out[k] = v
    return out


class Sidecar:
    def __init__(self, base: str, region: str, account: str):
        self.base = base.rstrip("/")
        self.region = region
        self.account = account

    def call(self, op: str, params: dict) -> tuple[int, str]:
        form: list[tuple[str, str]] = [("Action", op), ("Version", "2016-11-15")]
        form.extend(self._flattened(op, params))
        body = urllib.parse.urlencode(form).encode()
        req = urllib.request.Request(
            self.base,
            data=body,
            method="POST",
            headers={
                "X-Robotocore-Service": "ec2",
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": (
                    "AWS4-HMAC-SHA256 Credential="
                    f"{self.account}/20240101/{self.region}/ec2/aws4_request"
                ),
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.status, resp.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()

    @staticmethod
    def _flattened(op: str, params: dict) -> list[tuple[str, str]]:
        """Approximate botocore's EC2 serialization for the shapes we replay."""
        out: list[tuple[str, str]] = []
        for key, val in params.items():
            base_key = key.split(".")[0]
            if isinstance(val, list):
                for i, item in enumerate(val, 1):
                    if isinstance(item, dict):
                        out.extend(
                            Sidecar._flattened(
                                f"{base_key}.{i}", {f"{base_key}.{i}.{k}": item[k] for k in item}
                            )
                        )
                    else:
                        out.append((f"{key}.{i}", str(item)))
            elif isinstance(val, dict):
                for k, v in val.items():
                    sub = f"{key}.{k}"
                    if isinstance(v, list):
                        for i, item in enumerate(v, 1):
                            if isinstance(item, dict):
                                out.extend(
                                    Sidecar._flattened(
                                        sub, {f"{sub}.{i}.{sk}": item[sk] for sk in item}
                                    )
                                )
                            else:
                                out.append((f"{sub}.{i}", str(item)))
                    elif isinstance(v, dict):
                        out.extend(Sidecar._flattened(sub, {f"{sub}.{sk}": v[sk] for sk in v}))
                    else:
                        out.append((sub, str(v)))
            else:
                out.append((key, str(val)))
        return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sidecar", default="http://127.0.0.1:4802")
    ap.add_argument("--out", default="tooling/ec2_capture.json.gz")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--account", default="123456789012")
    ap.add_argument("--limit", type=int, default=0, help="only capture the first N tests")
    args = ap.parse_args()

    calls = collect_calls()
    print(f"collected {len(calls)} calls", flush=True)

    sidecar = Sidecar(args.sidecar, args.region, args.account)
    records: list[dict] = []
    for idx, call in enumerate(calls, 1):
        params = apply_param_typing(call["params"])
        # boto3 methods are snake_case; wire Actions are PascalCase.
        action = re.sub(r"_(\w)", lambda m: m.group(1).upper(), call["op"])
        action = action[0].upper() + action[1:]
        try:
            status, xml = sidecar.call(action, params)
        except Exception as e:  # noqa: BLE001
            status, xml = 599, f"<transport>{e}</transport>"
        records.append(
            {
                **call,
                "action": action,
                "params": params,
                "status": status,
                "xml": xml,
            }
        )
        if idx % 50 == 0:
            print(f"  {idx}/{len(calls)}", flush=True)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "wt") as fh:
        json.dump(records, fh)
    uniq_ops = len({r["action"] for r in records})
    print(f"captured {len(records)} calls over {uniq_ops} ops -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
