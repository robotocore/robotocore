#!/usr/bin/env python3
"""Generate native provider boilerplate from botocore service specs.

Reads botocore's service-2.json to generate:
- provider.py with handler stubs for all operations
- Correct protocol handling (rest-json, json, query, rest-xml, ec2)
- Request/response shapes

Usage:
    uv run python scripts/gen_provider.py lambda
    uv run python scripts/gen_provider.py stepfunctions --output-dir src/robotocore/services/stepfunctions/
"""

import json
import os
import sys
from pathlib import Path


def find_service_model(service_name: str) -> dict | None:
    """Find and load botocore service model JSON."""
    # Map common names to botocore service names
    name_map = {
        "lambda": "lambda",
        "stepfunctions": "stepfunctions",
        "events": "events",
        "eventbridge": "events",
        "sqs": "sqs",
        "sns": "sns",
        "s3": "s3",
        "dynamodb": "dynamodb",
        "kinesis": "kinesis",
        "firehose": "firehose",
        "cloudformation": "cloudformation",
        "cloudwatch": "cloudwatch",
        "logs": "logs",
        "iam": "iam",
        "sts": "sts",
        "kms": "kms",
        "secretsmanager": "secretsmanager",
        "ssm": "ssm",
        "apigateway": "apigateway",
        "ec2": "ec2",
        "ses": "ses",
        "route53": "route53",
    }

    botocore_name = name_map.get(service_name, service_name)

    # Search common locations
    search_paths = [
        # uv-managed botocore
        Path(os.path.expanduser("~/.local/share/uv")),
        # pip-installed botocore
        Path(sys.prefix) / "lib",
        # Vendored
        Path("vendor/moto"),
    ]

    for base in search_paths:
        for model_path in base.rglob(f"botocore/data/{botocore_name}/*/service-2.json"):
            try:
                return json.loads(model_path.read_text())
            except Exception:
                continue

    # Try importing botocore directly
    try:
        import botocore.loaders
        loader = botocore.loaders.Loader()
        return loader.load_service_model(botocore_name, "service-2")
    except Exception:
        return None


def get_protocol(model: dict) -> str:
    """Get the protocol from the service model metadata."""
    return model.get("metadata", {}).get("protocol", "unknown")


def get_operations(model: dict) -> list[dict]:
    """Extract operation details from service model."""
    ops = []
    for name, spec in model.get("operations", {}).items():
        http = spec.get("http", {})
        op = {
            "name": name,
            "method": http.get("method", "POST"),
            "uri": http.get("requestUri", "/"),
            "input_shape": spec.get("input", {}).get("shape"),
            "output_shape": spec.get("output", {}).get("shape"),
        }
        ops.append(op)
    return ops


def generate_rest_json_provider(service_name: str, model: dict, operations: list[dict]) -> str:
    """Generate provider for rest-json protocol services (Lambda, API Gateway, etc.)."""
    service_full = model.get("metadata", {}).get("serviceFullName", service_name)

    lines = [
        f'"""Native {service_full} provider — generated from botocore spec."""',
        "",
        "import json",
        "",
        "from starlette.requests import Request",
        "from starlette.responses import Response",
        "",
        "",
        f"async def handle_{service_name}_request(request: Request, region: str, account_id: str) -> Response:",
        f'    """Handle a {service_full} API request."""',
        "    path = request.url.path",
        "    method = request.method.upper()",
        "    body = await request.body()",
        "",
        "    # TODO: Implement routing based on path patterns",
        "    # Operations:",
    ]

    for op in operations:
        lines.append(f"    #   {op['method']} {op['uri']} — {op['name']}")

    lines.extend([
        "",
        '    return _error("NotImplemented", f"Operation not implemented: {method} {path}", 501)',
        "",
        "",
        "def _json(status_code: int, data) -> Response:",
        "    if data is None:",
        '        return Response(content=b"", status_code=status_code)',
        "    return Response(",
        "        content=json.dumps(data),",
        "        status_code=status_code,",
        '        media_type="application/json",',
        "    )",
        "",
        "",
        "def _error(code: str, message: str, status: int) -> Response:",
        '    body = json.dumps({"__type": code, "message": message})',
        '    return Response(content=body, status_code=status, media_type="application/json")',
    ])

    return "\n".join(lines) + "\n"


def generate_json_provider(service_name: str, model: dict, operations: list[dict]) -> str:
    """Generate provider for json protocol services (DynamoDB, KMS, StepFunctions, etc.)."""
    service_full = model.get("metadata", {}).get("serviceFullName", service_name)
    target_prefix = model.get("metadata", {}).get("targetPrefix", "")

    lines = [
        f'"""Native {service_full} provider — generated from botocore spec."""',
        "",
        "import json",
        "",
        "from starlette.requests import Request",
        "from starlette.responses import Response",
        "",
        "",
        f"async def handle_{service_name}_request(request: Request, region: str, account_id: str) -> Response:",
        f'    """Handle a {service_full} API request."""',
        "    body = await request.body()",
        '    target = request.headers.get("x-amz-target", "")',
        "",
        "    # Extract operation name from X-Amz-Target header",
        f'    # Target prefix: {target_prefix}',
        '    operation = target.split(".")[-1] if "." in target else target',
        "",
        "    handler = _ACTION_MAP.get(operation)",
        "    if handler is None:",
        '        return _error("UnknownOperation", f"Unknown operation: {operation}", 400)',
        "",
        "    params = json.loads(body) if body else {}",
        "    try:",
        "        result = handler(params, region, account_id)",
        "        return _json(200, result)",
        "    except Exception as e:",
        '        return _error(type(e).__name__, str(e), 400)',
        "",
        "",
    ]

    # Generate stub for each operation
    for op in operations:
        func_name = _to_snake_case(op["name"])
        lines.extend([
            f"def _{func_name}(params: dict, region: str, account_id: str) -> dict:",
            f'    """TODO: Implement {op["name"]}."""',
            f'    raise NotImplementedError("{op["name"]}")',
            "",
            "",
        ])

    # Generate action map
    lines.append("_ACTION_MAP = {")
    for op in operations:
        func_name = _to_snake_case(op["name"])
        lines.append(f'    "{op["name"]}": _{func_name},')
    lines.append("}")
    lines.append("")

    # Add helpers
    lines.extend([
        "",
        "def _json(status_code: int, data) -> Response:",
        "    if data is None:",
        '        return Response(content=b"", status_code=status_code)',
        "    return Response(",
        "        content=json.dumps(data),",
        "        status_code=status_code,",
        '        media_type="application/x-amz-json-1.0",',
        "    )",
        "",
        "",
        "def _error(code: str, message: str, status: int) -> Response:",
        '    body = json.dumps({"__type": code, "message": message})',
        '    return Response(content=body, status_code=status, media_type="application/x-amz-json-1.0")',
        "",
    ])

    return "\n".join(lines) + "\n"


def generate_query_provider(service_name: str, model: dict, operations: list[dict]) -> str:
    """Generate provider for query protocol services (SQS, SNS, IAM, STS, etc.)."""
    service_full = model.get("metadata", {}).get("serviceFullName", service_name)

    lines = [
        f'"""Native {service_full} provider — generated from botocore spec."""',
        "",
        "import json",
        "import uuid",
        "from urllib.parse import parse_qs",
        "",
        "from starlette.requests import Request",
        "from starlette.responses import Response",
        "",
        "",
        f"async def handle_{service_name}_request(request: Request, region: str, account_id: str) -> Response:",
        f'    """Handle a {service_full} API request."""',
        "    body = await request.body()",
        '    content_type = request.headers.get("content-type", "")',
        "",
        '    if "x-www-form-urlencoded" in content_type:',
        "        parsed = parse_qs(body.decode(), keep_blank_values=True)",
        "    else:",
        "        parsed = parse_qs(str(request.url.query), keep_blank_values=True)",
        "",
        "    params = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}",
        '    action = params.get("Action", "")',
        "",
        "    handler = _ACTION_MAP.get(action)",
        "    if handler is None:",
        '        return _xml_error("InvalidAction", f"Unknown action: {action}", 400)',
        "",
        "    try:",
        "        result = handler(params, region, account_id)",
        "        return _xml_response(action + 'Response', result)",
        "    except Exception as e:",
        "        return _xml_error(type(e).__name__, str(e), 400)",
        "",
        "",
    ]

    # Generate stub for each operation
    for op in operations:
        func_name = _to_snake_case(op["name"])
        lines.extend([
            f"def _{func_name}(params: dict, region: str, account_id: str) -> dict:",
            f'    """TODO: Implement {op["name"]}."""',
            f'    raise NotImplementedError("{op["name"]}")',
            "",
            "",
        ])

    # Generate action map
    lines.append("_ACTION_MAP = {")
    for op in operations:
        func_name = _to_snake_case(op["name"])
        lines.append(f'    "{op["name"]}": _{func_name},')
    lines.append("}")
    lines.append("")

    # Add helpers
    lines.extend([
        "",
        "def _xml_response(action: str, data: dict) -> Response:",
        '    result_name = action.replace("Response", "Result")',
        "    body_xml = _dict_to_xml(data)",
        "    xml = (",
        """        f'<?xml version="1.0"?>'""",
        """        f'<{action}>'""",
        """        f'<{result_name}>{body_xml}</{result_name}>'""",
        """        f'<ResponseMetadata><RequestId>{uuid.uuid4()}</RequestId></ResponseMetadata>'""",
        """        f'</{action}>'""",
        "    )",
        '    return Response(content=xml, status_code=200, media_type="text/xml")',
        "",
        "",
        "def _xml_error(code: str, message: str, status: int) -> Response:",
        "    xml = (",
        """        f'<?xml version="1.0"?>'""",
        """        f'<ErrorResponse>'""",
        """        f'<Error><Type>Sender</Type><Code>{code}</Code><Message>{message}</Message></Error>'""",
        """        f'<RequestId>{uuid.uuid4()}</RequestId>'""",
        """        f'</ErrorResponse>'""",
        "    )",
        '    return Response(content=xml, status_code=status, media_type="text/xml")',
        "",
        "",
        "def _dict_to_xml(d: dict) -> str:",
        "    parts = []",
        "    for k, v in d.items():",
        "        if isinstance(v, list):",
        "            parts.append(f'<{k}>')",
        "            for item in v:",
        "                if isinstance(item, dict):",
        "                    parts.append(f'<member>{_dict_to_xml(item)}</member>')",
        "                else:",
        "                    parts.append(f'<member>{item}</member>')",
        "            parts.append(f'</{k}>')",
        "        elif isinstance(v, dict):",
        "            parts.append(f'<{k}>{_dict_to_xml(v)}</{k}>')",
        "        else:",
        "            parts.append(f'<{k}>{v}</{k}>')",
        '    return "".join(parts)',
        "",
    ])

    return "\n".join(lines) + "\n"


def _to_snake_case(name: str) -> str:
    """Convert PascalCase to snake_case."""
    import re
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate native provider from botocore spec")
    parser.add_argument("service", help="AWS service name (e.g., lambda, stepfunctions)")
    parser.add_argument("--output-dir", help="Output directory (default: stdout)")
    parser.add_argument("--list-ops", action="store_true", help="Just list operations")
    args = parser.parse_args()

    model = find_service_model(args.service)
    if not model:
        print(f"Could not find botocore service model for '{args.service}'", file=sys.stderr)
        sys.exit(1)

    protocol = get_protocol(model)
    operations = get_operations(model)
    service_name = args.service.replace("-", "_")

    if args.list_ops:
        print(f"Service: {model['metadata'].get('serviceFullName', args.service)}")
        print(f"Protocol: {protocol}")
        print(f"Operations ({len(operations)}):")
        for op in sorted(operations, key=lambda x: x["name"]):
            print(f"  {op['method']:6} {op['uri']:<50} {op['name']}")
        return

    # Generate based on protocol
    if protocol in ("rest-json",):
        code = generate_rest_json_provider(service_name, model, operations)
    elif protocol in ("json",):
        code = generate_json_provider(service_name, model, operations)
    elif protocol in ("query", "ec2"):
        code = generate_query_provider(service_name, model, operations)
    else:
        print(f"Unsupported protocol: {protocol}", file=sys.stderr)
        code = generate_rest_json_provider(service_name, model, operations)

    if args.output_dir:
        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "__init__.py").touch()
        (out_dir / "provider.py").write_text(code)
        print(f"Generated {out_dir / 'provider.py'} ({len(operations)} operations, protocol={protocol})")
    else:
        print(code)


if __name__ == "__main__":
    main()
