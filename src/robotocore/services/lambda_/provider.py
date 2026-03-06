"""Native Lambda provider — wraps Moto for CRUD, uses in-process executor for Invoke."""

import base64
import json
import threading
import time
import uuid

from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.lambda_.executor import execute_python_handler

# Native event source mapping store (bypasses Moto's validation)
_esm_store: dict[str, dict] = {}  # uuid -> mapping config
_esm_lock = threading.Lock()


def get_event_source_mappings() -> list[dict]:
    """Return all event source mappings (used by the event source engine)."""
    with _esm_lock:
        return list(_esm_store.values())


def _get_moto_backend(account_id: str, region: str):
    from moto.backends import get_backend
    from moto.core import DEFAULT_ACCOUNT_ID

    acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
    return get_backend("lambda")[acct][region]


async def handle_lambda_request(request: Request, region: str, account_id: str) -> Response:
    """Handle a Lambda API request using REST-JSON protocol."""
    path = request.url.path
    method = request.method.upper()
    body = await request.body()

    # Route based on path patterns (Lambda uses REST API)
    # /2015-03-31/functions
    # /2015-03-31/functions/{name}
    # /2015-03-31/functions/{name}/invocations
    # /2015-03-31/functions/{name}/configuration
    # /2015-03-31/functions/{name}/versions
    # /2015-03-31/functions/{name}/aliases
    # /2015-03-31/functions/{name}/policy
    # /2015-03-31/event-source-mappings
    # /2015-03-31/layers

    parts = [p for p in path.split("/") if p]
    # Strip API version prefix (e.g., "2015-03-31")
    if parts and parts[0].startswith("20"):
        parts = parts[1:]

    try:
        if not parts:
            return _error("InvalidRequest", "No path specified", 400)

        # /functions
        if parts[0] == "functions":
            return await _handle_functions(parts, method, body, request, region, account_id)
        # /event-source-mappings
        elif parts[0] == "event-source-mappings":
            return await _handle_event_source_mappings(
                parts, method, body, request, region, account_id
            )
        # /layers
        elif parts[0] == "layers":
            return await _handle_layers(parts, method, body, request, region, account_id)
        # /tags
        elif parts[0] == "tags":
            return await _handle_tags(parts, method, body, request, region, account_id)
        # /account-settings
        elif parts[0] == "account-settings":
            return _handle_account_settings(region, account_id)
        else:
            return _error("InvalidRequest", f"Unknown path: {path}", 400)

    except Exception as e:
        error_type = type(e).__name__
        error_msg = str(e)

        # Map Moto exceptions to AWS error codes
        if "ResourceNotFoundException" in error_type or "UnknownFunction" in error_type:
            return _error("ResourceNotFoundException", error_msg, 404)
        if "ResourceConflictException" in error_type or "FunctionAlreadyExists" in error_type:
            return _error("ResourceConflictException", error_msg, 409)
        if "InvalidParameterValue" in error_type:
            return _error("InvalidParameterValueException", error_msg, 400)
        if "UnknownEventConfig" in error_type:
            return _error("ResourceNotFoundException", error_msg, 404)
        if "GenericResourcNotFound" in error_type:
            return _error("ResourceNotFoundException", error_msg, 404)
        if "ValidationException" in error_type:
            return _error("ValidationException", error_msg, 400)
        # Moto LambdaClientError has a 'code' attribute with HTTP status
        if hasattr(e, "code"):
            status = getattr(e, "code", 500)
            return _error(error_type, error_msg, int(status))
        return _error("ServiceException", error_msg, 500)


async def _handle_functions(
    parts: list[str], method: str, body: bytes, request: Request, region: str, account_id: str
) -> Response:
    backend = _get_moto_backend(account_id, region)

    # GET /functions — ListFunctions
    if len(parts) == 1 and method == "GET":
        fns = backend.list_functions()
        return _json(200, {"Functions": [_fn_config(fn) for fn in fns]})

    # POST /functions — CreateFunction
    if len(parts) == 1 and method == "POST":
        spec = json.loads(body) if body else {}
        fn = backend.create_function(spec)
        return _json(201, _fn_config(fn))

    if len(parts) < 2:
        return _error("InvalidRequest", "Missing function name", 400)

    func_name = parts[1]

    # Subresource routing
    if len(parts) == 2:
        if method == "GET":
            fn = backend.get_function(func_name)
            code_info = {
                "RepositoryType": "S3",
                "Location": f"https://awslambda-{region}-tasks.s3.{region}.amazonaws.com/...",
            }
            return _json(200, {"Configuration": _fn_config(fn), "Code": code_info})
        elif method == "DELETE":
            qualifier = request.query_params.get("Qualifier")
            backend.delete_function(func_name, qualifier)
            return _json(204, None)

    if len(parts) >= 3:
        sub = parts[2]

        # /functions/{name}/invocations — Invoke
        if sub == "invocations":
            return await _invoke(func_name, body, request, region, account_id)

        # /functions/{name}/configuration — GetFunctionConfiguration / UpdateFunctionConfiguration
        if sub == "configuration":
            if method == "GET":
                fn = backend.get_function(func_name)
                return _json(200, _fn_config(fn))
            elif method == "PUT":
                spec = json.loads(body) if body else {}
                qualifier = request.query_params.get("Qualifier")
                fn = backend.update_function_configuration(func_name, qualifier, spec)
                return _json(200, fn if isinstance(fn, dict) else _fn_config(fn))

        # /functions/{name}/code — UpdateFunctionCode / GetFunctionCode
        if sub == "code":
            if method == "PUT":
                spec = json.loads(body) if body else {}
                qualifier = request.query_params.get("Qualifier")
                result = backend.update_function_code(func_name, qualifier, spec)
                return _json(200, result if isinstance(result, dict) else _fn_config(result))

        # /functions/{name}/versions — PublishVersion / ListVersionsByFunction
        if sub == "versions":
            if method == "POST":
                ver = backend.publish_version(func_name)
                return _json(201, _fn_config(ver))
            elif method == "GET":
                versions = backend.list_versions_by_function(func_name)
                return _json(200, {"Versions": [_fn_config(v) for v in versions]})

        # /functions/{name}/aliases
        if sub == "aliases":
            if len(parts) == 3:
                if method == "POST":
                    spec = json.loads(body) if body else {}
                    alias = backend.create_alias(
                        func_name,
                        spec.get("Name"),
                        spec.get("FunctionVersion"),
                        spec.get("Description", ""),
                        spec.get("RoutingConfig"),
                    )
                    return _json(201, _alias_dict(alias))
                elif method == "GET":
                    aliases = backend.list_aliases(func_name)
                    return _json(200, {"Aliases": [_alias_dict(a) for a in aliases]})
            elif len(parts) == 4:
                alias_name = parts[3]
                if method == "GET":
                    alias = backend.get_alias(func_name, alias_name)
                    return _json(200, _alias_dict(alias))
                elif method == "PUT":
                    spec = json.loads(body) if body else {}
                    alias = backend.update_alias(
                        func_name,
                        alias_name,
                        spec.get("FunctionVersion"),
                        spec.get("Description"),
                        spec.get("RoutingConfig"),
                    )
                    return _json(200, _alias_dict(alias))
                elif method == "DELETE":
                    backend.delete_alias(func_name, alias_name)
                    return _json(204, None)

        # /functions/{name}/policy
        if sub == "policy":
            if len(parts) == 3:
                if method == "GET":
                    policy = backend.get_policy(func_name, None)
                    return Response(content=policy, status_code=200, media_type="application/json")
                elif method == "POST":
                    statement = backend.add_permission(func_name, None, body.decode())
                    return _json(201, {"Statement": json.dumps(statement)})
            elif len(parts) == 4:
                statement_id = parts[3]
                if method == "DELETE":
                    backend.remove_permission(func_name, statement_id, "")
                    return _json(204, None)

        # /functions/{name}/concurrency
        if sub == "concurrency":
            if method == "PUT":
                spec = json.loads(body) if body else {}
                reserved = spec.get("ReservedConcurrentExecutions", 0)
                result = backend.put_function_concurrency(func_name, reserved)
                return _json(
                    200, {"ReservedConcurrentExecutions": int(result) if result is not None else 0}
                )
            elif method == "DELETE":
                backend.delete_function_concurrency(func_name)
                return _json(204, None)
            elif method == "GET":
                result = backend.get_function_concurrency(func_name)
                if result is not None:
                    return _json(200, {"ReservedConcurrentExecutions": int(result)})
                return _json(200, {})

        # /functions/{name}/url — Function URLs
        if sub == "url":
            if method == "POST":
                spec = json.loads(body) if body else {}
                url_config = backend.create_function_url_config(func_name, spec)
                return _json(201, _url_config_dict(url_config, func_name, region, account_id))
            elif method == "GET":
                url_config = backend.get_function_url_config(func_name)
                return _json(200, _url_config_dict(url_config, func_name, region, account_id))
            elif method == "PUT":
                spec = json.loads(body) if body else {}
                url_config = backend.update_function_url_config(func_name, spec)
                return _json(200, _url_config_dict(url_config, func_name, region, account_id))
            elif method == "DELETE":
                backend.delete_function_url_config(func_name)
                return _json(204, None)

        # /functions/{name}/event-invoke-config
        if sub == "event-invoke-config":
            if method == "PUT":
                spec = json.loads(body) if body else {}
                config = backend.put_function_event_invoke_config(func_name, spec)
                return _json(200, config)
            elif method == "GET":
                config = backend.get_function_event_invoke_config(func_name)
                return _json(200, config)
            elif method == "DELETE":
                backend.delete_function_event_invoke_config(func_name)
                return _json(204, None)

    return _error("InvalidRequest", f"Unhandled Lambda path: {'/'.join(parts)}", 400)


def _handle_account_settings(region: str, account_id: str) -> Response:
    """Handle GET /account-settings — returns Lambda account-level settings.

    Moto does not implement this, so we return sensible defaults matching
    the real AWS response shape.
    """
    backend = _get_moto_backend(account_id, region)

    # Calculate total code size and function count from Moto state
    functions = backend.list_functions()
    total_code_size = 0
    function_count = len(functions)
    total_concurrency = 0

    for fn in functions:
        code_size = getattr(fn, "code_size", 0) or 0
        total_code_size += int(code_size)
        if fn.reserved_concurrency is not None:
            total_concurrency += int(fn.reserved_concurrency)

    return _json(
        200,
        {
            "AccountLimit": {
                "TotalCodeSize": 80530636800,
                "CodeSizeUnzipped": 262144000,
                "CodeSizeZipped": 52428800,
                "ConcurrentExecutions": 1000,
                "UnreservedConcurrentExecutions": max(0, 1000 - total_concurrency),
            },
            "AccountUsage": {
                "TotalCodeSize": total_code_size,
                "FunctionCount": function_count,
            },
        },
    )


async def _handle_event_source_mappings(
    parts: list[str], method: str, body: bytes, request: Request, region: str, account_id: str
) -> Response:
    """Native event source mapping CRUD — bypasses Moto to avoid cross-service validation issues."""

    if len(parts) == 1:
        if method == "POST":
            spec = json.loads(body) if body else {}
            esm_uuid = str(uuid.uuid4())

            # Resolve function ARN
            func_name = spec.get("FunctionName", "")
            if not func_name.startswith("arn:"):
                func_arn = f"arn:aws:lambda:{region}:{account_id}:function:{func_name}"
            else:
                func_arn = func_name

            config = {
                "UUID": esm_uuid,
                "EventSourceArn": spec.get("EventSourceArn", ""),
                "FunctionArn": func_arn,
                "BatchSize": spec.get("BatchSize", 10),
                "MaximumBatchingWindowInSeconds": spec.get("MaximumBatchingWindowInSeconds", 0),
                "State": "Enabled",
                "StateTransitionReason": "User action",
                "LastModified": time.time(),
                "FunctionResponseTypes": spec.get("FunctionResponseTypes", []),
                "_region": region,
                "_account_id": account_id,
            }

            with _esm_lock:
                _esm_store[esm_uuid] = config

            # Start engine if not already running
            from robotocore.services.lambda_.event_source import get_engine

            get_engine().start()

            return _json(202, _sanitize_esm(config))

        elif method == "GET":
            event_source_arn = request.query_params.get("EventSourceArn")
            func_name = request.query_params.get("FunctionName")

            with _esm_lock:
                mappings = list(_esm_store.values())

            if event_source_arn:
                mappings = [m for m in mappings if m.get("EventSourceArn") == event_source_arn]
            if func_name:
                mappings = [m for m in mappings if func_name in m.get("FunctionArn", "")]

            return _json(200, {"EventSourceMappings": [_sanitize_esm(m) for m in mappings]})

    elif len(parts) == 2:
        esm_uuid = parts[1]

        with _esm_lock:
            config = _esm_store.get(esm_uuid)

        if not config:
            return _error(
                "ResourceNotFoundException", f"Event source mapping not found: {esm_uuid}", 404
            )

        if method == "GET":
            return _json(200, _sanitize_esm(config))

        elif method == "PUT":
            spec = json.loads(body) if body else {}
            with _esm_lock:
                if esm_uuid in _esm_store:
                    for key in [
                        "BatchSize",
                        "MaximumBatchingWindowInSeconds",
                        "Enabled",
                        "FunctionResponseTypes",
                    ]:
                        if key in spec:
                            _esm_store[esm_uuid][key] = spec[key]
                    if "Enabled" in spec:
                        _esm_store[esm_uuid]["State"] = "Enabled" if spec["Enabled"] else "Disabled"
                    _esm_store[esm_uuid]["LastModified"] = time.time()
                    config = _esm_store[esm_uuid]
            return _json(200, _sanitize_esm(config))

        elif method == "DELETE":
            with _esm_lock:
                config = _esm_store.pop(esm_uuid, config)
            return _json(200, _sanitize_esm(config))

    return _error("InvalidRequest", "Unhandled event-source-mappings path", 400)


async def _handle_layers(
    parts: list[str], method: str, body: bytes, request: Request, region: str, account_id: str
) -> Response:
    backend = _get_moto_backend(account_id, region)

    if len(parts) == 1 and method == "GET":
        # list_layers() returns dicts from Layer.to_dict() already
        layers = list(backend.list_layers())
        return _json(200, {"Layers": layers})

    if len(parts) >= 2:
        layer_name = parts[1]

        # /layers/{name}/versions
        if len(parts) >= 3 and parts[2] == "versions":
            if len(parts) == 3:
                if method == "POST":
                    spec = json.loads(body) if body else {}
                    spec["LayerName"] = layer_name
                    layer_ver = backend.publish_layer_version(spec)
                    return _json(201, _layer_version_dict(layer_ver))
                elif method == "GET":
                    versions = backend.list_layer_versions(layer_name)
                    return _json(200, {"LayerVersions": [_layer_version_dict(v) for v in versions]})
            elif len(parts) == 4:
                version_num = int(parts[3])
                if method == "GET":
                    layer_ver = backend.get_layer_version(layer_name, version_num)
                    return _json(200, _layer_version_dict(layer_ver))
                elif method == "DELETE":
                    backend.delete_layer_version(layer_name, version_num)
                    return _json(204, None)

    return _error("InvalidRequest", "Unhandled layers path", 400)


async def _handle_tags(
    parts: list[str], method: str, body: bytes, request: Request, region: str, account_id: str
) -> Response:
    backend = _get_moto_backend(account_id, region)
    # /tags/{arn} — the ARN is the rest of the path
    arn = "/".join(parts[1:]) if len(parts) > 1 else ""
    # Reconstruct full ARN from URL-encoded path
    from urllib.parse import unquote

    arn = unquote(arn)

    if method == "GET":
        fn = backend.get_function(arn)
        return _json(200, {"Tags": fn.tags or {}})
    elif method == "POST":
        spec = json.loads(body) if body else {}
        backend.tag_resource(arn, spec.get("Tags", {}))
        return _json(204, None)
    elif method == "DELETE":
        tag_keys = request.query_params.getlist("tagKeys")
        backend.untag_resource(arn, tag_keys)
        return _json(204, None)

    return _error("InvalidRequest", "Unhandled tags path", 400)


async def _invoke(
    func_name: str, body: bytes, request: Request, region: str, account_id: str
) -> Response:
    """Invoke a Lambda function — uses in-process execution for Python runtimes."""
    backend = _get_moto_backend(account_id, region)
    fn = backend.get_function(func_name)

    invocation_type = request.headers.get("x-amz-invocation-type", "RequestResponse")
    log_type = request.headers.get("x-amz-log-type", "None")

    event = json.loads(body) if body else {}

    # Check if this is a Python runtime and we have code
    runtime = getattr(fn, "run_time", "") or ""
    is_python = runtime.startswith("python")
    code_zip = None

    if is_python and hasattr(fn, "code") and fn.code:
        code_zip = fn.code.get("ZipFile")
        if isinstance(code_zip, str):
            code_zip = base64.b64decode(code_zip)

    if is_python and code_zip:
        # In-process execution
        env_vars = getattr(fn, "environment_vars", {}) or {}
        handler = getattr(fn, "handler", "lambda_function.handler")
        timeout = int(getattr(fn, "timeout", 3) or 3)
        memory_size = int(getattr(fn, "memory_size", 128) or 128)

        result, error_type, logs = execute_python_handler(
            code_zip=code_zip,
            handler=handler,
            event=event,
            function_name=func_name,
            timeout=timeout,
            memory_size=memory_size,
            env_vars=env_vars,
            region=region,
            account_id=account_id,
        )

        headers = {
            "x-amz-request-id": str(uuid.uuid4()),
            "x-amz-executed-version": "$LATEST",
        }

        if error_type:
            headers["x-amz-function-error"] = error_type

        if log_type == "Tail" and logs:
            # Return last 4KB of logs, base64 encoded
            log_tail = logs[-4096:] if len(logs) > 4096 else logs
            headers["x-amz-log-result"] = base64.b64encode(log_tail.encode()).decode()

        if invocation_type == "Event":
            return Response(content=b"", status_code=202, headers=headers)
        elif invocation_type == "DryRun":
            return Response(content=b"", status_code=204, headers=headers)

        # RequestResponse
        if result is None:
            payload = b"null"
        elif isinstance(result, (dict, list)):
            payload = json.dumps(result).encode()
        elif isinstance(result, str):
            payload = json.dumps(result).encode()
        else:
            payload = str(result).encode()

        return Response(
            content=payload, status_code=200, headers=headers, media_type="application/json"
        )
    else:
        # Fallback: return a simple success response (like Moto's simple mode)
        headers = {
            "x-amz-request-id": str(uuid.uuid4()),
            "x-amz-executed-version": "$LATEST",
        }
        if invocation_type == "Event":
            return Response(content=b"", status_code=202, headers=headers)
        elif invocation_type == "DryRun":
            return Response(content=b"", status_code=204, headers=headers)

        payload = json.dumps("Simple Lambda happy path OK").encode()
        if log_type == "Tail":
            headers["x-amz-log-result"] = base64.b64encode(b"").decode()
        return Response(
            content=payload, status_code=200, headers=headers, media_type="application/json"
        )


# --- Helpers ---


def _sanitize_esm(config: dict) -> dict:
    """Remove internal fields from event source mapping config."""
    return {k: v for k, v in config.items() if not k.startswith("_")}


def _fn_config(fn) -> dict:
    """Extract function configuration as a dict."""
    if hasattr(fn, "get_configuration"):
        config = fn.get_configuration()
        if isinstance(config, str):
            return json.loads(config)
        return config
    return {}


def _alias_dict(alias) -> dict:
    if hasattr(alias, "to_json"):
        result = alias.to_json()
        if isinstance(result, str):
            return json.loads(result)
        return result
    return {}


def _layer_dict(layer) -> dict:
    if hasattr(layer, "to_dict"):
        return layer.to_dict()
    return {}


def _layer_version_dict(layer_ver) -> dict:
    if hasattr(layer_ver, "get_layer_version"):
        result = layer_ver.get_layer_version()
        if isinstance(result, str):
            return json.loads(result)
        return result
    return {}


def _url_config_dict(url_config, func_name: str, region: str, account_id: str) -> dict:
    if hasattr(url_config, "to_dict"):
        return url_config.to_dict()
    return {}


def _json(status_code: int, data) -> Response:
    if data is None:
        return Response(content=b"", status_code=status_code)
    return Response(
        content=json.dumps(data),
        status_code=status_code,
        media_type="application/json",
    )


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"Type": "User", "Message": message, "__type": code})
    return Response(content=body, status_code=status, media_type="application/json")
