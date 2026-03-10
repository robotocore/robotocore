"""Native Lambda provider — wraps Moto for CRUD, uses in-process executor for Invoke."""

import base64
import json
import logging
import threading
import time
import uuid

from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.lambda_.executor import get_code_cache
from robotocore.services.lambda_.hot_reload import (
    get_file_watcher,
    get_mount_path,
    is_hot_reload_enabled,
    is_hot_reload_for_function,
)
from robotocore.services.lambda_.runtimes import get_executor_for_runtime

logger = logging.getLogger(__name__)

# Native event source mapping store (bypasses Moto's validation)
_esm_store: dict[str, dict] = {}  # uuid -> mapping config
_esm_lock = threading.Lock()

# Native provisioned concurrency store
# key: (account_id, region, func_name, qualifier)
_provisioned_concurrency: dict[tuple[str, str, str, str], dict] = {}
_provisioned_lock = threading.Lock()

# Native dead letter config store (Moto doesn't support it)
# key: (account_id, region, func_name)
_dlq_configs: dict[tuple[str, str, str], dict] = {}
_dlq_lock = threading.Lock()

# Native recursion config store
# key: (account_id, region, func_name)
_recursion_configs: dict[tuple[str, str, str], str] = {}
_recursion_lock = threading.Lock()

# Native scaling config store
# key: (account_id, region, func_name, qualifier)
_scaling_configs: dict[tuple[str, str, str, str], dict] = {}
_scaling_lock = threading.Lock()

# Native code signing config store
# key: (account_id, region, func_name)
_code_signing_configs: dict[tuple[str, str, str], str] = {}  # func -> csc ARN
_code_signing_lock = threading.Lock()

# Native runtime management config store
# key: (account_id, region, func_name)
_runtime_mgmt_configs: dict[tuple[str, str, str], dict] = {}
_runtime_mgmt_lock = threading.Lock()

# Native layer version permissions store
# key: (account_id, region, layer_name, version_number)
_layer_permissions: dict[tuple[str, str, str, int], dict[str, dict]] = {}  # -> {sid: statement}
_layer_perm_lock = threading.Lock()


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
        if "FunctionUrlConfigExists" in error_type:
            return _error("ResourceConflictException", error_msg, 409)
        if "FunctionUrlConfigNotFoundError" in error_type:
            return _error("ResourceNotFoundException", error_msg, 404)
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
            # Cascade: clean up native stores for this function
            if qualifier is None:
                _cascade_delete_function(func_name, region, account_id)
            return _json(204, None)

    if len(parts) >= 3:
        sub = parts[2]

        # /functions/{name}/invocations — Invoke
        if sub == "invocations":
            return await _invoke(func_name, body, request, region, account_id)

        # /functions/{name}/invoke-async — InvokeAsync (deprecated)
        if sub == "invoke-async":
            return await _invoke_async(func_name, body, request, region, account_id)

        # /functions/{name}/configuration — Get/UpdateFunctionConfiguration
        if sub == "configuration":
            if method == "GET":
                fn = backend.get_function(func_name)
                return _json(200, _fn_config(fn))
            elif method == "PUT":
                spec = json.loads(body) if body else {}
                # Track DeadLetterConfig in our native store
                if "DeadLetterConfig" in spec:
                    _store_dlq_config(account_id, region, func_name, spec["DeadLetterConfig"])
                qualifier = request.query_params.get("Qualifier")
                fn = backend.update_function_configuration(func_name, qualifier, spec)
                result = fn if isinstance(fn, dict) else _fn_config(fn)
                # Merge DLQ config into response
                dlq = _get_dlq_config(account_id, region, func_name)
                if dlq and isinstance(result, dict):
                    result["DeadLetterConfig"] = dlq
                return _json(200, result)

        # /functions/{name}/code — UpdateFunctionCode / GetFunctionCode
        if sub == "code":
            if method == "PUT":
                spec = json.loads(body) if body else {}
                qualifier = request.query_params.get("Qualifier")
                result = backend.update_function_code(func_name, qualifier, spec)
                # Invalidate code cache so next invocation picks up new code
                get_code_cache().invalidate(func_name)
                return _json(200, result if isinstance(result, dict) else _fn_config(result))

        # /functions/{name}/versions — PublishVersion / ListVersionsByFunction
        if sub == "versions":
            if method == "POST":
                spec = json.loads(body) if body else {}
                description = spec.get("Description", "")
                ver = backend.publish_version(func_name, description)
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
                        spec.get("Name"),
                        func_name,
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
                    alias = backend.get_alias(alias_name, func_name)
                    return _json(200, _alias_dict(alias))
                elif method == "PUT":
                    spec = json.loads(body) if body else {}
                    alias = backend.update_alias(
                        alias_name,
                        func_name,
                        spec.get("FunctionVersion"),
                        spec.get("Description"),
                        spec.get("RoutingConfig"),
                    )
                    return _json(200, _alias_dict(alias))
                elif method == "DELETE":
                    backend.delete_alias(alias_name, func_name)
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

        # /functions/{name}/url — Function URLs (native store)
        if sub == "url":
            return _handle_function_url(func_name, method, body, region, account_id)

        # /functions/{name}/urls — ListFunctionUrlConfigs (plural)
        if sub == "urls" and method == "GET":
            from robotocore.services.lambda_.urls import list_function_url_configs

            configs = list_function_url_configs(func_name, region, account_id)
            return _json(200, {"FunctionUrlConfigs": configs})

        # /functions/{name}/event-invoke-config — Event invoke config
        if sub == "event-invoke-config":
            return _handle_event_invoke_config(func_name, method, body, backend, region, account_id)

        # /functions/{name}/provisioned-concurrency — Provisioned concurrency
        if sub == "provisioned-concurrency":
            # ListProvisionedConcurrencyConfigs uses ?List=ALL
            if method == "GET" and request.query_params.get("List") == "ALL":
                configs = list_provisioned_concurrency_configs(func_name, region, account_id)
                return _json(200, {"ProvisionedConcurrencyConfigs": configs})
            return _handle_provisioned_concurrency(
                func_name, method, body, request, region, account_id
            )

        # /functions/{name}/recursion-config — Recursion config
        if sub == "recursion-config":
            return _handle_recursion_config(func_name, method, body, region, account_id)

        # /functions/{name}/function-scaling-config — Scaling config
        if sub == "function-scaling-config":
            return _handle_scaling_config(func_name, method, body, request, region, account_id)

        # /functions/{name}/code-signing-config — Code signing config
        if sub == "code-signing-config":
            return _handle_code_signing_config(func_name, method, body, region, account_id)

        # /functions/{name}/runtime-management-config — Runtime management
        if sub == "runtime-management-config":
            return _handle_runtime_management_config(
                func_name,
                method,
                body,
                request,
                region,
                account_id,
            )

        # /functions/{name}/durable-executions — ListDurableExecutionsByFunction
        if sub == "durable-executions" and method == "GET":
            return _json(200, {"DurableExecutions": []})

        # /functions/{name}/response-streaming-invocations — InvokeWithResponseStream
        if sub == "response-streaming-invocations" and method == "POST":
            return await _invoke_with_response_stream(func_name, body, request, region, account_id)

    return _error("InvalidRequest", f"Unhandled Lambda path: {'/'.join(parts)}", 400)


def _handle_recursion_config(
    func_name: str, method: str, body: bytes, region: str, account_id: str
) -> Response:
    """Handle GET/PUT for function recursion config."""
    from moto.backends import get_backend

    backend = get_backend("lambda")[account_id][region]
    # Verify function exists
    try:
        backend.get_function(func_name)
    except Exception:
        return _error(
            "ResourceNotFoundException",
            f"Function not found: arn:aws:lambda:{region}:{account_id}:function:{func_name}",
            404,
        )

    key = (account_id, region, func_name)
    if method == "GET":
        with _recursion_lock:
            value = _recursion_configs.get(key, "Terminate")
        return _json(200, {"RecursiveLoop": value})
    elif method == "PUT":
        spec = json.loads(body) if body else {}
        value = spec.get("RecursiveLoop", "Terminate")
        if value not in ("Allow", "Terminate"):
            return _error(
                "InvalidParameterValueException",
                f"Invalid value for RecursiveLoop: {value}. Must be Allow or Terminate.",
                400,
            )
        with _recursion_lock:
            _recursion_configs[key] = value
        return _json(200, {"RecursiveLoop": value})
    return _error("InvalidRequest", "Method not allowed", 405)


def _handle_scaling_config(
    func_name: str,
    method: str,
    body: bytes,
    request: Request,
    region: str,
    account_id: str,
) -> Response:
    """Handle GET/PUT/DELETE for function scaling config."""
    backend = _get_moto_backend(account_id, region)
    try:
        backend.get_function(func_name)
    except Exception:
        return _error(
            "ResourceNotFoundException",
            f"Function not found: arn:aws:lambda:{region}:{account_id}:function:{func_name}",
            404,
        )

    qualifier = request.query_params.get("Qualifier", "$LATEST")
    key = (account_id, region, func_name, qualifier)

    func_arn = f"arn:aws:lambda:{region}:{account_id}:function:{func_name}"

    if method == "GET":
        with _scaling_lock:
            config = _scaling_configs.get(key, {})
        return _json(
            200,
            {
                "FunctionArn": func_arn,
                "AppliedFunctionScalingConfig": config,
                "RequestedFunctionScalingConfig": config,
            },
        )
    elif method == "PUT":
        spec = json.loads(body) if body else {}
        sc = spec.get("FunctionScalingConfig", {})
        with _scaling_lock:
            _scaling_configs[key] = sc
        return _json(200, {"FunctionState": "Active"})
    elif method == "DELETE":
        with _scaling_lock:
            _scaling_configs.pop(key, None)
        return _json(204, None)
    return _error("InvalidRequest", "Method not allowed", 405)


def _handle_code_signing_config(
    func_name: str, method: str, body: bytes, region: str, account_id: str
) -> Response:
    """Handle GET/PUT/DELETE for function code signing config."""
    backend = _get_moto_backend(account_id, region)
    try:
        backend.get_function(func_name)
    except Exception:
        return _error(
            "ResourceNotFoundException",
            f"Function not found: arn:aws:lambda:{region}:{account_id}:function:{func_name}",
            404,
        )

    key = (account_id, region, func_name)
    func_arn = f"arn:aws:lambda:{region}:{account_id}:function:{func_name}"

    if method == "GET":
        with _code_signing_lock:
            csc_arn = _code_signing_configs.get(key)
        if csc_arn is None:
            return _error(
                "ResourceNotFoundException",
                f"Code signing configuration not found for function: {func_arn}",
                404,
            )
        return _json(200, {"CodeSigningConfigArn": csc_arn, "FunctionName": func_name})
    elif method == "PUT":
        spec = json.loads(body) if body else {}
        csc_arn = spec.get("CodeSigningConfigArn", "")
        with _code_signing_lock:
            _code_signing_configs[key] = csc_arn
        return _json(200, {"CodeSigningConfigArn": csc_arn, "FunctionName": func_name})
    elif method == "DELETE":
        with _code_signing_lock:
            _code_signing_configs.pop(key, None)
        return _json(204, None)
    return _error("InvalidRequest", "Method not allowed", 405)


def _handle_runtime_management_config(
    func_name: str,
    method: str,
    body: bytes,
    request: Request,
    region: str,
    account_id: str,
) -> Response:
    """Handle GET/PUT for runtime management config."""
    backend = _get_moto_backend(account_id, region)
    try:
        backend.get_function(func_name)
    except Exception:
        return _error(
            "ResourceNotFoundException",
            f"Function not found: arn:aws:lambda:{region}:{account_id}:function:{func_name}",
            404,
        )

    key = (account_id, region, func_name)
    func_arn = f"arn:aws:lambda:{region}:{account_id}:function:{func_name}"

    if method == "GET":
        with _runtime_mgmt_lock:
            config = _runtime_mgmt_configs.get(key)
        if config is None:
            return _json(200, {"UpdateRuntimeOn": "Auto", "FunctionArn": func_arn})
        return _json(200, {**config, "FunctionArn": func_arn})
    elif method == "PUT":
        spec = json.loads(body) if body else {}
        update_on = spec.get("UpdateRuntimeOn", "Auto")
        config = {"UpdateRuntimeOn": update_on}
        if update_on == "Manual" and "RuntimeVersionArn" in spec:
            config["RuntimeVersionArn"] = spec["RuntimeVersionArn"]
        with _runtime_mgmt_lock:
            _runtime_mgmt_configs[key] = config
        return _json(200, {**config, "FunctionArn": func_arn})
    return _error("InvalidRequest", "Method not allowed", 405)


def _handle_layer_version_permission(
    layer_name: str,
    version_num: int,
    parts: list[str],
    method: str,
    body: bytes,
    request: Request,
    region: str,
    account_id: str,
) -> Response:
    """Handle layer version permission CRUD."""
    key = (account_id, region, layer_name, version_num)

    if method == "GET" and len(parts) == 5:
        # GetLayerVersionPolicy
        with _layer_perm_lock:
            stmts = _layer_permissions.get(key, {})
        if not stmts:
            return _error(
                "ResourceNotFoundException",
                f"No policy is associated with layer {layer_name} version {version_num}",
                404,
            )
        policy = {
            "Version": "2012-10-17",
            "Id": "default",
            "Statement": list(stmts.values()),
        }
        return _json(200, {"Policy": json.dumps(policy), "RevisionId": str(uuid.uuid4())})
    elif method == "POST" and len(parts) == 5:
        # AddLayerVersionPermission
        spec = json.loads(body) if body else {}
        sid = spec.get("StatementId", "")
        statement = {
            "Sid": sid,
            "Effect": "Allow",
            "Principal": spec.get("Principal", "*"),
            "Action": spec.get("Action", "lambda:GetLayerVersion"),
            "Resource": f"arn:aws:lambda:{region}:{account_id}:layer:{layer_name}:{version_num}",
        }
        if spec.get("OrganizationId"):
            statement["Condition"] = {
                "StringEquals": {"aws:PrincipalOrgID": spec["OrganizationId"]}
            }
        with _layer_perm_lock:
            if key not in _layer_permissions:
                _layer_permissions[key] = {}
            _layer_permissions[key][sid] = statement
        return _json(201, {"Statement": json.dumps(statement), "RevisionId": str(uuid.uuid4())})
    elif method == "DELETE" and len(parts) == 6:
        # RemoveLayerVersionPermission — /layers/{name}/versions/{num}/policy/{sid}
        sid = parts[5]
        with _layer_perm_lock:
            stmts = _layer_permissions.get(key, {})
            if sid not in stmts:
                return _error(
                    "ResourceNotFoundException",
                    f"Permission statement {sid} not found",
                    404,
                )
            del stmts[sid]
            if not stmts:
                _layer_permissions.pop(key, None)
        return _json(204, None)

    return _error("InvalidRequest", "Unhandled layer version policy path", 400)


def _handle_function_url(
    func_name: str, method: str, body: bytes, region: str, account_id: str
) -> Response:
    """Handle function URL CRUD via native store."""
    from robotocore.services.lambda_.urls import (
        create_function_url_config,
        delete_function_url_config,
        get_function_url_config,
        update_function_url_config,
    )

    if method == "POST":
        spec = json.loads(body) if body else {}
        url_config = create_function_url_config(func_name, region, account_id, spec)
        return _json(201, url_config)
    elif method == "GET":
        url_config = get_function_url_config(func_name, region, account_id)
        return _json(200, url_config)
    elif method == "PUT":
        spec = json.loads(body) if body else {}
        url_config = update_function_url_config(func_name, region, account_id, spec)
        return _json(200, url_config)
    elif method == "DELETE":
        delete_function_url_config(func_name, region, account_id)
        return _json(204, None)

    return _error("InvalidRequest", "Unsupported method for function URL", 400)


def _handle_event_invoke_config(
    func_name: str,
    method: str,
    body: bytes,
    backend,
    region: str,
    account_id: str,
) -> Response:
    """Handle event invoke config CRUD."""
    if method == "PUT":
        spec = json.loads(body) if body else {}
        config = backend.put_function_event_invoke_config(func_name, spec)
        return _json(200, config)
    elif method == "POST":
        # UpdateFunctionEventInvokeConfig uses POST
        spec = json.loads(body) if body else {}
        config = backend.update_function_event_invoke_config(func_name, spec)
        return _json(200, config)
    elif method == "GET":
        config = backend.get_function_event_invoke_config(func_name)
        return _json(200, config)
    elif method == "DELETE":
        backend.delete_function_event_invoke_config(func_name)
        return _json(204, None)

    return _error("InvalidRequest", "Unsupported method for event-invoke-config", 400)


def _handle_provisioned_concurrency(
    func_name: str,
    method: str,
    body: bytes,
    request: Request,
    region: str,
    account_id: str,
) -> Response:
    """Handle provisioned concurrency CRUD (simulated — just stores config)."""
    qualifier = request.query_params.get("Qualifier", "$LATEST")

    if method == "PUT":
        spec = json.loads(body) if body else {}
        config = _put_provisioned_concurrency(
            account_id,
            region,
            func_name,
            qualifier,
            spec.get("ProvisionedConcurrentExecutions", 0),
        )
        return _json(202, config)
    elif method == "GET":
        config = _get_provisioned_concurrency(account_id, region, func_name, qualifier)
        if config is None:
            return _error(
                "ProvisionedConcurrencyConfigNotFoundException",
                f"No provisioned concurrency config for {func_name}:{qualifier}",
                404,
            )
        return _json(200, config)
    elif method == "DELETE":
        deleted = _delete_provisioned_concurrency(account_id, region, func_name, qualifier)
        if not deleted:
            return _error(
                "ProvisionedConcurrencyConfigNotFoundException",
                f"No provisioned concurrency config for {func_name}:{qualifier}",
                404,
            )
        return _json(204, None)

    return _error("InvalidRequest", "Unsupported method for provisioned-concurrency", 400)


def list_provisioned_concurrency_configs(
    func_name: str, region: str, account_id: str
) -> list[dict]:
    """List all provisioned concurrency configs for a function."""
    with _provisioned_lock:
        result = []
        for (acct, reg, fn, qual), config in _provisioned_concurrency.items():
            if acct == account_id and reg == region and fn == func_name:
                result.append(config)
        return result


def _put_provisioned_concurrency(
    account_id: str, region: str, func_name: str, qualifier: str, count: int
) -> dict:
    key = (account_id, region, func_name, qualifier)
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    config = {
        "RequestedProvisionedConcurrentExecutions": count,
        "AvailableProvisionedConcurrentExecutions": count,
        "AllocatedProvisionedConcurrentExecutions": count,
        "Status": "READY",
        "LastModified": now,
    }
    with _provisioned_lock:
        _provisioned_concurrency[key] = config
    return config


def _get_provisioned_concurrency(
    account_id: str, region: str, func_name: str, qualifier: str
) -> dict | None:
    key = (account_id, region, func_name, qualifier)
    with _provisioned_lock:
        return _provisioned_concurrency.get(key)


def _delete_provisioned_concurrency(
    account_id: str, region: str, func_name: str, qualifier: str
) -> bool:
    key = (account_id, region, func_name, qualifier)
    with _provisioned_lock:
        if key in _provisioned_concurrency:
            del _provisioned_concurrency[key]
            return True
        return False


def _store_dlq_config(account_id: str, region: str, func_name: str, dlq_config: dict) -> None:
    """Store dead letter queue config for a function."""
    key = (account_id, region, func_name)
    with _dlq_lock:
        if dlq_config.get("TargetArn"):
            _dlq_configs[key] = dlq_config
        elif key in _dlq_configs:
            del _dlq_configs[key]


def _get_dlq_config(account_id: str, region: str, func_name: str) -> dict | None:
    """Get dead letter queue config for a function."""
    key = (account_id, region, func_name)
    with _dlq_lock:
        return _dlq_configs.get(key)


def dispatch_to_dlq(
    func_name: str,
    payload: dict,
    error: str | None,
    region: str,
    account_id: str,
) -> None:
    """Send failed async invocation to the configured DLQ."""
    dlq = _get_dlq_config(account_id, region, func_name)
    if not dlq:
        return

    target_arn = dlq.get("TargetArn", "")
    if not target_arn:
        return

    record = json.dumps(
        {
            "requestContext": {
                "requestId": str(uuid.uuid4()),
                "functionArn": f"arn:aws:lambda:{region}:{account_id}:function:{func_name}",
                "condition": "RetriesExhausted",
            },
            "requestPayload": payload,
            "errorMessage": error or "Unknown error",
        }
    )

    try:
        if ":sqs:" in target_arn:
            from robotocore.services.sqs.provider import _get_store

            queue_name = target_arn.rsplit(":", 1)[-1]
            store = _get_store(region, account_id)
            queue = store.get_queue(queue_name)
            if queue:
                queue.send_message(body=record)
        elif ":sns:" in target_arn:
            from robotocore.services.sns.provider import _get_store as get_store

            store = get_store(region, account_id)
            topic = store.get_topic(target_arn)
            if topic:
                topic.publish(message=record, subject="Lambda DLQ")
    except Exception:
        logger.exception("Failed to send to DLQ %s", target_arn)


def _handle_account_settings(region: str, account_id: str) -> Response:
    """Handle GET /account-settings."""
    backend = _get_moto_backend(account_id, region)

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


def _cascade_delete_function(func_name: str, region: str, account_id: str) -> None:
    """Clean up all native stores when a function is deleted.

    This handles parent-child cascade for resources stored outside Moto:
    event source mappings, provisioned concurrency configs, and DLQ configs.
    """
    func_arn = f"arn:aws:lambda:{region}:{account_id}:function:{func_name}"

    # Remove ESMs referencing this function
    with _esm_lock:
        to_remove = [
            uuid for uuid, config in _esm_store.items() if config.get("FunctionArn") == func_arn
        ]
        for uuid in to_remove:
            del _esm_store[uuid]

    # Remove provisioned concurrency configs for this function
    with _provisioned_lock:
        to_remove = [
            key
            for key in _provisioned_concurrency
            if key[0] == account_id and key[1] == region and key[2] == func_name
        ]
        for key in to_remove:
            del _provisioned_concurrency[key]

    # Remove DLQ config for this function
    dlq_key = (account_id, region, func_name)
    with _dlq_lock:
        _dlq_configs.pop(dlq_key, None)

    # Remove scaling configs for this function
    with _scaling_lock:
        to_remove = [
            key
            for key in _scaling_configs
            if key[0] == account_id and key[1] == region and key[2] == func_name
        ]
        for key in to_remove:
            del _scaling_configs[key]

    # Remove code signing config for this function
    csc_key = (account_id, region, func_name)
    with _code_signing_lock:
        _code_signing_configs.pop(csc_key, None)

    # Remove runtime management config for this function
    rmc_key = (account_id, region, func_name)
    with _runtime_mgmt_lock:
        _runtime_mgmt_configs.pop(rmc_key, None)


async def _handle_event_source_mappings(
    parts: list[str], method: str, body: bytes, request: Request, region: str, account_id: str
) -> Response:
    """Native event source mapping CRUD — bypasses Moto."""

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
                "FilterCriteria": spec.get("FilterCriteria"),
                "BisectBatchOnFunctionError": spec.get("BisectBatchOnFunctionError", False),
                "MaximumRetryAttempts": spec.get("MaximumRetryAttempts", -1),
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
                        "FilterCriteria",
                        "BisectBatchOnFunctionError",
                        "MaximumRetryAttempts",
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
            # /layers/{name}/versions/{num}/policy[/{sid}]
            elif len(parts) >= 5 and parts[4] == "policy":
                version_num = int(parts[3])
                return _handle_layer_version_permission(
                    layer_name, version_num, parts, method, body, request, region, account_id
                )

    return _error("InvalidRequest", "Unhandled layers path", 400)


async def _handle_tags(
    parts: list[str], method: str, body: bytes, request: Request, region: str, account_id: str
) -> Response:
    backend = _get_moto_backend(account_id, region)
    # /tags/{arn} — the ARN is the rest of the path
    arn = "/".join(parts[1:]) if len(parts) > 1 else ""
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

    runtime = getattr(fn, "run_time", "") or ""
    code_zip = None

    if hasattr(fn, "code") and fn.code:
        code_zip = fn.code.get("ZipFile")
        if isinstance(code_zip, str):
            code_zip = base64.b64decode(code_zip)

    if not code_zip:
        code_zip = getattr(fn, "code_bytes", None)

    env_vars = getattr(fn, "environment_vars", {}) or {}
    handler = getattr(fn, "handler", "lambda_function.handler")
    timeout = int(getattr(fn, "timeout", 3) or 3)
    memory_size = int(getattr(fn, "memory_size", 128) or 128)

    # Hot reload: check for mounted code directory
    code_dir = get_mount_path(func_name)
    use_hot_reload = False
    if code_dir:
        use_hot_reload = is_hot_reload_enabled() or is_hot_reload_for_function(env_vars)
        if use_hot_reload:
            watcher = get_file_watcher()
            if watcher.check_for_changes(func_name, code_dir):
                get_code_cache().invalidate(func_name)

    result, error_type, logs = None, None, ""

    if code_dir or code_zip:
        executor = get_executor_for_runtime(runtime)
        # Build kwargs — pass code_dir/hot_reload for Python executor
        kwargs: dict = {
            "code_zip": code_zip or b"",
            "handler": handler,
            "event": event,
            "function_name": func_name,
            "timeout": timeout,
            "memory_size": memory_size,
            "env_vars": env_vars,
            "region": region,
            "account_id": account_id,
        }
        # Only PythonExecutor accepts code_dir/hot_reload
        if code_dir:
            kwargs["code_dir"] = code_dir
            kwargs["hot_reload"] = use_hot_reload
        result, error_type, logs = executor.execute(**kwargs)
    else:
        # No code — return a simple success (like Moto's simple mode)
        result, error_type, logs = "Simple Lambda happy path OK", None, ""

    # Handle async invocation with destinations and DLQ
    if invocation_type == "Event":
        _dispatch_async_result(
            func_name=func_name,
            event=event,
            result=result,
            error_type=error_type,
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


async def _invoke_with_response_stream(
    func_name: str, body: bytes, request: Request, region: str, account_id: str
) -> Response:
    """InvokeWithResponseStream — execute and return result.

    Real AWS returns an event stream; we return the result as a simple JSON response
    which boto3 will parse into the EventStream shape.
    """
    backend = _get_moto_backend(account_id, region)
    fn = backend.get_function(func_name)

    event = json.loads(body) if body else {}
    runtime = getattr(fn, "run_time", "") or ""
    code_zip = None
    if hasattr(fn, "code") and fn.code:
        code_zip = fn.code.get("ZipFile")
        if isinstance(code_zip, str):
            code_zip = base64.b64decode(code_zip)
    if not code_zip:
        code_zip = getattr(fn, "code_bytes", None)

    env_vars = getattr(fn, "environment_vars", {}) or {}
    handler_name = getattr(fn, "handler", "lambda_function.handler")
    timeout = int(getattr(fn, "timeout", 3) or 3)
    memory_size = int(getattr(fn, "memory_size", 128) or 128)

    result = None
    if code_zip:
        executor = get_executor_for_runtime(runtime)
        result, error_type, logs = executor.execute(
            code_zip=code_zip,
            handler=handler_name,
            event=event,
            function_name=func_name,
            timeout=timeout,
            memory_size=memory_size,
            env_vars=env_vars,
            region=region,
            account_id=account_id,
        )

    if result is None:
        payload = b"null"
    elif isinstance(result, (dict, list)):
        payload = json.dumps(result).encode()
    elif isinstance(result, str):
        payload = json.dumps(result).encode()
    else:
        payload = str(result).encode()

    headers = {
        "x-amz-request-id": str(uuid.uuid4()),
        "x-amz-executed-version": "$LATEST",
        "content-type": "application/json",
    }
    return Response(
        content=payload,
        status_code=200,
        headers=headers,
        media_type="application/json",
    )


async def _invoke_async(
    func_name: str, body: bytes, request: Request, region: str, account_id: str
) -> Response:
    """InvokeAsync (deprecated) — queue async invocation and return 202."""
    backend = _get_moto_backend(account_id, region)
    # Verify function exists
    backend.get_function(func_name)
    # Just return 202 — in a real environment the function would execute asynchronously
    return Response(content=b'{"Status": 202}', status_code=202, media_type="application/json")


def _dispatch_async_result(
    func_name: str,
    event: dict,
    result,
    error_type: str | None,
    region: str,
    account_id: str,
) -> None:
    """After async invocation, dispatch to destinations and/or DLQ."""
    func_arn = f"arn:aws:lambda:{region}:{account_id}:function:{func_name}"

    # Try to get event invoke config from Moto
    try:
        backend = _get_moto_backend(account_id, region)
        invoke_config = backend.get_function_event_invoke_config(func_name)
    except Exception:
        invoke_config = None

    is_success = error_type is None

    if invoke_config:
        dest_config = invoke_config.get("DestinationConfig", {})
        if is_success and dest_config.get("OnSuccess", {}).get("Destination"):
            dest_arn = dest_config["OnSuccess"]["Destination"]
            from robotocore.services.lambda_.destinations import dispatch_destination

            dispatch_destination(
                destination_arn=dest_arn,
                function_arn=func_arn,
                payload=event,
                is_success=True,
                result=result,
                error=None,
                region=region,
                account_id=account_id,
            )
        elif not is_success and dest_config.get("OnFailure", {}).get("Destination"):
            dest_arn = dest_config["OnFailure"]["Destination"]
            from robotocore.services.lambda_.destinations import dispatch_destination

            dispatch_destination(
                destination_arn=dest_arn,
                function_arn=func_arn,
                payload=event,
                is_success=False,
                result=result,
                error=error_type,
                region=region,
                account_id=account_id,
            )

    # DLQ on failure
    if not is_success:
        dispatch_to_dlq(
            func_name=func_name,
            payload=event,
            error=error_type,
            region=region,
            account_id=account_id,
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
