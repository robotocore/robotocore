"""ASGI application -- the main HTTP entry point for Robotocore."""

import json
import os
import re
import time

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from robotocore import __version__
from robotocore.gateway.handler_chain import HandlerChain, RequestContext
from robotocore.gateway.handlers import (
    audit_response_handler,
    cors_handler,
    cors_response_handler,
    error_normalizer,
    logging_response_handler,
    populate_context_handler,
)
from robotocore.gateway.router import route_to_service
from robotocore.observability.hooks import run_init_hooks
from robotocore.observability.metrics import request_counter
from robotocore.observability.tracing import TracingMiddleware
from robotocore.providers.moto_bridge import forward_to_moto
from robotocore.services.acm.provider import handle_acm_request
from robotocore.services.apigateway.provider import handle_apigateway_request
from robotocore.services.apigatewayv2.provider import handle_apigatewayv2_request
from robotocore.services.appsync.provider import handle_appsync_request
from robotocore.services.batch.provider import handle_batch_request
from robotocore.services.cloudformation.provider import handle_cloudformation_request
from robotocore.services.cloudwatch.logs_provider import handle_logs_request
from robotocore.services.cloudwatch.provider import handle_cloudwatch_request
from robotocore.services.cognito.provider import handle_cognito_request
from robotocore.services.config.provider import handle_config_request
from robotocore.services.dynamodb.provider import handle_dynamodb_request
from robotocore.services.dynamodbstreams.provider import handle_dynamodbstreams_request
from robotocore.services.ec2.provider import handle_ec2_request
from robotocore.services.ecr.provider import handle_ecr_request
from robotocore.services.ecs.provider import handle_ecs_request
from robotocore.services.events.provider import handle_events_request
from robotocore.services.firehose.provider import handle_firehose_request
from robotocore.services.iam.provider import handle_iam_request
from robotocore.services.kinesis.provider import handle_kinesis_request
from robotocore.services.lambda_.provider import handle_lambda_request
from robotocore.services.opensearch.provider import handle_es_request, handle_opensearch_request
from robotocore.services.registry import SERVICE_REGISTRY, ServiceStatus
from robotocore.services.rekognition.provider import handle_rekognition_request
from robotocore.services.resource_groups.provider import handle_resource_groups_request
from robotocore.services.route53.provider import handle_route53_request
from robotocore.services.s3.provider import handle_s3_request
from robotocore.services.scheduler.provider import handle_scheduler_request
from robotocore.services.secretsmanager.provider import handle_secretsmanager_request
from robotocore.services.ses.provider import handle_ses_request
from robotocore.services.ses.sesv2_provider import handle_sesv2_request
from robotocore.services.sns.provider import handle_sns_request
from robotocore.services.sqs.provider import handle_sqs_request
from robotocore.services.ssm.provider import handle_ssm_request
from robotocore.services.stepfunctions.provider import handle_stepfunctions_request
from robotocore.services.sts.provider import handle_sts_request
from robotocore.services.support.provider import handle_support_request
from robotocore.services.tagging.provider import handle_tagging_request
from robotocore.services.xray.provider import handle_xray_request

# Services with native providers (bypass Moto)
NATIVE_PROVIDERS = {
    "apigateway": handle_apigateway_request,
    "apigatewayv2": handle_apigatewayv2_request,
    "appsync": handle_appsync_request,
    "batch": handle_batch_request,
    "cloudformation": handle_cloudformation_request,
    "cloudwatch": handle_cloudwatch_request,
    "config": handle_config_request,
    "cognito-idp": handle_cognito_request,
    "dynamodb": handle_dynamodb_request,
    "dynamodbstreams": handle_dynamodbstreams_request,
    "ecs": handle_ecs_request,
    "events": handle_events_request,
    "firehose": handle_firehose_request,
    "kinesis": handle_kinesis_request,
    "lambda": handle_lambda_request,
    "logs": handle_logs_request,
    "s3": handle_s3_request,
    "scheduler": handle_scheduler_request,
    "secretsmanager": handle_secretsmanager_request,
    "ses": handle_ses_request,
    "sesv2": handle_sesv2_request,
    "sqs": handle_sqs_request,
    "sns": handle_sns_request,
    "stepfunctions": handle_stepfunctions_request,
    "sts": handle_sts_request,
    "resourcegroupstaggingapi": handle_tagging_request,
    "acm": handle_acm_request,
    "ec2": handle_ec2_request,
    "ecr": handle_ecr_request,
    "es": handle_es_request,
    "iam": handle_iam_request,
    "opensearch": handle_opensearch_request,
    "rekognition": handle_rekognition_request,
    "resource-groups": handle_resource_groups_request,
    "route53": handle_route53_request,
    "ssm": handle_ssm_request,
    "support": handle_support_request,
    "xray": handle_xray_request,
}

# Default account ID
DEFAULT_ACCOUNT_ID = "123456789012"

# Regex to extract account ID from SigV4 Credential
_CREDENTIAL_RE = re.compile(r"Credential=(\d+)/")

# Track server start time for uptime
_server_start_time: float = 0.0


def _build_handler_chain() -> HandlerChain:
    """Build the default handler chain for AWS requests."""
    from robotocore.chaos.middleware import chaos_handler
    from robotocore.gateway.iam_middleware import iam_enforcement_handler

    chain = HandlerChain()
    chain.request_handlers.append(cors_handler)
    chain.request_handlers.append(populate_context_handler)
    chain.request_handlers.append(chaos_handler)
    chain.request_handlers.append(iam_enforcement_handler)
    chain.response_handlers.append(cors_response_handler)
    chain.response_handlers.append(audit_response_handler)
    chain.response_handlers.append(logging_response_handler)
    chain.exception_handlers.append(error_normalizer)
    return chain


_handler_chain = _build_handler_chain()


def _extract_account_id(request: Request) -> str:
    """Extract account ID from SigV4 credentials, defaulting to 000000000000."""
    auth = request.headers.get("authorization", "")
    match = _CREDENTIAL_RE.search(auth)
    if match:
        return match.group(1)
    # Check query param for presigned URLs
    credential = request.query_params.get("X-Amz-Credential", "")
    if credential:
        parts = credential.split("/")
        if parts and len(parts[0]) == 12 and parts[0].isdigit():
            return parts[0]
    return DEFAULT_ACCOUNT_ID


def _extract_region_account(request: Request) -> tuple[str, str]:
    """Extract region and account from auth header."""
    region = "us-east-1"
    account_id = _extract_account_id(request)
    auth = request.headers.get("authorization", "")
    region_match = re.search(r"Credential=[^/]+/\d{8}/([^/]+)/", auth)
    if region_match:
        region = region_match.group(1)
    return region, account_id


# ---------------------------------------------------------------------------
# Management endpoints
# ---------------------------------------------------------------------------


async def health(request: Request) -> JSONResponse:
    """Enhanced health endpoint with per-service status and request counts."""
    uptime = time.monotonic() - _server_start_time if _server_start_time else 0

    counts = request_counter.get_all()
    services_status = {}
    for name, info in sorted(SERVICE_REGISTRY.items()):
        stype = "native" if info.status == ServiceStatus.NATIVE else "moto"
        services_status[name] = {
            "status": "running",
            "type": stype,
            "requests": counts.get(name, 0),
        }

    return JSONResponse(
        {
            "status": "running",
            "version": __version__,
            "uptime_seconds": round(uptime, 1),
            "services": services_status,
        }
    )


async def services_endpoint(request: Request) -> JSONResponse:
    """List all registered services with their status and protocol."""
    services = []
    for name, info in sorted(SERVICE_REGISTRY.items()):
        stype = "native" if info.status == ServiceStatus.NATIVE else "moto"
        services.append(
            {
                "name": name,
                "status": stype,
                "protocol": info.protocol,
                "description": info.description,
            }
        )
    return JSONResponse({"services": services})


async def config_endpoint(request: Request) -> JSONResponse:
    """Return current Robotocore configuration."""
    native_count = sum(1 for s in SERVICE_REGISTRY.values() if s.status == ServiceStatus.NATIVE)
    return JSONResponse(
        {
            "enforce_iam": False,
            "persistence": os.environ.get("PERSISTENCE", "0") == "1",
            "log_level": os.environ.get("LOG_LEVEL", "INFO").upper(),
            "debug": os.environ.get("DEBUG", "0") == "1",
            "region": os.environ.get("DEFAULT_REGION", "us-east-1"),
            "services_count": len(SERVICE_REGISTRY),
            "native_providers": native_count,
        }
    )


async def save_state(request: Request) -> JSONResponse:
    """Save emulator state to disk (Cloud Pods-like feature)."""
    from robotocore.state.manager import get_state_manager

    body = await request.body()
    params = {}
    if body:
        params = json.loads(body)

    manager = get_state_manager()
    path = params.get("path") or manager.state_dir
    if not path:
        return JSONResponse(
            {"error": "No state directory configured. Set ROBOTOCORE_STATE_DIR or pass 'path'."},
            status_code=400,
        )

    saved_path = manager.save(
        path=path,
        name=params.get("name"),
        services=params.get("services"),
    )
    return JSONResponse({"status": "saved", "path": saved_path})


async def load_state(request: Request) -> JSONResponse:
    """Load emulator state from disk."""
    from robotocore.state.manager import get_state_manager

    body = await request.body()
    params = {}
    if body:
        params = json.loads(body)

    manager = get_state_manager()
    path = params.get("path") or manager.state_dir
    if not path:
        return JSONResponse(
            {"error": "No state directory configured. Set ROBOTOCORE_STATE_DIR or pass 'path'."},
            status_code=400,
        )

    success = manager.load(
        path=path,
        name=params.get("name"),
        services=params.get("services"),
    )
    return JSONResponse({"status": "loaded" if success else "no_state_found", "path": str(path)})


async def list_snapshots(request: Request) -> JSONResponse:
    """List all named state snapshots."""
    from robotocore.state.manager import get_state_manager

    manager = get_state_manager()
    snapshots = manager.list_snapshots()
    return JSONResponse({"snapshots": snapshots})


async def reset_state(request: Request) -> JSONResponse:
    """Reset all emulator state."""
    from robotocore.state.manager import get_state_manager

    get_state_manager().reset()
    return JSONResponse({"status": "reset"})


async def export_state(request: Request) -> JSONResponse:
    """Export emulator state as JSON."""
    from robotocore.state.manager import get_state_manager

    manager = get_state_manager()
    data = manager.export_json()
    return JSONResponse(data)


async def import_state(request: Request) -> JSONResponse:
    """Import emulator state from JSON."""
    from robotocore.state.manager import get_state_manager

    body = await request.body()
    if not body:
        return JSONResponse({"error": "No data provided"}, status_code=400)

    data = json.loads(body)
    manager = get_state_manager()
    manager.import_json(data)
    return JSONResponse({"status": "imported"})


# ---------------------------------------------------------------------------
# Chaos engineering endpoints
# ---------------------------------------------------------------------------


async def chaos_list_rules(request: Request) -> JSONResponse:
    """List all fault injection rules."""
    from robotocore.chaos.fault_rules import get_fault_store

    rules = get_fault_store().list_rules()
    return JSONResponse({"rules": rules})


async def chaos_add_rule(request: Request) -> JSONResponse:
    """Add a fault injection rule."""
    from robotocore.chaos.fault_rules import FaultRule, get_fault_store

    body = await request.body()
    if not body:
        return JSONResponse({"error": "No rule provided"}, status_code=400)

    try:
        data = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    if not isinstance(data, dict):
        return JSONResponse({"error": "Expected JSON object"}, status_code=400)
    rule = FaultRule.from_dict(data)
    rule_id = get_fault_store().add(rule)
    return JSONResponse({"status": "created", "rule_id": rule_id}, status_code=201)


async def chaos_delete_rule(request: Request) -> JSONResponse:
    """Delete a fault injection rule by ID."""
    from robotocore.chaos.fault_rules import get_fault_store

    rule_id = request.path_params["rule_id"]
    removed = get_fault_store().remove(rule_id)
    if removed:
        return JSONResponse({"status": "deleted", "rule_id": rule_id})
    return JSONResponse({"error": "Rule not found"}, status_code=404)


async def chaos_clear_rules(request: Request) -> JSONResponse:
    """Clear all fault injection rules."""
    from robotocore.chaos.fault_rules import get_fault_store

    count = get_fault_store().clear()
    return JSONResponse({"status": "cleared", "count": count})


# ---------------------------------------------------------------------------
# Resource browser endpoints
# ---------------------------------------------------------------------------


async def resources_overview(request: Request) -> JSONResponse:
    """List resource counts per service."""
    from robotocore.resources.browser import get_resource_counts

    counts = get_resource_counts()
    return JSONResponse({"resources": counts})


async def resources_for_service(request: Request) -> JSONResponse:
    """List resources for a specific service."""
    from robotocore.resources.browser import get_service_resources

    service = request.path_params["service"]
    resources = get_service_resources(service)
    return JSONResponse({"service": service, "resources": resources})


# ---------------------------------------------------------------------------
# Request audit log endpoints
# ---------------------------------------------------------------------------


async def audit_log(request: Request) -> JSONResponse:
    """Return recent API requests."""
    from robotocore.audit.log import get_audit_log

    limit = int(request.query_params.get("limit", "100"))
    entries = get_audit_log().recent(limit)
    return JSONResponse({"entries": entries, "count": len(entries)})


# ---------------------------------------------------------------------------
# AWS request handler
# ---------------------------------------------------------------------------


async def handle_aws_request(request: Request) -> Response:
    """Main handler: route, build context, run handler chain, forward to Moto."""
    service_name = route_to_service(request)
    if service_name is None:
        return JSONResponse(
            {"error": "Could not determine target AWS service from request"},
            status_code=400,
        )

    # Multi-account support: extract account ID from request
    account_id = _extract_account_id(request)

    context = RequestContext(
        request=request,
        service_name=service_name,
        account_id=account_id,
    )

    # Pre-read the body so synchronous handlers (populate_context_handler) can
    # access it via request._body for form-encoded Action parsing.
    await request.body()

    _handler_chain.handle(context)

    # If a handler already set a response (e.g. CORS preflight), return it
    if context.response is not None:
        return context.response

    # Track request count
    request_counter.increment(service_name)

    # Use native provider if available, otherwise forward to Moto
    native_handler = NATIVE_PROVIDERS.get(service_name)
    if native_handler:
        response = await native_handler(request, context.region, context.account_id)
    else:
        response = await forward_to_moto(request, service_name)

    # Run response handlers with the Moto response
    context.response = response
    for handler in _handler_chain.response_handlers:
        handler(context)

    # Auto-save if PERSISTENCE=1
    if os.environ.get("PERSISTENCE", "0") == "1":
        _maybe_persist()

    return context.response


def _maybe_persist() -> None:
    """Debounced auto-save: at most once per second."""
    from robotocore.state.manager import get_state_manager

    manager = get_state_manager()
    if not manager.state_dir:
        default_dir = os.environ.get("ROBOTOCORE_STATE_DIR", "/tmp/robotocore/state")
        from pathlib import Path

        manager.state_dir = Path(default_dir)

    manager.save_debounced()


# ---------------------------------------------------------------------------
# API Gateway execution endpoints
# ---------------------------------------------------------------------------


async def handle_execute_api(
    request: Request, rest_api_id: str, stage: str, proxy_path: str
) -> Response:
    """Handle API Gateway execute-api requests (invoke deployed APIs)."""
    from robotocore.services.apigateway.executor import execute_api_request

    body = await request.body()
    headers = dict(request.headers)
    query_params = dict(request.query_params)
    region, account_id = _extract_region_account(request)

    status_code, resp_headers, resp_body = execute_api_request(
        rest_api_id=rest_api_id,
        stage=stage,
        method=request.method,
        path="/" + proxy_path if proxy_path else "/",
        body=body,
        headers=headers,
        query_params=query_params,
        region=region,
        account_id=account_id,
    )
    return Response(
        content=resp_body,
        status_code=status_code,
        headers=resp_headers,
        media_type="application/json",
    )


async def handle_execute_api_v2(
    request: Request, api_id: str, stage: str, proxy_path: str
) -> Response:
    """Handle API Gateway V2 HTTP API execute-api requests."""
    from robotocore.services.apigatewayv2.executor import execute_v2_request

    body = await request.body()
    headers = dict(request.headers)
    query_params = dict(request.query_params)
    region, account_id = _extract_region_account(request)

    status_code, resp_headers, resp_body = execute_v2_request(
        api_id=api_id,
        stage=stage,
        method=request.method,
        path="/" + proxy_path if proxy_path else "/",
        body=body,
        headers=headers,
        query_params=query_params,
        region=region,
        account_id=account_id,
    )
    return Response(
        content=resp_body,
        status_code=status_code,
        headers=resp_headers,
        media_type="application/json",
    )


async def handle_connections_api(
    request: Request,
    api_id: str,
    stage: str,
    connection_id: str,
) -> Response:
    """Handle @connections API for WebSocket management."""
    from robotocore.services.apigatewayv2.provider import (
        delete_connection,
        get_connection,
        post_to_connection,
    )

    method = request.method.upper()

    if method == "POST":
        body = await request.body()
        success = post_to_connection(api_id, connection_id, body)
        if success:
            return Response(status_code=200)
        return Response(
            content=json.dumps({"message": "Connection not found"}),
            status_code=410,
            media_type="application/json",
        )
    elif method == "DELETE":
        delete_connection(api_id, connection_id)
        return Response(status_code=204)
    elif method == "GET":
        conn = get_connection(api_id, connection_id)
        if conn:
            return Response(
                content=json.dumps(conn),
                status_code=200,
                media_type="application/json",
            )
        return Response(
            content=json.dumps({"message": "Connection not found"}),
            status_code=410,
            media_type="application/json",
        )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

management_routes = [
    Route("/_robotocore/health", health, methods=["GET"]),
    Route("/_robotocore/services", services_endpoint, methods=["GET"]),
    Route("/_robotocore/config", config_endpoint, methods=["GET"]),
    Route("/_robotocore/state/save", save_state, methods=["POST"]),
    Route("/_robotocore/state/load", load_state, methods=["POST"]),
    Route("/_robotocore/state/snapshots", list_snapshots, methods=["GET"]),
    Route("/_robotocore/state/reset", reset_state, methods=["POST"]),
    Route("/_robotocore/state/export", export_state, methods=["GET"]),
    Route("/_robotocore/state/import", import_state, methods=["POST"]),
    # Chaos engineering
    Route("/_robotocore/chaos/rules", chaos_list_rules, methods=["GET"]),
    Route("/_robotocore/chaos/rules", chaos_add_rule, methods=["POST"]),
    Route("/_robotocore/chaos/rules/clear", chaos_clear_rules, methods=["POST"]),
    Route("/_robotocore/chaos/rules/{rule_id}", chaos_delete_rule, methods=["DELETE"]),
    # Resource browser
    Route("/_robotocore/resources", resources_overview, methods=["GET"]),
    Route("/_robotocore/resources/{service}", resources_for_service, methods=["GET"]),
    # Audit log
    Route("/_robotocore/audit", audit_log, methods=["GET"]),
]


def _start_background_engines():
    """Start background engines for cross-service integrations."""
    global _server_start_time
    _server_start_time = time.monotonic()

    from robotocore.services.lambda_.event_source import get_engine

    get_engine().start()
    from robotocore.services.cloudwatch.alarm_scheduler import get_alarm_scheduler

    get_alarm_scheduler().start()

    # Auto-load state if configured
    if os.environ.get("ROBOTOCORE_STATE_DIR"):
        from robotocore.state.manager import get_state_manager

        get_state_manager().load()

    # Run ready hooks
    run_init_hooks("ready")


def _shutdown():
    """Shutdown hook -- auto-save state if configured."""
    if os.environ.get("ROBOTOCORE_PERSIST", "0") == "1":
        from robotocore.state.manager import get_state_manager

        manager = get_state_manager()
        if manager.state_dir:
            manager.save()

    # Run shutdown hooks
    run_init_hooks("shutdown")


app = Starlette(
    routes=management_routes,
    on_startup=[_start_background_engines],
    on_shutdown=[_shutdown],
)


class AWSRoutingMiddleware:
    """Lightweight ASGI middleware for routing AWS vs management requests."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")

        if path.startswith("/_robotocore/"):
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)

        # V1: /restapis/{id}/{stage}/_user_request_/{path}
        exec_match = re.match(r"^/restapis/([^/]+)/([^/]+)/_user_request_/?(.*)", path)
        if exec_match:
            response = await handle_execute_api(
                request,
                rest_api_id=exec_match.group(1),
                stage=exec_match.group(2),
                proxy_path=exec_match.group(3),
            )
            await response(scope, receive, send)
            return

        # V2: /@connections/{connection_id} (WebSocket management)
        conn_match = re.match(r"^/@connections/([^/]+)$", path)
        if conn_match:
            api_id = request.query_params.get("apiId", "")
            stage = request.query_params.get("stage", "$default")
            response = await handle_connections_api(
                request,
                api_id=api_id,
                stage=stage,
                connection_id=conn_match.group(1),
            )
            await response(scope, receive, send)
            return

        # V2: /v2-exec/{api_id}/{stage}/{path} (HTTP API execution)
        v2_match = re.match(r"^/v2-exec/([^/]+)/([^/]+)/?(.*)", path)
        if v2_match:
            response = await handle_execute_api_v2(
                request,
                api_id=v2_match.group(1),
                stage=v2_match.group(2),
                proxy_path=v2_match.group(3),
            )
            await response(scope, receive, send)
            return

        response = await handle_aws_request(request)
        await response(scope, receive, send)


# Order matters: AWSRoutingMiddleware is added first (inner), TracingMiddleware second (outer).
# Request flow: TracingMiddleware -> AWSRoutingMiddleware -> app/handler
app.add_middleware(AWSRoutingMiddleware)
app.add_middleware(TracingMiddleware)
