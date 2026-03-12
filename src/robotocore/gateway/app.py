"""ASGI application -- the main HTTP entry point for Robotocore."""

import json
import os
import re
import time
from pathlib import Path

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from robotocore import __version__
from robotocore.console.app import get_console_routes
from robotocore.dashboard.app import dashboard_endpoint
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
from robotocore.gateway.s3_routing import (
    get_s3_routing_config,
    parse_s3_vhost,
    rewrite_vhost_to_path,
)
from robotocore.gateway.tls import TLSConfig, get_cert_info
from robotocore.observability.hooks import run_init_hooks
from robotocore.observability.metrics import request_counter
from robotocore.observability.request_context import (
    RequestContext as ObsRequestContext,
)
from robotocore.observability.request_context import (
    set_current_context,
)
from robotocore.observability.timeline import handle_timeline
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
from robotocore.services.cognito.hosted_ui import (
    confirm_forgot_password_endpoint,
    forgot_password_endpoint,
    jwks_json,
    login_get,
    login_post,
    logout,
    oauth2_authorize,
    oauth2_token,
    oauth2_userinfo,
    openid_configuration,
)
from robotocore.services.cognito.provider import handle_cognito_request
from robotocore.services.config.provider import handle_config_request
from robotocore.services.dynamodb.provider import handle_dynamodb_request
from robotocore.services.dynamodbstreams.provider import handle_dynamodbstreams_request
from robotocore.services.ec2.provider import handle_ec2_request
from robotocore.services.ecr.provider import handle_ecr_request
from robotocore.services.ecs.provider import handle_ecs_request
from robotocore.services.eks.provider import handle_eks_request
from robotocore.services.elasticache.provider import handle_elasticache_request
from robotocore.services.events.provider import handle_events_request
from robotocore.services.firehose.provider import handle_firehose_request
from robotocore.services.iam.provider import handle_iam_request
from robotocore.services.iot.data_provider import handle_iot_data_request
from robotocore.services.iot.provider import handle_iot_request
from robotocore.services.kinesis.provider import handle_kinesis_request
from robotocore.services.lambda_.provider import handle_lambda_request
from robotocore.services.loader import (
    get_allowed_services,
    get_effective_provider,
    get_service_info_with_status,
    init_loader,
    is_service_allowed,
)
from robotocore.services.opensearch.provider import handle_es_request, handle_opensearch_request
from robotocore.services.pipes.provider import handle_pipes_request
from robotocore.services.rds.data_provider import handle_rdsdata_request
from robotocore.services.rds.provider import handle_rds_request
from robotocore.services.registry import SERVICE_REGISTRY, ServiceStatus
from robotocore.services.rekognition.provider import handle_rekognition_request
from robotocore.services.resource_groups.provider import handle_resource_groups_request
from robotocore.services.route53.provider import handle_route53_request
from robotocore.services.s3.provider import handle_s3_request
from robotocore.services.s3.website import (
    handle_website_request,
    is_website_request,
    parse_website_host,
)
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
from robotocore.services.synthetics.provider import handle_synthetics_request
from robotocore.services.tagging.provider import handle_tagging_request
from robotocore.services.xray.provider import handle_xray_request

# Module-level TLS state (set during startup in main.py or tests)
_tls_config: TLSConfig = TLSConfig(enabled=False)
_tls_cert_path: Path | None = None

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
    "eks": handle_eks_request,
    "events": handle_events_request,
    "firehose": handle_firehose_request,
    "iot": handle_iot_request,
    "iotdata": handle_iot_data_request,
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
    "pipes": handle_pipes_request,
    "rekognition": handle_rekognition_request,
    "resource-groups": handle_resource_groups_request,
    "route53": handle_route53_request,
    "ssm": handle_ssm_request,
    "support": handle_support_request,
    "xray": handle_xray_request,
    "rds": handle_rds_request,
    "rdsdata": handle_rdsdata_request,
    "elasticache": handle_elasticache_request,
    "synthetics": handle_synthetics_request,
}

# Default account ID
DEFAULT_ACCOUNT_ID = "123456789012"

# Regex to extract account ID from SigV4 Credential
_CREDENTIAL_RE = re.compile(r"Credential=(\d{12})/")

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

    allowed = get_allowed_services()
    counts = request_counter.get_all()
    services_status = {}
    for name, info in sorted(SERVICE_REGISTRY.items()):
        if allowed is not None and name not in allowed:
            continue
        stype = "native" if info.status == ServiceStatus.NATIVE else "moto"
        services_status[name] = {
            "status": "running",
            "type": stype,
            "requests": counts.get(name, 0),
        }

    services_env = os.environ.get("SERVICES", "").strip()
    services_filter = services_env if services_env else "all"

    result: dict[str, object] = {
        "status": "running",
        "version": __version__,
        "uptime_seconds": round(uptime, 1),
        "services_filter": services_filter,
        "services": services_status,
    }
    if allowed is not None:
        result["enabled_services"] = sorted(allowed)

    return JSONResponse(result)


async def services_endpoint(request: Request) -> JSONResponse:
    """List all registered services with their status, protocol, and enabled state."""
    services = []
    for name in sorted(SERVICE_REGISTRY.keys()):
        services.append(get_service_info_with_status(name))
    return JSONResponse({"services": services})


async def config_endpoint(request: Request) -> JSONResponse:
    """Return current Robotocore configuration (GET) or update it (POST)."""
    from robotocore.config.runtime import get_runtime_config

    rt = get_runtime_config()

    if request.method == "POST":
        if not rt.updates_enabled:
            return JSONResponse(
                {"error": "Runtime config updates are disabled. Set ENABLE_CONFIG_UPDATES=1."},
                status_code=403,
            )
        body = await request.body()
        if not body:
            return JSONResponse({"error": "No settings provided"}, status_code=400)
        try:
            updates = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return JSONResponse({"error": "Invalid JSON"}, status_code=400)

        if not isinstance(updates, dict):
            return JSONResponse({"error": "Expected JSON object"}, status_code=400)

        results: dict[str, str] = {}
        for key, value in updates.items():
            try:
                rt.set(key, str(value))
                results[key] = str(value)
            except ValueError as e:
                return JSONResponse({"error": str(e)}, status_code=400)

        return JSONResponse({"status": "updated", "updated": results})

    # GET — return config with detailed settings
    native_count = sum(1 for s in SERVICE_REGISTRY.values() if s.status == ServiceStatus.NATIVE)
    log_level = rt.get("LOG_LEVEL", "INFO")
    debug_val = rt.get("DEBUG", "0")
    return JSONResponse(
        {
            "enforce_iam": rt.get("ENFORCE_IAM", "0") == "1",
            "persistence": os.environ.get("PERSISTENCE", "0") == "1",
            "log_level": (log_level or "INFO").upper(),
            "debug": debug_val == "1",
            "region": os.environ.get("DEFAULT_REGION", "us-east-1"),
            "services_count": len(SERVICE_REGISTRY),
            "native_providers": native_count,
            "updates_enabled": rt.updates_enabled,
            "settings": rt.list_all(),
        }
    )


async def config_delete_endpoint(request: Request) -> JSONResponse:
    """Reset a runtime config override back to its original value."""
    from robotocore.config.runtime import get_runtime_config

    rt = get_runtime_config()
    key = request.path_params["key"]
    old = rt.delete(key)
    if old is None:
        return JSONResponse({"error": f"No runtime override for {key}"}, status_code=404)
    return JSONResponse({"status": "reset", "key": key, "previous_value": old})


async def save_state(request: Request) -> JSONResponse:
    """Save emulator state. When a name is provided, creates a versioned in-memory snapshot."""
    from robotocore.state.manager import get_state_manager

    body = await request.body()
    params = {}
    if body:
        params = json.loads(body)

    manager = get_state_manager()
    name = params.get("name")

    # If a name is provided, use versioned in-memory snapshots
    if name:
        try:
            result = manager.save_versioned(
                name=name,
                services=params.get("services"),
            )
            return JSONResponse(
                {
                    "status": "saved",
                    "name": result["name"],
                    "version": result["version"],
                }
            )
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)

    # No name: fall back to disk-based save
    path = params.get("path") or manager.state_dir
    if not path:
        return JSONResponse(
            {"error": "No state directory configured. Set ROBOTOCORE_STATE_DIR or pass 'path'."},
            status_code=400,
        )

    saved_path = manager.save(
        path=path,
        services=params.get("services"),
    )
    return JSONResponse({"status": "saved", "path": saved_path})


async def load_state(request: Request) -> JSONResponse:
    """Load emulator state. When a name is provided, loads from versioned in-memory snapshots."""
    from robotocore.state.manager import get_state_manager

    body = await request.body()
    params = {}
    if body:
        params = json.loads(body)

    manager = get_state_manager()
    name = params.get("name")

    # If a name is provided, try versioned in-memory snapshots first
    if name:
        version = params.get("version")
        try:
            result = manager.load_versioned(
                name=name,
                version=version,
                services=params.get("services"),
            )
            return JSONResponse(
                {
                    "status": "loaded",
                    "name": result["name"],
                    "version": result["version"],
                }
            )
        except ValueError:
            # Fall through to disk-based load if not found in memory
            pass

    # Fall back to disk-based load
    path = params.get("path") or manager.state_dir
    if not path:
        return JSONResponse(
            {"error": "No state directory configured. Set ROBOTOCORE_STATE_DIR or pass 'path'."},
            status_code=400,
        )

    success = manager.load(
        path=path,
        name=name,
        services=params.get("services"),
    )
    return JSONResponse({"status": "loaded" if success else "no_state_found", "path": str(path)})


async def list_snapshots(request: Request) -> JSONResponse:
    """List all named state snapshots (disk-based)."""
    from robotocore.state.manager import get_state_manager

    manager = get_state_manager()
    snapshots = manager.list_snapshots()
    return JSONResponse({"snapshots": snapshots})


async def list_versioned_snapshots(request: Request) -> JSONResponse:
    """List all versioned in-memory snapshots with metadata."""
    from robotocore.state.manager import get_state_manager

    manager = get_state_manager()
    snapshots = manager.list_versioned()
    return JSONResponse({"snapshots": snapshots})


async def delete_versioned_snapshot(request: Request) -> JSONResponse:
    """Delete a versioned snapshot or a specific version.

    Body: {"name": "x"} to delete all versions, or {"name": "x", "version": 3}.
    """
    from robotocore.state.manager import get_state_manager

    body = await request.body()
    if not body:
        return JSONResponse({"error": "Request body required"}, status_code=400)

    params = json.loads(body)
    name = params.get("name")
    if not name:
        return JSONResponse({"error": "Missing 'name' parameter"}, status_code=400)

    version = params.get("version")
    manager = get_state_manager()

    try:
        result = manager.delete_versioned(name=name, version=version)
        return JSONResponse({"status": "deleted", **result})
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=404)


async def snapshot_versions(request: Request) -> JSONResponse:
    """Return version history for a specific snapshot.

    Query param: name=<snapshot-name>
    """
    from robotocore.state.manager import get_state_manager

    name = request.query_params.get("name")
    if not name:
        return JSONResponse({"error": "Missing 'name' query parameter"}, status_code=400)

    manager = get_state_manager()
    try:
        versions = manager.versions_for_snapshot(name)
        return JSONResponse({"name": name, "versions": versions})
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=404)


async def reset_state(request: Request) -> JSONResponse:
    """Reset all emulator state."""
    from robotocore.state.manager import get_state_manager

    get_state_manager().reset()
    return JSONResponse({"status": "reset"})


async def list_state_hooks(request: Request) -> JSONResponse:
    """List all registered state lifecycle hooks."""
    from robotocore.state.hooks import state_hooks

    return JSONResponse({"hooks": state_hooks.list_hooks()})


async def state_consistency_status(request: Request) -> JSONResponse:
    """Return current state consistency status."""
    from robotocore.state.consistency import get_consistent_state_manager

    csm = get_consistent_state_manager()
    return JSONResponse(csm.status_dict())


async def export_state(request: Request) -> Response:
    """Export emulator state.

    Query params:
        format=json (default) — returns JSON of native provider state
        format=snapshot — returns compressed tar.gz of full state (Moto + native)
        name=<snapshot> — export a specific named snapshot
    """
    from robotocore.state.manager import get_state_manager

    manager = get_state_manager()
    fmt = request.query_params.get("format", "json")
    snap_name = request.query_params.get("name")

    if fmt == "snapshot":
        try:
            data = manager.export_snapshot_bytes(name=snap_name)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        filename = f"{snap_name or 'state'}.tar.gz"
        return Response(
            content=data,
            status_code=200,
            media_type="application/gzip",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    data = manager.export_json()
    return JSONResponse(data)


async def import_state(request: Request) -> JSONResponse:
    """Import emulator state.

    Content-Type:
        application/json — import native provider state from JSON body
        application/gzip or application/octet-stream — import full snapshot from tar.gz
    Query params:
        name=<name> — assign a name to the imported snapshot (for snapshot format)
    """
    from robotocore.state.manager import get_state_manager

    body = await request.body()
    if not body:
        return JSONResponse({"error": "No data provided"}, status_code=400)

    manager = get_state_manager()
    content_type = request.headers.get("content-type", "")

    if "gzip" in content_type or "octet-stream" in content_type:
        snap_name = request.query_params.get("name")
        try:
            imported_name = manager.import_snapshot_bytes(data=body, name=snap_name)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        return JSONResponse({"status": "imported", "name": imported_name})

    # Default: JSON import
    data = json.loads(body)
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

    account_id = request.query_params.get("account_id", DEFAULT_ACCOUNT_ID)
    counts = get_resource_counts(account_id=account_id)
    return JSONResponse({"resources": counts})


async def resources_for_service(request: Request) -> JSONResponse:
    """List resources for a specific service."""
    from robotocore.resources.browser import get_service_resources

    service = request.path_params["service"]
    account_id = request.query_params.get("account_id", DEFAULT_ACCOUNT_ID)
    resources = get_service_resources(service, account_id=account_id)
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
# Usage analytics endpoints
# ---------------------------------------------------------------------------


async def usage_summary(request: Request) -> JSONResponse:
    """Return overall usage summary."""
    from robotocore.audit.analytics import get_usage_analytics

    analytics = get_usage_analytics()
    return JSONResponse(analytics.get_usage_summary())


async def usage_services(request: Request) -> JSONResponse:
    """Return per-service usage breakdown."""
    from robotocore.audit.analytics import get_usage_analytics

    analytics = get_usage_analytics()
    return JSONResponse({"services": analytics.get_all_service_stats()})


async def usage_service_detail(request: Request) -> JSONResponse:
    """Return detailed stats for a specific service."""
    from robotocore.audit.analytics import get_usage_analytics

    service = request.path_params["service"]
    analytics = get_usage_analytics()
    stats = analytics.get_service_stats(service)
    stats["service"] = service
    return JSONResponse(stats)


async def usage_errors(request: Request) -> JSONResponse:
    """Return error breakdown."""
    from robotocore.audit.analytics import get_usage_analytics

    analytics = get_usage_analytics()
    return JSONResponse(analytics.get_error_summary())


async def usage_timeline(request: Request) -> JSONResponse:
    """Return per-minute request timeline."""
    from robotocore.audit.analytics import get_usage_analytics

    analytics = get_usage_analytics()
    return JSONResponse({"timeline": analytics.get_timeline()})


# ---------------------------------------------------------------------------
# CI analytics endpoints
# ---------------------------------------------------------------------------


def _ci_analytics_state_dir():
    """Resolve the CI analytics state directory."""
    from pathlib import Path

    base = os.environ.get("ROBOTOCORE_STATE_DIR", "")
    if base:
        return Path(base) / "ci_analytics"
    return None


async def ci_sessions_list(request: Request) -> JSONResponse:
    """List recent CI sessions."""
    from robotocore.audit.ci_analytics import list_sessions

    state_dir = _ci_analytics_state_dir()
    if not state_dir:
        return JSONResponse({"sessions": [], "error": "ROBOTOCORE_STATE_DIR not set"})
    sessions = list_sessions(state_dir)
    return JSONResponse({"sessions": sessions, "count": len(sessions)})


async def ci_session_detail(request: Request) -> JSONResponse:
    """Get CI session detail by ID."""
    from robotocore.audit.ci_analytics import get_session_detail

    session_id = request.path_params["session_id"]
    state_dir = _ci_analytics_state_dir()
    if not state_dir:
        return JSONResponse({"error": "ROBOTOCORE_STATE_DIR not set"}, status_code=400)
    detail = get_session_detail(state_dir, session_id)
    if detail is None:
        return JSONResponse({"error": "Session not found"}, status_code=404)
    return JSONResponse(detail)


async def ci_summary(request: Request) -> JSONResponse:
    """Aggregate CI analytics summary."""
    from robotocore.audit.ci_analytics import compute_aggregate_summary

    state_dir = _ci_analytics_state_dir()
    if not state_dir:
        return JSONResponse({"error": "ROBOTOCORE_STATE_DIR not set"}, status_code=400)
    summary = compute_aggregate_summary(state_dir)
    return JSONResponse(summary)


async def ci_sessions_clear(request: Request) -> JSONResponse:
    """Clear all CI session history."""
    from robotocore.audit.ci_analytics import clear_sessions

    state_dir = _ci_analytics_state_dir()
    if not state_dir:
        return JSONResponse({"error": "ROBOTOCORE_STATE_DIR not set"}, status_code=400)
    count = clear_sessions(state_dir)
    return JSONResponse({"status": "cleared", "count": count})


# ---------------------------------------------------------------------------
# SES SMTP email inspection endpoints
# ---------------------------------------------------------------------------


async def _endpoints_config(request: Request) -> JSONResponse:
    """Return current endpoint strategy configuration."""
    from robotocore.services.opensearch.endpoint_strategy import get_opensearch_endpoint_strategy
    from robotocore.services.sqs.endpoint_strategy import get_sqs_endpoint_strategy

    return JSONResponse(
        {
            "sqs_endpoint_strategy": get_sqs_endpoint_strategy().value,
            "opensearch_endpoint_strategy": get_opensearch_endpoint_strategy().value,
        }
    )


async def s3_routing_config(request: Request) -> JSONResponse:
    """Return current S3 routing configuration."""
    return JSONResponse(get_s3_routing_config())


async def ses_messages_list(request: Request) -> JSONResponse:
    """List emails received via the SMTP server."""
    from robotocore.services.ses.email_store import get_email_store

    limit = int(request.query_params.get("limit", "100"))
    messages = get_email_store().get_messages(limit)
    return JSONResponse({"messages": messages, "count": len(messages)})


async def ses_messages_clear(request: Request) -> JSONResponse:
    """Clear all stored SMTP emails."""
    from robotocore.services.ses.email_store import get_email_store

    count = get_email_store().clear_messages()
    return JSONResponse({"status": "cleared", "count": count})


async def init_summary(request: Request) -> JSONResponse:
    """Return summary of all init script stages."""
    from robotocore.init.tracker import get_init_tracker

    tracker = get_init_tracker()
    return JSONResponse(tracker.get_summary())


async def init_stage(request: Request) -> JSONResponse:
    """Return detailed list of scripts for a specific stage."""
    from robotocore.init.tracker import get_init_tracker

    stage = request.path_params["stage"]
    tracker = get_init_tracker()
    scripts = tracker.get_scripts(stage)
    return JSONResponse({"stage": stage, "scripts": scripts})


async def plugins_list(request: Request) -> JSONResponse:
    """List all discovered plugins with version, capabilities, and dependency info."""
    from robotocore.extensions.api_version import CURRENT_API_VERSION
    from robotocore.extensions.plugin_status import get_plugin_status_collector
    from robotocore.extensions.registry import get_extension_registry

    collector = get_plugin_status_collector()
    registry = get_extension_registry()
    return JSONResponse(
        {
            "api_version": CURRENT_API_VERSION,
            "plugins": collector.list_plugins(),
            "dependency_graph": registry.get_dependency_graph(),
        }
    )


async def plugin_detail(request: Request) -> JSONResponse:
    """Return detailed info for a specific plugin."""
    from robotocore.extensions.plugin_status import get_plugin_status_collector
    from robotocore.extensions.registry import get_extension_registry

    name = request.path_params["name"]
    collector = get_plugin_status_collector()
    detail = collector.get_plugin_detail(name)
    if detail is None:
        return JSONResponse({"error": f"Plugin '{name}' not found"}, status_code=404)

    # Enrich with registry info (capabilities, api_compat, dependencies)
    registry = get_extension_registry()
    for p in registry.plugins:
        if p.name == name:
            detail["api_version"] = p.api_version
            detail["capabilities"] = sorted(p.get_capabilities())
            detail["dependencies"] = getattr(p, "dependencies", [])
            compat = registry._compat_results.get(name)
            if compat:
                detail["api_compat"] = {
                    "compatible": compat.compatible,
                    "warnings": compat.warnings,
                    "errors": compat.errors,
                }
            config_schema = p.get_config_schema()
            if config_schema:
                detail["config_schema"] = config_schema
            break

    return JSONResponse(detail)


async def plugins_migrations(request: Request) -> JSONResponse:
    """Show migration guidance for deprecated plugin API versions."""
    from robotocore.extensions.api_version import (
        CURRENT_API_VERSION,
        SUPPORTED_VERSIONS,
        PluginAPIVersion,
    )

    return JSONResponse(
        {
            "current_api_version": CURRENT_API_VERSION,
            "supported_versions": sorted(SUPPORTED_VERSIONS),
            "migrations": PluginAPIVersion.get_migration_guide(),
        }
    )


async def tls_info_endpoint(request: Request) -> JSONResponse:
    """Return TLS/HTTPS configuration and certificate info."""
    if not _tls_config.enabled or _tls_cert_path is None:
        return JSONResponse(
            {
                "enabled": False,
                "certificate": None,
                "custom_certificate": False,
                "https_port": _tls_config.https_port,
            }
        )

    cert_info = get_cert_info(_tls_cert_path)
    is_custom = _tls_config.custom_cert_path is not None

    return JSONResponse(
        {
            "enabled": True,
            "certificate": cert_info,
            "custom_certificate": is_custom,
            "https_port": _tls_config.https_port,
        }
    )


# ---------------------------------------------------------------------------
# DNS server management endpoint
# ---------------------------------------------------------------------------


async def _diagnose_handler(request: Request) -> JSONResponse:
    """Diagnostic bundle endpoint -- delegates to diagnostics_bundle module."""
    from robotocore.diagnostics_bundle import diagnose_endpoint

    return await diagnose_endpoint(request)


async def dns_config_endpoint(request: Request) -> JSONResponse:
    """Return current DNS server configuration."""
    from robotocore.dns.resolver import get_config

    config = get_config()
    return JSONResponse(
        {
            "dns": {
                "disabled": config["disabled"],
                "address": config["address"],
                "port": config["port"],
                "resolve_ip": config["resolve_ip"],
                "upstream_server": config["upstream_server"] or "(system default)",
                "local_patterns": config["local_patterns"],
                "upstream_patterns": config["upstream_patterns"],
                "ttl": config["ttl"],
            }
        }
    )


# ---------------------------------------------------------------------------
# Configuration profile endpoints
# ---------------------------------------------------------------------------


async def config_profiles_list(request: Request) -> JSONResponse:
    """List available configuration profiles."""
    from robotocore.config.profiles import list_available_profiles

    profiles = list_available_profiles()
    return JSONResponse({"profiles": profiles})


async def config_active_endpoint(request: Request) -> JSONResponse:
    """Show active profiles and resolved configuration values."""
    from robotocore.config.profiles import get_active_profiles, get_resolved_config

    return JSONResponse(
        {
            "active_profiles": get_active_profiles(),
            "resolved_config": get_resolved_config(),
        }
    )


# ---------------------------------------------------------------------------
# IAM policy stream endpoints
# ---------------------------------------------------------------------------


async def iam_policy_stream_list(request: Request) -> JSONResponse:
    """Return recent IAM policy evaluations."""
    from robotocore.services.iam.policy_stream import (
        format_stream_response,
        get_policy_stream,
        is_stream_enabled,
    )

    if not is_stream_enabled():
        return JSONResponse(
            {"error": "IAM policy stream is disabled. Set IAM_POLICY_STREAM=1 to enable."},
            status_code=400,
        )

    stream = get_policy_stream()
    limit = int(request.query_params.get("limit", "100"))
    principal = request.query_params.get("principal")
    action = request.query_params.get("action")
    decision = request.query_params.get("decision")
    entries = stream.recent(limit=limit, principal=principal, action=action, decision=decision)
    return JSONResponse(format_stream_response(entries))


async def iam_policy_stream_clear(request: Request) -> JSONResponse:
    """Clear the IAM policy stream."""
    from robotocore.services.iam.policy_stream import get_policy_stream

    count = get_policy_stream().clear()
    return JSONResponse({"status": "cleared", "count": count})


async def iam_policy_stream_summary(request: Request) -> JSONResponse:
    """Return aggregate summary of IAM evaluations."""
    from robotocore.services.iam.policy_stream import get_policy_stream

    return JSONResponse(get_policy_stream().summary())


async def iam_policy_stream_suggest(request: Request) -> JSONResponse:
    """Generate least-privilege policy for a principal."""
    from robotocore.services.iam.policy_stream import get_policy_stream

    principal = request.query_params.get("principal", "")
    if not principal:
        return JSONResponse({"error": "principal query parameter is required"}, status_code=400)

    policy = get_policy_stream().suggest_policy(principal)
    return JSONResponse(policy)


# ---------------------------------------------------------------------------
# Cloud Pods endpoints
# ---------------------------------------------------------------------------


async def pods_save(request: Request) -> JSONResponse:
    """Save a Cloud Pod -- snapshot state and push to backend."""
    from robotocore.state.cloud_pods import CloudPodsError, get_cloud_pods_manager
    from robotocore.state.manager import get_state_manager

    body = await request.body()
    params = json.loads(body) if body else {}

    try:
        mgr = get_cloud_pods_manager()
        version = mgr.save_pod(
            name=params.get("name", f"pod-{int(time.time())}"),
            state_manager=get_state_manager(),
            services=params.get("services"),
        )
        return JSONResponse({"status": "saved", "name": params.get("name"), "version": version})
    except CloudPodsError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def pods_load(request: Request) -> JSONResponse:
    """Load a Cloud Pod -- pull from backend and restore state."""
    from robotocore.state.cloud_pods import CloudPodsError, get_cloud_pods_manager
    from robotocore.state.manager import get_state_manager

    body = await request.body()
    params = json.loads(body) if body else {}

    name = params.get("name")
    if not name:
        return JSONResponse({"error": "Missing 'name' parameter"}, status_code=400)

    try:
        mgr = get_cloud_pods_manager()
        mgr.load_pod(
            name=name,
            state_manager=get_state_manager(),
            version=params.get("version"),
        )
        return JSONResponse({"status": "loaded", "name": name})
    except CloudPodsError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def pods_list(request: Request) -> JSONResponse:
    """List all available Cloud Pods."""
    from robotocore.state.cloud_pods import CloudPodsError, get_cloud_pods_manager

    try:
        pods = get_cloud_pods_manager().list_pods()
        return JSONResponse({"pods": pods})
    except CloudPodsError as e:
        return JSONResponse({"error": str(e)}, status_code=400)


async def pods_info(request: Request) -> JSONResponse:
    """Get info about a Cloud Pod including version history."""
    from robotocore.state.cloud_pods import CloudPodsError, get_cloud_pods_manager

    name = request.path_params["name"]
    try:
        info = get_cloud_pods_manager().pod_info(name)
        return JSONResponse(
            {
                "name": info.name,
                "created_at": info.created_at,
                "size_bytes": info.size_bytes,
                "version_count": info.version_count,
                "services_filter": info.services_filter,
                "versions": info.versions,
            }
        )
    except CloudPodsError as e:
        return JSONResponse({"error": str(e)}, status_code=404)


async def pods_delete(request: Request) -> JSONResponse:
    """Delete a Cloud Pod and all its versions."""
    from robotocore.state.cloud_pods import CloudPodsError, get_cloud_pods_manager

    name = request.path_params["name"]
    try:
        get_cloud_pods_manager().delete_pod(name)
        return JSONResponse({"status": "deleted", "name": name})
    except CloudPodsError as e:
        return JSONResponse({"error": str(e)}, status_code=404)


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

    # Check if service is allowed by SERVICES env var filter
    if not is_service_allowed(service_name):
        return JSONResponse(
            {
                "error": f"Service {service_name} is not enabled. "
                "Set SERVICES env var to include it."
            },
            status_code=501,
        )

    # Multi-account support: extract account ID from request
    account_id = _extract_account_id(request)

    context = RequestContext(
        request=request,
        service_name=service_name,
        account_id=account_id,
    )

    # Set up per-request observability context for chaos/audit correlation
    obs_ctx = ObsRequestContext(service=service_name)
    set_current_context(obs_ctx)

    # Pre-read the body so synchronous handlers (populate_context_handler) can
    # access it via request._body for form-encoded Action parsing.
    await request.body()

    _handler_chain.handle(context)

    # If a handler already set a response (e.g. CORS preflight), return it
    if context.response is not None:
        return context.response

    # Track request count
    request_counter.increment(service_name)

    # Use effective provider (respects PROVIDER_OVERRIDE_* env vars)
    effective_handler = get_effective_provider(service_name, NATIVE_PROVIDERS)
    if effective_handler:
        response = await effective_handler(request, context.region, context.account_id)
    else:
        response = await forward_to_moto(request, service_name, account_id=account_id)

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
    Route("/_robotocore/dashboard", dashboard_endpoint, methods=["GET"]),
    Route("/_robotocore/health", health, methods=["GET"]),
    Route("/_robotocore/services", services_endpoint, methods=["GET"]),
    Route("/_robotocore/config", config_endpoint, methods=["GET", "POST"]),
    Route("/_robotocore/config/{key}", config_delete_endpoint, methods=["DELETE"]),
    Route("/_robotocore/state/save", save_state, methods=["POST"]),
    Route("/_robotocore/state/load", load_state, methods=["POST"]),
    Route("/_robotocore/state/snapshots", list_snapshots, methods=["GET"]),
    Route("/_robotocore/state/list", list_versioned_snapshots, methods=["GET"]),
    Route("/_robotocore/state/delete", delete_versioned_snapshot, methods=["DELETE"]),
    Route("/_robotocore/state/versions", snapshot_versions, methods=["GET"]),
    Route("/_robotocore/state/reset", reset_state, methods=["POST"]),
    Route("/_robotocore/state/hooks", list_state_hooks, methods=["GET"]),
    Route("/_robotocore/state/consistency", state_consistency_status, methods=["GET"]),
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
    # Unified timeline (chaos + audit)
    Route("/_robotocore/timeline", handle_timeline, methods=["GET"]),
    # Usage analytics
    Route("/_robotocore/usage", usage_summary, methods=["GET"]),
    Route("/_robotocore/usage/services", usage_services, methods=["GET"]),
    Route("/_robotocore/usage/services/{service}", usage_service_detail, methods=["GET"]),
    Route("/_robotocore/usage/errors", usage_errors, methods=["GET"]),
    Route("/_robotocore/usage/timeline", usage_timeline, methods=["GET"]),
    # CI analytics
    Route("/_robotocore/ci/sessions", ci_sessions_list, methods=["GET"]),
    Route("/_robotocore/ci/sessions", ci_sessions_clear, methods=["DELETE"]),
    Route("/_robotocore/ci/sessions/{session_id}", ci_session_detail, methods=["GET"]),
    Route("/_robotocore/ci/summary", ci_summary, methods=["GET"]),
    # Endpoint strategies
    Route("/_robotocore/endpoints/config", lambda r: _endpoints_config(r), methods=["GET"]),
    # S3 routing config
    Route("/_robotocore/s3/routing", s3_routing_config, methods=["GET"]),
    # SES SMTP email inspection
    Route("/_robotocore/ses/messages", ses_messages_list, methods=["GET"]),
    Route("/_robotocore/ses/messages", ses_messages_clear, methods=["DELETE"]),
    # DNS server
    Route("/_robotocore/dns/config", dns_config_endpoint, methods=["GET"]),
    # Configuration profiles
    Route("/_robotocore/config/profiles", config_profiles_list, methods=["GET"]),
    Route("/_robotocore/config/active", config_active_endpoint, methods=["GET"]),
    # Init scripts status
    Route("/_robotocore/init", init_summary, methods=["GET"]),
    Route("/_robotocore/init/{stage}", init_stage, methods=["GET"]),
    # Plugins status
    Route("/_robotocore/plugins", plugins_list, methods=["GET"]),
    Route("/_robotocore/plugins/migrations", plugins_migrations, methods=["GET"]),
    Route("/_robotocore/plugins/{name}", plugin_detail, methods=["GET"]),
    # Diagnostics bundle
    Route("/_robotocore/diagnose", _diagnose_handler, methods=["GET"]),
    # IAM policy stream
    Route("/_robotocore/iam/policy-stream", iam_policy_stream_list, methods=["GET"]),
    Route("/_robotocore/iam/policy-stream", iam_policy_stream_clear, methods=["DELETE"]),
    Route("/_robotocore/iam/policy-stream/summary", iam_policy_stream_summary, methods=["GET"]),
    Route(
        "/_robotocore/iam/policy-stream/suggest-policy",
        iam_policy_stream_suggest,
        methods=["GET"],
    ),
    # Cloud Pods
    Route("/_robotocore/pods/save", pods_save, methods=["POST"]),
    Route("/_robotocore/pods/load", pods_load, methods=["POST"]),
    Route("/_robotocore/pods", pods_list, methods=["GET"]),
    Route("/_robotocore/pods/{name}", pods_info, methods=["GET"]),
    Route("/_robotocore/pods/{name}", pods_delete, methods=["DELETE"]),
    # TLS info
    Route("/_robotocore/tls/info", tls_info_endpoint, methods=["GET"]),
    # Console web UI
    *get_console_routes(),
]


def _start_background_engines():
    """Start background engines for cross-service integrations."""
    global _server_start_time
    _server_start_time = time.monotonic()

    # Initialize service loader (SERVICES filter, provider overrides, eager loading)
    init_loader()

    from robotocore.services.lambda_.event_source import get_engine

    get_engine().start()
    from robotocore.services.cloudwatch.alarm_scheduler import get_alarm_scheduler

    get_alarm_scheduler().start()
    from robotocore.services.synthetics.scheduler import get_canary_scheduler

    get_canary_scheduler().start()
    from robotocore.services.sqs.metrics import get_sqs_metrics_publisher

    get_sqs_metrics_publisher().start()
    from robotocore.services.events.rule_scheduler import get_rule_scheduler

    get_rule_scheduler().start()
    from robotocore.services.scheduler.provider import get_schedule_executor

    get_schedule_executor().start()

    # Start DynamoDB TTL scanner
    from robotocore.services.dynamodb.ttl import get_ttl_scanner

    get_ttl_scanner().start()

    # Auto-load state if configured
    if os.environ.get("ROBOTOCORE_STATE_DIR"):
        from robotocore.state.manager import get_state_manager

        manager = get_state_manager()
        # Try auto-restore from ROBOTOCORE_RESTORE_SNAPSHOT first
        if not manager.restore_on_startup():
            # Fall back to loading default state
            manager.load()

    # Start SMTP server
    from robotocore.services.ses.smtp_server import start_smtp_server

    start_smtp_server()

    # Run ready hooks
    run_init_hooks("ready")


def _shutdown():
    """Shutdown hook -- auto-save state and CI analytics if configured."""
    # Save CI analytics session
    from robotocore.audit.ci_analytics import get_ci_analytics

    analytics = get_ci_analytics()
    if analytics is not None:
        analytics.end_session()
        state_dir = _ci_analytics_state_dir()
        if state_dir:
            analytics.save_session(state_dir)

    if os.environ.get("ROBOTOCORE_PERSIST", "0") == "1":
        from robotocore.state.manager import get_state_manager

        manager = get_state_manager()
        if manager.state_dir:
            manager.save()

    # Stop DynamoDB TTL scanner
    from robotocore.services.dynamodb.ttl import get_ttl_scanner

    get_ttl_scanner().stop()

    # Stop SMTP server
    from robotocore.services.ses.smtp_server import stop_smtp_server

    stop_smtp_server()

    # Stop DNS server
    from robotocore.dns.server import stop_dns_server

    stop_dns_server()

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
        if scope["type"] == "websocket":
            await self._handle_websocket(scope, receive, send)
            return

        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")

        # Rewrite /_localstack/* to /_robotocore/* for drop-in compatibility
        if path.startswith("/_localstack/"):
            scope = dict(scope, path="/_robotocore/" + path[len("/_localstack/") :])
            path = scope["path"]

        if path.startswith("/_robotocore/"):
            await self.app(scope, receive, send)
            return

        # --- S3 website hosting: check for s3-website Host header ---
        if is_website_request(scope):
            request = Request(scope, receive)
            host = request.headers.get("host", "")
            parsed = parse_website_host(host)
            if parsed:
                response = await handle_website_request(
                    request,
                    bucket_name=parsed["bucket"],
                    region=parsed.get("region", "us-east-1"),
                )
                await response(scope, receive, send)
                return

        # --- S3 virtual-hosted-style: rewrite to path-style ---
        host_header = b""
        for key, val in scope.get("headers", []):
            if key == b"host":
                host_header = val
                break
        if host_header:
            parsed_vhost = parse_s3_vhost(host_header.decode("latin-1"))
            if parsed_vhost is not None:
                new_scope = rewrite_vhost_to_path(scope)
                if new_scope is not None:
                    scope = new_scope
                    path = scope.get("path", "")

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

        # Cognito Hosted UI / OAuth2 endpoints
        method = scope.get("method", "GET")
        if path == "/oauth2/authorize" and method == "GET":
            response = await oauth2_authorize(request)
            await response(scope, receive, send)
            return
        if path == "/oauth2/token" and method == "POST":
            response = await oauth2_token(request)
            await response(scope, receive, send)
            return
        if path == "/oauth2/userInfo" and method == "GET":
            response = await oauth2_userinfo(request)
            await response(scope, receive, send)
            return
        if path == "/.well-known/openid-configuration" and method == "GET":
            response = await openid_configuration(request)
            await response(scope, receive, send)
            return
        if path == "/.well-known/jwks.json" and method == "GET":
            response = await jwks_json(request)
            await response(scope, receive, send)
            return
        if path == "/login" and method == "GET":
            response = await login_get(request)
            await response(scope, receive, send)
            return
        if path == "/login" and method == "POST":
            response = await login_post(request)
            await response(scope, receive, send)
            return
        if path == "/logout" and method == "GET":
            response = await logout(request)
            await response(scope, receive, send)
            return
        if path == "/forgotpassword" and method == "POST":
            response = await forgot_password_endpoint(request)
            await response(scope, receive, send)
            return
        if path == "/confirmforgotpassword" and method == "POST":
            response = await confirm_forgot_password_endpoint(request)
            await response(scope, receive, send)
            return

        response = await handle_aws_request(request)
        await response(scope, receive, send)

    async def _handle_websocket(self, scope, receive, send):
        """Route WebSocket connections to API Gateway V2 WebSocket APIs."""
        from robotocore.services.apigatewayv2.websocket import handle_websocket

        await handle_websocket(scope, receive, send)


# Order matters: AWSRoutingMiddleware is added first (inner), TracingMiddleware second (outer).
# Request flow: TracingMiddleware -> AWSRoutingMiddleware -> app/handler
app.add_middleware(AWSRoutingMiddleware)
app.add_middleware(TracingMiddleware)
