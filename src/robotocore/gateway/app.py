"""ASGI application — the main HTTP entry point for Robotocore."""

from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from robotocore.gateway.handler_chain import HandlerChain, RequestContext
from robotocore.gateway.handlers import (
    cors_handler,
    cors_response_handler,
    error_normalizer,
    logging_response_handler,
    populate_context_handler,
)
from robotocore.gateway.router import route_to_service
from robotocore.providers.moto_bridge import forward_to_moto
from robotocore.services.cloudformation.provider import handle_cloudformation_request
from robotocore.services.dynamodb.provider import handle_dynamodb_request
from robotocore.services.dynamodbstreams.provider import handle_dynamodbstreams_request
from robotocore.services.events.provider import handle_events_request
from robotocore.services.firehose.provider import handle_firehose_request
from robotocore.services.kinesis.provider import handle_kinesis_request
from robotocore.services.lambda_.provider import handle_lambda_request
from robotocore.services.s3.provider import handle_s3_request
from robotocore.services.scheduler.provider import handle_scheduler_request
from robotocore.services.sns.provider import handle_sns_request
from robotocore.services.sqs.provider import handle_sqs_request
from robotocore.services.stepfunctions.provider import handle_stepfunctions_request

# Services with native providers (bypass Moto)
NATIVE_PROVIDERS = {
    "cloudformation": handle_cloudformation_request,
    "dynamodb": handle_dynamodb_request,
    "dynamodbstreams": handle_dynamodbstreams_request,
    "events": handle_events_request,
    "firehose": handle_firehose_request,
    "kinesis": handle_kinesis_request,
    "lambda": handle_lambda_request,
    "s3": handle_s3_request,
    "scheduler": handle_scheduler_request,
    "sqs": handle_sqs_request,
    "sns": handle_sns_request,
    "stepfunctions": handle_stepfunctions_request,
}


def _build_handler_chain() -> HandlerChain:
    """Build the default handler chain for AWS requests."""
    chain = HandlerChain()
    chain.request_handlers.append(cors_handler)
    chain.request_handlers.append(populate_context_handler)
    chain.response_handlers.append(cors_response_handler)
    chain.response_handlers.append(logging_response_handler)
    chain.exception_handlers.append(error_normalizer)
    return chain


_handler_chain = _build_handler_chain()


async def health(request: Request) -> JSONResponse:
    return JSONResponse({"status": "running", "services": "all"})


async def save_state(request: Request) -> JSONResponse:
    """Save emulator state to disk (Cloud Pods-like feature)."""
    from robotocore.state.manager import get_state_manager

    body = await request.body()
    params = {}
    if body:
        import json

        params = json.loads(body)

    manager = get_state_manager()
    path = params.get("path") or manager.state_dir
    if not path:
        return JSONResponse(
            {"error": "No state directory configured. Set ROBOTOCORE_STATE_DIR or pass 'path'."},
            status_code=400,
        )

    saved_path = manager.save(path)
    return JSONResponse({"status": "saved", "path": saved_path})


async def load_state(request: Request) -> JSONResponse:
    """Load emulator state from disk."""
    from robotocore.state.manager import get_state_manager

    body = await request.body()
    params = {}
    if body:
        import json

        params = json.loads(body)

    manager = get_state_manager()
    path = params.get("path") or manager.state_dir
    if not path:
        return JSONResponse(
            {"error": "No state directory configured. Set ROBOTOCORE_STATE_DIR or pass 'path'."},
            status_code=400,
        )

    success = manager.load(path)
    return JSONResponse({"status": "loaded" if success else "no_state_found", "path": str(path)})


async def reset_state(request: Request) -> JSONResponse:
    """Reset all emulator state."""
    from robotocore.state.manager import get_state_manager

    get_state_manager().reset()
    return JSONResponse({"status": "reset"})


async def handle_aws_request(request: Request) -> Response:
    """Main handler: route, build context, run handler chain, forward to Moto."""
    service_name = route_to_service(request)
    if service_name is None:
        return JSONResponse(
            {"error": "Could not determine target AWS service from request"},
            status_code=400,
        )

    context = RequestContext(request=request, service_name=service_name)

    _handler_chain.handle(context)

    # If a handler already set a response (e.g. CORS preflight), return it
    if context.response is not None:
        return context.response

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

    return context.response


# Health and management endpoints
management_routes = [
    Route("/_robotocore/health", health, methods=["GET"]),
    Route("/_robotocore/state/save", save_state, methods=["POST"]),
    Route("/_robotocore/state/load", load_state, methods=["POST"]),
    Route("/_robotocore/state/reset", reset_state, methods=["POST"]),
]


def _start_background_engines():
    """Start background engines for cross-service integrations."""
    import os

    from robotocore.services.lambda_.event_source import get_engine

    get_engine().start()
    from robotocore.services.cloudwatch.alarm_scheduler import get_alarm_scheduler

    get_alarm_scheduler().start()

    # Auto-load state if configured
    if os.environ.get("ROBOTOCORE_STATE_DIR"):
        from robotocore.state.manager import get_state_manager

        get_state_manager().load()


def _shutdown():
    """Shutdown hook — auto-save state if configured."""
    import os

    if os.environ.get("ROBOTOCORE_PERSIST", "0") == "1":
        from robotocore.state.manager import get_state_manager

        manager = get_state_manager()
        if manager.state_dir:
            manager.save()


app = Starlette(
    routes=management_routes,
    on_startup=[_start_background_engines],
    on_shutdown=[_shutdown],
)


async def handle_execute_api(
    request: Request, rest_api_id: str, stage: str, proxy_path: str
) -> Response:
    """Handle API Gateway execute-api requests (invoke deployed APIs)."""
    import re

    from robotocore.services.apigateway.executor import execute_api_request

    body = await request.body()
    headers = dict(request.headers)
    query_params = dict(request.query_params)

    # Extract region/account from auth header
    region = "us-east-1"
    account_id = "123456789012"
    auth = request.headers.get("authorization", "")
    region_match = re.search(r"Credential=[^/]+/\d{8}/([^/]+)/", auth)
    if region_match:
        region = region_match.group(1)

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


class AWSRoutingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path.startswith("/_robotocore/"):
            return await call_next(request)

        # Check for API Gateway execute-api path: /restapis/{id}/{stage}/_user_request_/{path}
        import re

        exec_match = re.match(r"^/restapis/([^/]+)/([^/]+)/_user_request_/?(.*)", request.url.path)
        if exec_match:
            return await handle_execute_api(
                request,
                rest_api_id=exec_match.group(1),
                stage=exec_match.group(2),
                proxy_path=exec_match.group(3),
            )

        return await handle_aws_request(request)


app.add_middleware(AWSRoutingMiddleware)
