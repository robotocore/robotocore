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
from robotocore.services.firehose.provider import handle_firehose_request
from robotocore.services.lambda_.provider import handle_lambda_request
from robotocore.services.s3.provider import handle_s3_request
from robotocore.services.sns.provider import handle_sns_request
from robotocore.services.sqs.provider import handle_sqs_request

# Services with native providers (bypass Moto)
NATIVE_PROVIDERS = {
    "cloudformation": handle_cloudformation_request,
    "firehose": handle_firehose_request,
    "lambda": handle_lambda_request,
    "s3": handle_s3_request,
    "sqs": handle_sqs_request,
    "sns": handle_sns_request,
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
]

def _start_background_engines():
    """Start background engines for cross-service integrations."""
    from robotocore.services.lambda_.event_source import get_engine
    get_engine().start()


app = Starlette(
    routes=management_routes,
    on_startup=[_start_background_engines],
)


class AWSRoutingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path.startswith("/_robotocore/"):
            return await call_next(request)
        return await handle_aws_request(request)


app.add_middleware(AWSRoutingMiddleware)
