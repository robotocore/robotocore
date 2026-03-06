"""Bridge layer that forwards incoming AWS requests to Moto backends.

Uses Werkzeug URL routing against Moto's flask_paths to find the correct
BaseResponse.dispatch endpoint, matching the pattern used by LocalStack.
"""

import os
import re
from functools import lru_cache

import moto.backends as moto_backends
from moto.core.base_backend import BackendDict
from starlette.requests import Request
from starlette.responses import Response
from werkzeug.routing import Map, Rule
from werkzeug.routing.converters import BaseConverter
from werkzeug.test import EnvironBuilder
from werkzeug.wrappers import Request as WerkzeugRequest

os.environ.setdefault("MOTO_ALLOW_NONEXISTENT_REGION", "true")

DEFAULT_ACCOUNT_ID = "123456789012"

_REGION_RE = re.compile(r"Credential=[^/]+/\d{8}/([^/]+)/")


class _RegexConverter(BaseConverter):
    """Werkzeug converter that allows regex patterns to match across path segments."""

    part_isolating = False

    def __init__(self, map, *args, **kwargs):
        super().__init__(map, *args, **kwargs)
        self.regex = args[0] if args else ".*"


@lru_cache
def _get_moto_routing_table(service: str) -> Map:
    """Build and cache a Werkzeug URL Map from a Moto backend's flask_paths."""
    backend_dict = moto_backends.get_backend(service)
    if isinstance(backend_dict, BackendDict):
        if "us-east-1" in backend_dict[DEFAULT_ACCOUNT_ID]:
            backend = backend_dict[DEFAULT_ACCOUNT_ID]["us-east-1"]
        else:
            backend = backend_dict[DEFAULT_ACCOUNT_ID]["global"]
    else:
        backend = backend_dict["global"]

    url_map = Map()
    url_map.converters["regex"] = _RegexConverter

    for url_path, handler in backend.flask_paths.items():
        url_map.add(Rule(url_path, endpoint=handler, strict_slashes=False))

    return url_map


def _get_dispatcher(service: str, path: str):
    """Match a request path to the correct Moto dispatch function."""
    url_map = _get_moto_routing_table(service)

    if len(url_map._rules) == 1:
        return next(url_map.iter_rules()).endpoint

    matcher = url_map.bind("localhost")
    endpoint, _ = matcher.match(path_info=path)
    return endpoint


def _extract_region(headers: dict) -> str:
    auth = headers.get("authorization", "")
    match = _REGION_RE.search(auth)
    if match:
        return match.group(1)
    return "us-east-1"


def _build_werkzeug_request(request: Request, body: bytes) -> WerkzeugRequest:
    """Convert a Starlette Request to a Werkzeug Request for Moto."""
    builder = EnvironBuilder(
        method=request.method,
        path=request.url.path,
        query_string=str(request.url.query) if request.url.query else "",
        data=body,
        headers=dict(request.headers),
    )
    return WerkzeugRequest(builder.get_environ())


async def forward_to_moto(request: Request, service_name: str) -> Response:
    """Forward an AWS API request to the appropriate Moto backend."""
    body = await request.body()

    raw_path = request.url.path
    try:
        dispatch = _get_dispatcher(service_name, raw_path)
    except Exception:
        return Response(
            content=(
                f"<ErrorResponse><Error><Code>NotImplemented</Code>"
                f"<Message>Service {service_name} is not yet implemented</Message>"
                f"</Error></ErrorResponse>"
            ),
            status_code=501,
            media_type="application/xml",
        )

    werkzeug_request = _build_werkzeug_request(request, body)

    # Build the full URL as Moto expects
    full_url = str(request.url)

    try:
        result = dispatch(werkzeug_request, full_url, werkzeug_request.headers)
        if not result:
            raise NotImplementedError(f"Moto returned None for {service_name}")
        status, response_headers, response_body = result
        if isinstance(response_body, str) and len(response_body) == 0:
            response_body = None
        headers_dict = dict(response_headers) if response_headers else {}
        is_head = request.method == "HEAD"
        # For HEAD requests, keep content-length (it's object metadata)
        # but ensure body is empty. For other requests, drop content-length
        # and let Starlette recompute it to avoid h11 "Too much data" errors.
        if is_head:
            clean_headers = headers_dict
            response_body = None
        else:
            clean_headers = {k: v for k, v in headers_dict.items() if k.lower() != "content-length"}
        return Response(
            content=response_body,
            status_code=status,
            headers=clean_headers,
        )
    except Exception as e:
        return Response(
            content=(
                f"<ErrorResponse><Error><Code>InternalError</Code>"
                f"<Message>{e}</Message></Error></ErrorResponse>"
            ),
            status_code=500,
            media_type="application/xml",
        )
