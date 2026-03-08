"""Bridge layer that forwards incoming AWS requests to Moto backends.

Uses Werkzeug URL routing against Moto's flask_paths to find the correct
BaseResponse.dispatch endpoint, matching the pattern used by LocalStack.
"""

import os
from functools import lru_cache
from xml.sax.saxutils import escape as _xml_escape

import botocore.model
import moto.backends as moto_backends
from moto.core.base_backend import BackendDict
from starlette.requests import Request
from starlette.responses import Response
from werkzeug.routing import Map, Rule
from werkzeug.routing.converters import BaseConverter
from werkzeug.test import EnvironBuilder
from werkzeug.wrappers import Request as WerkzeugRequest

from robotocore.diagnostics import header_value as _diag_header
from robotocore.diagnostics import record as _diag_record

os.environ.setdefault("MOTO_ALLOW_NONEXISTENT_REGION", "true")

DEFAULT_ACCOUNT_ID = "123456789012"


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
        # Moto uses regex catch-all patterns like '/.*' or '/.+' that aren't valid
        # Werkzeug Rules.  Convert them to a Werkzeug <path:> converter.
        if url_path in ("", "/"):
            url_map.add(Rule("/", endpoint=handler, strict_slashes=False))
            continue
        if url_path in ("/.*", "/.+"):
            url_map.add(Rule("/<path:__catch_all>", endpoint=handler, strict_slashes=False))
            # Also add a rule for the root path itself
            url_map.add(Rule("/", endpoint=handler, strict_slashes=False))
            continue
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


def _build_werkzeug_request(request: Request, body: bytes) -> WerkzeugRequest:
    """Convert a Starlette Request to a Werkzeug Request for Moto."""
    builder = EnvironBuilder(
        method=request.method,
        path=request.url.path,
        query_string=str(request.url.query) if request.url.query else "",
        data=body,
        headers=dict(request.headers),
    )
    env = builder.get_environ()
    # Werkzeug EnvironBuilder strips Content-Length when data is empty, but some
    # Moto handlers (e.g. S3 _bucket_response_put) require it to be present.
    if "Content-Length" in request.headers and "CONTENT_LENGTH" not in env:
        env["CONTENT_LENGTH"] = request.headers["Content-Length"]
    return WerkzeugRequest(env)


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
                f"<Message>Service {_xml_escape(service_name)} is not yet implemented</Message>"
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
            return Response(
                content=(
                    f"<ErrorResponse><Error><Code>NotImplemented</Code>"
                    f"<Message>Operation not implemented for {_xml_escape(service_name)}</Message>"
                    f"</Error></ErrorResponse>"
                ),
                status_code=501,
                media_type="application/xml",
            )
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
    except botocore.model.OperationNotFoundError as e:
        _diag_record(
            exc=e,
            service=service_name,
            method=request.method,
            path=raw_path,
            status=400,
        )
        return Response(
            content=(
                f"<ErrorResponse><Error><Code>InvalidAction</Code>"
                f"<Message>Could not find operation {_xml_escape(str(e))}</Message>"
                f"</Error></ErrorResponse>"
            ),
            status_code=400,
            media_type="application/xml",
            headers={"x-robotocore-diag": _diag_header(e)},
        )
    except NotImplementedError as e:
        _diag_record(
            exc=e,
            service=service_name,
            method=request.method,
            path=raw_path,
            status=501,
        )
        return Response(
            content=(
                f"<ErrorResponse><Error><Code>NotImplemented</Code>"
                f"<Message>{_xml_escape(str(e))}</Message></Error></ErrorResponse>"
            ),
            status_code=501,
            media_type="application/xml",
            headers={"x-robotocore-diag": _diag_header(e)},
        )
    except Exception as e:
        _diag_record(
            exc=e,
            service=service_name,
            method=request.method,
            path=raw_path,
            status=500,
        )
        return Response(
            content=(
                f"<ErrorResponse><Error><Code>InternalError</Code>"
                f"<Message>{_xml_escape(str(e))}</Message></Error></ErrorResponse>"
            ),
            status_code=500,
            media_type="application/xml",
            headers={"x-robotocore-diag": _diag_header(e)},
        )


async def forward_to_moto_with_body(request: Request, service_name: str, body: bytes) -> Response:
    """Forward to Moto with a custom body (for request body modifications)."""
    raw_path = request.url.path
    try:
        dispatch = _get_dispatcher(service_name, raw_path)
    except Exception:
        return Response(
            content=(
                f"<ErrorResponse><Error><Code>NotImplemented</Code>"
                f"<Message>Service {_xml_escape(service_name)} is not yet implemented</Message>"
                f"</Error></ErrorResponse>"
            ),
            status_code=501,
            media_type="application/xml",
        )

    werkzeug_request = _build_werkzeug_request(request, body)
    full_url = str(request.url)

    try:
        result = dispatch(werkzeug_request, full_url, werkzeug_request.headers)
        if not result:
            return Response(
                content=(
                    f"<ErrorResponse><Error><Code>NotImplemented</Code>"
                    f"<Message>Operation not implemented for {_xml_escape(service_name)}</Message>"
                    f"</Error></ErrorResponse>"
                ),
                status_code=501,
                media_type="application/xml",
            )
        status, response_headers, response_body = result
        if isinstance(response_body, str) and len(response_body) == 0:
            response_body = None
        headers_dict = dict(response_headers) if response_headers else {}
        clean_headers = {k: v for k, v in headers_dict.items() if k.lower() != "content-length"}
        return Response(
            content=response_body,
            status_code=status,
            headers=clean_headers,
        )
    except botocore.model.OperationNotFoundError as e:
        _diag_record(
            exc=e,
            service=service_name,
            method=request.method,
            path=raw_path,
            status=400,
        )
        return Response(
            content=(
                f"<ErrorResponse><Error><Code>InvalidAction</Code>"
                f"<Message>Could not find operation {_xml_escape(str(e))}</Message>"
                f"</Error></ErrorResponse>"
            ),
            status_code=400,
            media_type="application/xml",
            headers={"x-robotocore-diag": _diag_header(e)},
        )
    except NotImplementedError as e:
        _diag_record(
            exc=e,
            service=service_name,
            method=request.method,
            path=raw_path,
            status=501,
        )
        return Response(
            content=(
                f"<ErrorResponse><Error><Code>NotImplemented</Code>"
                f"<Message>{_xml_escape(str(e))}</Message></Error></ErrorResponse>"
            ),
            status_code=501,
            media_type="application/xml",
            headers={"x-robotocore-diag": _diag_header(e)},
        )
    except Exception as e:
        _diag_record(
            exc=e,
            service=service_name,
            method=request.method,
            path=raw_path,
            status=500,
        )
        return Response(
            content=(
                f"<ErrorResponse><Error><Code>InternalError</Code>"
                f"<Message>{_xml_escape(str(e))}</Message></Error></ErrorResponse>"
            ),
            status_code=500,
            media_type="application/xml",
            headers={"x-robotocore-diag": _diag_header(e)},
        )
