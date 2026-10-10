"""Bridge layer that forwards incoming AWS requests to Moto backends.

Uses Werkzeug URL routing against Moto's flask_paths to find the correct
BaseResponse.dispatch endpoint.
"""

import json
import os
from functools import lru_cache
from urllib.parse import quote
from xml.sax.saxutils import escape as _xml_escape

import botocore.model
import moto.backends as moto_backends
from moto.core.base_backend import BackendDict
from starlette.requests import Request
from starlette.responses import Response
from werkzeug.exceptions import NotFound as WerkzeugNotFound
from werkzeug.routing import Map, Rule
from werkzeug.routing.converters import BaseConverter
from werkzeug.routing.exceptions import NoMatch as WerkzeugNoMatch
from werkzeug.test import EnvironBuilder
from werkzeug.wrappers import Request as WerkzeugRequest

from robotocore.diagnostics import header_value as _diag_header
from robotocore.diagnostics import record as _diag_record
from robotocore.protocols.service_info import get_service_protocol as _gp  # noqa: F401

os.environ.setdefault("MOTO_ALLOW_NONEXISTENT_REGION", "true")

DEFAULT_ACCOUNT_ID = "123456789012"


class WerkzeugRawBodyRequest(WerkzeugRequest):
    """Werkzeug request that preserves a raw body for Moto.

    Moto's BaseResponse.setup_class reads the body via two paths:
      1. ``if hasattr(request, "body"): self.body = request.body``  (Boto path)
      2. ``else: self.body = request.data``                         (Flask path)

    For ``application/x-www-form-urlencoded``, Werkzeug eagerly parses the
    request stream into ``form`` and ``files``, leaving both ``data`` and any
    synthetic ``body`` attribute empty. That corrupts raw S3 uploads where the
    Content-Type header describes the *object* metadata, not the wire encoding.

    This wrapper fixes both body-access paths:
    - Sets ``self.body`` directly so Moto takes the Boto path.
    - Overrides ``data`` / ``get_data()`` to return the same raw bytes, so any
      code that falls through to the Flask path still gets the real payload.
    - Raises ``AttributeError`` from ``form`` / ``files`` properties so that
      ``hasattr(request, "form")`` returns False, preventing Moto from
      overwriting ``self.body`` with form-field contents.
    """

    def __init__(self, environ: dict, body: bytes):
        super().__init__(environ)
        self.body = body

    @property
    def data(self) -> bytes:  # type: ignore[override]
        return self.body

    def get_data(self, *args: object, **kwargs: object) -> bytes:  # type: ignore[override]
        return self.body

    @property
    def form(self):  # type: ignore[override]
        # Raising AttributeError causes hasattr(request, "form") == False,
        # which makes Moto skip the form-body extraction logic in setup_class.
        raise AttributeError("form parsing disabled for raw-body request")

    @property
    def files(self):  # type: ignore[override]
        # Same as form: prevents Moto from overwriting self.body with file data.
        raise AttributeError("file parsing disabled for raw-body request")


def _error_response(
    service_name: str,
    error_code: str,
    message: str,
    status_code: int,
    extra_headers: dict | None = None,
) -> Response:
    """Build a protocol-aware error response (JSON for JSON services, XML otherwise)."""
    import json

    from robotocore.protocols.service_info import get_service_json_version, get_service_protocol

    protocol = get_service_protocol(service_name) or "query"
    headers = extra_headers or {}

    if protocol in ("json", "rest-json", "smithy-rpc-v2-cbor"):
        body = json.dumps({"__type": error_code, "message": message})
        json_version = get_service_json_version(service_name) or "1.0"
        return Response(
            content=body,
            status_code=status_code,
            media_type=f"application/x-amz-json-{json_version}",
            headers=headers,
        )
    else:
        safe_message = _xml_escape(message)
        body = (
            f"<ErrorResponse><Error><Code>{error_code}</Code>"
            f"<Message>{safe_message}</Message></Error></ErrorResponse>"
        )
        return Response(
            content=body,
            status_code=status_code,
            media_type="application/xml",
            headers=headers,
        )


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
        # Also add a trailing-slash variant: Werkzeug's strict_slashes=False only
        # prevents redirecting FROM trailing-slash TO non-trailing-slash, but
        # rules with regex converters still fail to match the slash variant.
        # Explicitly adding both versions ensures paths like /plans/{id}/versions/
        # (sent by boto3) match the /plans/{id}/versions rule.
        if not url_path.endswith("/"):
            url_map.add(Rule(url_path + "/", endpoint=handler, strict_slashes=False))

    return url_map


def _get_dispatcher(service: str, path: str):
    """Match a request path to the correct Moto dispatch function."""
    url_map = _get_moto_routing_table(service)

    if len(url_map._rules) == 1:
        return next(url_map.iter_rules()).endpoint

    matcher = url_map.bind("localhost")
    endpoint, _ = matcher.match(path_info=path)
    return endpoint


def _build_werkzeug_request(
    request: Request,
    body: bytes,
    account_id: str = DEFAULT_ACCOUNT_ID,
    service_name: str | None = None,
) -> WerkzeugRequest:
    """Convert a Starlette Request to a Werkzeug Request for Moto."""
    # Use raw_path from ASGI scope to preserve percent-encoding.  Starlette's
    # request.url.path is already decoded, but Werkzeug's EnvironBuilder will
    # decode again — causing double-decoding.  raw_path has the original wire
    # encoding so Werkzeug's single decode produces the correct result.
    raw_path = getattr(request, "scope", {}).get("raw_path", b"")
    if raw_path:
        # raw_path may include the query string (e.g. after S3 vhost rewriting).
        # EnvironBuilder takes path and query_string separately, so strip any
        # query portion from path to avoid "Query string defined in both" error.
        raw_path_str = raw_path.decode("latin-1")
        path = raw_path_str.split("?", 1)[0]
    else:
        # Fallback: re-encode what Starlette decoded so Werkzeug's decode is a no-op.
        path = quote(request.url.path, safe="/:@!$&'()*+,;=-._~")
    headers = dict(request.headers)
    # Inject Moto's multi-account header so the correct backend is used.
    headers["x-moto-account-id"] = account_id
    builder = EnvironBuilder(
        method=request.method,
        path=path,
        query_string=str(request.url.query) if request.url.query else "",
        data=body,
        headers=headers,
    )
    env = builder.get_environ()
    # Werkzeug EnvironBuilder strips Content-Length when data is empty, but some
    # Moto handlers (e.g. S3 _bucket_response_put) require it to be present.
    if "Content-Length" in request.headers and "CONTENT_LENGTH" not in env:
        env["CONTENT_LENGTH"] = request.headers["Content-Length"]
    content_type = request.headers.get("content-type", "")
    if service_name == "s3" and "x-www-form-urlencoded" in content_type:
        return WerkzeugRawBodyRequest(env, body)
    return WerkzeugRequest(env)


async def forward_to_moto(
    request: Request, service_name: str, account_id: str = DEFAULT_ACCOUNT_ID
) -> Response:
    """Forward an AWS API request to the appropriate Moto backend."""
    body = await request.body()

    # Use the raw (percent-encoded) path from the ASGI scope for URL matching.
    # Starlette's request.url.path decodes %2F to '/', which would break route
    # matching against patterns like [^/]+ when ARNs (e.g. principalArn) are
    # passed as URL path segments.
    scope_raw = getattr(request, "scope", {}).get("raw_path", b"")
    if scope_raw:
        raw_path = scope_raw.decode("latin-1").split("?", 1)[0]
    else:
        raw_path = request.url.path
    try:
        dispatch = _get_dispatcher(service_name, raw_path)
    except (WerkzeugNotFound, WerkzeugNoMatch):
        # Moto has no route for this op shape (or service) — the "not
        # implemented" gap contract (AGENTS.md: only 501 is a gap).
        return _error_response(
            service_name,
            "NotImplemented",
            f"Service {service_name} is not yet implemented",
            501,
        )
    except Exception as dispatch_err:  # noqa: BLE001
        # Any other dispatch failure is a real bug: answer the 500 contract
        # here instead of filing it as a coverage gap.
        _diag_record(
            exc=dispatch_err,
            service=service_name,
            method=request.method,
            path=raw_path,
            status=500,
        )
        return _error_response(
            service_name,
            "InternalError",
            str(dispatch_err),
            500,
            {"x-robotocore-diag": _diag_header(dispatch_err)},
        )

    werkzeug_request = _build_werkzeug_request(
        request,
        body,
        account_id=account_id,
        service_name=service_name,
    )

    # Build the full URL as Moto expects
    full_url = str(request.url)

    try:
        result = dispatch(werkzeug_request, full_url, werkzeug_request.headers)
        if not result:
            return _error_response(
                service_name,
                "NotImplemented",
                f"Operation not implemented for {service_name}",
                501,
            )
        status, response_headers, response_body = result
        if isinstance(response_body, (str, bytes)) and len(response_body) == 0:
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
        return _error_response(
            service_name,
            "InvalidAction",
            f"Could not find operation {e}",
            400,
            {"x-robotocore-diag": _diag_header(e)},
        )
    except NotImplementedError as e:
        _diag_record(
            exc=e,
            service=service_name,
            method=request.method,
            path=raw_path,
            status=501,
        )
        return _error_response(
            service_name,
            "NotImplemented",
            str(e),
            501,
            {"x-robotocore-diag": _diag_header(e)},
        )
    except json.JSONDecodeError as e:
        # A malformed JSON body is a client error, not a server fault: AWS answers
        # ValidationException (400) for it, and a raw 500 here hid real probes.
        _diag_record(
            exc=e,
            service=service_name,
            method=request.method,
            path=raw_path,
            status=400,
        )
        return _error_response(
            service_name,
            "ValidationException",
            f"Malformed input: the request body is not valid JSON ({e})",
            400,
            {"x-robotocore-diag": _diag_header(e)},
        )
    except Exception as e:  # noqa: BLE001
        # Werkzeug HTTPExceptions from Moto contain the proper error response
        from werkzeug.exceptions import HTTPException as WerkzeugHTTPException

        if isinstance(e, WerkzeugHTTPException):
            resp = e.get_response()
            body_text = resp.get_data(as_text=True) if resp else str(e)
            # Label the body with moto's own content type (RESTError sends
            # XML with X-Amzn-ErrorType) instead of declaring XML "JSON".
            headers = {"Content-Type": resp.content_type or "application/json"}
            if resp.headers.get("X-Amzn-ErrorType"):
                headers["x-amzn-ErrorType"] = resp.headers["X-Amzn-ErrorType"]
            return Response(
                content=body_text,
                status_code=e.code or 400,
                headers=headers,
            )
        _diag_record(
            exc=e,
            service=service_name,
            method=request.method,
            path=raw_path,
            status=500,
        )
        return _error_response(
            service_name,
            "InternalError",
            str(e),
            500,
            {"x-robotocore-diag": _diag_header(e)},
        )


async def forward_to_moto_with_body(
    request: Request, service_name: str, body: bytes, account_id: str = DEFAULT_ACCOUNT_ID
) -> Response:
    """Forward to Moto with a custom body (for request body modifications)."""
    scope_raw = getattr(request, "scope", {}).get("raw_path", b"")
    if scope_raw:
        raw_path = scope_raw.decode("latin-1").split("?", 1)[0]
    else:
        raw_path = request.url.path
    try:
        dispatch = _get_dispatcher(service_name, raw_path)
    except (WerkzeugNotFound, WerkzeugNoMatch):
        # Moto has no route for this op shape (or service) — the "not
        # implemented" gap contract (AGENTS.md: only 501 is a gap).
        return _error_response(
            service_name,
            "NotImplemented",
            f"Service {service_name} is not yet implemented",
            501,
        )
    except Exception as dispatch_err:  # noqa: BLE001
        # Any other dispatch failure is a real bug: answer the 500 contract
        # here instead of filing it as a coverage gap.
        _diag_record(
            exc=dispatch_err,
            service=service_name,
            method=request.method,
            path=raw_path,
            status=500,
        )
        return _error_response(
            service_name,
            "InternalError",
            str(dispatch_err),
            500,
            {"x-robotocore-diag": _diag_header(dispatch_err)},
        )

    werkzeug_request = _build_werkzeug_request(
        request,
        body,
        account_id=account_id,
        service_name=service_name,
    )
    full_url = str(request.url)

    try:
        result = dispatch(werkzeug_request, full_url, werkzeug_request.headers)
        if not result:
            return _error_response(
                service_name,
                "NotImplemented",
                f"Operation not implemented for {service_name}",
                501,
            )
        status, response_headers, response_body = result
        if isinstance(response_body, (str, bytes)) and len(response_body) == 0:
            response_body = None
        headers_dict = dict(response_headers) if response_headers else {}
        is_head = request.method == "HEAD"
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
        return _error_response(
            service_name,
            "InvalidAction",
            f"Could not find operation {e}",
            400,
            {"x-robotocore-diag": _diag_header(e)},
        )
    except NotImplementedError as e:
        _diag_record(
            exc=e,
            service=service_name,
            method=request.method,
            path=raw_path,
            status=501,
        )
        return _error_response(
            service_name,
            "NotImplemented",
            str(e),
            501,
            {"x-robotocore-diag": _diag_header(e)},
        )
    except json.JSONDecodeError as e:
        # A malformed JSON body is a client error, not a server fault: AWS answers
        # ValidationException (400) for it, and a raw 500 here hid real probe defects.
        _diag_record(
            exc=e,
            service=service_name,
            method=request.method,
            path=raw_path,
            status=400,
        )
        return _error_response(
            service_name,
            "ValidationException",
            f"Malformed input: the request body is not valid JSON ({e})",
            400,
            {"x-robotocore-diag": _diag_header(e)},
        )
    except Exception as e:  # noqa: BLE001
        from werkzeug.exceptions import HTTPException as WerkzeugHTTPException

        if isinstance(e, WerkzeugHTTPException):
            resp = e.get_response()
            body_text = resp.get_data(as_text=True) if resp else str(e)
            # Label the body with moto's own content type (RESTError sends
            # XML with X-Amzn-ErrorType) instead of declaring XML "JSON".
            headers = {"Content-Type": resp.content_type or "application/json"}
            if resp.headers.get("X-Amzn-ErrorType"):
                headers["x-amzn-ErrorType"] = resp.headers["X-Amzn-ErrorType"]
            return Response(
                content=body_text,
                status_code=e.code or 400,
                headers=headers,
            )
        _diag_record(
            exc=e,
            service=service_name,
            method=request.method,
            path=raw_path,
            status=500,
        )
        return _error_response(
            service_name,
            "InternalError",
            str(e),
            500,
            {"x-robotocore-diag": _diag_header(e)},
        )
