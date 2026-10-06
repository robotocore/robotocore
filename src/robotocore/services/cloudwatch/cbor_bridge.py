"""Serve Smithy RPCv2 CBOR CloudWatch calls that robotocore has no native handler for.

aws-sdk-go-v2 (and therefore Terraform's AWS provider) speaks rpc-v2-cbor to CloudWatch. Moto has
no CBOR support, so those calls -- PutMetricAlarm, DescribeAlarms, DeleteAlarms, tagging, ... --
used to fail with 501. This bridge re-encodes the request as AWS JSON 1.0 (which Moto serves),
forwards it, and re-encodes Moto's answer as CBOR. Timestamps and blobs are converted using the
botocore service model, since the two protocols encode them differently (JSON: epoch numbers and
base64 strings; CBOR: tag-1 epoch datetimes and byte strings).
"""

import base64
import json
import logging
from datetime import UTC, datetime
from typing import Any

import cbor2
from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto_with_body

logger = logging.getLogger(__name__)

_TARGET_PREFIX = "GraniteServiceVersion20100801"
_model = None


def _service_model():
    global _model
    if _model is None:
        import botocore.session

        _model = botocore.session.get_session().get_service_model("cloudwatch")
    return _model


def _to_json_value(value: Any, shape) -> Any:
    """CBOR-decoded request value -> AWS JSON 1.0 value for ``shape``."""
    if shape is None or value is None:
        return value
    t = shape.type_name
    if t == "structure" and isinstance(value, dict):
        return {k: _to_json_value(v, shape.members.get(k)) for k, v in value.items()}
    if t == "list" and isinstance(value, list):
        return [_to_json_value(v, shape.member) for v in value]
    if t == "map" and isinstance(value, dict):
        return {k: _to_json_value(v, shape.value) for k, v in value.items()}
    if t == "timestamp" and isinstance(value, datetime):
        return value.timestamp()
    if t == "blob" and isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode()
    return value


def _parse_timestamp(value: Any) -> datetime | Any:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=UTC)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return value


def _to_cbor_value(value: Any, shape) -> Any:
    """Moto's AWS JSON 1.0 response value -> rpc-v2-cbor value for ``shape``."""
    if shape is None or value is None:
        return value
    t = shape.type_name
    if t == "structure" and isinstance(value, dict):
        return {k: _to_cbor_value(v, shape.members.get(k)) for k, v in value.items()}
    if t == "list" and isinstance(value, list):
        return [_to_cbor_value(v, shape.member) for v in value]
    if t == "map" and isinstance(value, dict):
        return {k: _to_cbor_value(v, shape.value) for k, v in value.items()}
    if t == "timestamp":
        return _parse_timestamp(value)
    if t == "blob" and isinstance(value, str):
        try:
            return base64.b64decode(value)
        except (ValueError, TypeError):
            return value
    if t in ("double", "float") and isinstance(value, int):
        return float(value)
    return value


def _json_request(request: Request, action: str, body: bytes) -> Request:
    headers = [
        (k, v)
        for k, v in request.scope.get("headers", [])
        if k.lower()
        not in (b"content-type", b"x-amz-target", b"smithy-protocol", b"content-length", b"accept")
    ]
    headers += [
        (b"content-type", b"application/x-amz-json-1.0"),
        (b"x-amz-target", f"{_TARGET_PREFIX}.{action}".encode()),
        (b"content-length", str(len(body)).encode()),
    ]
    scope = dict(request.scope)
    scope.setdefault("type", "http")
    scope.setdefault("method", "POST")
    scope["headers"] = headers
    scope["path"] = "/"
    scope["raw_path"] = b"/"
    scope["query_string"] = b""
    return Request(scope, request.receive)


async def forward_cbor_via_json(
    request: Request, action: str, params: dict, account_id: str
) -> Response:
    model = _service_model()
    try:
        op = model.operation_model(action)
    except Exception:  # noqa: BLE001
        return _cbor_error("UnknownOperationException", f"Unknown operation {action}", 400)
    payload = {k: v for k, v in params.items() if k != "Action"}
    payload = _to_json_value(payload, op.input_shape)
    body = json.dumps(payload).encode()
    response = await forward_to_moto_with_body(
        _json_request(request, action, body), "cloudwatch", body, account_id=account_id
    )
    raw = response.body or b""
    try:
        data = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        logger.debug("cloudwatch cbor bridge: non-JSON moto response for %s: %r", action, raw[:200])
        return _cbor_error("InternalError", f"Unexpected response for {action}", 500)
    if response.status_code >= 400:
        code = data.get("__type", "InternalError").rsplit("#", 1)[-1]
        message = data.get("message") or data.get("Message") or ""
        return _cbor_error(code, message, response.status_code)
    out = _to_cbor_value(data, op.output_shape) if op.output_shape is not None else {}
    return Response(
        content=cbor2.dumps(out, datetime_as_timestamp=True, timezone=UTC),
        status_code=200,
        media_type="application/cbor",
        headers={"smithy-protocol": "rpc-v2-cbor"},
    )


def _cbor_error(code: str, message: str, status: int) -> Response:
    return Response(
        content=cbor2.dumps({"__type": code, "message": message}),
        status_code=status,
        media_type="application/cbor",
        headers={"smithy-protocol": "rpc-v2-cbor"},
    )
