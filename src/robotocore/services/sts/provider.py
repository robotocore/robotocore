"""Native STS provider.

Intercepts operations Moto doesn't support:
- GetAccessKeyInfo
- AssumeRole policy size validation
- AssumeRoleWithSAML (skips SAML XML validation)
- DecodeAuthorizationMessage

Delegates everything else to Moto via forward_to_moto.
Uses query protocol (Action parameter).
"""

import json
import uuid
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs
from xml.sax.saxutils import escape as xml_escape

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto_with_body

DEFAULT_ACCOUNT_ID = "123456789012"
MAX_PACKED_POLICY_SIZE = 2048
MIN_DURATION_SECONDS = 900
MAX_DURATION_SECONDS = 43200


def _new_request_id() -> str:
    """Generate a unique RequestId for each response."""
    return str(uuid.uuid4())


async def handle_sts_request(request: Request, region: str, account_id: str) -> Response:
    """Handle STS API requests."""
    body = await request.body()
    parsed = parse_qs(body.decode(), keep_blank_values=True)
    params = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
    action = params.get("Action", "")

    if action == "GetAccessKeyInfo":
        return _get_access_key_info(params, account_id)

    if action == "DecodeAuthorizationMessage":
        return _decode_authorization_message(params)

    if action == "AssumeRole":
        policy = params.get("Policy", "")
        if policy:
            packed = _pack_policy(policy)
            if len(packed) > MAX_PACKED_POLICY_SIZE:
                return _error_response(
                    "PackedPolicyTooLarge",
                    f"Packed size of the policy is too large "
                    f"({len(packed)} bytes, max {MAX_PACKED_POLICY_SIZE})",
                    400,
                )

    if action == "AssumeRoleWithSAML":
        return _assume_role_with_saml(params, account_id)

    return await forward_to_moto_with_body(request, "sts", body)


def _require_param(params: dict, name: str) -> str | None:
    """Return param value if present and non-empty, or None."""
    val = params.get(name, "")
    return val if val else None


def _missing_param_response(param_name: str) -> Response:
    """Return a MissingParameter error response."""
    return _error_response(
        "MissingParameter",
        f"The request must contain the parameter {param_name}.",
        400,
    )


def _decode_authorization_message(params: dict) -> Response:
    """DecodeAuthorizationMessage -- return the message as-is (mock decode)."""
    encoded = _require_param(params, "EncodedMessage")
    if not encoded:
        return _missing_param_response("EncodedMessage")
    xml = (
        "<DecodeAuthorizationMessageResponse "
        'xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
        "<DecodeAuthorizationMessageResult>"
        f"<DecodedMessage>{xml_escape(encoded)}</DecodedMessage>"
        "</DecodeAuthorizationMessageResult>"
        "<ResponseMetadata>"
        f"<RequestId>{_new_request_id()}</RequestId>"
        "</ResponseMetadata>"
        "</DecodeAuthorizationMessageResponse>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")


def _pack_policy(policy: str) -> str:
    """Simulate AWS policy packing -- minify JSON."""
    try:
        parsed = json.loads(policy)
        return json.dumps(parsed, separators=(",", ":"))
    except (json.JSONDecodeError, TypeError):
        return policy


def _error_response(code: str, message: str, status_code: int) -> Response:
    xml = (
        '<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
        "<Error>"
        f"<Type>Sender</Type>"
        f"<Code>{xml_escape(code)}</Code>"
        f"<Message>{xml_escape(message)}</Message>"
        "</Error>"
        f"<RequestId>{_new_request_id()}</RequestId>"
        "</ErrorResponse>"
    )
    return Response(content=xml, status_code=status_code, media_type="text/xml")


def _validate_duration(params: dict) -> int | Response:
    """Validate DurationSeconds, returning int or error Response."""
    raw = params.get("DurationSeconds", "3600")
    try:
        duration = int(raw)
    except (ValueError, TypeError):
        return _error_response(
            "ValidationError",
            f"Value '{raw}' at 'DurationSeconds' failed to satisfy constraint: "
            f"Member must be a valid integer.",
            400,
        )
    if duration < MIN_DURATION_SECONDS or duration > MAX_DURATION_SECONDS:
        return _error_response(
            "ValidationError",
            f"Value '{duration}' at 'DurationSeconds' failed to satisfy constraint: "
            f"Member must have value between {MIN_DURATION_SECONDS} and "
            f"{MAX_DURATION_SECONDS}.",
            400,
        )
    return duration


def _assume_role_with_saml(params: dict, account_id: str) -> Response:
    """AssumeRoleWithSAML -- skip SAML validation, return credentials."""
    # Validate required parameters
    role_arn = _require_param(params, "RoleArn")
    if not role_arn:
        return _missing_param_response("RoleArn")
    principal_arn = _require_param(params, "PrincipalArn")
    if not principal_arn:
        return _missing_param_response("PrincipalArn")
    if not _require_param(params, "SAMLAssertion"):
        return _missing_param_response("SAMLAssertion")

    # Validate duration
    duration_or_err = _validate_duration(params)
    if isinstance(duration_or_err, Response):
        return duration_or_err
    duration = duration_or_err

    # Extract role name from ARN
    role_name = role_arn.split("/")[-1] if "/" in role_arn else "SAMLRole"
    session_name = f"saml-session-{uuid.uuid4().hex[:8]}"
    assumed_role_id = f"AROA{uuid.uuid4().hex[:16].upper()}"

    now = datetime.now(UTC)
    expiration = now + timedelta(seconds=duration)
    exp_str = expiration.strftime("%Y-%m-%dT%H:%M:%SZ")

    assumed_arn = f"arn:aws:sts::{account_id}:assumed-role/{role_name}/{session_name}"

    # Generate a realistic session token (AWS tokens are ~356 chars)
    session_token = "FwoGZXIvYXdzE" + uuid.uuid4().hex + uuid.uuid4().hex + uuid.uuid4().hex
    request_id = _new_request_id()

    xml = (
        "<AssumeRoleWithSAMLResponse "
        'xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
        "<AssumeRoleWithSAMLResult>"
        "<Credentials>"
        f"<AccessKeyId>ASIA{uuid.uuid4().hex[:16].upper()}</AccessKeyId>"
        f"<SecretAccessKey>{uuid.uuid4().hex}</SecretAccessKey>"
        f"<SessionToken>{session_token}</SessionToken>"
        f"<Expiration>{exp_str}</Expiration>"
        "</Credentials>"
        "<AssumedRoleUser>"
        f"<AssumedRoleId>{assumed_role_id}:{session_name}</AssumedRoleId>"
        f"<Arn>{xml_escape(assumed_arn)}</Arn>"
        "</AssumedRoleUser>"
        f"<Audience>https://signin.aws.amazon.com/saml</Audience>"
        f"<Issuer>{xml_escape(principal_arn)}</Issuer>"
        f"<NameQualifier>{uuid.uuid4().hex[:20]}</NameQualifier>"
        f"<Subject>saml-user</Subject>"
        f"<SubjectType>persistent</SubjectType>"
        f"<PackedPolicySize>0</PackedPolicySize>"
        "</AssumeRoleWithSAMLResult>"
        "<ResponseMetadata>"
        f"<RequestId>{request_id}</RequestId>"
        "</ResponseMetadata>"
        "</AssumeRoleWithSAMLResponse>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")


def _get_access_key_info(params: dict, account_id: str) -> Response:
    access_key = _require_param(params, "AccessKeyId")
    if not access_key:
        return _missing_param_response("AccessKeyId")
    xml = (
        '<GetAccessKeyInfoResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
        "<GetAccessKeyInfoResult>"
        f"<Account>{xml_escape(account_id)}</Account>"
        "</GetAccessKeyInfoResult>"
        "<ResponseMetadata>"
        f"<RequestId>{_new_request_id()}</RequestId>"
        "</ResponseMetadata>"
        "</GetAccessKeyInfoResponse>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")
