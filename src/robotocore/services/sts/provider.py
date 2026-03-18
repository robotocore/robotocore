"""Native STS provider.

Intercepts operations Moto doesn't support:
- GetAccessKeyInfo
- AssumeRole policy size validation
- AssumeRoleWithSAML (skips SAML XML validation)
- DecodeAuthorizationMessage
- GetWebIdentityToken (moto's response class uses missing _get_multi_param)

Delegates everything else to Moto via forward_to_moto.
Uses query protocol (Action parameter).
"""

import base64
import hashlib
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
WIT_MIN_DURATION = 60
WIT_MAX_DURATION = 3600
WIT_DEFAULT_DURATION = 300
VALID_SIGNING_ALGORITHMS = ("RS256", "ES384")


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

    if action == "GetWebIdentityToken":
        return _get_web_identity_token(parsed, account_id)

    return await forward_to_moto_with_body(request, "sts", body, account_id=account_id)


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


def _extract_audiences(parsed: dict[str, list[str]]) -> list[str] | Response:
    """Extract Audience.member.N values from raw query params."""
    audiences: list[str] = []
    for key, vals in parsed.items():
        if key.startswith("Audience.member."):
            audiences.extend(vals)
    if not audiences:
        return _missing_param_response("Audience")
    if len(audiences) > 10:
        return _error_response(
            "ValidationError",
            "Value at 'audience' failed to satisfy constraint: "
            "Member must have length less than or equal to 10",
            400,
        )
    return audiences


def _validate_signing_algorithm(parsed: dict[str, list[str]]) -> str | Response:
    """Validate and return the SigningAlgorithm parameter."""
    algo_vals = parsed.get("SigningAlgorithm", [])
    algo = algo_vals[0] if algo_vals else ""
    if not algo:
        return _missing_param_response("SigningAlgorithm")
    if algo not in VALID_SIGNING_ALGORITHMS:
        return _error_response(
            "ValidationError",
            f"Value '{algo}' at 'signingAlgorithm' failed to satisfy "
            f"constraint: Member must satisfy enum value set: [RS256, ES384]",
            400,
        )
    return algo


def _validate_wit_duration(parsed: dict[str, list[str]]) -> int | Response:
    """Validate DurationSeconds for GetWebIdentityToken (60-3600, default 300)."""
    dur_vals = parsed.get("DurationSeconds", [])
    raw = dur_vals[0] if dur_vals else str(WIT_DEFAULT_DURATION)
    try:
        duration = int(raw)
    except (ValueError, TypeError):
        return _error_response(
            "ValidationError",
            f"Value '{raw}' at 'durationSeconds' failed to satisfy constraint: "
            f"Member must be a valid integer.",
            400,
        )
    if duration < WIT_MIN_DURATION or duration > WIT_MAX_DURATION:
        return _error_response(
            "ValidationError",
            f"Value '{duration}' at 'durationSeconds' failed to satisfy constraint: "
            f"Member must have value between {WIT_MIN_DURATION} and {WIT_MAX_DURATION}.",
            400,
        )
    return duration


def _build_mock_jwt(
    audiences: list[str],
    signing_algorithm: str,
    account_id: str,
    issued_at: datetime,
    expires_at: datetime,
) -> str:
    """Build a plausible mock JWT (header.payload.signature)."""

    def _b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

    header = {"alg": signing_algorithm, "typ": "JWT", "kid": uuid.uuid4().hex[:16]}
    payload = {
        "sub": f"arn:aws:iam::{account_id}:root",
        "aud": audiences,
        "iss": f"https://sts.amazonaws.com/{account_id}",
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
        "jti": uuid.uuid4().hex,
    }
    h = _b64url(json.dumps(header, separators=(",", ":")).encode())
    p = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    sig = _b64url(hashlib.sha256(f"{h}.{p}".encode()).digest())
    return f"{h}.{p}.{sig}"


def _get_web_identity_token(parsed: dict[str, list[str]], account_id: str) -> Response:
    """GetWebIdentityToken -- return a mock signed JWT.

    Uses ``parsed`` (raw parse_qs output with list values) because
    Audience is a multi-value param (Audience.member.1, …).
    """
    audiences = _extract_audiences(parsed)
    if isinstance(audiences, Response):
        return audiences

    algo = _validate_signing_algorithm(parsed)
    if isinstance(algo, Response):
        return algo

    duration = _validate_wit_duration(parsed)
    if isinstance(duration, Response):
        return duration

    now = datetime.now(UTC)
    expiration = now + timedelta(seconds=duration)
    exp_str = expiration.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
    token = _build_mock_jwt(audiences, algo, account_id, now, expiration)

    request_id = _new_request_id()
    xml = (
        "<GetWebIdentityTokenResponse "
        'xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
        "<GetWebIdentityTokenResult>"
        f"<WebIdentityToken>{xml_escape(token)}</WebIdentityToken>"
        f"<Expiration>{xml_escape(exp_str)}</Expiration>"
        "</GetWebIdentityTokenResult>"
        "<ResponseMetadata>"
        f"<RequestId>{request_id}</RequestId>"
        "</ResponseMetadata>"
        "</GetWebIdentityTokenResponse>"
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
