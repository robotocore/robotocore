"""Native STS provider.

Intercepts operations Moto doesn't support:
- GetAccessKeyInfo
- AssumeRole policy size validation
- AssumeRoleWithSAML (skips SAML XML validation)

Delegates everything else to Moto via forward_to_moto.
Uses query protocol (Action parameter).
"""

import json
import uuid
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto_with_body

DEFAULT_ACCOUNT_ID = "123456789012"
MAX_PACKED_POLICY_SIZE = 2048


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


def _decode_authorization_message(params: dict) -> Response:
    """DecodeAuthorizationMessage — return the message as-is (mock decode)."""
    encoded = params.get("EncodedMessage", "")
    xml = (
        "<DecodeAuthorizationMessageResponse "
        'xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
        "<DecodeAuthorizationMessageResult>"
        f"<DecodedMessage>{encoded}</DecodedMessage>"
        "</DecodeAuthorizationMessageResult>"
        "<ResponseMetadata>"
        "<RequestId>12345678-1234-1234-1234-123456789012</RequestId>"
        "</ResponseMetadata>"
        "</DecodeAuthorizationMessageResponse>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")


def _pack_policy(policy: str) -> str:
    """Simulate AWS policy packing — minify JSON."""
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
        f"<Code>{code}</Code>"
        f"<Message>{message}</Message>"
        "</Error>"
        "<RequestId>12345678-1234-1234-1234-123456789012</RequestId>"
        "</ErrorResponse>"
    )
    return Response(content=xml, status_code=status_code, media_type="text/xml")


def _assume_role_with_saml(params: dict, account_id: str) -> Response:
    """AssumeRoleWithSAML — skip SAML validation, return credentials."""
    role_arn = params.get("RoleArn", "")
    principal_arn = params.get("PrincipalArn", "")
    duration = int(params.get("DurationSeconds", "3600"))

    # Extract role name from ARN
    role_name = role_arn.split("/")[-1] if "/" in role_arn else "SAMLRole"
    session_name = f"saml-session-{uuid.uuid4().hex[:8]}"
    assumed_role_id = f"AROA{uuid.uuid4().hex[:16].upper()}"

    now = datetime.now(UTC)
    expiration = now + timedelta(seconds=duration)
    exp_str = expiration.strftime("%Y-%m-%dT%H:%M:%SZ")

    assumed_arn = f"arn:aws:sts::{account_id}:assumed-role/{role_name}/{session_name}"

    xml = (
        "<AssumeRoleWithSAMLResponse "
        'xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
        "<AssumeRoleWithSAMLResult>"
        "<Credentials>"
        f"<AccessKeyId>ASIA{uuid.uuid4().hex[:16].upper()}</AccessKeyId>"
        f"<SecretAccessKey>{uuid.uuid4().hex}</SecretAccessKey>"
        f"<SessionToken>{uuid.uuid4().hex}{uuid.uuid4().hex}</SessionToken>"
        f"<Expiration>{exp_str}</Expiration>"
        "</Credentials>"
        "<AssumedRoleUser>"
        f"<AssumedRoleId>{assumed_role_id}:{session_name}</AssumedRoleId>"
        f"<Arn>{assumed_arn}</Arn>"
        "</AssumedRoleUser>"
        f"<Audience>https://signin.aws.amazon.com/saml</Audience>"
        f"<Issuer>{principal_arn}</Issuer>"
        f"<NameQualifier>{uuid.uuid4().hex[:20]}</NameQualifier>"
        f"<Subject>saml-user</Subject>"
        f"<SubjectType>persistent</SubjectType>"
        f"<PackedPolicySize>0</PackedPolicySize>"
        "</AssumeRoleWithSAMLResult>"
        "<ResponseMetadata>"
        "<RequestId>12345678-1234-1234-1234-123456789012</RequestId>"
        "</ResponseMetadata>"
        "</AssumeRoleWithSAMLResponse>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")


def _get_access_key_info(params: dict, account_id: str) -> Response:
    xml = (
        '<GetAccessKeyInfoResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
        "<GetAccessKeyInfoResult>"
        f"<Account>{account_id}</Account>"
        "</GetAccessKeyInfoResult>"
        "<ResponseMetadata>"
        "<RequestId>12345678-1234-1234-1234-123456789012</RequestId>"
        "</ResponseMetadata>"
        "</GetAccessKeyInfoResponse>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")
