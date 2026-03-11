"""Unit tests for the STS native provider."""

import asyncio
import re
from unittest.mock import AsyncMock, MagicMock, patch

from robotocore.services.sts.provider import (
    handle_sts_request,
)


def _make_request(body: bytes = b"", headers: dict | None = None):
    request = MagicMock()
    request.body = AsyncMock(return_value=body)
    request.headers = headers or {}
    request.method = "POST"
    request.url = MagicMock()
    request.url.path = "/"
    request.url.query = None
    return request


class TestSTSProvider:
    def test_get_access_key_info(self):
        body = b"Action=GetAccessKeyInfo&AccessKeyId=AKIAIOSFODNN7EXAMPLE"
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 200
        assert b"<Account>123456789012</Account>" in response.body

    def test_get_access_key_info_different_account(self):
        body = b"Action=GetAccessKeyInfo&AccessKeyId=AKIAIOSFODNN7EXAMPLE"
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "999888777666"))
        assert b"<Account>999888777666</Account>" in response.body

    @patch("robotocore.services.sts.provider.forward_to_moto_with_body")
    def test_non_intercepted_action_forwards_to_moto(self, mock_forward):
        mock_forward.return_value = MagicMock(status_code=200)
        body = b"Action=GetCallerIdentity"
        request = _make_request(body)
        asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        mock_forward.assert_called_once_with(request, "sts", body, account_id="123456789012")

    @patch("robotocore.services.sts.provider.forward_to_moto_with_body")
    def test_assume_role_forwards_to_moto(self, mock_forward):
        mock_forward.return_value = MagicMock(status_code=200)
        body = b"Action=AssumeRole&RoleArn=arn:aws:iam::123456789012:role/test&RoleSessionName=s"
        request = _make_request(body)
        asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        mock_forward.assert_called_once()

    def test_assume_role_packed_policy_too_large(self):
        """AssumeRole with an oversized packed policy returns PackedPolicyTooLarge error."""
        # Build a policy that exceeds 2048 bytes when packed
        large_policy = (
            '{"Version":"2012-10-17","Statement":['
            + ",".join(
                [
                    '{"Effect":"Allow","Action":"s3:GetObject",'
                    f'"Resource":"arn:aws:s3:::bucket-{i}/*"}}'
                    for i in range(100)
                ]
            )
            + "]}"
        )
        body = (
            b"Action=AssumeRole"
            b"&RoleArn=arn:aws:iam::123456789012:role/test"
            b"&RoleSessionName=s"
            b"&Policy=" + large_policy.encode()
        )
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 400
        assert b"PackedPolicyTooLarge" in response.body

    def test_assume_role_with_saml_returns_credentials(self):
        """AssumeRoleWithSAML bypasses SAML validation and returns credentials."""
        body = (
            b"Action=AssumeRoleWithSAML"
            b"&RoleArn=arn:aws:iam::123456789012:role/SAMLRole"
            b"&PrincipalArn=arn:aws:iam::123456789012:saml-provider/MyIdP"
            b"&SAMLAssertion=not-real-saml"
        )
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 200
        assert b"<Credentials>" in response.body
        assert b"<AccessKeyId>" in response.body

    def test_decode_authorization_message(self):
        """DecodeAuthorizationMessage returns the encoded message as-is."""
        body = b"Action=DecodeAuthorizationMessage&EncodedMessage=test-encoded-msg"
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 200
        assert b"<DecodedMessage>test-encoded-msg</DecodedMessage>" in response.body

    def test_empty_action_forwards_to_moto(self):
        """A request with no Action parameter forwards to Moto."""
        with patch(
            "robotocore.services.sts.provider.forward_to_moto_with_body",
            new_callable=AsyncMock,
        ) as mock_forward:
            mock_forward.return_value = MagicMock(status_code=200)
            body = b""
            request = _make_request(body)
            asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
            mock_forward.assert_called_once()


class TestSTSParameterValidation:
    """Categorical: missing required parameter validation.

    AWS returns specific errors when required parameters are absent.
    Many native providers skip this validation entirely, silently
    accepting empty strings and producing malformed responses.
    """

    def test_get_access_key_info_missing_access_key_id(self):
        """GetAccessKeyInfo without AccessKeyId should return an error."""
        body = b"Action=GetAccessKeyInfo"
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 400
        assert b"AccessKeyId" in response.body

    def test_assume_role_with_saml_missing_role_arn(self):
        """AssumeRoleWithSAML without RoleArn should return an error."""
        body = (
            b"Action=AssumeRoleWithSAML"
            b"&PrincipalArn=arn:aws:iam::123456789012:saml-provider/MyIdP"
            b"&SAMLAssertion=not-real-saml"
        )
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 400
        assert b"RoleArn" in response.body

    def test_assume_role_with_saml_missing_principal_arn(self):
        """AssumeRoleWithSAML without PrincipalArn should return an error."""
        body = (
            b"Action=AssumeRoleWithSAML"
            b"&RoleArn=arn:aws:iam::123456789012:role/SAMLRole"
            b"&SAMLAssertion=not-real-saml"
        )
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 400
        assert b"PrincipalArn" in response.body

    def test_assume_role_with_saml_missing_saml_assertion(self):
        """AssumeRoleWithSAML without SAMLAssertion should return an error."""
        body = (
            b"Action=AssumeRoleWithSAML"
            b"&RoleArn=arn:aws:iam::123456789012:role/SAMLRole"
            b"&PrincipalArn=arn:aws:iam::123456789012:saml-provider/MyIdP"
        )
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 400
        assert b"SAMLAssertion" in response.body

    def test_decode_authorization_message_missing_encoded_message(self):
        """DecodeAuthorizationMessage without EncodedMessage should return an error."""
        body = b"Action=DecodeAuthorizationMessage"
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 400
        assert b"EncodedMessage" in response.body


class TestSTSDurationValidation:
    """Categorical: unhandled exceptions from invalid user input.

    When providers parse user input (int(), json.loads(), etc.) without
    try/except, invalid values cause unhandled exceptions that become 500s
    instead of proper 400 validation errors.
    """

    def test_assume_role_with_saml_non_numeric_duration(self):
        """Non-numeric DurationSeconds should return 400, not crash with ValueError."""
        body = (
            b"Action=AssumeRoleWithSAML"
            b"&RoleArn=arn:aws:iam::123456789012:role/SAMLRole"
            b"&PrincipalArn=arn:aws:iam::123456789012:saml-provider/MyIdP"
            b"&SAMLAssertion=not-real-saml"
            b"&DurationSeconds=notanumber"
        )
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 400
        assert b"DurationSeconds" in response.body or b"ValidationError" in response.body

    def test_assume_role_with_saml_duration_too_low(self):
        """DurationSeconds below 900 should return validation error."""
        body = (
            b"Action=AssumeRoleWithSAML"
            b"&RoleArn=arn:aws:iam::123456789012:role/SAMLRole"
            b"&PrincipalArn=arn:aws:iam::123456789012:saml-provider/MyIdP"
            b"&SAMLAssertion=not-real-saml"
            b"&DurationSeconds=100"
        )
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 400

    def test_assume_role_with_saml_duration_too_high(self):
        """DurationSeconds above 43200 should return validation error."""
        body = (
            b"Action=AssumeRoleWithSAML"
            b"&RoleArn=arn:aws:iam::123456789012:role/SAMLRole"
            b"&PrincipalArn=arn:aws:iam::123456789012:saml-provider/MyIdP"
            b"&SAMLAssertion=not-real-saml"
            b"&DurationSeconds=99999"
        )
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        assert response.status_code == 400


class TestSTSRequestIdUniqueness:
    """Categorical: hardcoded RequestId in responses.

    AWS returns a unique RequestId per request. Hardcoded values break
    clients that correlate requests via RequestId (e.g., for debugging).
    """

    def test_request_ids_are_unique_across_calls(self):
        """Two calls should return different RequestIds."""
        body = b"Action=GetAccessKeyInfo&AccessKeyId=AKIAIOSFODNN7EXAMPLE"
        req1 = _make_request(body)
        req2 = _make_request(body)
        resp1 = asyncio.run(handle_sts_request(req1, "us-east-1", "123456789012"))
        resp2 = asyncio.run(handle_sts_request(req2, "us-east-1", "123456789012"))
        # Extract RequestId from both responses
        id_pattern = re.compile(rb"<RequestId>([^<]+)</RequestId>")
        id1 = id_pattern.search(resp1.body)
        id2 = id_pattern.search(resp2.body)
        assert id1 and id2
        assert id1.group(1) != id2.group(1), "RequestIds must be unique per request"

    def test_request_id_is_valid_uuid(self):
        """RequestId should be a valid UUID format."""
        body = b"Action=GetAccessKeyInfo&AccessKeyId=AKIAIOSFODNN7EXAMPLE"
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        id_pattern = re.compile(
            rb"<RequestId>([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
            rb"</RequestId>"
        )
        match = id_pattern.search(response.body)
        assert match, "RequestId should be a valid UUID"


class TestSTSCredentialFormat:
    """Categorical: generated credentials must match AWS format conventions.

    Clients and SDKs validate credential format. If AccessKeyId doesn't
    start with ASIA (for temp creds), or SessionToken is too short,
    downstream code may reject them.
    """

    def test_saml_access_key_starts_with_asia(self):
        """Temporary access keys must start with ASIA."""
        body = (
            b"Action=AssumeRoleWithSAML"
            b"&RoleArn=arn:aws:iam::123456789012:role/SAMLRole"
            b"&PrincipalArn=arn:aws:iam::123456789012:saml-provider/MyIdP"
            b"&SAMLAssertion=not-real-saml"
        )
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        key_match = re.search(rb"<AccessKeyId>(ASIA[A-Z0-9]+)</AccessKeyId>", response.body)
        assert key_match, "AccessKeyId must start with ASIA"
        key = key_match.group(1)
        assert len(key) == 20, f"AccessKeyId should be 20 chars, got {len(key)}"

    def test_saml_assumed_role_arn_uses_provided_account(self):
        """The assumed-role ARN must use the account_id passed to the handler."""
        body = (
            b"Action=AssumeRoleWithSAML"
            b"&RoleArn=arn:aws:iam::999888777666:role/MyRole"
            b"&PrincipalArn=arn:aws:iam::999888777666:saml-provider/MyIdP"
            b"&SAMLAssertion=not-real-saml"
        )
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "999888777666"))
        assert b"arn:aws:sts::999888777666:assumed-role/MyRole/" in response.body

    def test_saml_session_token_minimum_length(self):
        """Session tokens should be at least 100 chars to look realistic."""
        body = (
            b"Action=AssumeRoleWithSAML"
            b"&RoleArn=arn:aws:iam::123456789012:role/SAMLRole"
            b"&PrincipalArn=arn:aws:iam::123456789012:saml-provider/MyIdP"
            b"&SAMLAssertion=not-real-saml"
        )
        request = _make_request(body)
        response = asyncio.run(handle_sts_request(request, "us-east-1", "123456789012"))
        token_match = re.search(rb"<SessionToken>([^<]+)</SessionToken>", response.body)
        assert token_match
        token = token_match.group(1)
        assert len(token) >= 100, f"SessionToken too short ({len(token)} chars)"
