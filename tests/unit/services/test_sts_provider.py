"""Unit tests for the STS native provider."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from robotocore.services.sts.provider import handle_sts_request


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
        response = asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
        assert response.status_code == 200
        assert b"<Account>123456789012</Account>" in response.body

    def test_get_access_key_info_different_account(self):
        body = b"Action=GetAccessKeyInfo&AccessKeyId=AKIAIOSFODNN7EXAMPLE"
        request = _make_request(body)
        response = asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "999888777666")
        )
        assert b"<Account>999888777666</Account>" in response.body

    @patch("robotocore.services.sts.provider.forward_to_moto_with_body")
    def test_non_intercepted_action_forwards_to_moto(self, mock_forward):
        mock_forward.return_value = MagicMock(status_code=200)
        body = b"Action=GetCallerIdentity"
        request = _make_request(body)
        asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
        mock_forward.assert_called_once_with(request, "sts", body)

    @patch("robotocore.services.sts.provider.forward_to_moto_with_body")
    def test_assume_role_forwards_to_moto(self, mock_forward):
        mock_forward.return_value = MagicMock(status_code=200)
        body = b"Action=AssumeRole&RoleArn=arn:aws:iam::123456789012:role/test&RoleSessionName=s"
        request = _make_request(body)
        asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
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
        response = asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
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
        response = asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
        assert response.status_code == 200
        assert b"<Credentials>" in response.body
        assert b"<AccessKeyId>" in response.body

    def test_decode_authorization_message(self):
        """DecodeAuthorizationMessage returns the encoded message as-is."""
        body = b"Action=DecodeAuthorizationMessage&EncodedMessage=test-encoded-msg"
        request = _make_request(body)
        response = asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
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
            asyncio.get_event_loop().run_until_complete(
                handle_sts_request(request, "us-east-1", "123456789012")
            )
            mock_forward.assert_called_once()
