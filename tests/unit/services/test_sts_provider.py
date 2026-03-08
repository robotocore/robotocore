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

    @patch("robotocore.services.sts.provider.forward_to_moto")
    def test_non_intercepted_action_forwards_to_moto(self, mock_forward):
        mock_forward.return_value = MagicMock(status_code=200)
        body = b"Action=GetCallerIdentity"
        request = _make_request(body)
        asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
        mock_forward.assert_called_once_with(request, "sts")

    @patch("robotocore.services.sts.provider.forward_to_moto")
    def test_assume_role_forwards_to_moto(self, mock_forward):
        mock_forward.return_value = MagicMock(status_code=200)
        body = b"Action=AssumeRole&RoleArn=arn:aws:iam::123456789012:role/test&RoleSessionName=s"
        request = _make_request(body)
        asyncio.get_event_loop().run_until_complete(
            handle_sts_request(request, "us-east-1", "123456789012")
        )
        mock_forward.assert_called_once()
