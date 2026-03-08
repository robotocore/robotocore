"""Tests for the gateway ASGI application."""

from unittest.mock import MagicMock

from robotocore.gateway.app import _extract_account_id


def test_health_endpoint(client):
    response = client.get("/_robotocore/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "running"


class TestExtractAccountId:
    """Bug fix 1F: Account ID validation must require exactly 12 digits."""

    def _request_with_credential(self, credential: str) -> MagicMock:
        req = MagicMock()
        req.headers = {"authorization": ""}
        req.query_params = {"X-Amz-Credential": credential}
        return req

    def test_valid_12_digit_account(self):
        req = self._request_with_credential("123456789012/20260305/us-east-1/s3/aws4_request")
        assert _extract_account_id(req) == "123456789012"

    def test_rejects_5_digit_string(self):
        req = self._request_with_credential("12345/20260305/us-east-1/s3/aws4_request")
        assert _extract_account_id(req) != "12345"

    def test_rejects_15_digit_string(self):
        req = self._request_with_credential("123456789012345/20260305/us-east-1/s3/aws4_request")
        assert _extract_account_id(req) != "123456789012345"

    def test_rejects_1_digit_string(self):
        req = self._request_with_credential("1/20260305/us-east-1/s3/aws4_request")
        assert _extract_account_id(req) != "1"
