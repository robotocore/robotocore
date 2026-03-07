"""Tests for multi-account support."""

from unittest.mock import MagicMock

from robotocore.gateway.app import DEFAULT_ACCOUNT_ID, _extract_account_id


def _make_request(
    headers: dict | None = None,
    query_params: dict | None = None,
) -> MagicMock:
    req = MagicMock()
    req.headers = headers or {}
    req.query_params = query_params or {}
    return req


class TestExtractAccountId:
    def test_default_account_id(self):
        req = _make_request()
        assert _extract_account_id(req) == DEFAULT_ACCOUNT_ID

    def test_default_is_moto_default(self):
        assert DEFAULT_ACCOUNT_ID == "123456789012"

    def test_extracts_from_sigv4_auth(self):
        req = _make_request(headers={
            "authorization": (
                "AWS4-HMAC-SHA256 Credential=123456789012/20240101/"
                "us-east-1/s3/aws4_request"
            )
        })
        assert _extract_account_id(req) == "123456789012"

    def test_extracts_from_presigned_url(self):
        req = _make_request(query_params={
            "X-Amz-Credential": "999888777666/20240101/us-east-1/s3/aws4_request"
        })
        assert _extract_account_id(req) == "999888777666"

    def test_auth_takes_precedence_over_query(self):
        req = _make_request(
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 Credential=111111111111/20240101/"
                    "us-east-1/s3/aws4_request"
                )
            },
            query_params={
                "X-Amz-Credential": "222222222222/20240101/us-east-1/s3/aws4_request"
            },
        )
        assert _extract_account_id(req) == "111111111111"

    def test_no_credentials_returns_default(self):
        req = _make_request(headers={"content-type": "application/json"})
        assert _extract_account_id(req) == DEFAULT_ACCOUNT_ID
