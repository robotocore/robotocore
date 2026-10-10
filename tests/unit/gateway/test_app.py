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


class TestUnroutableRequestHints:
    """The 400 body agents see for unroutable requests should name the missing
    routing cue and how to satisfy it, not just report failure."""

    def test_unsigned_query_action_suggests_signing(self, client):
        resp = client.get("/", params={"Action": "DescribeNotAThing", "Version": "2016-11-15"})
        assert resp.status_code == 400
        data = resp.json()
        assert data["error"] == "Could not determine target AWS service from request"
        assert any("Authorization" in h for h in data["hints"])
        assert any("DescribeNotAThing" in h for h in data["hints"])

    def test_unknown_prefix_gets_a_target_hint(self, client):
        resp = client.post(
            "/",
            headers={
                "x-amz-target": "NotAService_20200101.NotAnOperation",
                "content-type": "application/x-amz-json-1.1",
            },
            content=b"{}",
        )
        assert resp.status_code == 400
        assert any("NotAService_20200101.NotAnOperation" in h for h in resp.json()["hints"])

    def test_no_cues_falls_back_to_a_neutral_hint(self, client):
        resp = client.get("/", headers={"authorization": "Bearer sev1"})
        assert resp.status_code == 400
        assert resp.json()["hints"], "hints must never be empty"
