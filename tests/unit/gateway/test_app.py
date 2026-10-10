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


class TestProviderCrashErrorContract:
    """Provider crashes must surface as the AWS error contract (the AGENTS.md
    501/500 table), never as Starlette's plain-text 'Internal Server Error'."""

    def test_moto_path_crash_becomes_internalfailure(self, client, monkeypatch):
        import robotocore.gateway.app as app_mod

        async def boom(request, service_name, **kwargs):
            raise KeyError("region")

        monkeypatch.setattr(app_mod, "get_effective_provider", lambda *a, **k: None)
        monkeypatch.setattr(app_mod, "forward_to_moto", boom)
        # sts is moto-backed and needs no account/region fixtures
        resp = client.post(
            "/",
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 Credential=123456789012/20260310/us-east-1/sts/"
                    "aws4_request, SignedHeaders=host, Signature=s"
                ),
                "content-type": "application/x-www-form-urlencoded",
            },
            data={"Action": "GetCallerIdentity"},
        )
        assert resp.status_code == 500
        assert resp.headers["x-robotocore-diag"]
        if resp.headers.get("content-type", "").endswith("json") or b"__type" in resp.content:
            assert resp.json()["__type"] == "InternalFailure", resp.content[:200]
        else:
            assert b"InternalError" in resp.content, resp.content[:200]

    def test_unregistered_service_answers_notimplemented(self, client, monkeypatch):
        import robotocore.gateway.app as app_mod

        monkeypatch.setattr(app_mod, "is_service_allowed", lambda name: False)
        resp = client.post(
            "/",
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 Credential=123456789012/20260310/us-east-1/"
                    "notarealservice/aws4_request, SignedHeaders=host, Signature=s"
                )
            },
        )
        assert resp.status_code == 501
        assert b"NotImplemented" in resp.content
        assert b"has not been implemented" in resp.content

    def test_services_filtered_registered_service_names_the_filter(self, client, monkeypatch):
        import robotocore.gateway.app as app_mod

        allowed = set()
        monkeypatch.setattr(app_mod, "is_service_allowed", lambda name: False)
        monkeypatch.setattr(app_mod, "get_allowed_services", lambda: allowed, raising=False)
        resp = client.post(
            "/",
            headers={
                "authorization": (
                    "AWS4-HMAC-SHA256 Credential=123456789012/20260310/us-east-1/sqs/"
                    "aws4_request, SignedHeaders=host, Signature=s"
                )
            },
        )
        assert resp.status_code == 501
        assert b"SERVICES env var filter" in resp.content
