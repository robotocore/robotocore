"""Integration tests for the app with handler chain wired in."""

import pytest
from starlette.testclient import TestClient

from robotocore.gateway.app import app


@pytest.fixture
def client():
    return TestClient(app)


class TestCORSIntegration:
    def test_options_returns_cors_headers(self, client):
        response = client.options(
            "/",
            headers={
                "Authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=testing/20260305/us-east-1/sts/aws4_request, "
                    "SignedHeaders=host, Signature=abc"
                )
            },
        )
        assert response.status_code == 200
        assert response.headers["Access-Control-Allow-Origin"] == "*"

    def test_normal_request_gets_cors_headers(self, client):
        response = client.post(
            "/",
            data="Action=GetCallerIdentity&Version=2011-06-15",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=testing/20260305/us-east-1/sts/aws4_request, "
                    "SignedHeaders=host, Signature=abc"
                ),
            },
        )
        assert response.status_code == 200
        assert response.headers["Access-Control-Allow-Origin"] == "*"


class TestHandlerChainIntegration:
    def test_sts_flows_through_chain(self, client):
        response = client.post(
            "/",
            data="Action=GetCallerIdentity&Version=2011-06-15",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=testing/20260305/us-east-1/sts/aws4_request, "
                    "SignedHeaders=host, Signature=abc"
                ),
            },
        )
        assert response.status_code == 200
        assert "GetCallerIdentityResult" in response.text

    def test_unknown_service_returns_400(self, client):
        response = client.get("/unknown-path")
        assert response.status_code == 400
