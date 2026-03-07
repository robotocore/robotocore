"""API Gateway v2 (HTTP APIs) compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def apigwv2():
    return make_client("apigatewayv2")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestHttpApiCrud:
    def test_create_api(self, apigwv2):
        name = _unique("test-api")
        resp = apigwv2.create_api(Name=name, ProtocolType="HTTP")
        assert resp["Name"] == name
        assert resp["ProtocolType"] == "HTTP"
        assert "ApiId" in resp
        apigwv2.delete_api(ApiId=resp["ApiId"])

    def test_get_api(self, apigwv2):
        name = _unique("get-api")
        created = apigwv2.create_api(Name=name, ProtocolType="HTTP")
        api_id = created["ApiId"]
        try:
            resp = apigwv2.get_api(ApiId=api_id)
            assert resp["Name"] == name
            assert resp["ApiId"] == api_id
        finally:
            apigwv2.delete_api(ApiId=api_id)

    def test_get_apis(self, apigwv2):
        name = _unique("list-api")
        created = apigwv2.create_api(Name=name, ProtocolType="HTTP")
        api_id = created["ApiId"]
        try:
            resp = apigwv2.get_apis()
            names = [a["Name"] for a in resp["Items"]]
            assert name in names
        finally:
            apigwv2.delete_api(ApiId=api_id)

    def test_update_api(self, apigwv2):
        name = _unique("upd-api")
        created = apigwv2.create_api(Name=name, ProtocolType="HTTP")
        api_id = created["ApiId"]
        try:
            new_name = _unique("updated-api")
            resp = apigwv2.update_api(ApiId=api_id, Name=new_name)
            assert resp["Name"] == new_name
        finally:
            apigwv2.delete_api(ApiId=api_id)

    def test_delete_api(self, apigwv2):
        name = _unique("del-api")
        created = apigwv2.create_api(Name=name, ProtocolType="HTTP")
        api_id = created["ApiId"]
        apigwv2.delete_api(ApiId=api_id)
        resp = apigwv2.get_apis()
        api_ids = [a["ApiId"] for a in resp["Items"]]
        assert api_id not in api_ids


class TestRoutes:
    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("route-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_create_route(self, apigwv2, api):
        resp = apigwv2.create_route(ApiId=api, RouteKey="GET /test")
        assert resp["RouteKey"] == "GET /test"
        assert "RouteId" in resp

    def test_get_route(self, apigwv2, api):
        created = apigwv2.create_route(ApiId=api, RouteKey="POST /items")
        resp = apigwv2.get_route(ApiId=api, RouteId=created["RouteId"])
        assert resp["RouteKey"] == "POST /items"

    def test_get_routes(self, apigwv2, api):
        apigwv2.create_route(ApiId=api, RouteKey="GET /list")
        resp = apigwv2.get_routes(ApiId=api)
        keys = [r["RouteKey"] for r in resp["Items"]]
        assert "GET /list" in keys

    def test_delete_route(self, apigwv2, api):
        created = apigwv2.create_route(ApiId=api, RouteKey="DELETE /item")
        apigwv2.delete_route(ApiId=api, RouteId=created["RouteId"])
        resp = apigwv2.get_routes(ApiId=api)
        route_ids = [r["RouteId"] for r in resp["Items"]]
        assert created["RouteId"] not in route_ids


class TestStages:
    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("stage-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_create_stage(self, apigwv2, api):
        resp = apigwv2.create_stage(ApiId=api, StageName="dev")
        assert resp["StageName"] == "dev"

    def test_get_stage(self, apigwv2, api):
        apigwv2.create_stage(ApiId=api, StageName="staging")
        resp = apigwv2.get_stage(ApiId=api, StageName="staging")
        assert resp["StageName"] == "staging"

    def test_get_stages(self, apigwv2, api):
        apigwv2.create_stage(ApiId=api, StageName="prod")
        resp = apigwv2.get_stages(ApiId=api)
        names = [s["StageName"] for s in resp["Items"]]
        assert "prod" in names

    def test_delete_stage(self, apigwv2, api):
        apigwv2.create_stage(ApiId=api, StageName="temp")
        apigwv2.delete_stage(ApiId=api, StageName="temp")
        resp = apigwv2.get_stages(ApiId=api)
        names = [s["StageName"] for s in resp["Items"]]
        assert "temp" not in names


class TestIntegrations:
    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("int-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_create_integration(self, apigwv2, api):
        resp = apigwv2.create_integration(
            ApiId=api,
            IntegrationType="AWS_PROXY",
            IntegrationUri="arn:aws:lambda:us-east-1:123456789012:function:my-fn",
            PayloadFormatVersion="2.0",
        )
        assert resp["IntegrationType"] == "AWS_PROXY"
        assert "IntegrationId" in resp

    def test_get_integration(self, apigwv2, api):
        created = apigwv2.create_integration(
            ApiId=api,
            IntegrationType="HTTP_PROXY",
            IntegrationMethod="GET",
            IntegrationUri="https://example.com",
            PayloadFormatVersion="1.0",
        )
        resp = apigwv2.get_integration(ApiId=api, IntegrationId=created["IntegrationId"])
        assert resp["IntegrationType"] == "HTTP_PROXY"

    def test_get_integrations(self, apigwv2, api):
        apigwv2.create_integration(
            ApiId=api,
            IntegrationType="AWS_PROXY",
            IntegrationUri="arn:aws:lambda:us-east-1:123456789012:function:fn2",
            PayloadFormatVersion="2.0",
        )
        resp = apigwv2.get_integrations(ApiId=api)
        assert len(resp["Items"]) >= 1
