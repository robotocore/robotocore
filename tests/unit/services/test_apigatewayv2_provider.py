"""Tests for API Gateway V2 provider (HTTP API + WebSocket API CRUD)."""

import json

import pytest
from starlette.testclient import TestClient

from robotocore.services.apigatewayv2.provider import (
    _apis,
    _authorizers,
    _connections,
    _deployments,
    _integrations,
    _routes,
    _stages,
    create_connection,
    delete_connection,
    get_connection,
    handle_apigatewayv2_request,
    list_connections,
    post_to_connection,
)

REGION = "us-east-1"
ACCOUNT_ID = "123456789012"


@pytest.fixture(autouse=True)
def _clear_stores():
    """Clear all stores between tests."""
    for store in (_apis, _routes, _integrations, _stages,
                  _authorizers, _deployments, _connections):
        store.clear()
    yield
    for store in (_apis, _routes, _integrations, _stages,
                  _authorizers, _deployments, _connections):
        store.clear()


def _make_request(method, path, body=None):
    """Helper to build a mock Starlette request."""
    from starlette.applications import Starlette
    from starlette.routing import Route

    async def handler(request):
        return await handle_apigatewayv2_request(request, REGION, ACCOUNT_ID)

    app = Starlette(routes=[Route("/{path:path}", handler, methods=[
        "GET", "POST", "PUT", "PATCH", "DELETE"
    ])])
    client = TestClient(app)
    fn = getattr(client, method.lower())
    kwargs = {}
    if body and method.upper() != "GET":
        kwargs["data"] = json.dumps(body).encode()
    return fn(path, **kwargs)


# ---------------------------------------------------------------------------
# API CRUD
# ---------------------------------------------------------------------------


class TestApiCrud:
    def test_create_http_api(self):
        resp = _make_request("POST", "/v2/apis", {
            "Name": "my-api",
            "ProtocolType": "HTTP",
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["name"] == "my-api"
        assert data["protocolType"] == "HTTP"
        assert "apiId" in data
        assert "apiEndpoint" in data

    def test_create_websocket_api(self):
        resp = _make_request("POST", "/v2/apis", {
            "Name": "ws-api",
            "ProtocolType": "WEBSOCKET",
            "RouteSelectionExpression": "$request.body.action",
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["protocolType"] == "WEBSOCKET"
        assert "$request.body.action" in data["routeSelectionExpression"]

    def test_get_api(self):
        create_resp = _make_request("POST", "/v2/apis", {
            "Name": "test", "ProtocolType": "HTTP"
        })
        api_id = create_resp.json()["apiId"]

        resp = _make_request("GET", f"/v2/apis/{api_id}")
        assert resp.status_code == 200
        assert resp.json()["name"] == "test"

    def test_get_api_not_found(self):
        resp = _make_request("GET", "/v2/apis/nonexistent")
        assert resp.status_code == 404

    def test_list_apis(self):
        _make_request("POST", "/v2/apis", {
            "Name": "a1", "ProtocolType": "HTTP"
        })
        _make_request("POST", "/v2/apis", {
            "Name": "a2", "ProtocolType": "HTTP"
        })

        resp = _make_request("GET", "/v2/apis")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 2

    def test_update_api(self):
        create_resp = _make_request("POST", "/v2/apis", {
            "Name": "original", "ProtocolType": "HTTP"
        })
        api_id = create_resp.json()["apiId"]

        resp = _make_request("PATCH", f"/v2/apis/{api_id}", {
            "Name": "updated",
            "Description": "new desc",
        })
        assert resp.status_code == 200
        assert resp.json()["name"] == "updated"
        assert resp.json()["description"] == "new desc"

    def test_delete_api(self):
        create_resp = _make_request("POST", "/v2/apis", {
            "Name": "to-delete", "ProtocolType": "HTTP"
        })
        api_id = create_resp.json()["apiId"]

        resp = _make_request("DELETE", f"/v2/apis/{api_id}")
        assert resp.status_code == 204

        resp = _make_request("GET", f"/v2/apis/{api_id}")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Route CRUD
# ---------------------------------------------------------------------------


class TestRouteCrud:
    def _create_api(self):
        resp = _make_request("POST", "/v2/apis", {
            "Name": "test", "ProtocolType": "HTTP"
        })
        return resp.json()["apiId"]

    def test_create_route(self):
        api_id = self._create_api()
        resp = _make_request("POST", f"/v2/apis/{api_id}/routes", {
            "RouteKey": "GET /pets",
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["routeKey"] == "GET /pets"
        assert "routeId" in data

    def test_create_default_route(self):
        api_id = self._create_api()
        resp = _make_request("POST", f"/v2/apis/{api_id}/routes", {
            "RouteKey": "$default",
        })
        assert resp.status_code == 201

    def test_create_websocket_routes(self):
        resp = _make_request("POST", "/v2/apis", {
            "Name": "ws", "ProtocolType": "WEBSOCKET",
            "RouteSelectionExpression": "$request.body.action",
        })
        api_id = resp.json()["apiId"]

        for key in ("$connect", "$disconnect", "$default"):
            r = _make_request("POST", f"/v2/apis/{api_id}/routes", {
                "RouteKey": key,
            })
            assert r.status_code == 201

    def test_get_route(self):
        api_id = self._create_api()
        create_resp = _make_request("POST", f"/v2/apis/{api_id}/routes", {
            "RouteKey": "GET /items",
        })
        route_id = create_resp.json()["routeId"]

        resp = _make_request("GET", f"/v2/apis/{api_id}/routes/{route_id}")
        assert resp.status_code == 200
        assert resp.json()["routeKey"] == "GET /items"

    def test_list_routes(self):
        api_id = self._create_api()
        _make_request("POST", f"/v2/apis/{api_id}/routes", {
            "RouteKey": "GET /a"
        })
        _make_request("POST", f"/v2/apis/{api_id}/routes", {
            "RouteKey": "POST /b"
        })

        resp = _make_request("GET", f"/v2/apis/{api_id}/routes")
        assert len(resp.json()["items"]) == 2

    def test_delete_route(self):
        api_id = self._create_api()
        create_resp = _make_request("POST", f"/v2/apis/{api_id}/routes", {
            "RouteKey": "GET /x"
        })
        route_id = create_resp.json()["routeId"]

        resp = _make_request("DELETE", f"/v2/apis/{api_id}/routes/{route_id}")
        assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Integration CRUD
# ---------------------------------------------------------------------------


class TestIntegrationCrud:
    def _create_api(self):
        resp = _make_request("POST", "/v2/apis", {
            "Name": "test", "ProtocolType": "HTTP"
        })
        return resp.json()["apiId"]

    def test_create_integration(self):
        api_id = self._create_api()
        resp = _make_request("POST", f"/v2/apis/{api_id}/integrations", {
            "IntegrationType": "AWS_PROXY",
            "IntegrationUri": "arn:aws:lambda:us-east-1:123:function:my-fn",
            "PayloadFormatVersion": "2.0",
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["integrationType"] == "AWS_PROXY"
        assert "integrationId" in data

    def test_get_integration(self):
        api_id = self._create_api()
        create_resp = _make_request(
            "POST", f"/v2/apis/{api_id}/integrations",
            {
                "IntegrationType": "AWS_PROXY",
                "IntegrationUri": "arn:aws:lambda:us-east-1:123:function:fn",
            },
        )
        integ_id = create_resp.json()["integrationId"]

        resp = _make_request(
            "GET", f"/v2/apis/{api_id}/integrations/{integ_id}"
        )
        assert resp.status_code == 200

    def test_list_integrations(self):
        api_id = self._create_api()
        _make_request("POST", f"/v2/apis/{api_id}/integrations", {
            "IntegrationType": "AWS_PROXY", "IntegrationUri": "a"
        })
        _make_request("POST", f"/v2/apis/{api_id}/integrations", {
            "IntegrationType": "HTTP_PROXY", "IntegrationUri": "b"
        })

        resp = _make_request("GET", f"/v2/apis/{api_id}/integrations")
        assert len(resp.json()["items"]) == 2

    def test_delete_integration(self):
        api_id = self._create_api()
        create_resp = _make_request(
            "POST", f"/v2/apis/{api_id}/integrations",
            {"IntegrationType": "AWS_PROXY", "IntegrationUri": "x"},
        )
        integ_id = create_resp.json()["integrationId"]

        resp = _make_request(
            "DELETE", f"/v2/apis/{api_id}/integrations/{integ_id}"
        )
        assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Stage CRUD
# ---------------------------------------------------------------------------


class TestStageCrud:
    def _create_api(self):
        resp = _make_request("POST", "/v2/apis", {
            "Name": "test", "ProtocolType": "HTTP"
        })
        return resp.json()["apiId"]

    def test_create_stage(self):
        api_id = self._create_api()
        resp = _make_request("POST", f"/v2/apis/{api_id}/stages", {
            "StageName": "$default",
            "AutoDeploy": True,
        })
        assert resp.status_code == 201
        assert resp.json()["stageName"] == "$default"
        assert resp.json()["autoDeploy"] is True

    def test_create_stage_with_variables(self):
        api_id = self._create_api()
        resp = _make_request("POST", f"/v2/apis/{api_id}/stages", {
            "StageName": "prod",
            "StageVariables": {"env": "production"},
        })
        assert resp.status_code == 201
        assert resp.json()["stageVariables"]["env"] == "production"

    def test_duplicate_stage(self):
        api_id = self._create_api()
        _make_request("POST", f"/v2/apis/{api_id}/stages", {
            "StageName": "dev"
        })
        resp = _make_request("POST", f"/v2/apis/{api_id}/stages", {
            "StageName": "dev"
        })
        assert resp.status_code == 409

    def test_get_stage(self):
        api_id = self._create_api()
        _make_request("POST", f"/v2/apis/{api_id}/stages", {
            "StageName": "test"
        })
        resp = _make_request("GET", f"/v2/apis/{api_id}/stages/test")
        assert resp.status_code == 200

    def test_list_stages(self):
        api_id = self._create_api()
        _make_request("POST", f"/v2/apis/{api_id}/stages", {
            "StageName": "s1"
        })
        _make_request("POST", f"/v2/apis/{api_id}/stages", {
            "StageName": "s2"
        })
        resp = _make_request("GET", f"/v2/apis/{api_id}/stages")
        assert len(resp.json()["items"]) == 2

    def test_delete_stage(self):
        api_id = self._create_api()
        _make_request("POST", f"/v2/apis/{api_id}/stages", {
            "StageName": "temp"
        })
        resp = _make_request(
            "DELETE", f"/v2/apis/{api_id}/stages/temp"
        )
        assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Authorizer CRUD
# ---------------------------------------------------------------------------


class TestAuthorizerCrud:
    def _create_api(self):
        resp = _make_request("POST", "/v2/apis", {
            "Name": "test", "ProtocolType": "HTTP"
        })
        return resp.json()["apiId"]

    def test_create_jwt_authorizer(self):
        api_id = self._create_api()
        resp = _make_request("POST", f"/v2/apis/{api_id}/authorizers", {
            "AuthorizerType": "JWT",
            "Name": "my-jwt",
            "IdentitySource": "$request.header.Authorization",
            "JwtConfiguration": {
                "Issuer": "https://issuer.example.com",
                "Audience": ["my-app"],
            },
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["authorizerType"] == "JWT"
        assert data["name"] == "my-jwt"
        assert data["jwtConfiguration"]["issuer"] == "https://issuer.example.com"

    def test_get_authorizer(self):
        api_id = self._create_api()
        create_resp = _make_request(
            "POST", f"/v2/apis/{api_id}/authorizers",
            {"AuthorizerType": "JWT", "Name": "a1"},
        )
        auth_id = create_resp.json()["authorizerId"]

        resp = _make_request(
            "GET", f"/v2/apis/{api_id}/authorizers/{auth_id}"
        )
        assert resp.status_code == 200

    def test_list_authorizers(self):
        api_id = self._create_api()
        _make_request("POST", f"/v2/apis/{api_id}/authorizers", {
            "AuthorizerType": "JWT", "Name": "a1"
        })
        _make_request("POST", f"/v2/apis/{api_id}/authorizers", {
            "AuthorizerType": "JWT", "Name": "a2"
        })
        resp = _make_request("GET", f"/v2/apis/{api_id}/authorizers")
        assert len(resp.json()["items"]) == 2

    def test_delete_authorizer(self):
        api_id = self._create_api()
        create_resp = _make_request(
            "POST", f"/v2/apis/{api_id}/authorizers",
            {"AuthorizerType": "JWT", "Name": "temp"},
        )
        auth_id = create_resp.json()["authorizerId"]
        resp = _make_request(
            "DELETE", f"/v2/apis/{api_id}/authorizers/{auth_id}"
        )
        assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Deployment CRUD
# ---------------------------------------------------------------------------


class TestDeploymentCrud:
    def _create_api(self):
        resp = _make_request("POST", "/v2/apis", {
            "Name": "test", "ProtocolType": "HTTP"
        })
        return resp.json()["apiId"]

    def test_create_deployment(self):
        api_id = self._create_api()
        resp = _make_request("POST", f"/v2/apis/{api_id}/deployments", {
            "Description": "initial deploy",
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["deploymentStatus"] == "DEPLOYED"
        assert "deploymentId" in data

    def test_get_deployment(self):
        api_id = self._create_api()
        create_resp = _make_request(
            "POST", f"/v2/apis/{api_id}/deployments", {}
        )
        deploy_id = create_resp.json()["deploymentId"]

        resp = _make_request(
            "GET", f"/v2/apis/{api_id}/deployments/{deploy_id}"
        )
        assert resp.status_code == 200

    def test_list_deployments(self):
        api_id = self._create_api()
        _make_request("POST", f"/v2/apis/{api_id}/deployments", {})
        _make_request("POST", f"/v2/apis/{api_id}/deployments", {})

        resp = _make_request("GET", f"/v2/apis/{api_id}/deployments")
        assert len(resp.json()["items"]) == 2


# ---------------------------------------------------------------------------
# Auto-deploy
# ---------------------------------------------------------------------------


class TestAutoDeploy:
    def test_auto_deploy_creates_deployment(self):
        api_resp = _make_request("POST", "/v2/apis", {
            "Name": "auto", "ProtocolType": "HTTP"
        })
        api_id = api_resp.json()["apiId"]

        # Create stage with auto-deploy
        _make_request("POST", f"/v2/apis/{api_id}/stages", {
            "StageName": "$default",
            "AutoDeploy": True,
        })

        # Creating a route should trigger auto-deploy
        _make_request("POST", f"/v2/apis/{api_id}/routes", {
            "RouteKey": "GET /test"
        })

        # Check deployments
        resp = _make_request("GET", f"/v2/apis/{api_id}/deployments")
        items = resp.json()["items"]
        assert len(items) >= 1
        assert any(d.get("autoDeployed") for d in items)


# ---------------------------------------------------------------------------
# WebSocket connection management
# ---------------------------------------------------------------------------


class TestWebSocketConnections:
    def test_create_connection(self):
        conn_id = create_connection("api-1")
        assert conn_id is not None
        assert len(conn_id) > 0

    def test_get_connection(self):
        conn_id = create_connection("api-1")
        conn = get_connection("api-1", conn_id)
        assert conn is not None
        assert conn["connectionId"] == conn_id
        assert "connectedAt" in conn

    def test_delete_connection(self):
        conn_id = create_connection("api-1")
        assert delete_connection("api-1", conn_id) is True
        assert get_connection("api-1", conn_id) is None

    def test_delete_nonexistent(self):
        assert delete_connection("api-1", "nonexistent") is False

    def test_list_connections(self):
        create_connection("api-1", "conn-1")
        create_connection("api-1", "conn-2")
        conns = list_connections("api-1")
        assert len(conns) == 2

    def test_post_to_connection(self):
        conn_id = create_connection("api-1")
        assert post_to_connection("api-1", conn_id, b"hello") is True
        conn = get_connection("api-1", conn_id)
        assert conn["lastMessage"] == "hello"

    def test_post_to_nonexistent(self):
        assert post_to_connection("api-1", "gone", b"hi") is False


# ---------------------------------------------------------------------------
# Tags endpoint
# ---------------------------------------------------------------------------


class TestTags:
    def test_get_tags(self):
        resp = _make_request("GET", "/v2/tags/some-arn")
        assert resp.status_code == 200

    def test_post_tags(self):
        resp = _make_request("POST", "/v2/tags/some-arn", {"Tags": {"k": "v"}})
        assert resp.status_code == 200

    def test_delete_tags(self):
        resp = _make_request("DELETE", "/v2/tags/some-arn")
        assert resp.status_code == 204


# ---------------------------------------------------------------------------
# Unknown path
# ---------------------------------------------------------------------------


class TestUnknownPath:
    def test_unknown_path(self):
        resp = _make_request("GET", "/v2/unknown")
        assert resp.status_code == 404
