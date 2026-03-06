"""Tests for API Gateway V2 executor (route matching, JWT, Lambda proxy, WebSocket)."""

import base64
import json
import time
from unittest.mock import patch

import pytest

from robotocore.services.apigatewayv2.executor import (
    _evaluate_route_selection,
    _extract_function_name,
    _match_route,
    _path_matches_v2,
    _path_specificity_v2,
    _validate_jwt,
    execute_v2_request,
    execute_websocket_connect,
    execute_websocket_disconnect,
    execute_websocket_message,
)
from robotocore.services.apigatewayv2.provider import (
    _apis,
    _authorizers,
    _connections,
    _deployments,
    _integrations,
    _routes,
    _stages,
)

REGION = "us-east-1"
ACCOUNT_ID = "123456789012"


@pytest.fixture(autouse=True)
def _clear_stores():
    for store in (_apis, _routes, _integrations, _stages,
                  _authorizers, _deployments, _connections):
        store.clear()
    yield
    for store in (_apis, _routes, _integrations, _stages,
                  _authorizers, _deployments, _connections):
        store.clear()


# ---------------------------------------------------------------------------
# Path matching
# ---------------------------------------------------------------------------


class TestPathMatchingV2:
    def test_exact_match(self):
        match, params = _path_matches_v2("/pets", "/pets")
        assert match is True
        assert params == {}

    def test_root_match(self):
        match, params = _path_matches_v2("/", "/")
        assert match is True

    def test_param_match(self):
        match, params = _path_matches_v2("/pets/{id}", "/pets/123")
        assert match is True
        assert params == {"id": "123"}

    def test_multi_param(self):
        match, params = _path_matches_v2(
            "/users/{uid}/posts/{pid}", "/users/1/posts/2"
        )
        assert match is True
        assert params == {"uid": "1", "pid": "2"}

    def test_greedy_param(self):
        match, params = _path_matches_v2("/proxy/{path+}", "/proxy/a/b/c")
        assert match is True
        assert params == {"path": "a/b/c"}

    def test_no_match(self):
        match, params = _path_matches_v2("/pets", "/users")
        assert match is False

    def test_trailing_slash(self):
        match, params = _path_matches_v2("/pets/", "/pets")
        assert match is True


class TestPathSpecificityV2:
    def test_exact_beats_param(self):
        assert _path_specificity_v2("/pets/list") > _path_specificity_v2(
            "/pets/{id}"
        )

    def test_param_beats_greedy(self):
        assert _path_specificity_v2("/{id}") > _path_specificity_v2(
            "/{path+}"
        )


# ---------------------------------------------------------------------------
# Route matching
# ---------------------------------------------------------------------------


class TestRouteMatching:
    def test_exact_route_match(self):
        routes = {
            "r1": {"RouteKey": "GET /pets", "RouteId": "r1"},
            "r2": {"RouteKey": "POST /pets", "RouteId": "r2"},
        }
        route, params = _match_route(routes, "GET /pets", "GET", "/pets", "HTTP")
        assert route["RouteId"] == "r1"

    def test_param_route_match(self):
        routes = {
            "r1": {"RouteKey": "GET /pets/{id}", "RouteId": "r1"},
        }
        route, params = _match_route(
            routes, "GET /pets/123", "GET", "/pets/123", "HTTP"
        )
        assert route is not None
        assert params == {"id": "123"}

    def test_default_fallback(self):
        routes = {
            "r1": {"RouteKey": "GET /specific", "RouteId": "r1"},
            "r2": {"RouteKey": "$default", "RouteId": "r2"},
        }
        route, params = _match_route(
            routes, "GET /unmatched", "GET", "/unmatched", "HTTP"
        )
        assert route["RouteId"] == "r2"

    def test_any_method_fallback(self):
        routes = {
            "r1": {"RouteKey": "ANY /pets", "RouteId": "r1"},
        }
        route, params = _match_route(
            routes, "PATCH /pets", "PATCH", "/pets", "HTTP"
        )
        assert route is not None

    def test_no_match(self):
        routes = {
            "r1": {"RouteKey": "GET /pets", "RouteId": "r1"},
        }
        route, params = _match_route(
            routes, "POST /users", "POST", "/users", "HTTP"
        )
        assert route is None

    def test_specific_beats_param(self):
        routes = {
            "r1": {"RouteKey": "GET /pets/list", "RouteId": "r1"},
            "r2": {"RouteKey": "GET /pets/{id}", "RouteId": "r2"},
        }
        route, _ = _match_route(
            routes, "GET /pets/list", "GET", "/pets/list", "HTTP"
        )
        assert route["RouteId"] == "r1"


# ---------------------------------------------------------------------------
# JWT authorizer
# ---------------------------------------------------------------------------


class TestJwtAuthorizer:
    def _make_jwt(self, payload: dict) -> str:
        header = base64.urlsafe_b64encode(
            json.dumps({"alg": "RS256"}).encode()
        ).decode().rstrip("=")
        body = base64.urlsafe_b64encode(
            json.dumps(payload).encode()
        ).decode().rstrip("=")
        sig = base64.urlsafe_b64encode(b"sig").decode().rstrip("=")
        return f"{header}.{body}.{sig}"

    def test_missing_token(self):
        authorizer = {
            "IdentitySource": "$request.header.Authorization",
            "JwtConfiguration": {},
        }
        result = _validate_jwt(authorizer, {})
        assert result is not None
        assert result[0] == 401

    def test_valid_token(self):
        token = self._make_jwt({
            "sub": "user1",
            "iss": "https://issuer.example.com",
            "aud": ["my-app"],
            "exp": time.time() + 3600,
        })
        authorizer = {
            "IdentitySource": "$request.header.Authorization",
            "JwtConfiguration": {
                "Issuer": "https://issuer.example.com",
                "Audience": ["my-app"],
            },
        }
        result = _validate_jwt(
            authorizer, {"authorization": f"Bearer {token}"}
        )
        assert result is None

    def test_expired_token(self):
        token = self._make_jwt({
            "sub": "user1",
            "exp": time.time() - 100,
        })
        authorizer = {
            "IdentitySource": "$request.header.Authorization",
            "JwtConfiguration": {},
        }
        result = _validate_jwt(
            authorizer, {"authorization": f"Bearer {token}"}
        )
        assert result is not None
        assert result[0] == 401

    def test_wrong_issuer(self):
        token = self._make_jwt({
            "sub": "user1",
            "iss": "https://wrong.com",
            "exp": time.time() + 3600,
        })
        authorizer = {
            "IdentitySource": "$request.header.Authorization",
            "JwtConfiguration": {
                "Issuer": "https://correct.com",
            },
        }
        result = _validate_jwt(
            authorizer, {"authorization": f"Bearer {token}"}
        )
        assert result is not None
        assert result[0] == 401

    def test_wrong_audience(self):
        token = self._make_jwt({
            "sub": "user1",
            "aud": ["other-app"],
            "exp": time.time() + 3600,
        })
        authorizer = {
            "IdentitySource": "$request.header.Authorization",
            "JwtConfiguration": {
                "Audience": ["my-app"],
            },
        }
        result = _validate_jwt(
            authorizer, {"authorization": f"Bearer {token}"}
        )
        assert result is not None
        assert result[0] == 401

    def test_invalid_jwt_structure(self):
        authorizer = {
            "IdentitySource": "$request.header.Authorization",
            "JwtConfiguration": {},
        }
        result = _validate_jwt(
            authorizer, {"authorization": "Bearer not.valid"}
        )
        assert result is not None
        assert result[0] == 401

    def test_custom_identity_source(self):
        token = self._make_jwt({
            "sub": "user1",
            "exp": time.time() + 3600,
        })
        authorizer = {
            "IdentitySource": "$request.header.X-Auth",
            "JwtConfiguration": {},
        }
        result = _validate_jwt(authorizer, {"x-auth": token})
        assert result is None


# ---------------------------------------------------------------------------
# WebSocket route selection
# ---------------------------------------------------------------------------


class TestRouteSelection:
    def test_body_action(self):
        result = _evaluate_route_selection(
            "$request.body.action",
            json.dumps({"action": "sendMessage"}),
        )
        assert result == "sendMessage"

    def test_missing_field(self):
        result = _evaluate_route_selection(
            "$request.body.action",
            json.dumps({"other": "value"}),
        )
        assert result == "$default"

    def test_invalid_json(self):
        result = _evaluate_route_selection(
            "$request.body.action",
            "not json",
        )
        assert result == "$default"

    def test_bytes_input(self):
        result = _evaluate_route_selection(
            "$request.body.action",
            json.dumps({"action": "test"}).encode(),
        )
        assert result == "test"


# ---------------------------------------------------------------------------
# Extract function name
# ---------------------------------------------------------------------------


class TestExtractFunctionName:
    def test_invocation_uri(self):
        uri = (
            "arn:aws:apigateway:us-east-1:lambda:path/"
            "2015-03-31/functions/arn:aws:lambda:us-east-1:123:function:fn/invocations"
        )
        assert _extract_function_name(uri) == "fn"

    def test_direct_arn(self):
        uri = "arn:aws:lambda:us-east-1:123456:function:my-func"
        assert _extract_function_name(uri) == "my-func"

    def test_simple_name(self):
        uri = "my-function"
        assert _extract_function_name(uri) == "my-function"

    def test_empty(self):
        assert _extract_function_name("") is None
        assert _extract_function_name(None) is None


# ---------------------------------------------------------------------------
# V2 payload format
# ---------------------------------------------------------------------------


class TestV2PayloadFormat:
    def _setup_api(self, with_authorizer=False):
        """Set up a V2 API with route and integration in the stores."""
        from robotocore.services.apigatewayv2.provider import (
            _store,
        )

        api_id = "test-api"
        apis = _store(_apis, REGION)
        apis[api_id] = {
            "ApiId": api_id,
            "Name": "test",
            "ProtocolType": "HTTP",
            "RouteSelectionExpression": "${request.method} ${request.path}",
        }

        # Create integration
        integrations = _store(_integrations, REGION, api_id)
        integrations["integ-1"] = {
            "IntegrationId": "integ-1",
            "IntegrationType": "AWS_PROXY",
            "IntegrationUri": (
                "arn:aws:apigateway:us-east-1:lambda:path/"
                "2015-03-31/functions/my-func/invocations"
            ),
            "PayloadFormatVersion": "2.0",
        }

        # Create route
        route_store = _store(_routes, REGION, api_id)
        route = {
            "RouteId": "route-1",
            "RouteKey": "GET /pets",
            "Target": "integrations/integ-1",
            "AuthorizationType": "NONE",
        }
        if with_authorizer:
            route["AuthorizationType"] = "JWT"
            route["AuthorizerId"] = "auth-1"

            authorizers = _store(_authorizers, REGION, api_id)
            authorizers["auth-1"] = {
                "AuthorizerId": "auth-1",
                "AuthorizerType": "JWT",
                "IdentitySource": "$request.header.Authorization",
                "JwtConfiguration": {
                    "Issuer": "https://issuer.com",
                    "Audience": ["my-app"],
                },
            }

        route_store["route-1"] = route

        # Create stage
        stages = _store(_stages, REGION, api_id)
        stages["$default"] = {
            "StageName": "$default",
            "AutoDeploy": False,
            "StageVariables": {},
        }

        return api_id

    def test_v2_lambda_proxy_event_format(self):
        api_id = self._setup_api()

        lambda_response = {
            "statusCode": 200,
            "headers": {"content-type": "application/json"},
            "body": json.dumps({"pets": []}),
        }

        with patch(
            "robotocore.services.apigatewayv2.executor._invoke_lambda",
            return_value=lambda_response,
        ) as mock_invoke:
            status, headers, body = execute_v2_request(
                api_id, "$default", "GET", "/pets", None,
                {"user-agent": "test"}, {"page": "1"},
                REGION, ACCOUNT_ID,
            )

        assert status == 200
        parsed = json.loads(body)
        assert parsed["pets"] == []

        # Verify the event format passed to Lambda
        event = mock_invoke.call_args[0][1]
        assert event["version"] == "2.0"
        assert event["routeKey"] == "GET /pets"
        assert event["rawPath"] == "/pets"
        assert event["rawQueryString"] == "page=1"
        assert event["requestContext"]["http"]["method"] == "GET"
        assert event["requestContext"]["stage"] == "$default"
        assert event["requestContext"]["apiId"] == api_id

    def test_v2_api_not_found(self):
        status, _, body = execute_v2_request(
            "nonexistent", "$default", "GET", "/", None,
            {}, {}, REGION, ACCOUNT_ID,
        )
        assert status == 404

    def test_v2_route_not_found(self):
        api_id = self._setup_api()
        status, _, body = execute_v2_request(
            api_id, "$default", "DELETE", "/unmatched", None,
            {}, {}, REGION, ACCOUNT_ID,
        )
        assert status == 404

    def test_v2_lambda_returns_none_502(self):
        api_id = self._setup_api()

        with patch(
            "robotocore.services.apigatewayv2.executor._invoke_lambda",
            return_value=None,
        ):
            status, _, _ = execute_v2_request(
                api_id, "$default", "GET", "/pets", None,
                {}, {}, REGION, ACCOUNT_ID,
            )
        assert status == 502

    def test_v2_base64_response(self):
        api_id = self._setup_api()
        encoded = base64.b64encode(b"binary-data").decode()
        lambda_response = {
            "statusCode": 200,
            "headers": {},
            "body": encoded,
            "isBase64Encoded": True,
        }

        with patch(
            "robotocore.services.apigatewayv2.executor._invoke_lambda",
            return_value=lambda_response,
        ):
            status, _, body = execute_v2_request(
                api_id, "$default", "GET", "/pets", None,
                {}, {}, REGION, ACCOUNT_ID,
            )
        assert status == 200
        assert body == "binary-data"

    def test_v2_with_body(self):
        api_id = self._setup_api()

        # Add POST /pets route
        from robotocore.services.apigatewayv2.provider import _store
        routes = _store(_routes, REGION, api_id)
        routes["route-2"] = {
            "RouteId": "route-2",
            "RouteKey": "POST /pets",
            "Target": "integrations/integ-1",
            "AuthorizationType": "NONE",
        }

        lambda_response = {"statusCode": 201, "body": "created"}

        with patch(
            "robotocore.services.apigatewayv2.executor._invoke_lambda",
            return_value=lambda_response,
        ) as mock_invoke:
            status, _, body = execute_v2_request(
                api_id, "$default", "POST", "/pets",
                b'{"name": "Rex"}',
                {"content-type": "application/json"}, {},
                REGION, ACCOUNT_ID,
            )

        assert status == 201
        event = mock_invoke.call_args[0][1]
        assert event["body"] == '{"name": "Rex"}'

    def test_v2_jwt_authorizer_blocks(self):
        api_id = self._setup_api(with_authorizer=True)

        status, _, body = execute_v2_request(
            api_id, "$default", "GET", "/pets", None,
            {}, {}, REGION, ACCOUNT_ID,
        )
        assert status == 401


# ---------------------------------------------------------------------------
# WebSocket execution
# ---------------------------------------------------------------------------


class TestWebSocketExecution:
    def _setup_ws_api(self):
        from robotocore.services.apigatewayv2.provider import _store

        api_id = "ws-api"
        apis = _store(_apis, REGION)
        apis[api_id] = {
            "ApiId": api_id,
            "Name": "ws",
            "ProtocolType": "WEBSOCKET",
            "RouteSelectionExpression": "$request.body.action",
        }

        integrations = _store(_integrations, REGION, api_id)
        integrations["integ-1"] = {
            "IntegrationId": "integ-1",
            "IntegrationType": "AWS_PROXY",
            "IntegrationUri": "arn:aws:lambda:us-east-1:123:function:ws-fn",
        }

        routes = _store(_routes, REGION, api_id)
        routes["r-connect"] = {
            "RouteId": "r-connect",
            "RouteKey": "$connect",
            "Target": "integrations/integ-1",
        }
        routes["r-disconnect"] = {
            "RouteId": "r-disconnect",
            "RouteKey": "$disconnect",
            "Target": "integrations/integ-1",
        }
        routes["r-default"] = {
            "RouteId": "r-default",
            "RouteKey": "$default",
            "Target": "integrations/integ-1",
        }
        routes["r-sendmsg"] = {
            "RouteId": "r-sendmsg",
            "RouteKey": "sendMessage",
            "Target": "integrations/integ-1",
        }

        return api_id

    def test_websocket_connect(self):
        api_id = self._setup_ws_api()

        lambda_response = {"statusCode": 200}

        with patch(
            "robotocore.services.apigatewayv2.executor._invoke_lambda",
            return_value=lambda_response,
        ) as mock_invoke:
            status, _, _ = execute_websocket_connect(
                api_id, "conn-1", {"header": "val"}, {},
                REGION, ACCOUNT_ID,
            )
        assert status == 200
        event = mock_invoke.call_args[0][1]
        assert event["requestContext"]["routeKey"] == "$connect"
        assert event["requestContext"]["eventType"] == "CONNECT"
        assert event["requestContext"]["connectionId"] == "conn-1"

    def test_websocket_disconnect(self):
        api_id = self._setup_ws_api()

        lambda_response = {"statusCode": 200}

        with patch(
            "robotocore.services.apigatewayv2.executor._invoke_lambda",
            return_value=lambda_response,
        ) as mock_invoke:
            status, _, _ = execute_websocket_disconnect(
                api_id, "conn-1", REGION, ACCOUNT_ID,
            )
        assert status == 200
        event = mock_invoke.call_args[0][1]
        assert event["requestContext"]["routeKey"] == "$disconnect"

    def test_websocket_message_route_selection(self):
        api_id = self._setup_ws_api()

        lambda_response = {"statusCode": 200, "body": "ok"}

        msg = json.dumps({"action": "sendMessage", "data": "hi"})

        with patch(
            "robotocore.services.apigatewayv2.executor._invoke_lambda",
            return_value=lambda_response,
        ) as mock_invoke:
            status, _, _ = execute_websocket_message(
                api_id, "conn-1", msg, REGION, ACCOUNT_ID,
            )
        assert status == 200
        event = mock_invoke.call_args[0][1]
        assert event["requestContext"]["routeKey"] == "sendMessage"

    def test_websocket_message_default_fallback(self):
        api_id = self._setup_ws_api()

        lambda_response = {"statusCode": 200}

        msg = json.dumps({"action": "unknownAction"})

        with patch(
            "robotocore.services.apigatewayv2.executor._invoke_lambda",
            return_value=lambda_response,
        ):
            status, _, _ = execute_websocket_message(
                api_id, "conn-1", msg, REGION, ACCOUNT_ID,
            )
        # Should match "unknownAction" route or fall to $default
        assert status == 200

    def test_websocket_no_connect_route(self):
        """If no $connect route, accept by default."""
        from robotocore.services.apigatewayv2.provider import _store

        api_id = "ws-no-connect"
        apis = _store(_apis, REGION)
        apis[api_id] = {
            "ApiId": api_id,
            "ProtocolType": "WEBSOCKET",
            "RouteSelectionExpression": "$request.body.action",
        }
        # No routes at all
        _store(_routes, REGION, api_id)
        _store(_integrations, REGION, api_id)

        status, _, _ = execute_websocket_connect(
            api_id, "conn-1", {}, {}, REGION, ACCOUNT_ID,
        )
        assert status == 200

    def test_websocket_api_not_found(self):
        status, _, _ = execute_websocket_message(
            "nonexistent", "conn-1", "hi", REGION, ACCOUNT_ID,
        )
        assert status == 404
