"""Tests for enhanced API Gateway v1 executor features.

Covers: API key validation, Lambda/Cognito authorizers, mock integration
with VTL, gateway responses, stage variables, binary media types,
request body validation, AWS integration.
"""

import base64
import json
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from robotocore.services.apigateway.executor import (
    _build_context_vars,
    _check_api_key,
    _check_authorizer,
    _check_cognito_authorizer,
    _gateway_response,
    _get_stage_variables,
    _invoke_mock,
    _is_binary_content,
    _substitute_stage_variables,
    _validate_json_schema,
    _validate_request_body,
    execute_api_request,
)

REGION = "us-east-1"
ACCOUNT_ID = "111111111111"


# ---------------------------------------------------------------------------
# API Key validation
# ---------------------------------------------------------------------------


class TestApiKeyValidation:
    def test_no_key_required(self):
        method_obj = SimpleNamespace(api_key_required=False)
        result = _check_api_key(None, method_obj, {}, MagicMock(), "req-1")
        assert result is None  # No check needed

    def test_key_required_missing(self):
        method_obj = SimpleNamespace(api_key_required=True)
        status, _, body = _check_api_key(None, method_obj, {}, MagicMock(), "r")
        assert status == 403
        assert "Forbidden" in body

    def test_key_required_invalid(self):
        method_obj = SimpleNamespace(api_key_required=True)
        backend = MagicMock()
        key_obj = SimpleNamespace(value="valid-key", enabled=True)
        backend.keys = {"k1": key_obj}
        status, _, body = _check_api_key(None, method_obj, {"x-api-key": "wrong-key"}, backend, "r")
        assert status == 403

    def test_key_required_valid(self):
        method_obj = SimpleNamespace(api_key_required=True)
        backend = MagicMock()
        key_obj = SimpleNamespace(value="my-api-key", enabled=True)
        backend.keys = {"k1": key_obj}
        result = _check_api_key(None, method_obj, {"x-api-key": "my-api-key"}, backend, "r")
        assert result is None  # Authorized

    def test_key_required_disabled_key(self):
        method_obj = SimpleNamespace(api_key_required=True)
        backend = MagicMock()
        key_obj = SimpleNamespace(value="my-api-key", enabled=False)
        backend.keys = {"k1": key_obj}
        status, _, _ = _check_api_key(None, method_obj, {"x-api-key": "my-api-key"}, backend, "r")
        assert status == 403


# ---------------------------------------------------------------------------
# Cognito authorizer
# ---------------------------------------------------------------------------


class TestCognitoAuthorizer:
    def _make_jwt(self, payload: dict) -> str:
        header = (
            base64.urlsafe_b64encode(json.dumps({"alg": "RS256"}).encode()).decode().rstrip("=")
        )
        body = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
        sig = base64.urlsafe_b64encode(b"signature").decode().rstrip("=")
        return f"{header}.{body}.{sig}"

    def test_missing_token(self):
        auth = SimpleNamespace(
            identity_source="method.request.header.Authorization",
            provider_arns=[],
        )
        status, _, _ = _check_cognito_authorizer(auth, {}, "req-1")
        assert status == 401

    def test_valid_token(self):
        token = self._make_jwt({"sub": "user1", "exp": time.time() + 3600})
        auth = SimpleNamespace(
            identity_source="method.request.header.Authorization",
            provider_arns=[],
        )
        result = _check_cognito_authorizer(auth, {"authorization": f"Bearer {token}"}, "req-1")
        assert result is None  # Authorized

    def test_expired_token(self):
        token = self._make_jwt({"sub": "user1", "exp": time.time() - 100})
        auth = SimpleNamespace(
            identity_source="method.request.header.Authorization",
            provider_arns=[],
        )
        status, _, body = _check_cognito_authorizer(
            auth, {"authorization": f"Bearer {token}"}, "req-1"
        )
        assert status == 401
        assert "expired" in body.lower()

    def test_invalid_jwt_structure(self):
        auth = SimpleNamespace(
            identity_source="method.request.header.Authorization",
            provider_arns=[],
        )
        status, _, _ = _check_cognito_authorizer(
            auth, {"authorization": "Bearer not-a-jwt"}, "req-1"
        )
        assert status == 401

    def test_bearer_prefix_stripped(self):
        token = self._make_jwt({"sub": "user1", "exp": time.time() + 3600})
        auth = SimpleNamespace(
            identity_source="method.request.header.Authorization",
            provider_arns=[],
        )
        result = _check_cognito_authorizer(auth, {"authorization": f"Bearer {token}"}, "req-1")
        assert result is None

    def test_custom_header(self):
        token = self._make_jwt({"sub": "user1", "exp": time.time() + 3600})
        auth = SimpleNamespace(
            identity_source="method.request.header.X-Auth",
            provider_arns=[],
        )
        result = _check_cognito_authorizer(auth, {"x-auth": token}, "req-1")
        assert result is None


# ---------------------------------------------------------------------------
# Lambda authorizer
# ---------------------------------------------------------------------------


class TestLambdaAuthorizer:
    def test_no_authorizer_passes(self):
        method_obj = SimpleNamespace(authorization_type="NONE", authorizer_id=None)
        rest_api = SimpleNamespace(authorizers={})
        result = _check_authorizer(
            rest_api, method_obj, {}, {}, {}, {}, REGION, ACCOUNT_ID, "req-1", {}
        )
        assert result is None

    def test_token_authorizer_missing_token(self):
        authorizer = SimpleNamespace(
            type="TOKEN",
            identity_source="method.request.header.Authorization",
            auth_type="method.request.header.Authorization",
            authorizer_uri=(
                "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/auth-fn/invocations"
            ),
        )
        method_obj = SimpleNamespace(authorization_type="CUSTOM", authorizer_id="auth1")
        rest_api = SimpleNamespace(authorizers={"auth1": authorizer})
        status, _, body = _check_authorizer(
            rest_api, method_obj, {}, {}, {}, {}, REGION, ACCOUNT_ID, "req-1", {}
        )
        assert status == 401

    def test_token_authorizer_allow(self):
        authorizer = SimpleNamespace(
            type="TOKEN",
            identity_source="method.request.header.Authorization",
            auth_type="method.request.header.Authorization",
            authorizer_uri=(
                "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/auth-fn/invocations"
            ),
        )
        method_obj = SimpleNamespace(authorization_type="CUSTOM", authorizer_id="auth1")
        rest_api = SimpleNamespace(authorizers={"auth1": authorizer})

        allow_policy = {"policyDocument": {"Statement": [{"Effect": "Allow", "Resource": "*"}]}}

        with patch(
            "robotocore.services.apigateway.executor._invoke_lambda",
            return_value=allow_policy,
        ):
            result = _check_authorizer(
                rest_api,
                method_obj,
                {"authorization": "my-token"},
                {},
                {},
                {},
                REGION,
                ACCOUNT_ID,
                "req-1",
                {},
            )
        assert result is None  # Authorized

    def test_token_authorizer_deny(self):
        authorizer = SimpleNamespace(
            type="TOKEN",
            auth_type="method.request.header.Authorization",
            authorizer_uri=(
                "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/auth-fn/invocations"
            ),
        )
        method_obj = SimpleNamespace(authorization_type="CUSTOM", authorizer_id="auth1")
        rest_api = SimpleNamespace(authorizers={"auth1": authorizer})

        deny_policy = {"policyDocument": {"Statement": [{"Effect": "Deny", "Resource": "*"}]}}

        with patch(
            "robotocore.services.apigateway.executor._invoke_lambda",
            return_value=deny_policy,
        ):
            status, _, _ = _check_authorizer(
                rest_api,
                method_obj,
                {"authorization": "bad-token"},
                {},
                {},
                {},
                REGION,
                ACCOUNT_ID,
                "req-1",
                {},
            )
        assert status == 403

    def test_request_authorizer(self):
        authorizer = SimpleNamespace(
            type="REQUEST",
            authorizer_uri=(
                "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/auth-fn/invocations"
            ),
        )
        method_obj = SimpleNamespace(authorization_type="CUSTOM", authorizer_id="auth1")
        rest_api = SimpleNamespace(authorizers={"auth1": authorizer})

        allow_policy = {"policyDocument": {"Statement": [{"Effect": "Allow"}]}}

        with patch(
            "robotocore.services.apigateway.executor._invoke_lambda",
            return_value=allow_policy,
        ):
            result = _check_authorizer(
                rest_api,
                method_obj,
                {"authorization": "token"},
                {"key": "val"},
                {"id": "123"},
                {"env": "test"},
                REGION,
                ACCOUNT_ID,
                "req-1",
                {},
            )
        assert result is None


# ---------------------------------------------------------------------------
# Mock integration with VTL
# ---------------------------------------------------------------------------


class TestMockIntegrationEnhanced:
    def test_mock_with_vtl_template(self):
        resp_200 = SimpleNamespace(
            response_templates={"application/json": '{"requestId": "$context.requestId"}'},
            response_parameters={},
        )
        integration = SimpleNamespace(
            integration_responses={"200": resp_200},
            request_templates={},
        )
        method_obj = MagicMock()

        status, headers, body = _invoke_mock(
            integration,
            method_obj,
            body_str='{"test": true}',
            headers={},
            query_params={},
            path_params={},
            stage_vars={},
            context_vars={"requestId": "test-req-id"},
        )
        assert status == 200
        parsed = json.loads(body)
        assert parsed["requestId"] == "test-req-id"

    def test_mock_with_response_parameters(self):
        resp_200 = SimpleNamespace(
            response_templates={"application/json": '{"ok": true}'},
            response_parameters={"method.response.header.X-Custom": "'custom-value'"},
        )
        integration = SimpleNamespace(
            integration_responses={"200": resp_200},
            request_templates={},
        )
        method_obj = MagicMock()

        status, headers, body = _invoke_mock(
            integration,
            method_obj,
            headers={},
            query_params={},
            path_params={},
            stage_vars={},
            context_vars={},
        )
        assert headers.get("X-Custom") == "custom-value"


# ---------------------------------------------------------------------------
# Stage variables
# ---------------------------------------------------------------------------


class TestStageVariables:
    def test_get_stage_variables(self):
        stage = SimpleNamespace(name="prod", variables={"env": "production"})
        rest_api = SimpleNamespace(stages={"prod": stage})
        result = _get_stage_variables(rest_api, "prod")
        assert result == {"env": "production"}

    def test_get_stage_variables_missing(self):
        rest_api = SimpleNamespace(stages={})
        result = _get_stage_variables(rest_api, "nonexistent")
        assert result == {}

    def test_substitute_stage_variables(self):
        uri = "https://${stageVariables.host}/api"
        result = _substitute_stage_variables(uri, {"host": "example.com"})
        assert result == "https://example.com/api"

    def test_substitute_no_variables(self):
        uri = "https://example.com/api"
        result = _substitute_stage_variables(uri, {})
        assert result == uri


# ---------------------------------------------------------------------------
# Build context variables
# ---------------------------------------------------------------------------


class TestBuildContextVars:
    def test_context_vars(self):
        resource = SimpleNamespace(path_part="/users")
        ctx = _build_context_vars(
            "api-1",
            "prod",
            "GET",
            "/users",
            resource,
            "req-123",
            ACCOUNT_ID,
            {"user-agent": "test-agent"},
        )
        assert ctx["apiId"] == "api-1"
        assert ctx["stage"] == "prod"
        assert ctx["httpMethod"] == "GET"
        assert ctx["requestId"] == "req-123"
        assert ctx["accountId"] == ACCOUNT_ID
        assert ctx["path"] == "/prod/users"


# ---------------------------------------------------------------------------
# Gateway responses
# ---------------------------------------------------------------------------


class TestGatewayResponses:
    def test_default_not_found(self):
        rest_api = SimpleNamespace(gateway_responses={})
        status, _, body = _gateway_response(rest_api, "RESOURCE_NOT_FOUND", "req-1")
        assert status == 404

    def test_default_unauthorized(self):
        rest_api = SimpleNamespace(gateway_responses={})
        status, _, body = _gateway_response(rest_api, "UNAUTHORIZED", "req-1")
        assert status == 401

    def test_custom_gateway_response(self):
        custom = SimpleNamespace(
            status_code="418",
            response_templates={"application/json": '{"error": "custom"}'},
            response_parameters={"gatewayresponse.header.X-Error": "'custom-error'"},
        )
        rest_api = SimpleNamespace(gateway_responses={"UNAUTHORIZED": custom})
        status, headers, body = _gateway_response(rest_api, "UNAUTHORIZED", "req-1")
        assert status == 418
        assert headers.get("X-Error") == "custom-error"
        assert "custom" in body

    def test_unknown_type_uses_default_4xx(self):
        rest_api = SimpleNamespace(gateway_responses={})
        status, _, _ = _gateway_response(rest_api, "UNKNOWN_TYPE", "req-1")
        assert status == 400  # DEFAULT_4XX


# ---------------------------------------------------------------------------
# Request body validation
# ---------------------------------------------------------------------------


class TestRequestBodyValidation:
    def test_no_validator(self):
        method_obj = SimpleNamespace(request_validator_id=None)
        rest_api = SimpleNamespace(validators={})
        result = _validate_request_body(rest_api, method_obj, b"{}", {})
        assert result is None

    def test_validate_missing_required_field(self):
        schema = {
            "type": "object",
            "required": ["name"],
            "properties": {"name": {"type": "string"}},
        }
        err = _validate_json_schema({"age": 30}, schema)
        assert err is not None
        assert "name" in err

    def test_validate_wrong_type(self):
        schema = {"type": "string"}
        err = _validate_json_schema(42, schema)
        assert err is not None
        assert "string" in err.lower()

    def test_validate_valid_object(self):
        schema = {
            "type": "object",
            "required": ["name"],
            "properties": {"name": {"type": "string"}},
        }
        err = _validate_json_schema({"name": "Alice"}, schema)
        assert err is None

    def test_validate_array(self):
        schema = {"type": "array"}
        assert _validate_json_schema([], schema) is None
        assert _validate_json_schema({}, schema) is not None

    def test_validate_nested(self):
        schema = {
            "type": "object",
            "properties": {
                "address": {
                    "type": "object",
                    "required": ["city"],
                    "properties": {"city": {"type": "string"}},
                }
            },
        }
        err = _validate_json_schema({"address": {}}, schema)
        assert err is not None
        assert "city" in err


# ---------------------------------------------------------------------------
# Binary media types
# ---------------------------------------------------------------------------


class TestBinaryMediaTypes:
    def test_not_binary_json(self):
        integration = SimpleNamespace(content_handling=None)
        assert not _is_binary_content({"content-type": "application/json"}, integration)

    def test_binary_octet_stream(self):
        integration = SimpleNamespace(content_handling=None)
        assert _is_binary_content({"content-type": "application/octet-stream"}, integration)

    def test_binary_image(self):
        integration = SimpleNamespace(content_handling=None)
        assert _is_binary_content({"content-type": "image/png"}, integration)

    def test_binary_convert_to_binary(self):
        integration = SimpleNamespace(content_handling="CONVERT_TO_BINARY")
        assert _is_binary_content({"content-type": "text/plain"}, integration)

    def test_no_content_type(self):
        integration = SimpleNamespace(content_handling=None)
        assert not _is_binary_content({}, integration)


# ---------------------------------------------------------------------------
# Full execute_api_request with enhanced features
# ---------------------------------------------------------------------------


class TestExecuteApiRequestEnhanced:
    def _make_api_with_key_requirement(self, backend):
        integration = SimpleNamespace(
            integration_type="MOCK",
            uri="",
            integration_responses={
                "200": SimpleNamespace(
                    response_templates={"application/json": '{"status": "ok"}'},
                    response_parameters={},
                )
            },
            request_templates={},
        )
        method_obj = SimpleNamespace(
            method_integration=integration,
            api_key_required=True,
            authorization_type="NONE",
            authorizer_id=None,
            request_validator_id=None,
            request_models={},
        )
        resource = SimpleNamespace(
            path_part="/",
            parent_id=None,
            resource_methods={"GET": method_obj},
        )
        api = SimpleNamespace(
            id="abc123",
            resources={"root": resource},
            stages={},
            authorizers={},
            validators={},
            models={},
            gateway_responses={},
        )
        return api

    def test_api_key_required_forbidden(self):
        mock_backend = MagicMock()
        key_obj = SimpleNamespace(value="valid-key", enabled=True)
        mock_backend.keys = {"k1": key_obj}
        api = self._make_api_with_key_requirement(mock_backend)
        mock_backend.apis = {"abc123": api}

        with patch(
            "moto.backends.get_backend",
            return_value={ACCOUNT_ID: {REGION: mock_backend}},
        ):
            status, _, body = execute_api_request(
                "abc123",
                "prod",
                "GET",
                "/",
                None,
                {},
                {},
                REGION,
                ACCOUNT_ID,
            )
        assert status == 403

    def test_api_key_required_valid(self):
        mock_backend = MagicMock()
        key_obj = SimpleNamespace(value="valid-key", enabled=True)
        mock_backend.keys = {"k1": key_obj}
        api = self._make_api_with_key_requirement(mock_backend)
        mock_backend.apis = {"abc123": api}

        with patch(
            "moto.backends.get_backend",
            return_value={ACCOUNT_ID: {REGION: mock_backend}},
        ):
            status, _, body = execute_api_request(
                "abc123",
                "prod",
                "GET",
                "/",
                None,
                {"x-api-key": "valid-key"},
                {},
                REGION,
                ACCOUNT_ID,
            )
        assert status == 200
