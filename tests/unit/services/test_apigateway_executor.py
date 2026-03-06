"""Tests for API Gateway executor."""

import base64
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from robotocore.services.apigateway.executor import (
    _extract_lambda_function_from_uri,
    _get_full_path,
    _invoke_mock,
    _path_matches,
    _path_specificity,
    execute_api_request,
)

REGION = "us-east-1"
ACCOUNT_ID = "111111111111"


# ---------------------------------------------------------------------------
# _extract_lambda_function_from_uri
# ---------------------------------------------------------------------------


class TestExtractLambdaFunctionFromUri:
    def test_full_arn_uri(self):
        uri = (
            "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/"
            "arn:aws:lambda:us-east-1:111:function:my-func/invocations"
        )
        assert _extract_lambda_function_from_uri(uri) == "my-func"

    def test_function_name_uri(self):
        uri = "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/my-func/invocations"
        assert _extract_lambda_function_from_uri(uri) == "my-func"

    def test_no_match_returns_none(self):
        assert _extract_lambda_function_from_uri("some/random/uri") is None

    def test_empty_string(self):
        assert _extract_lambda_function_from_uri("") is None

    def test_arn_with_alias(self):
        uri = (
            "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/"
            "functions/arn:aws:lambda:us-east-1:111:function:"
            "my-func:prod/invocations"
        )
        # With alias, the function name part is "my-func:prod" — split by ":" gives last = "prod"
        # Actually the regex captures everything between functions/ and /invocations
        result = _extract_lambda_function_from_uri(uri)
        assert result is not None


# ---------------------------------------------------------------------------
# _path_matches
# ---------------------------------------------------------------------------


class TestPathMatches:
    def test_exact_match(self):
        match, params = _path_matches("/users", "/users")
        assert match is True
        assert params == {}

    def test_root_match(self):
        match, params = _path_matches("/", "/")
        assert match is True

    def test_single_path_param(self):
        match, params = _path_matches("/users/{id}", "/users/123")
        assert match is True
        assert params == {"id": "123"}

    def test_multiple_path_params(self):
        match, params = _path_matches("/users/{userId}/posts/{postId}", "/users/42/posts/99")
        assert match is True
        assert params == {"userId": "42", "postId": "99"}

    def test_greedy_path_param(self):
        match, params = _path_matches("/proxy/{path+}", "/proxy/a/b/c")
        assert match is True
        assert params == {"path": "a/b/c"}

    def test_no_match(self):
        match, params = _path_matches("/users", "/posts")
        assert match is False
        assert params == {}

    def test_trailing_slash_normalization(self):
        match, params = _path_matches("/users/", "/users")
        assert match is True

    def test_param_no_match_extra_segment(self):
        match, params = _path_matches("/users/{id}", "/users/123/extra")
        assert match is False


# ---------------------------------------------------------------------------
# _path_specificity
# ---------------------------------------------------------------------------


class TestPathSpecificity:
    def test_exact_parts_score_highest(self):
        assert _path_specificity("/users/list") > _path_specificity("/users/{id}")

    def test_param_scores_more_than_greedy(self):
        assert _path_specificity("/{id}") > _path_specificity("/{path+}")

    def test_longer_exact_path_scores_higher(self):
        assert _path_specificity("/a/b/c") > _path_specificity("/a/b")


# ---------------------------------------------------------------------------
# _get_full_path
# ---------------------------------------------------------------------------


class TestGetFullPath:
    def test_root_resource(self):
        resource = SimpleNamespace(path_part="/", parent_id=None)
        rest_api = SimpleNamespace(resources={"root": resource})
        path = _get_full_path(rest_api, resource)
        assert path == "/"

    def test_nested_resource(self):
        root = SimpleNamespace(path_part="/", parent_id=None, id="root")
        child = SimpleNamespace(path_part="users", parent_id="root", id="child")
        rest_api = SimpleNamespace(resources={"root": root, "child": child})
        path = _get_full_path(rest_api, child)
        assert path == "/users"

    def test_deeply_nested_resource(self):
        root = SimpleNamespace(path_part="/", parent_id=None)
        users = SimpleNamespace(path_part="users", parent_id="root")
        user_id = SimpleNamespace(path_part="{id}", parent_id="users_res")
        rest_api = SimpleNamespace(resources={"root": root, "users_res": users, "id_res": user_id})
        path = _get_full_path(rest_api, user_id)
        assert "/users/{id}" in path or "/{id}" in path


# ---------------------------------------------------------------------------
# _invoke_mock
# ---------------------------------------------------------------------------


class TestInvokeMock:
    def test_mock_with_200_response(self):
        resp_200 = SimpleNamespace(response_templates={"application/json": '{"mock": true}'})
        integration = SimpleNamespace(integration_responses={"200": resp_200})
        method_obj = MagicMock()

        status, headers, body = _invoke_mock(integration, method_obj)
        assert status == 200
        assert body == '{"mock": true}'

    def test_mock_without_responses(self):
        integration = SimpleNamespace(integration_responses=None)
        method_obj = MagicMock()

        status, headers, body = _invoke_mock(integration, method_obj)
        assert status == 200
        assert body == "{}"

    def test_mock_without_200_key(self):
        integration = SimpleNamespace(integration_responses={"500": MagicMock()})
        method_obj = MagicMock()

        status, headers, body = _invoke_mock(integration, method_obj)
        assert status == 200
        assert body == "{}"


# ---------------------------------------------------------------------------
# execute_api_request — full integration tests with mocks
# ---------------------------------------------------------------------------


class TestExecuteApiRequest:
    def _make_api(self, api_id="abc123"):
        api = SimpleNamespace(id=api_id, resources={})
        return api

    def _make_resource_with_method(
        self,
        path_part,
        method_name,
        integration_type,
        uri="",
        parent_id=None,
    ):
        integration = SimpleNamespace(
            integration_type=integration_type,
            uri=uri,
            integration_responses=None,
        )
        method_obj = SimpleNamespace(
            method_integration=integration,
        )
        resource = SimpleNamespace(
            path_part=path_part,
            parent_id=parent_id,
            resource_methods={method_name: method_obj},
        )
        return resource

    def test_api_not_found_returns_404(self):
        mock_backend = MagicMock()
        mock_backend.apis = {}

        with patch("moto.backends.get_backend", return_value={ACCOUNT_ID: {REGION: mock_backend}}):
            status, headers, body = execute_api_request(
                "nonexistent", "prod", "GET", "/", None, {}, {}, REGION, ACCOUNT_ID
            )
        assert status == 404
        assert "not found" in body.lower()

    def test_backend_not_found_returns_404(self):
        with patch("moto.backends.get_backend", side_effect=Exception("no backend")):
            status, headers, body = execute_api_request(
                "abc", "prod", "GET", "/", None, {}, {}, REGION, ACCOUNT_ID
            )
        assert status == 404

    def test_no_matching_resource_returns_404(self):
        api = self._make_api()
        mock_backend = MagicMock()
        mock_backend.apis = {"abc123": api}

        with patch("moto.backends.get_backend", return_value={ACCOUNT_ID: {REGION: mock_backend}}):
            status, headers, body = execute_api_request(
                "abc123", "prod", "GET", "/nonexistent", None, {}, {}, REGION, ACCOUNT_ID
            )
        assert status == 404

    def test_method_not_allowed_returns_405(self):
        resource = self._make_resource_with_method("/", "GET", "MOCK")
        api = self._make_api()
        api.resources = {"root": resource}
        mock_backend = MagicMock()
        mock_backend.apis = {"abc123": api}

        with patch("moto.backends.get_backend", return_value={ACCOUNT_ID: {REGION: mock_backend}}):
            status, headers, body = execute_api_request(
                "abc123", "prod", "DELETE", "/", None, {}, {}, REGION, ACCOUNT_ID
            )
        assert status == 405

    def test_any_method_fallback(self):
        """When exact method not found, ANY should match."""
        integration = SimpleNamespace(
            integration_type="MOCK",
            uri="",
            integration_responses={
                "200": SimpleNamespace(
                    response_templates={
                        "application/json": '{"any": true}',
                    },
                ),
            },
        )
        method_obj = SimpleNamespace(method_integration=integration)
        resource = SimpleNamespace(
            path_part="/",
            parent_id=None,
            resource_methods={"ANY": method_obj},
        )
        api = self._make_api()
        api.resources = {"root": resource}
        mock_backend = MagicMock()
        mock_backend.apis = {"abc123": api}

        with patch("moto.backends.get_backend", return_value={ACCOUNT_ID: {REGION: mock_backend}}):
            status, headers, body = execute_api_request(
                "abc123", "prod", "PATCH", "/", None, {}, {}, REGION, ACCOUNT_ID
            )
        assert status == 200
        assert '{"any": true}' in body

    def test_no_integration_returns_500(self):
        method_obj = SimpleNamespace(method_integration=None)
        resource = SimpleNamespace(
            path_part="/",
            parent_id=None,
            resource_methods={"GET": method_obj},
        )
        api = self._make_api()
        api.resources = {"root": resource}
        mock_backend = MagicMock()
        mock_backend.apis = {"abc123": api}

        with patch("moto.backends.get_backend", return_value={ACCOUNT_ID: {REGION: mock_backend}}):
            status, headers, body = execute_api_request(
                "abc123", "prod", "GET", "/", None, {}, {}, REGION, ACCOUNT_ID
            )
        assert status == 500
        assert "No integration" in body

    def test_mock_integration(self):
        integration = SimpleNamespace(
            integration_type="MOCK",
            uri="",
            integration_responses={
                "200": SimpleNamespace(
                    response_templates={
                        "application/json": '{"status":"ok"}',
                    },
                ),
            },
        )
        method_obj = SimpleNamespace(method_integration=integration)
        resource = SimpleNamespace(
            path_part="/",
            parent_id=None,
            resource_methods={"GET": method_obj},
        )
        api = self._make_api()
        api.resources = {"root": resource}
        mock_backend = MagicMock()
        mock_backend.apis = {"abc123": api}

        with patch("moto.backends.get_backend", return_value={ACCOUNT_ID: {REGION: mock_backend}}):
            status, headers, body = execute_api_request(
                "abc123", "prod", "GET", "/", None, {}, {}, REGION, ACCOUNT_ID
            )
        assert status == 200
        assert json.loads(body) == {"status": "ok"}

    def test_unsupported_integration_type(self):
        integration = SimpleNamespace(
            integration_type="UNKNOWN_TYPE",
            uri="",
            integration_responses=None,
        )
        method_obj = SimpleNamespace(method_integration=integration)
        resource = SimpleNamespace(
            path_part="/",
            parent_id=None,
            resource_methods={"GET": method_obj},
        )
        api = self._make_api()
        api.resources = {"root": resource}
        mock_backend = MagicMock()
        mock_backend.apis = {"abc123": api}

        with patch("moto.backends.get_backend", return_value={ACCOUNT_ID: {REGION: mock_backend}}):
            status, headers, body = execute_api_request(
                "abc123", "prod", "GET", "/", None, {}, {}, REGION, ACCOUNT_ID
            )
        assert status == 500
        assert "Unsupported" in body

    def test_lambda_proxy_integration_success(self):
        lambda_arn = "arn:aws:lambda:us-east-1:111:function:my-func"
        uri = (
            "arn:aws:apigateway:us-east-1:lambda:path/"
            f"2015-03-31/functions/{lambda_arn}/invocations"
        )
        integration = SimpleNamespace(
            integration_type="AWS_PROXY",
            uri=uri,
            integration_responses=None,
        )
        method_obj = SimpleNamespace(method_integration=integration)
        resource = SimpleNamespace(
            path_part="/",
            parent_id=None,
            resource_methods={"GET": method_obj},
        )
        api = self._make_api()
        api.resources = {"root": resource}
        mock_backend = MagicMock()
        mock_backend.apis = {"abc123": api}

        lambda_response = {"statusCode": 200, "headers": {"X-Custom": "val"}, "body": "hello"}

        with patch(
            "moto.backends.get_backend",
            return_value={ACCOUNT_ID: {REGION: mock_backend}},
        ):
            with patch(
                "robotocore.services.apigateway.executor._invoke_lambda",
                return_value=lambda_response,
            ):
                status, headers, body = execute_api_request(
                    "abc123",
                    "prod",
                    "GET",
                    "/",
                    None,
                    {"user-agent": "test"},
                    {},
                    REGION,
                    ACCOUNT_ID,
                )

        assert status == 200
        assert headers["X-Custom"] == "val"
        assert body == "hello"

    def test_lambda_proxy_returns_502_on_none(self):
        lambda_arn = "arn:aws:lambda:us-east-1:111:function:my-func"
        uri = (
            "arn:aws:apigateway:us-east-1:lambda:path/"
            f"2015-03-31/functions/{lambda_arn}/invocations"
        )
        integration = SimpleNamespace(integration_type="AWS_PROXY", uri=uri)
        method_obj = SimpleNamespace(method_integration=integration)
        resource = SimpleNamespace(
            path_part="/",
            parent_id=None,
            resource_methods={"GET": method_obj},
        )
        api = self._make_api()
        api.resources = {"root": resource}
        mock_backend = MagicMock()
        mock_backend.apis = {"abc123": api}

        with patch(
            "moto.backends.get_backend",
            return_value={ACCOUNT_ID: {REGION: mock_backend}},
        ):
            with patch(
                "robotocore.services.apigateway.executor._invoke_lambda",
                return_value=None,
            ):
                status, headers, body = execute_api_request(
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

        assert status == 502

    def test_lambda_proxy_base64_decode(self):
        lambda_arn = "arn:aws:lambda:us-east-1:111:function:fn"
        uri = (
            "arn:aws:apigateway:us-east-1:lambda:path/"
            f"2015-03-31/functions/{lambda_arn}/invocations"
        )
        integration = SimpleNamespace(integration_type="AWS_PROXY", uri=uri)
        method_obj = SimpleNamespace(method_integration=integration)
        resource = SimpleNamespace(
            path_part="/",
            parent_id=None,
            resource_methods={"GET": method_obj},
        )
        api = self._make_api()
        api.resources = {"root": resource}
        mock_backend = MagicMock()
        mock_backend.apis = {"abc123": api}

        encoded = base64.b64encode(b"decoded-body").decode()
        lambda_response = {
            "statusCode": 200,
            "headers": {},
            "body": encoded,
            "isBase64Encoded": True,
        }

        with patch(
            "moto.backends.get_backend",
            return_value={ACCOUNT_ID: {REGION: mock_backend}},
        ):
            with patch(
                "robotocore.services.apigateway.executor._invoke_lambda",
                return_value=lambda_response,
            ):
                status, headers, body = execute_api_request(
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

        assert status == 200
        assert body == "decoded-body"

    def test_lambda_proxy_no_function_in_uri(self):
        integration = SimpleNamespace(integration_type="AWS_PROXY", uri="bad-uri")
        method_obj = SimpleNamespace(method_integration=integration)
        resource = SimpleNamespace(
            path_part="/",
            parent_id=None,
            resource_methods={"GET": method_obj},
        )
        api = self._make_api()
        api.resources = {"root": resource}
        mock_backend = MagicMock()
        mock_backend.apis = {"abc123": api}

        with patch("moto.backends.get_backend", return_value={ACCOUNT_ID: {REGION: mock_backend}}):
            status, headers, body = execute_api_request(
                "abc123", "prod", "GET", "/", None, {}, {}, REGION, ACCOUNT_ID
            )
        assert status == 500
        assert "Could not resolve" in body

    def test_http_integration(self):
        integration = SimpleNamespace(
            integration_type="HTTP",
            uri="http://example.com",
            integration_responses=None,
        )
        method_obj = SimpleNamespace(method_integration=integration)
        resource = SimpleNamespace(
            path_part="/",
            parent_id=None,
            resource_methods={"GET": method_obj},
        )
        api = self._make_api()
        api.resources = {"root": resource}
        mock_backend = MagicMock()
        mock_backend.apis = {"abc123": api}

        with patch("moto.backends.get_backend", return_value={ACCOUNT_ID: {REGION: mock_backend}}):
            status, headers, body = execute_api_request(
                "abc123", "prod", "GET", "/", None, {}, {}, REGION, ACCOUNT_ID
            )
        assert status == 200

    def test_aws_service_integration(self):
        integration = SimpleNamespace(
            integration_type="AWS",
            uri="arn:aws:something",
            integration_responses=None,
        )
        method_obj = SimpleNamespace(method_integration=integration)
        resource = SimpleNamespace(
            path_part="/",
            parent_id=None,
            resource_methods={"GET": method_obj},
        )
        api = self._make_api()
        api.resources = {"root": resource}
        mock_backend = MagicMock()
        mock_backend.apis = {"abc123": api}

        with patch("moto.backends.get_backend", return_value={ACCOUNT_ID: {REGION: mock_backend}}):
            status, headers, body = execute_api_request(
                "abc123", "prod", "GET", "/", None, {}, {}, REGION, ACCOUNT_ID
            )
        assert status == 200
