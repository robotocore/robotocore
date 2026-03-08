"""Failing tests for bugs found in API Gateway v1 and v2 providers.

Each test exposes a specific correctness bug. These tests are expected to FAIL
until the underlying code is fixed.
"""

import base64
import json
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

from robotocore.services.apigateway.executor import (
    _build_context_vars,
    _invoke_mock,
    _is_binary_content,
    _substitute_stage_variables,
)
from robotocore.services.apigateway.vtl import VtlContext, evaluate_vtl
from robotocore.services.apigatewayv2.provider import (
    _apis,
    _authorizers,
    _camel_keys,
    _connections,
    _deployments,
    _integrations,
    _pascal_keys,
    _routes,
    _stages,
)

REGION = "us-east-1"
ACCOUNT_ID = "111111111111"


# Note: _clear_v2_stores is needed to isolate v2 provider store state
def _clear_all_stores():
    for store in (_apis, _routes, _integrations, _stages, _authorizers, _deployments, _connections):
        store.clear()


class _V2StoreFixture:
    """Mixin that clears v2 stores before/after each test method."""

    def setup_method(self):
        _clear_all_stores()

    def teardown_method(self):
        _clear_all_stores()


# ---------------------------------------------------------------------------
# Bug 1: VTL $input.params() with no argument should return a merged map of
# all params (path + querystring + header), not an empty dict.
# AWS docs: "$input.params() returns a map of all the request parameters."
# ---------------------------------------------------------------------------


class TestVtlInputParamsNoArg:
    def test_input_params_no_arg_returns_merged_map(self):
        """$input.params() should return all path, query, and header params merged."""
        ctx = VtlContext(
            body="{}",
            headers={"x-custom": "hval"},
            query_params={"q": "search"},
            path_params={"id": "42"},
        )
        result = evaluate_vtl("$input.params()", ctx)
        # AWS returns a map with keys "path", "querystring", "header"
        parsed = json.loads(result)
        assert "path" in parsed or "id" in parsed, (
            "$input.params() should return merged params, got empty dict"
        )


# ---------------------------------------------------------------------------
# Bug 2: v2 _pascal_keys recursively converts ALL dict keys, including user
# data like Tags and StageVariables whose keys must be preserved verbatim.
# ---------------------------------------------------------------------------


class TestV2PascalKeysCorruptsUserData(_V2StoreFixture):
    def test_pascal_keys_corrupts_tag_keys(self):
        """Tag keys like 'myTag' should NOT be PascalCase-converted to 'MyTag'."""
        input_data = {"tags": {"myTag": "value", "another-tag": "value2"}}
        result = _pascal_keys(input_data)
        tags = result.get("Tags", {})
        assert "myTag" in tags, (
            f"Tag key 'myTag' was corrupted to '{list(tags.keys())}' by _pascal_keys"
        )

    def test_pascal_keys_corrupts_stage_variables(self):
        """Stage variable keys like 'my_var' should NOT be PascalCase-converted."""
        input_data = {"stageVariables": {"my_var": "val", "apiUrl": "http://example.com"}}
        result = _pascal_keys(input_data)
        stage_vars = result.get("StageVariables", {})
        assert "my_var" in stage_vars, (
            f"Stage variable key 'my_var' was corrupted to '{list(stage_vars.keys())}'"
        )

    def test_camel_keys_corrupts_tag_keys(self):
        """Tag keys stored PascalCase should NOT be lowered by _camel_keys on output."""
        input_data = {"Tags": {"MyCustomTag": "value"}}
        result = _camel_keys(input_data)
        tags = result.get("tags", {})
        assert "MyCustomTag" in tags, (
            f"Tag key 'MyCustomTag' was corrupted to '{list(tags.keys())}' by _camel_keys"
        )


# ---------------------------------------------------------------------------
# Bug 3: _build_context_vars uses resource.path_part (e.g., "{id}") for
# resourcePath instead of the full resource path (e.g., "/users/{id}").
# AWS $context.resourcePath is always the full path like "/users/{userId}".
# ---------------------------------------------------------------------------


class TestBuildContextVarsResourcePath:
    def test_resource_path_is_full_path_not_path_part(self):
        """$context.resourcePath should be the full path, not just the leaf segment."""
        user_id = SimpleNamespace(path_part="{id}", parent_id="users_res")

        context = _build_context_vars(
            api_id="api123",
            stage="prod",
            method="GET",
            path="/users/42",
            resource=user_id,
            request_id="req-1",
            account_id="123456789012",
            headers={},
        )
        # resourcePath should be "/users/{id}", not just "{id}"
        assert context["resourcePath"] == "/users/{id}", (
            f"Expected '/users/{{id}}' but got '{context['resourcePath']}'"
        )


# ---------------------------------------------------------------------------
# Bug 4: _invoke_mock always returns status 200 regardless of which
# integration response matched. If only a "500" response is configured,
# it should return 500, not 200.
# ---------------------------------------------------------------------------


class TestInvokeMockStatusCode:
    def test_mock_returns_non_200_status_from_integration_response(self):
        """Mock integration should use the matched response's status code."""
        resp_500 = SimpleNamespace(
            response_templates={"application/json": '{"error": "bad"}'},
            response_parameters={},
        )
        integration = SimpleNamespace(
            integration_responses={"500": resp_500},
            request_templates=None,
        )
        method_obj = MagicMock()

        status, _, _ = _invoke_mock(integration, method_obj)
        # With only a "500" response configured, the status should be 500
        assert status == 500, f"Expected 500 but got {status}"


# ---------------------------------------------------------------------------
# Bug 5: _is_binary_content doesn't check the REST API's binaryMediaTypes
# list. AWS API Gateway uses the API-level binaryMediaTypes to determine
# whether content should be base64-encoded, not just the integration's
# content_handling setting.
# ---------------------------------------------------------------------------


class TestBinaryMediaTypeFromRestApi:
    def test_binary_media_type_from_rest_api_config(self):
        """Should check binaryMediaTypes on the REST API, not just integration."""
        integration = SimpleNamespace(content_handling=None)
        headers_custom = {"content-type": "application/protobuf"}

        result = _is_binary_content(headers_custom, integration)
        # Current code returns False because "application/protobuf" isn't in the
        # hardcoded list and content_handling isn't CONVERT_TO_BINARY.
        # The function doesn't accept/check a rest_api.binaryMediaTypes at all.
        assert result is True, (
            "Should treat application/protobuf as binary when listed in binaryMediaTypes"
        )


# ---------------------------------------------------------------------------
# Bug 6: Stage variable substitution only handles ${stageVariables.key}
# (dot notation) but not ${stageVariables['key']} (bracket notation) which
# AWS also supports.
# ---------------------------------------------------------------------------


class TestStageVariableBracketNotation:
    def test_bracket_notation_substitution(self):
        """${stageVariables['key']} should also be substituted."""
        stage_vars = {"backend": "api.example.com"}
        uri = "https://${stageVariables['backend']}/path"
        result = _substitute_stage_variables(uri, stage_vars)
        assert result == "https://api.example.com/path", (
            f"Bracket notation not substituted, got: {result}"
        )


# ---------------------------------------------------------------------------
# Bug 7: Cognito authorizer is too lenient -- when provider_arns are
# configured and the token's issuer doesn't match any pool, it should
# reject the token, not silently allow it.
# ---------------------------------------------------------------------------


class TestCognitoAuthorizerLenient:
    def test_cognito_rejects_non_matching_issuer(self):
        """Cognito authorizer should reject tokens with non-matching issuer."""
        from robotocore.services.apigateway.executor import _check_cognito_authorizer

        # Create a JWT with a non-matching issuer
        payload = {
            "sub": "user1",
            "iss": "https://wrong-pool.auth.us-east-1.amazoncognito.com",
            "exp": time.time() + 3600,
        }
        header = (
            base64.urlsafe_b64encode(json.dumps({"alg": "RS256"}).encode()).decode().rstrip("=")
        )
        body = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
        sig = base64.urlsafe_b64encode(b"sig").decode().rstrip("=")
        token = f"{header}.{body}.{sig}"

        authorizer = SimpleNamespace(
            identity_source="method.request.header.Authorization",
            provider_arns=[
                "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_CorrectPool"
            ],
        )
        headers = {"authorization": f"Bearer {token}"}

        result = _check_cognito_authorizer(authorizer, headers, "req-1")
        # Should reject because the issuer doesn't contain "us-east-1_CorrectPool"
        assert result is not None, "Should reject token with non-matching issuer"
        assert result[0] == 401, f"Expected 401 but got {result[0]}"


# ---------------------------------------------------------------------------
# Bug 8: v1 executor -- _invoke_mock ignores integration responses keyed
# by regex selection pattern. In AWS, integration responses can use regex
# patterns to match on the Lambda error output or HTTP status code.
# When statusCode is not "200" in the key, the mock should still check
# for responses matching the actual mock status.
# ---------------------------------------------------------------------------


class TestInvokeMockResponseSelection:
    def test_mock_with_only_default_response_pattern(self):
        """Mock with a default (empty regex) response should use that response."""
        # AWS uses empty string "" as the default selection pattern
        default_resp = SimpleNamespace(
            response_templates={"application/json": '{"default": true}'},
            response_parameters={},
        )
        integration = SimpleNamespace(
            integration_responses={"default": default_resp},
            request_templates=None,
        )
        method_obj = MagicMock()

        _, _, body = _invoke_mock(integration, method_obj)
        # Currently returns "{}" because it only looks for key "200"
        assert body != "{}", f"Expected response template output, got '{body}'"


# ---------------------------------------------------------------------------
# Bug 9: VTL evaluate_vtl -- #set with a $input.json() expression loses
# the JSON type (returns string) which breaks subsequent template rendering.
# For example, #set($data = $input.json('$.items')) followed by
# #foreach($item in $data) should iterate over the parsed list.
# ---------------------------------------------------------------------------


class TestVtlSetWithJsonPath:
    def test_set_json_path_result_iterable_in_foreach(self):
        """#set($data = $input.json('$.items')) should produce an iterable list."""
        body = json.dumps({"items": ["a", "b", "c"]})
        ctx = VtlContext(body=body)
        template = """#set($data = $input.json('$.items'))
#foreach($item in $data)
$item
#end"""
        result = evaluate_vtl(template, ctx)
        # Should contain a, b, c from the foreach iteration
        assert "a" in result, f"Expected foreach to iterate items, got: {result!r}"
        assert "b" in result, f"Expected foreach to iterate items, got: {result!r}"
        assert "c" in result, f"Expected foreach to iterate items, got: {result!r}"
