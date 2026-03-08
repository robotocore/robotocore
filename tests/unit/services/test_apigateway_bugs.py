"""Tests for correctness bugs found and fixed in API Gateway v1 provider.

Each test documents a specific bug that has been fixed. Do NOT remove these tests.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from robotocore.services.apigateway.executor import (
    _invoke_mock,
    _substitute_stage_variables,
)

# ===========================================================================
# Bug 1: _invoke_mock always returns status 200 regardless of which
# integration response matched. If only a "500" response is configured,
# it should return 500, not 200.
# ===========================================================================


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
        assert status == 500, f"Expected 500 but got {status}"


# ===========================================================================
# Bug 2: Stage variable substitution only handles ${stageVariables.key}
# (dot notation) but not ${stageVariables['key']} (bracket notation) which
# AWS also supports.
# ===========================================================================


class TestStageVariableBracketNotation:
    def test_bracket_notation_substitution(self):
        """${stageVariables['key']} should also be substituted."""
        stage_vars = {"backend": "api.example.com"}
        uri = "https://${stageVariables['backend']}/path"
        result = _substitute_stage_variables(uri, stage_vars)
        assert result == "https://api.example.com/path", (
            f"Bracket notation not substituted, got: {result}"
        )

    def test_dot_notation_still_works(self):
        """${stageVariables.key} should still work after adding bracket support."""
        stage_vars = {"backend": "api.example.com"}
        uri = "https://${stageVariables.backend}/path"
        result = _substitute_stage_variables(uri, stage_vars)
        assert result == "https://api.example.com/path", f"Dot notation broken, got: {result}"


# ===========================================================================
# Bug 3: _invoke_mock only checks for key "200" in integration_responses,
# ignoring "default" and other response keys.
# ===========================================================================


class TestInvokeMockResponseSelection:
    def test_mock_with_only_default_response_pattern(self):
        """Mock with a default response should use that response."""
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
        assert body != "{}", f"Expected response template output, got '{body}'"

    def test_mock_prefers_200_over_default(self):
        """When both '200' and 'default' exist, '200' should be preferred."""
        resp_200 = SimpleNamespace(
            response_templates={"application/json": '{"from": "200"}'},
            response_parameters={},
        )
        resp_default = SimpleNamespace(
            response_templates={"application/json": '{"from": "default"}'},
            response_parameters={},
        )
        integration = SimpleNamespace(
            integration_responses={"200": resp_200, "default": resp_default},
            request_templates=None,
        )
        method_obj = MagicMock()

        status, _, body = _invoke_mock(integration, method_obj)
        assert status == 200
        assert '"200"' in body
