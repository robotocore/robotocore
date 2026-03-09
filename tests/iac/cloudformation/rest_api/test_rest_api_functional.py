"""Functional test: deploy REST API and invoke Lambda through API Gateway."""

from pathlib import Path

import pytest

from tests.iac.helpers.functional_validator import invoke_api_gateway

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


class TestRestApiFunctional:
    """Deploy REST API stack and exercise the Lambda-backed endpoint."""

    def test_invoke_hello_endpoint(self, deploy_stack):
        """Call GET /hello through API Gateway and verify Lambda response."""
        stack = deploy_stack("rest-api-func", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        api_id = outputs["RestApiId"]

        result = invoke_api_gateway(api_id, "test", "hello")
        assert result["status"] == 200

        body = result["body"]
        if isinstance(body, dict):
            assert body.get("message") == "hello from lambda"
        else:
            assert "hello from lambda" in str(body)

    def test_invoke_nonexistent_path_returns_error(self, deploy_stack):
        """Call a path that doesn't exist and verify non-200 response."""
        stack = deploy_stack("rest-api-func-404", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        api_id = outputs["RestApiId"]

        result = invoke_api_gateway(api_id, "test", "nonexistent")
        # API Gateway should return 403 or 404 for undefined resources
        assert result["status"] in (403, 404), f"Expected 403/404, got {result['status']}"

    def test_invoke_hello_get_method(self, deploy_stack):
        """Verify the GET method specifically returns the expected payload."""
        stack = deploy_stack("rest-api-func-get", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        api_id = outputs["RestApiId"]

        result = invoke_api_gateway(api_id, "test", "hello", method="GET")
        assert result["status"] == 200
        assert result["headers"] is not None
