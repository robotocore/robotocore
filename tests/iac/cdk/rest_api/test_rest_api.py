"""IaC test: CDK REST API with Lambda + API Gateway."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac

SCENARIO_DIR = Path(__file__).parent
STACK_NAME = "CdkRestApiStack"


class TestRestApi:
    """Deploy a CDK REST API stack and validate all resources."""

    @pytest.fixture(scope="class")
    def deployed(self, cdk_runner):
        """Deploy the CDK stack and tear it down after tests."""
        result = cdk_runner.deploy(SCENARIO_DIR, STACK_NAME)
        assert result.returncode == 0, f"cdk deploy failed:\n{result.stderr}"
        yield
        cdk_runner.destroy(SCENARIO_DIR, STACK_NAME)

    def test_api_created(self, deployed):
        """Verify the REST API exists via the apigateway client."""
        client = make_client("apigateway")
        apis = client.get_rest_apis()
        api_names = [a["name"] for a in apis["items"]]
        assert any("CdkRestApiStack" in name or "api" in name.lower() for name in api_names), (
            f"Expected REST API not found. APIs: {api_names}"
        )

    def test_lambda_created(self, deployed):
        """Verify the Lambda function exists."""
        client = make_client("lambda")
        functions = client.list_functions()
        func_names = [f["FunctionName"] for f in functions["Functions"]]
        assert any("hello" in name.lower() for name in func_names), (
            f"Expected Lambda function not found. Functions: {func_names}"
        )

    def test_stage_deployed(self, deployed):
        """Verify the 'test' stage exists on the REST API."""
        apigw_client = make_client("apigateway")
        apis = apigw_client.get_rest_apis()
        # Find our API
        target_api = None
        for api in apis["items"]:
            if "CdkRestApiStack" in api["name"] or "api" in api["name"].lower():
                target_api = api
                break
        assert target_api is not None, "REST API not found"

        stages = apigw_client.get_stages(restApiId=target_api["id"])
        stage_names = [s["stageName"] for s in stages["item"]]
        assert "test" in stage_names, f"Expected 'test' stage, found: {stage_names}"
