"""IaC test: pulumi - rest_api.

Deploys an API Gateway REST API backed by a Lambda function and validates
that both resources exist via boto3.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def rest_api_outputs(pulumi_runner, ensure_server, tmp_path_factory):
    """Deploy the rest_api Pulumi program and return stack outputs."""
    import shutil

    src_dir = Path(__file__).parent
    work_dir = tmp_path_factory.mktemp("pulumi-rest-api")

    # Copy Pulumi program files
    for f in src_dir.iterdir():
        if f.name.startswith("test_") or f.name == "__pycache__":
            continue
        if f.is_file():
            shutil.copy2(f, work_dir / f.name)

    # Initialize stack
    init_result = pulumi_runner.run(
        ["pulumi", "stack", "init", "test"],
        work_dir,
        env={"PULUMI_CONFIG_PASSPHRASE": "", "PULUMI_BACKEND_URL": "file://~"},
    )
    if init_result.returncode != 0 and "already exists" not in init_result.stderr:
        pytest.fail(f"pulumi stack init failed:\n{init_result.stderr}")

    result = pulumi_runner.up(work_dir, stack="test")
    if result.returncode != 0:
        pytest.fail(f"pulumi up failed:\n{result.stderr}\n{result.stdout}")

    outputs = pulumi_runner.stack_output(work_dir, stack="test")

    yield outputs

    # Teardown
    pulumi_runner.destroy(work_dir, stack="test")


@pytest.fixture(scope="module")
def apigw_client():
    return make_client("apigateway")


@pytest.fixture(scope="module")
def lambda_client():
    return make_client("lambda")


class TestRestApi:
    """Validate REST API resources created by Pulumi."""

    def test_api_created(self, rest_api_outputs, apigw_client):
        """Verify the REST API exists and has the correct name."""
        api_id = rest_api_outputs["rest_api_id"]
        resp = apigw_client.get_rest_api(restApiId=api_id)
        assert resp["id"] == api_id
        assert resp["name"] == "rest-api"

    def test_lambda_created(self, rest_api_outputs, lambda_client):
        """Verify the Lambda function exists."""
        fn_name = rest_api_outputs["lambda_function_name"]
        resp = lambda_client.get_function(FunctionName=fn_name)
        config = resp["Configuration"]
        assert config["FunctionName"] == fn_name
        assert config["Runtime"] == "python3.12"
        assert config["Handler"] == "index.handler"
