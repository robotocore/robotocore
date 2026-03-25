"""SageMaker Runtime compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def sagemaker_runtime():
    return make_client("sagemaker-runtime")


class TestSageMakerRuntimeInvokeEndpoint:
    def test_invoke_endpoint_returns_body(self, sagemaker_runtime):
        """InvokeEndpoint returns a body and content type."""
        resp = sagemaker_runtime.invoke_endpoint(
            EndpointName="test-endpoint",
            Body=b'{"input": "test"}',
            ContentType="application/json",
        )
        body = resp["Body"].read()
        assert body is not None
        assert isinstance(body, bytes)
        assert "ContentType" in resp

    def test_invoke_endpoint_with_accept(self, sagemaker_runtime):
        """InvokeEndpoint with Accept header returns a response."""
        resp = sagemaker_runtime.invoke_endpoint(
            EndpointName="my-endpoint",
            Body=b"test-data",
            ContentType="text/plain",
            Accept="application/json",
        )
        body = resp["Body"].read()
        assert body is not None


class TestSageMakerRuntimeInvokeEndpointAsync:
    def test_invoke_endpoint_async_returns_output_location(self, sagemaker_runtime):
        """InvokeEndpointAsync returns an S3 output location."""
        resp = sagemaker_runtime.invoke_endpoint_async(
            EndpointName="async-endpoint",
            InputLocation="s3://my-input-bucket/data/input.json",
        )
        assert "InferenceId" in resp
        assert "OutputLocation" in resp
        assert resp["OutputLocation"].startswith("s3://")

    def test_invoke_endpoint_async_returns_failure_location(self, sagemaker_runtime):
        """InvokeEndpointAsync returns both output and failure S3 locations."""
        resp = sagemaker_runtime.invoke_endpoint_async(
            EndpointName="async-endpoint-2",
            InputLocation="s3://my-input-bucket/data/input2.json",
        )
        assert "FailureLocation" in resp
        assert resp["FailureLocation"].startswith("s3://")
