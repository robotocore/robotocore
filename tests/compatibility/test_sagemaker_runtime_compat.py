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


class TestSageMakerRuntimeInvokeEndpointWithResponseStream:
    def test_invoke_endpoint_with_response_stream_returns_event_stream(self, sagemaker_runtime):
        """InvokeEndpointWithResponseStream returns an EventStream body."""
        resp = sagemaker_runtime.invoke_endpoint_with_response_stream(
            EndpointName="stream-endpoint",
            Body=b'{"input": "test"}',
            ContentType="application/json",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        stream = resp["Body"]
        events = list(stream)
        assert len(events) >= 1
        assert "PayloadPart" in events[0]

    def test_invoke_endpoint_with_response_stream_payload_bytes(self, sagemaker_runtime):
        """InvokeEndpointWithResponseStream PayloadPart contains bytes."""
        resp = sagemaker_runtime.invoke_endpoint_with_response_stream(
            EndpointName="stream-endpoint-2",
            Body=b"test-data",
            ContentType="text/plain",
        )
        stream = resp["Body"]
        events = list(stream)
        assert len(events) >= 1
        payload_part = events[0]["PayloadPart"]
        assert "Bytes" in payload_part
        assert isinstance(payload_part["Bytes"], bytes)
