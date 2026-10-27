"""SageMaker Runtime compatibility tests."""

import uuid

import botocore.exceptions
import pytest

from tests.compatibility.conftest import make_client


def _uid() -> str:
    return uuid.uuid4().hex[:12]


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

    def test_invoke_endpoint_returns_moto_static_default(self, sagemaker_runtime):
        """Without a configured results queue moto answers static data."""
        name = f"default-{_uid()}"
        resp = sagemaker_runtime.invoke_endpoint(
            EndpointName=name,
            Body=b'{"input": "test"}',
            ContentType="application/json",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["Body"].read() == b"body"
        assert resp["ContentType"] == "content_type"
        assert resp["InvokedProductionVariant"] == "invoked_production_variant"
        assert resp["CustomAttributes"] == "custom_attributes"

    def test_invoke_endpoint_repeatably_returns_the_same_payload(
        self, sagemaker_runtime
    ):
        """The result cache returns the identical response per identity."""
        name = f"cache-{_uid()}"
        kwargs = dict(
            EndpointName=name,
            Body=b'{"input": "test"}',
            ContentType="application/json",
            TargetVariant="prod",
        )
        first = sagemaker_runtime.invoke_endpoint(**kwargs)
        second = sagemaker_runtime.invoke_endpoint(**kwargs)
        assert first["Body"].read() == second["Body"].read()
        assert first["ContentType"] == second["ContentType"]
        assert (
            first["InvokedProductionVariant"] == second["InvokedProductionVariant"]
        )


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

    def test_invoke_endpoint_async_pays_the_s3_object(self, sagemaker_runtime):
        """The async output payload is written to a sagemaker-output bucket."""
        name = f"async-s3-{_uid()}"
        resp = sagemaker_runtime.invoke_endpoint_async(
            EndpointName=name,
            InputLocation="s3://in/data/input.json",
        )
        bucket = resp["OutputLocation"].removeprefix("s3://").split("/")[0]
        s3 = make_client("s3")
        keys = [
            obj["Key"] for obj in s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        ]
        assert keys == ["response.json"]
        assert (
            s3.get_object(Bucket=bucket, Key="response.json")["Body"].read()
            == b'{"default": "response"}'
        )

    def test_invoke_endpoint_async_repeats_input_location_is_deduped(
        self, sagemaker_runtime
    ):
        """Same InputLocation returns the cached locations and a fresh id."""
        name = f"async-dedupe-{_uid()}"
        first = sagemaker_runtime.invoke_endpoint_async(
            EndpointName=name,
            InputLocation="s3://in/data/same.json",
        )
        second = sagemaker_runtime.invoke_endpoint_async(
            EndpointName=name,
            InputLocation="s3://in/data/same.json",
        )
        assert first["OutputLocation"] == second["OutputLocation"]
        assert first["FailureLocation"] == second["FailureLocation"]
        # The InferenceId member is rebuilt per call, not cached.
        assert first["InferenceId"] != second["InferenceId"]

    def test_invoke_endpoint_async_echoes_inference_id(self, sagemaker_runtime):
        """A supplied InferenceId header is echoed in the response body."""
        resp = sagemaker_runtime.invoke_endpoint_async(
            EndpointName=f"async-id-{_uid()}",
            InputLocation="s3://in/data/id.json",
            InferenceId="my-inference-123",
        )
        assert resp["InferenceId"] == "my-inference-123"


class TestSageMakerRuntimeInvokeEndpointWithResponseStream:
    def test_invoke_endpoint_with_response_stream_not_implemented(
        self, sagemaker_runtime
    ):
        """InvokeEndpointWithResponseStream is outside moto's surface.

        The deployed fork's `moto/sagemakerruntime/urls.py` routes only the
        two invocation paths and no handler for the streaming op exists in
        the fork's sagemaker/sagemakerruntime modules, so it receives the
        fork-era 501 NotImplemented.
        """
        with pytest.raises(botocore.exceptions.ClientError) as exc:
            sagemaker_runtime.invoke_endpoint_with_response_stream(
                EndpointName="stream-endpoint",
                Body=b'{"input": "test"}',
                ContentType="application/json",
            )
        err = exc.value.response
        assert err["Error"]["Code"] == "NotImplemented"
        assert "invoke_endpoint_with_response_stream" in err["Error"]["Message"]
