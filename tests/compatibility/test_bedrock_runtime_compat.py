"""Bedrock Runtime compatibility tests."""

import json

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def bedrock_runtime():
    return make_client("bedrock-runtime")


class TestBedrockRuntimeGetAsyncInvoke:
    def test_get_async_invoke_nonexistent(self, bedrock_runtime):
        """GetAsyncInvoke raises an error for a nonexistent invocation ARN."""
        with pytest.raises(ClientError) as exc:
            bedrock_runtime.get_async_invoke(
                invocationArn="arn:aws:bedrock:us-east-1:123456789012:async-invoke/fake-id-12345"
            )
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ValidationException",
            "NotImplemented",
            "404",
        )


class TestBedrockRuntimeInvokeModel:
    def test_invoke_model_returns_response(self, bedrock_runtime):
        """InvokeModel returns a streaming body with contentType."""
        resp = bedrock_runtime.invoke_model(
            modelId="amazon.titan-text-lite-v1",
            body=json.dumps({"inputText": "Hello world"}),
            contentType="application/json",
            accept="application/json",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["contentType"] == "application/json"
        body = resp["body"].read()
        assert body is not None
