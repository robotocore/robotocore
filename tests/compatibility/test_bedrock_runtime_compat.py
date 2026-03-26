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


class TestBedrockRuntimeConverse:
    def test_converse_returns_message(self, bedrock_runtime):
        """Converse returns a message with assistant role."""
        resp = bedrock_runtime.converse(
            modelId="anthropic.claude-3-haiku-20240307-v1:0",
            messages=[{"role": "user", "content": [{"text": "Hello"}]}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "output" in resp
        assert "message" in resp["output"]
        assert resp["output"]["message"]["role"] == "assistant"
        assert "stopReason" in resp
        assert "usage" in resp
        assert resp["usage"]["inputTokens"] > 0

    def test_converse_stream_returns_stream(self, bedrock_runtime):
        """ConverseStream returns a stream response."""
        resp = bedrock_runtime.converse_stream(
            modelId="anthropic.claude-3-haiku-20240307-v1:0",
            messages=[{"role": "user", "content": [{"text": "Hello"}]}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "stream" in resp


class TestBedrockRuntimeAsyncInvoke:
    def test_start_and_get_async_invoke(self, bedrock_runtime):
        """StartAsyncInvoke returns an invocationArn; GetAsyncInvoke returns its details."""
        start_resp = bedrock_runtime.start_async_invoke(
            modelId="anthropic.claude-3-haiku-20240307-v1:0",
            modelInput={"prompt": "test"},
            outputDataConfig={"s3OutputDataConfig": {"s3Uri": "s3://mybucket/output/"}},
        )
        assert "invocationArn" in start_resp
        inv_arn = start_resp["invocationArn"]
        assert "bedrock" in inv_arn

        get_resp = bedrock_runtime.get_async_invoke(invocationArn=inv_arn)
        assert "status" in get_resp
        assert get_resp["status"] in ("InProgress", "Completed", "Failed")

    def test_list_async_invokes(self, bedrock_runtime):
        """ListAsyncInvokes returns a list of invocations."""
        # Start at least one invocation
        bedrock_runtime.start_async_invoke(
            modelId="anthropic.claude-3-haiku-20240307-v1:0",
            modelInput={"prompt": "test"},
            outputDataConfig={"s3OutputDataConfig": {"s3Uri": "s3://mybucket/output/"}},
        )
        resp = bedrock_runtime.list_async_invokes()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "asyncInvokeSummaries" in resp
        assert len(resp["asyncInvokeSummaries"]) >= 1


class TestBedrockRuntimeApplyGuardrail:
    def test_apply_guardrail_returns_action(self, bedrock_runtime):
        """ApplyGuardrail returns an action field."""
        resp = bedrock_runtime.apply_guardrail(
            guardrailIdentifier="test-guardrail",
            guardrailVersion="DRAFT",
            source="INPUT",
            content=[{"text": {"text": "Hello world"}}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "action" in resp
        assert resp["action"] in ("NONE", "GUARDRAIL_INTERVENED")


class TestBedrockRuntimeInvokeModelWithResponseStream:
    def test_invoke_model_with_response_stream(self, bedrock_runtime):
        """InvokeModelWithResponseStream returns a body with contentType."""
        resp = bedrock_runtime.invoke_model_with_response_stream(
            modelId="amazon.titan-text-lite-v1",
            body=json.dumps({"inputText": "Hello"}),
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "body" in resp
        assert "contentType" in resp
