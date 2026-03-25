"""Bedrock Runtime compatibility tests."""

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
            "404",
        )
