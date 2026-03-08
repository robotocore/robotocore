"""Failing tests for bugs found in ECR native provider.

Each test documents a specific correctness bug. All tests should FAIL
against the current provider implementation.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.ecr.provider import handle_ecr_request


def _make_request(action: str, body: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.headers = {
        "x-amz-target": f"AmazonEC2ContainerRegistry_V20150921.{action}",
    }
    req.method = "POST"
    req.url = MagicMock()
    req.url.path = "/"
    req.query_params = {}
    payload = json.dumps(body or {}).encode()
    req.body = AsyncMock(return_value=payload)
    return req


@pytest.mark.asyncio
class TestBatchCheckLayerAvailabilityBugs:
    """Bug: BatchCheckLayerAvailability response is missing repositoryName.

    AWS includes repositoryName in each layer object in the response.
    The provider only returns layerDigest and layerAvailability.
    """

    async def test_layers_include_repository_name(self):
        """Each layer in the response should include repositoryName."""
        req = _make_request(
            "BatchCheckLayerAvailability",
            {
                "repositoryName": "my-repo",
                "layerDigests": ["sha256:abc123"],
            },
        )
        resp = await handle_ecr_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        layers = body["layers"]
        assert len(layers) == 1

        # AWS includes repositoryName in each layer object
        assert "repositoryName" in layers[0], (
            f"Layer response is missing 'repositoryName'. Got keys: {list(layers[0].keys())}. "
            "AWS includes repositoryName in each layer of the BatchCheckLayerAvailability response."
        )
        assert layers[0]["repositoryName"] == "my-repo"

    async def test_layers_include_registry_id(self):
        """Each layer in the response should include registryId (the account ID)."""
        req = _make_request(
            "BatchCheckLayerAvailability",
            {
                "repositoryName": "my-repo",
                "registryId": "123456789012",
                "layerDigests": ["sha256:abc123"],
            },
        )
        resp = await handle_ecr_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        layers = body["layers"]
        assert len(layers) == 1

        # AWS includes registryId in each layer object
        assert "registryId" in layers[0], (
            f"Layer response is missing 'registryId'. Got keys: {list(layers[0].keys())}. "
            "AWS includes registryId in each layer of the BatchCheckLayerAvailability response."
        )
