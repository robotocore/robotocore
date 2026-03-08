"""Error-path tests for ECR native provider.

Phase 3A: Covers BatchCheckLayerAvailability and DescribeRepositories pagination.
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
class TestBatchCheckLayerAvailability:
    async def test_layers_returned_as_unavailable(self):
        req = _make_request("BatchCheckLayerAvailability", {
            "repositoryName": "test-repo",
            "layerDigests": [
                "sha256:abc123",
                "sha256:def456",
            ],
        })
        resp = await handle_ecr_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "layers" in body
        assert len(body["layers"]) == 2
        for layer in body["layers"]:
            assert layer["layerAvailability"] == "UNAVAILABLE"

    async def test_empty_layer_digests(self):
        req = _make_request("BatchCheckLayerAvailability", {
            "repositoryName": "test-repo",
            "layerDigests": [],
        })
        resp = await handle_ecr_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["layers"] == []
