"""Error-path tests for Rekognition native provider.

Phase 3A: Covers ResourceAlreadyExistsException, ResourceNotFoundException
for collections and tagging operations.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.rekognition.provider import handle_rekognition_request


def _make_request(action: str, body: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.headers = {"x-amz-target": f"RekognitionService.{action}"}
    req.method = "POST"
    req.url = MagicMock()
    req.url.path = "/"
    req.query_params = {}
    payload = json.dumps(body or {}).encode()
    req.body = AsyncMock(return_value=payload)
    return req


@pytest.mark.asyncio
class TestRekognitionCollectionErrors:
    async def test_create_duplicate_collection(self):
        req1 = _make_request("CreateCollection", {"CollectionId": "test-dup"})
        resp1 = await handle_rekognition_request(req1, "us-east-1", "123456789012")
        assert resp1.status_code == 200

        req2 = _make_request("CreateCollection", {"CollectionId": "test-dup"})
        resp2 = await handle_rekognition_request(req2, "us-east-1", "123456789012")
        assert resp2.status_code == 400
        body = json.loads(resp2.body)
        assert "ResourceAlreadyExistsException" in body.get("__type", "")

    async def test_describe_nonexistent_collection(self):
        req = _make_request("DescribeCollection", {"CollectionId": "nonexistent-xyz"})
        resp = await handle_rekognition_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert "ResourceNotFoundException" in body.get("__type", "")

    async def test_delete_nonexistent_collection(self):
        req = _make_request("DeleteCollection", {"CollectionId": "nonexistent-xyz"})
        resp = await handle_rekognition_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert "ResourceNotFoundException" in body.get("__type", "")

    async def test_create_then_describe_collection(self):
        req1 = _make_request("CreateCollection", {"CollectionId": "test-desc"})
        await handle_rekognition_request(req1, "us-west-2", "123456789012")

        req2 = _make_request("DescribeCollection", {"CollectionId": "test-desc"})
        resp = await handle_rekognition_request(req2, "us-west-2", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["CollectionARN"].endswith("test-desc")

    async def test_create_then_delete_then_describe(self):
        req1 = _make_request("CreateCollection", {"CollectionId": "test-del"})
        await handle_rekognition_request(req1, "eu-west-1", "123456789012")

        req2 = _make_request("DeleteCollection", {"CollectionId": "test-del"})
        resp2 = await handle_rekognition_request(req2, "eu-west-1", "123456789012")
        assert resp2.status_code == 200

        req3 = _make_request("DescribeCollection", {"CollectionId": "test-del"})
        resp3 = await handle_rekognition_request(req3, "eu-west-1", "123456789012")
        assert resp3.status_code == 400


@pytest.mark.asyncio
class TestRekognitionTaggingErrors:
    async def test_tag_nonexistent_resource(self):
        req = _make_request("TagResource", {
            "ResourceArn": "arn:aws:rekognition:us-east-1:123456789012:collection/nonexistent",
            "Tags": {"env": "test"},
        })
        resp = await handle_rekognition_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert "ResourceNotFoundException" in body.get("__type", "")

    async def test_list_tags_nonexistent_resource(self):
        req = _make_request("ListTagsForResource", {
            "ResourceArn": "arn:aws:rekognition:us-east-1:123456789012:collection/nonexistent",
        })
        resp = await handle_rekognition_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    async def test_untag_nonexistent_resource(self):
        req = _make_request("UntagResource", {
            "ResourceArn": "arn:aws:rekognition:us-east-1:123456789012:collection/nonexistent",
            "TagKeys": ["env"],
        })
        resp = await handle_rekognition_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
