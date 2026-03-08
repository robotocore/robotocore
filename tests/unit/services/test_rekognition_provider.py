"""Error-path tests for Rekognition native provider.

Phase 3A: Covers ResourceAlreadyExistsException, ResourceNotFoundException
for collections and tagging operations.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.rekognition import provider as rekog_module
from robotocore.services.rekognition.provider import handle_rekognition_request


@pytest.fixture(autouse=True)
def _clear_rekognition_state():
    """Clear global state before each test to avoid cross-test pollution."""
    rekog_module._collections.clear()
    rekog_module._tags.clear()
    yield
    rekog_module._collections.clear()
    rekog_module._tags.clear()


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
        req = _make_request(
            "TagResource",
            {
                "ResourceArn": "arn:aws:rekognition:us-east-1:123456789012:collection/nonexistent",
                "Tags": {"env": "test"},
            },
        )
        resp = await handle_rekognition_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert "ResourceNotFoundException" in body.get("__type", "")

    async def test_list_tags_nonexistent_resource(self):
        req = _make_request(
            "ListTagsForResource",
            {
                "ResourceArn": "arn:aws:rekognition:us-east-1:123456789012:collection/nonexistent",
            },
        )
        resp = await handle_rekognition_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400

    async def test_untag_nonexistent_resource(self):
        req = _make_request(
            "UntagResource",
            {
                "ResourceArn": "arn:aws:rekognition:us-east-1:123456789012:collection/nonexistent",
                "TagKeys": ["env"],
            },
        )
        resp = await handle_rekognition_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400


@pytest.mark.asyncio
class TestRekognitionCreateTags:
    """Categorical bug: Tags passed at resource creation time are silently dropped."""

    async def test_create_collection_with_tags_visible_via_list_tags(self):
        """Tags passed in CreateCollection should be stored and retrievable."""
        arn = "arn:aws:rekognition:us-east-1:123456789012:collection/tagged-col"
        req1 = _make_request(
            "CreateCollection",
            {"CollectionId": "tagged-col", "Tags": {"env": "prod", "team": "ml"}},
        )
        resp1 = await handle_rekognition_request(req1, "us-east-1", "123456789012")
        assert resp1.status_code == 200

        req2 = _make_request("ListTagsForResource", {"ResourceArn": arn})
        resp2 = await handle_rekognition_request(req2, "us-east-1", "123456789012")
        assert resp2.status_code == 200
        body = json.loads(resp2.body)
        assert body["Tags"] == {"env": "prod", "team": "ml"}

    async def test_create_collection_without_tags_returns_empty(self):
        """Collections created without tags should return empty tag map."""
        arn = "arn:aws:rekognition:us-east-1:123456789012:collection/no-tags"
        req1 = _make_request("CreateCollection", {"CollectionId": "no-tags"})
        resp1 = await handle_rekognition_request(req1, "us-east-1", "123456789012")
        assert resp1.status_code == 200

        req2 = _make_request("ListTagsForResource", {"ResourceArn": arn})
        resp2 = await handle_rekognition_request(req2, "us-east-1", "123456789012")
        body = json.loads(resp2.body)
        assert body["Tags"] == {}


@pytest.mark.asyncio
class TestRekognitionTagCascadeOnDelete:
    """Categorical bug: deleting a parent resource must clean up child state (tags)."""

    async def test_delete_collection_clears_tags_from_global_store(self):
        """After deletion, tags for the ARN should not linger in the global _tags dict."""
        req1 = _make_request(
            "CreateCollection",
            {"CollectionId": "cascade-col", "Tags": {"env": "test"}},
        )
        await handle_rekognition_request(req1, "us-east-1", "123456789012")

        arn = "arn:aws:rekognition:us-east-1:123456789012:collection/cascade-col"
        assert arn in rekog_module._tags  # precondition: tags exist

        req2 = _make_request("DeleteCollection", {"CollectionId": "cascade-col"})
        await handle_rekognition_request(req2, "us-east-1", "123456789012")

        # Tags should be cleaned up — no orphaned entries
        assert arn not in rekog_module._tags


@pytest.mark.asyncio
class TestRekognitionTagRoundTrip:
    """Full tag lifecycle: create with tags, add more, remove some, verify."""

    async def test_tag_then_untag_then_list(self):
        arn = "arn:aws:rekognition:us-east-1:123456789012:collection/rt-col"
        req = _make_request(
            "CreateCollection",
            {"CollectionId": "rt-col", "Tags": {"a": "1", "b": "2"}},
        )
        await handle_rekognition_request(req, "us-east-1", "123456789012")

        # Add more tags
        req2 = _make_request("TagResource", {"ResourceArn": arn, "Tags": {"c": "3"}})
        await handle_rekognition_request(req2, "us-east-1", "123456789012")

        # Remove one
        req3 = _make_request("UntagResource", {"ResourceArn": arn, "TagKeys": ["b"]})
        await handle_rekognition_request(req3, "us-east-1", "123456789012")

        # Verify
        req4 = _make_request("ListTagsForResource", {"ResourceArn": arn})
        resp = await handle_rekognition_request(req4, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        assert body["Tags"] == {"a": "1", "c": "3"}


@pytest.mark.asyncio
class TestRekognitionThreadSafety:
    """Categorical bug: module-level dicts with no locking can corrupt under concurrency."""

    async def test_concurrent_creates_no_crash(self):
        """Multiple threads creating collections should not raise or corrupt state."""
        import asyncio

        errors = []

        async def create_one(i: int):
            try:
                req = _make_request("CreateCollection", {"CollectionId": f"thread-{i}"})
                await handle_rekognition_request(req, "us-east-1", "123456789012")
            except Exception as e:
                errors.append(e)

        await asyncio.gather(*(create_one(i) for i in range(20)))
        assert errors == [], f"Concurrent creates raised: {errors}"
        # All 20 should exist
        store = rekog_module._get_collections("123456789012", "us-east-1")
        assert len(store) == 20


@pytest.mark.asyncio
class TestRekognitionListCollectionsPagination:
    """Verify pagination edge cases."""

    async def test_list_returns_all_collections(self):
        for i in range(5):
            req = _make_request("CreateCollection", {"CollectionId": f"list-{i:02d}"})
            await handle_rekognition_request(req, "us-east-1", "123456789012")

        req = _make_request("ListCollections", {})
        resp = await handle_rekognition_request(req, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        assert len(body["CollectionIds"]) == 5
        assert "NextToken" not in body

    async def test_list_pagination(self):
        for i in range(5):
            req = _make_request("CreateCollection", {"CollectionId": f"page-{i:02d}"})
            await handle_rekognition_request(req, "us-east-1", "123456789012")

        req = _make_request("ListCollections", {"MaxResults": 2})
        resp = await handle_rekognition_request(req, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        assert len(body["CollectionIds"]) == 2
        assert "NextToken" in body
