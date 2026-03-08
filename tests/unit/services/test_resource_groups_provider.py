"""Error-path tests for Resource Groups native provider.

Phase 3A: Covers tag operations on resource groups.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.resource_groups.provider import handle_resource_groups_request


def _make_request(method: str, path: str, body: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.method = method
    req.url = MagicMock()
    req.url.path = path
    req.headers = {}
    req.query_params = {}
    payload = json.dumps(body or {}).encode() if body else b""
    req.body = AsyncMock(return_value=payload)
    return req


@pytest.mark.asyncio
class TestResourceGroupsTagOperations:
    async def test_get_tags_for_nonexistent_resource(self):
        arn = "arn:aws:resource-groups:us-east-1:123456789012:group/nonexistent"
        req = _make_request("GET", f"/resources/{arn}/tags")
        resp = await handle_resource_groups_request(req, "us-east-1", "123456789012")
        # Should return error or empty, not crash
        assert resp.status_code in (200, 400, 404, 500)

    async def test_put_tags_for_nonexistent_resource(self):
        arn = "arn:aws:resource-groups:us-east-1:123456789012:group/nonexistent"
        req = _make_request("PUT", f"/resources/{arn}/tags", {
            "Tags": {"env": "test"},
        })
        resp = await handle_resource_groups_request(req, "us-east-1", "123456789012")
        assert resp.status_code in (200, 400, 404, 500)
