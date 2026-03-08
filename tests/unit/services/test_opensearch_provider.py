"""Error-path tests for OpenSearch/Elasticsearch native provider.

Phase 3A: Covers ListVersions and GetCompatibleElasticsearchVersions operations.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.opensearch.provider import (
    handle_es_request,
    handle_opensearch_request,
)


def _make_request(method: str, path: str) -> MagicMock:
    req = MagicMock()
    req.method = method
    req.url = MagicMock()
    req.url.path = path
    req.headers = {}
    req.query_params = {}
    req.body = AsyncMock(return_value=b"")
    return req


@pytest.mark.asyncio
class TestOpenSearchListVersions:
    async def test_list_versions_returns_versions(self):
        req = _make_request("GET", "/2021-01-01/opensearch/versions")
        resp = await handle_opensearch_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "Versions" in body
        assert len(body["Versions"]) > 0
        # Should include both OpenSearch and Elasticsearch versions
        versions = body["Versions"]
        has_opensearch = any(v.startswith("OpenSearch_") for v in versions)
        has_es = any(v.startswith("Elasticsearch_") for v in versions)
        assert has_opensearch
        assert has_es


@pytest.mark.asyncio
class TestElasticsearchCompatVersions:
    async def test_get_compatible_versions(self):
        req = _make_request("GET", "/2015-01-01/es/compatibleVersions")
        resp = await handle_es_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "CompatibleElasticsearchVersions" in body
        versions = body["CompatibleElasticsearchVersions"]
        assert len(versions) > 0
        # Each entry should have SourceVersion and TargetVersions
        for entry in versions:
            assert "SourceVersion" in entry
            assert "TargetVersions" in entry
            assert isinstance(entry["TargetVersions"], list)
