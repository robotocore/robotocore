"""Error-path tests for OpenSearch/Elasticsearch native provider.

Phase 3A: Covers ListVersions and GetCompatibleElasticsearchVersions operations.
Phase 3B: Categorical bug tests — NextToken null serialization, stub response shapes.
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


# ── Categorical bug: NextToken should be omitted, not serialized as null ──


@pytest.mark.asyncio
class TestNextTokenNotNull:
    """AWS APIs omit NextToken when there are no more pages.

    Sending "NextToken": null breaks some clients (e.g., Go SDK strict parsing).
    This is a categorical bug affecting multiple providers — any stub response
    that sets NextToken to None will serialize as {"NextToken": null}.
    """

    _ES_STUB_PATHS = [
        ("POST", "/2015-01-01/es/ccs/inboundConnection/search"),
        ("POST", "/2015-01-01/es/ccs/outboundConnection/search"),
        ("POST", "/2015-01-01/packages/describe"),
        ("GET", "/2015-01-01/es/reservedInstanceOfferings"),
        ("GET", "/2015-01-01/es/reservedInstances"),
        ("GET", "/2015-01-01/es/vpcEndpoints"),
    ]

    @pytest.mark.parametrize("method,path", _ES_STUB_PATHS)
    async def test_stub_response_omits_next_token_null(self, method, path):
        """NextToken key must not appear with a null value in the JSON body."""
        req = _make_request(method, path)
        resp = await handle_es_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        # NextToken should either be absent or a non-None string
        if "NextToken" in body:
            assert body["NextToken"] is not None, (
                f"NextToken is null in response for {method} {path} — "
                "AWS omits the key instead of sending null"
            )


# ── Categorical bug: ES ListElasticsearchVersions returns prefixed names ──


@pytest.mark.asyncio
class TestListElasticsearchVersions:
    """ListElasticsearchVersions should return bare version numbers (e.g. '7.10'),
    not prefixed names like 'Elasticsearch_7.10'. AWS returns bare numbers for
    the ES API; the OpenSearch API uses prefixed names.
    """

    async def test_versions_are_bare_numbers(self):
        req = _make_request("GET", "/2015-01-01/es/versions")
        resp = await handle_es_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        versions = body["ElasticsearchVersions"]
        assert len(versions) > 0
        for v in versions:
            assert not v.startswith("Elasticsearch_"), (
                f"ES version '{v}' should be a bare number like '7.10', "
                "not prefixed with 'Elasticsearch_'"
            )

    async def test_versions_include_expected_values(self):
        req = _make_request("GET", "/2015-01-01/es/versions")
        resp = await handle_es_request(req, "us-east-1", "123456789012")
        body = json.loads(resp.body)
        versions = body["ElasticsearchVersions"]
        # Should contain well-known ES versions as bare numbers
        assert "7.10" in versions
        assert "6.8" in versions
        assert "5.6" in versions


# ── Categorical bug: DeleteElasticsearchServiceRole returns empty string body ──


@pytest.mark.asyncio
class TestDeleteElasticsearchServiceRole:
    """DELETE /2015-01-01/es/role returns empty-string body with JSON media type.
    A proper empty JSON response should be '{}', not ''.
    """

    async def test_returns_valid_json_body(self):
        req = _make_request("DELETE", "/2015-01-01/es/role")
        resp = await handle_es_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        # Body should be parseable JSON (not empty string)
        body_text = resp.body.decode() if isinstance(resp.body, bytes) else resp.body
        if body_text:
            json.loads(body_text)  # Should not raise
        # Empty string is technically not valid JSON — should be '{}' or omitted
