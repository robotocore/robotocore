"""Native OpenSearch/ES provider.

Intercepts operations that Moto doesn't implement:
- OpenSearch ListVersions
- ES GetCompatibleElasticsearchVersions
"""

import json
import re

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

_OS_VERSIONS_RE = re.compile(r"^/2021-01-01/opensearch/versions$")
_ES_COMPAT_RE = re.compile(r"^/2015-01-01/es/compatibleVersions$")

_OPENSEARCH_VERSIONS = [
    "OpenSearch_2.13",
    "OpenSearch_2.11",
    "OpenSearch_2.9",
    "OpenSearch_2.7",
    "OpenSearch_2.5",
    "OpenSearch_2.3",
    "OpenSearch_1.3",
    "OpenSearch_1.2",
    "OpenSearch_1.1",
    "OpenSearch_1.0",
    "Elasticsearch_7.10",
    "Elasticsearch_7.9",
    "Elasticsearch_7.8",
    "Elasticsearch_7.7",
    "Elasticsearch_7.4",
    "Elasticsearch_7.1",
    "Elasticsearch_6.8",
    "Elasticsearch_6.7",
    "Elasticsearch_6.5",
    "Elasticsearch_6.4",
    "Elasticsearch_6.3",
    "Elasticsearch_6.2",
    "Elasticsearch_6.0",
    "Elasticsearch_5.6",
    "Elasticsearch_5.5",
    "Elasticsearch_5.3",
    "Elasticsearch_5.1",
]

_ES_COMPAT_VERSIONS = [
    {"SourceVersion": "7.10", "TargetVersions": ["OpenSearch_1.0", "OpenSearch_1.1"]},
    {"SourceVersion": "7.9", "TargetVersions": ["7.10", "OpenSearch_1.0"]},
    {"SourceVersion": "7.8", "TargetVersions": ["7.9", "7.10"]},
    {"SourceVersion": "7.7", "TargetVersions": ["7.8", "7.9", "7.10"]},
    {"SourceVersion": "7.4", "TargetVersions": ["7.7", "7.8", "7.9", "7.10"]},
    {"SourceVersion": "7.1", "TargetVersions": ["7.4", "7.7", "7.8", "7.9", "7.10"]},
    {"SourceVersion": "6.8", "TargetVersions": ["7.1", "7.4", "7.7", "7.8", "7.9", "7.10"]},
    {"SourceVersion": "6.7", "TargetVersions": ["6.8"]},
    {"SourceVersion": "6.5", "TargetVersions": ["6.7", "6.8"]},
    {"SourceVersion": "6.4", "TargetVersions": ["6.5", "6.7", "6.8"]},
    {"SourceVersion": "6.3", "TargetVersions": ["6.4", "6.5", "6.7", "6.8"]},
    {"SourceVersion": "6.2", "TargetVersions": ["6.3", "6.4", "6.5", "6.7", "6.8"]},
    {"SourceVersion": "6.0", "TargetVersions": ["6.2", "6.3", "6.4", "6.5", "6.7", "6.8"]},
    {"SourceVersion": "5.6", "TargetVersions": ["6.0", "6.2", "6.3", "6.4", "6.5", "6.7", "6.8"]},
    {"SourceVersion": "5.5", "TargetVersions": ["5.6"]},
    {"SourceVersion": "5.3", "TargetVersions": ["5.5", "5.6"]},
    {"SourceVersion": "5.1", "TargetVersions": ["5.3", "5.5", "5.6"]},
]


async def handle_opensearch_request(request: Request, region: str, account_id: str) -> Response:
    """Handle OpenSearch requests, intercepting unimplemented operations."""
    path = request.url.path

    if _OS_VERSIONS_RE.match(path) and request.method == "GET":
        return Response(
            content=json.dumps({"Versions": _OPENSEARCH_VERSIONS}),
            status_code=200,
            media_type="application/json",
        )

    return await forward_to_moto(request, "opensearch")


async def handle_es_request(request: Request, region: str, account_id: str) -> Response:
    """Handle Elasticsearch requests, intercepting unimplemented operations."""
    path = request.url.path

    if _ES_COMPAT_RE.match(path) and request.method == "GET":
        return Response(
            content=json.dumps({"CompatibleElasticsearchVersions": _ES_COMPAT_VERSIONS}),
            status_code=200,
            media_type="application/json",
        )

    return await forward_to_moto(request, "es")
