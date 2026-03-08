"""Native OpenSearch/ES provider.

Intercepts operations that Moto doesn't implement or can't route:
- OpenSearch ListVersions
- ES GetCompatibleElasticsearchVersions
- ES operations whose URL paths are missing from Moto's flask_paths:
  DeleteElasticsearchServiceRole, DescribeInboundCrossClusterSearchConnections,
  DescribeOutboundCrossClusterSearchConnections, DescribePackages,
  DescribeReservedElasticsearchInstanceOfferings,
  DescribeReservedElasticsearchInstances, ListElasticsearchVersions,
  ListVpcEndpoints
"""

import json
import re

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

_OS_VERSIONS_RE = re.compile(r"^/2021-01-01/opensearch/versions$")
_ES_COMPAT_RE = re.compile(r"^/2015-01-01/es/compatibleVersions$")

# ES operations missing from Moto's flask_paths routing table.
# These paths exist in the botocore ES service model but Moto's ES backend
# (which delegates to OpenSearchServiceResponse) doesn't register URL routes
# for them.
_ES_ROLE_RE = re.compile(r"^/2015-01-01/es/role$")
_ES_INBOUND_CCS_RE = re.compile(r"^/2015-01-01/es/ccs/inboundConnection/search$")
_ES_OUTBOUND_CCS_RE = re.compile(r"^/2015-01-01/es/ccs/outboundConnection/search$")
_ES_PACKAGES_RE = re.compile(r"^/2015-01-01/packages/describe$")
_ES_RESERVED_OFFERINGS_RE = re.compile(r"^/2015-01-01/es/reservedInstanceOfferings$")
_ES_RESERVED_INSTANCES_RE = re.compile(r"^/2015-01-01/es/reservedInstances$")
_ES_VERSIONS_RE = re.compile(r"^/2015-01-01/es/versions$")
_ES_VPC_ENDPOINTS_RE = re.compile(r"^/2015-01-01/es/vpcEndpoints$")

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

    # DeleteElasticsearchServiceRole (DELETE /2015-01-01/es/role)
    if _ES_ROLE_RE.match(path) and request.method == "DELETE":
        return Response(content="", status_code=200, media_type="application/json")

    # DescribeInboundCrossClusterSearchConnections (POST)
    if _ES_INBOUND_CCS_RE.match(path) and request.method == "POST":
        return Response(
            content=json.dumps({"CrossClusterSearchConnections": [], "NextToken": None}),
            status_code=200,
            media_type="application/json",
        )

    # DescribeOutboundCrossClusterSearchConnections (POST)
    if _ES_OUTBOUND_CCS_RE.match(path) and request.method == "POST":
        return Response(
            content=json.dumps({"CrossClusterSearchConnections": [], "NextToken": None}),
            status_code=200,
            media_type="application/json",
        )

    # DescribePackages (POST /2015-01-01/packages/describe)
    if _ES_PACKAGES_RE.match(path) and request.method == "POST":
        return Response(
            content=json.dumps({"PackageDetailsList": [], "NextToken": None}),
            status_code=200,
            media_type="application/json",
        )

    # DescribeReservedElasticsearchInstanceOfferings (GET)
    if _ES_RESERVED_OFFERINGS_RE.match(path) and request.method == "GET":
        return Response(
            content=json.dumps({"ReservedElasticsearchInstanceOfferings": [], "NextToken": None}),
            status_code=200,
            media_type="application/json",
        )

    # DescribeReservedElasticsearchInstances (GET)
    if _ES_RESERVED_INSTANCES_RE.match(path) and request.method == "GET":
        return Response(
            content=json.dumps({"ReservedElasticsearchInstances": [], "NextToken": None}),
            status_code=200,
            media_type="application/json",
        )

    # ListElasticsearchVersions (GET /2015-01-01/es/versions)
    if _ES_VERSIONS_RE.match(path) and request.method == "GET":
        es_versions = [v for v in _OPENSEARCH_VERSIONS if v.startswith("Elasticsearch_")]
        return Response(
            content=json.dumps({"ElasticsearchVersions": es_versions}),
            status_code=200,
            media_type="application/json",
        )

    # ListVpcEndpoints (GET /2015-01-01/es/vpcEndpoints)
    if _ES_VPC_ENDPOINTS_RE.match(path) and request.method == "GET":
        return Response(
            content=json.dumps({"VpcEndpointSummaryList": [], "NextToken": None}),
            status_code=200,
            media_type="application/json",
        )

    return await forward_to_moto(request, "es")
