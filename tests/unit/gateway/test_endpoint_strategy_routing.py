"""Tests for endpoint strategy routing through route_to_service().

Verifies that the new SQS and OpenSearch host/path patterns in router.py
correctly route requests to the right service via route_to_service().
"""

from unittest.mock import MagicMock

from robotocore.gateway.router import route_to_service


def _make_request(
    path: str = "/",
    headers: dict | None = None,
    query_params: dict | None = None,
) -> MagicMock:
    """Create a mock Starlette Request."""
    req = MagicMock()
    req.url.path = path
    req.headers = headers or {}
    req.query_params = query_params or {}
    return req


class TestSqsHostPatternRouting:
    """Test SQS host-based patterns in route_to_service."""

    def test_standard_sqs_host_routes_to_sqs(self):
        req = _make_request(
            path="/123456789012/my-queue",
            headers={"host": "sqs.us-east-1.localhost.localstack.cloud:4566"},
        )
        assert route_to_service(req) == "sqs"

    def test_domain_queue_host_routes_to_sqs(self):
        req = _make_request(
            path="/123456789012/my-queue",
            headers={"host": "us-east-1.queue.localhost.localstack.cloud:4566"},
        )
        assert route_to_service(req) == "sqs"

    def test_sqs_host_eu_west_1(self):
        req = _make_request(
            path="/111111111111/test-q",
            headers={"host": "sqs.eu-west-1.localhost.localstack.cloud:4566"},
        )
        assert route_to_service(req) == "sqs"

    def test_domain_queue_host_ap_southeast(self):
        req = _make_request(
            path="/123/q",
            headers={"host": "ap-southeast-1.queue.localhost.localstack.cloud:4566"},
        )
        assert route_to_service(req) == "sqs"

    def test_sqs_host_without_port(self):
        req = _make_request(
            path="/123/q",
            headers={"host": "sqs.us-east-1.localhost.localstack.cloud"},
        )
        assert route_to_service(req) == "sqs"

    def test_queue_host_without_port(self):
        req = _make_request(
            path="/123/q",
            headers={"host": "us-east-1.queue.localhost.localstack.cloud"},
        )
        assert route_to_service(req) == "sqs"


class TestOpenSearchHostPatternRouting:
    """Test OpenSearch host-based patterns in route_to_service."""

    def test_opensearch_domain_host_routes_to_opensearch(self):
        req = _make_request(
            path="/",
            headers={"host": "my-domain.us-east-1.opensearch.localhost.localstack.cloud:4566"},
        )
        assert route_to_service(req) == "opensearch"

    def test_opensearch_host_different_region(self):
        req = _make_request(
            path="/_search",
            headers={"host": "search-prod.eu-west-1.opensearch.localhost.localstack.cloud:4566"},
        )
        assert route_to_service(req) == "opensearch"

    def test_opensearch_host_without_port(self):
        req = _make_request(
            path="/",
            headers={"host": "my-domain.us-east-1.opensearch.localhost.localstack.cloud"},
        )
        assert route_to_service(req) == "opensearch"


class TestSqsPathPatternRouting:
    """Test SQS path-style pattern in PATH_PATTERNS via route_to_service."""

    def test_sqs_path_style_routes_to_sqs(self):
        req = _make_request(path="/queue/us-east-1/123456789012/my-queue")
        assert route_to_service(req) == "sqs"

    def test_sqs_path_style_eu_region(self):
        req = _make_request(path="/queue/eu-central-1/111111111111/test-queue")
        assert route_to_service(req) == "sqs"


class TestOpenSearchPathPatternRouting:
    """Test OpenSearch path-style pattern in PATH_PATTERNS via route_to_service."""

    def test_opensearch_path_style_routes_to_opensearch(self):
        req = _make_request(path="/opensearch/us-east-1/my-domain")
        assert route_to_service(req) == "opensearch"

    def test_opensearch_path_style_with_subpath(self):
        req = _make_request(path="/opensearch/us-west-2/search-domain/_search")
        assert route_to_service(req) == "opensearch"

    def test_opensearch_path_style_different_region(self):
        req = _make_request(path="/opensearch/ap-northeast-1/MyDomain")
        assert route_to_service(req) == "opensearch"


class TestHostPatternsDoNotFalseMatch:
    """Verify host patterns don't accidentally match unrelated hosts."""

    def test_plain_localhost_does_not_match_sqs(self):
        req = _make_request(path="/123/q", headers={"host": "localhost:4566"})
        # Should not route to sqs via the host pattern alone (no Action param either)
        assert route_to_service(req) != "sqs"

    def test_s3_host_does_not_match_sqs(self):
        req = _make_request(
            path="/bucket/key",
            headers={"host": "bucket.s3.localhost.localstack.cloud:4566"},
        )
        assert route_to_service(req) == "s3"

    def test_random_host_does_not_match_opensearch(self):
        req = _make_request(
            path="/",
            headers={"host": "my-domain.us-east-1.elasticsearch.localhost.localstack.cloud"},
        )
        assert route_to_service(req) != "opensearch"
