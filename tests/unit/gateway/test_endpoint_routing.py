"""Tests for endpoint strategy routing in the gateway."""

from robotocore.services.opensearch.endpoint_strategy import parse_opensearch_url
from robotocore.services.sqs.endpoint_strategy import parse_sqs_url


class TestSqsPathStyleRouting:
    """Test SQS path-style URL parsing for routing."""

    def test_path_style_routes_to_sqs(self):
        result = parse_sqs_url("/queue/us-east-1/123456789012/my-queue", "localhost:4566")
        assert result is not None
        assert result["region"] == "us-east-1"
        assert result["account_id"] == "123456789012"
        assert result["queue_name"] == "my-queue"

    def test_path_style_different_region(self):
        result = parse_sqs_url("/queue/eu-west-1/111111111111/test-q", "localhost:4566")
        assert result is not None
        assert result["region"] == "eu-west-1"

    def test_path_style_fifo_queue(self):
        result = parse_sqs_url("/queue/us-east-1/123/my-queue.fifo", "localhost:4566")
        assert result is not None
        assert result["queue_name"] == "my-queue.fifo"


class TestSqsDomainStyleRouting:
    """Test SQS domain-style routing via Host header."""

    def test_standard_host_routes_to_sqs(self):
        result = parse_sqs_url(
            "/123456789012/my-queue",
            "sqs.us-east-1.localhost.localstack.cloud:4566",
        )
        assert result is not None
        assert result["region"] == "us-east-1"
        assert result["queue_name"] == "my-queue"

    def test_domain_host_routes_to_sqs(self):
        result = parse_sqs_url(
            "/123456789012/my-queue",
            "us-east-1.queue.localhost.localstack.cloud:4566",
        )
        assert result is not None
        assert result["region"] == "us-east-1"
        assert result["queue_name"] == "my-queue"


class TestOpenSearchPathStyleRouting:
    """Test OpenSearch path-style URL routing."""

    def test_path_style_routes_to_opensearch(self):
        result = parse_opensearch_url("/opensearch/us-east-1/my-domain", "localhost:4566")
        assert result is not None
        assert result["region"] == "us-east-1"
        assert result["domain_name"] == "my-domain"

    def test_path_style_with_subpath(self):
        result = parse_opensearch_url("/opensearch/us-east-1/my-domain/_search", "localhost:4566")
        assert result is not None
        assert result["domain_name"] == "my-domain"


class TestOpenSearchDomainStyleRouting:
    """Test OpenSearch domain-style routing via Host header."""

    def test_domain_host_routes_to_opensearch(self):
        result = parse_opensearch_url(
            "/",
            "my-domain.us-east-1.opensearch.localhost.localstack.cloud:4566",
        )
        assert result is not None
        assert result["region"] == "us-east-1"
        assert result["domain_name"] == "my-domain"


class TestDynamicStrategyAcceptsAllFormats:
    """Dynamic strategy should accept all URL formats."""

    def test_accepts_path_style(self):
        result = parse_sqs_url("/queue/us-east-1/123/my-q", "localhost:4566")
        assert result is not None

    def test_accepts_standard_host(self):
        result = parse_sqs_url("/123/my-q", "sqs.us-east-1.localhost.localstack.cloud:4566")
        assert result is not None

    def test_accepts_domain_host(self):
        result = parse_sqs_url("/123/my-q", "us-east-1.queue.localhost.localstack.cloud:4566")
        assert result is not None
