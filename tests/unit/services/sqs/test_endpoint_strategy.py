"""Tests for SQS endpoint URL strategies."""

from urllib.parse import urlparse

from robotocore.services.sqs.endpoint_strategy import (
    SqsEndpointStrategy,
    get_sqs_endpoint_strategy,
    parse_sqs_url,
    sqs_queue_url,
)


class TestSqsEndpointStrategy:
    """Test strategy selection from env var."""

    def test_default_strategy_is_standard(self, monkeypatch):
        monkeypatch.delenv("SQS_ENDPOINT_STRATEGY", raising=False)
        assert get_sqs_endpoint_strategy() == SqsEndpointStrategy.STANDARD

    def test_standard_strategy_from_env(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "standard")
        assert get_sqs_endpoint_strategy() == SqsEndpointStrategy.STANDARD

    def test_domain_strategy_from_env(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "domain")
        assert get_sqs_endpoint_strategy() == SqsEndpointStrategy.DOMAIN

    def test_path_strategy_from_env(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "path")
        assert get_sqs_endpoint_strategy() == SqsEndpointStrategy.PATH

    def test_dynamic_strategy_from_env(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "dynamic")
        assert get_sqs_endpoint_strategy() == SqsEndpointStrategy.DYNAMIC

    def test_invalid_strategy_falls_back_to_standard(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "invalid_thing")
        assert get_sqs_endpoint_strategy() == SqsEndpointStrategy.STANDARD


class TestStandardStrategy:
    """Test standard strategy URL generation."""

    def test_url_format(self):
        url = sqs_queue_url("my-queue", "us-east-1", "123456789012", SqsEndpointStrategy.STANDARD)
        assert url == ("http://sqs.us-east-1.localhost.localstack.cloud:4566/123456789012/my-queue")

    def test_includes_correct_region(self):
        url = sqs_queue_url("q", "eu-west-1", "111111111111", SqsEndpointStrategy.STANDARD)
        assert "sqs.eu-west-1." in url

    def test_includes_correct_account_id(self):
        url = sqs_queue_url("q", "us-east-1", "999888777666", SqsEndpointStrategy.STANDARD)
        assert "/999888777666/" in url

    def test_includes_correct_queue_name(self):
        url = sqs_queue_url("my-special-queue", "us-east-1", "123", SqsEndpointStrategy.STANDARD)
        assert url.endswith("/my-special-queue")

    def test_fifo_queue_name(self):
        url = sqs_queue_url(
            "my-queue.fifo", "us-east-1", "123456789012", SqsEndpointStrategy.STANDARD
        )
        assert url.endswith("/my-queue.fifo")

    def test_url_is_parseable(self):
        url = sqs_queue_url("q", "us-east-1", "123", SqsEndpointStrategy.STANDARD)
        parsed = urlparse(url)
        assert parsed.scheme == "http"
        assert parsed.hostname is not None
        assert parsed.port == 4566


class TestDomainStrategy:
    """Test domain strategy URL generation."""

    def test_url_format(self):
        url = sqs_queue_url("my-queue", "us-east-1", "123456789012", SqsEndpointStrategy.DOMAIN)
        assert url == (
            "http://us-east-1.queue.localhost.localstack.cloud:4566/123456789012/my-queue"
        )

    def test_includes_correct_region(self):
        url = sqs_queue_url("q", "ap-southeast-1", "111", SqsEndpointStrategy.DOMAIN)
        assert url.startswith("http://ap-southeast-1.queue.")

    def test_includes_correct_account_id(self):
        url = sqs_queue_url("q", "us-east-1", "555444333222", SqsEndpointStrategy.DOMAIN)
        assert "/555444333222/" in url

    def test_includes_correct_queue_name(self):
        url = sqs_queue_url("test-q", "us-east-1", "123", SqsEndpointStrategy.DOMAIN)
        assert url.endswith("/test-q")

    def test_fifo_queue_name(self):
        url = sqs_queue_url("fifo-q.fifo", "us-east-1", "123", SqsEndpointStrategy.DOMAIN)
        assert url.endswith("/fifo-q.fifo")

    def test_url_is_parseable(self):
        url = sqs_queue_url("q", "us-east-1", "123", SqsEndpointStrategy.DOMAIN)
        parsed = urlparse(url)
        assert parsed.scheme == "http"
        assert parsed.port == 4566


class TestPathStrategy:
    """Test path strategy URL generation."""

    def test_url_format(self):
        url = sqs_queue_url("my-queue", "us-east-1", "123456789012", SqsEndpointStrategy.PATH)
        assert url == "http://localhost:4566/queue/us-east-1/123456789012/my-queue"

    def test_includes_correct_region(self):
        url = sqs_queue_url("q", "eu-central-1", "111", SqsEndpointStrategy.PATH)
        assert "/queue/eu-central-1/" in url

    def test_includes_correct_account_id(self):
        url = sqs_queue_url("q", "us-east-1", "888777666555", SqsEndpointStrategy.PATH)
        assert "/888777666555/" in url

    def test_includes_correct_queue_name(self):
        url = sqs_queue_url("path-queue", "us-east-1", "123", SqsEndpointStrategy.PATH)
        assert url.endswith("/path-queue")

    def test_fifo_queue_name(self):
        url = sqs_queue_url("f.fifo", "us-east-1", "123", SqsEndpointStrategy.PATH)
        assert url.endswith("/f.fifo")

    def test_url_is_parseable(self):
        url = sqs_queue_url("q", "us-east-1", "123", SqsEndpointStrategy.PATH)
        parsed = urlparse(url)
        assert parsed.scheme == "http"
        assert parsed.hostname == "localhost"
        assert parsed.port == 4566
        assert parsed.path == "/queue/us-east-1/123/q"


class TestDynamicStrategy:
    """Test dynamic strategy URL generation (returns path-style URLs)."""

    def test_url_format_matches_path(self):
        url = sqs_queue_url("my-queue", "us-east-1", "123456789012", SqsEndpointStrategy.DYNAMIC)
        expected = sqs_queue_url("my-queue", "us-east-1", "123456789012", SqsEndpointStrategy.PATH)
        assert url == expected

    def test_url_is_parseable(self):
        url = sqs_queue_url("q", "us-east-1", "123", SqsEndpointStrategy.DYNAMIC)
        parsed = urlparse(url)
        assert parsed.scheme == "http"
        assert parsed.path.startswith("/queue/")


class TestParseSqsUrl:
    """Test parsing incoming SQS request URLs."""

    def test_path_style_parsing(self):
        result = parse_sqs_url("/queue/us-east-1/123456789012/my-queue", "localhost:4566")
        assert result is not None
        assert result["region"] == "us-east-1"
        assert result["account_id"] == "123456789012"
        assert result["queue_name"] == "my-queue"

    def test_standard_host_parsing(self):
        result = parse_sqs_url(
            "/123456789012/my-queue",
            "sqs.us-east-1.localhost.localstack.cloud:4566",
        )
        assert result is not None
        assert result["region"] == "us-east-1"
        assert result["account_id"] == "123456789012"
        assert result["queue_name"] == "my-queue"

    def test_domain_host_parsing(self):
        result = parse_sqs_url(
            "/123456789012/my-queue",
            "us-east-1.queue.localhost.localstack.cloud:4566",
        )
        assert result is not None
        assert result["region"] == "us-east-1"
        assert result["account_id"] == "123456789012"
        assert result["queue_name"] == "my-queue"

    def test_unrecognized_returns_none(self):
        result = parse_sqs_url("/some/random/path", "example.com")
        assert result is None

    def test_fifo_queue_path_style(self):
        result = parse_sqs_url("/queue/us-east-1/123/my-queue.fifo", "localhost:4566")
        assert result is not None
        assert result["queue_name"] == "my-queue.fifo"
