"""Tests for OpenSearch endpoint strategies."""

import pytest

from robotocore.services.opensearch.endpoint_strategy import (
    OpenSearchEndpointStrategy,
    get_opensearch_endpoint_strategy,
    opensearch_endpoint,
    parse_opensearch_url,
    reset_port_allocations,
)


class TestOpenSearchEndpointStrategy:
    """Test strategy selection from env var."""

    def test_default_strategy_is_domain(self, monkeypatch):
        monkeypatch.delenv("OPENSEARCH_ENDPOINT_STRATEGY", raising=False)
        assert get_opensearch_endpoint_strategy() == OpenSearchEndpointStrategy.DOMAIN

    def test_domain_strategy_from_env(self, monkeypatch):
        monkeypatch.setenv("OPENSEARCH_ENDPOINT_STRATEGY", "domain")
        assert get_opensearch_endpoint_strategy() == OpenSearchEndpointStrategy.DOMAIN

    def test_path_strategy_from_env(self, monkeypatch):
        monkeypatch.setenv("OPENSEARCH_ENDPOINT_STRATEGY", "path")
        assert get_opensearch_endpoint_strategy() == OpenSearchEndpointStrategy.PATH

    def test_port_strategy_from_env(self, monkeypatch):
        monkeypatch.setenv("OPENSEARCH_ENDPOINT_STRATEGY", "port")
        assert get_opensearch_endpoint_strategy() == OpenSearchEndpointStrategy.PORT

    def test_invalid_strategy_falls_back_to_domain(self, monkeypatch):
        monkeypatch.setenv("OPENSEARCH_ENDPOINT_STRATEGY", "bogus")
        assert get_opensearch_endpoint_strategy() == OpenSearchEndpointStrategy.DOMAIN


class TestDomainStrategy:
    """Test domain strategy endpoint generation."""

    def test_endpoint_format(self):
        ep = opensearch_endpoint("my-domain", "us-east-1", OpenSearchEndpointStrategy.DOMAIN)
        assert ep == ("http://my-domain.us-east-1.opensearch.localhost.localstack.cloud:4566")

    def test_includes_correct_region(self):
        ep = opensearch_endpoint("d", "eu-west-1", OpenSearchEndpointStrategy.DOMAIN)
        assert ".eu-west-1.opensearch." in ep

    def test_includes_correct_domain_name(self):
        ep = opensearch_endpoint("search-prod", "us-east-1", OpenSearchEndpointStrategy.DOMAIN)
        assert ep.startswith("http://search-prod.")


class TestPathStrategy:
    """Test path strategy endpoint generation."""

    def test_endpoint_format(self):
        ep = opensearch_endpoint("my-domain", "us-east-1", OpenSearchEndpointStrategy.PATH)
        assert ep == "http://localhost:4566/opensearch/us-east-1/my-domain"

    def test_includes_correct_region(self):
        ep = opensearch_endpoint("d", "ap-northeast-1", OpenSearchEndpointStrategy.PATH)
        assert "/opensearch/ap-northeast-1/" in ep

    def test_includes_correct_domain_name(self):
        ep = opensearch_endpoint("my-search", "us-east-1", OpenSearchEndpointStrategy.PATH)
        assert ep.endswith("/my-search")


class TestPortStrategy:
    """Test port strategy with allocation."""

    def setup_method(self):
        reset_port_allocations()

    def test_port_allocation(self):
        ep = opensearch_endpoint("my-domain", "us-east-1", OpenSearchEndpointStrategy.PORT)
        assert ep == "http://localhost:4510"

    def test_port_reuse_for_same_domain(self):
        ep1 = opensearch_endpoint("same-domain", "us-east-1", OpenSearchEndpointStrategy.PORT)
        ep2 = opensearch_endpoint("same-domain", "us-east-1", OpenSearchEndpointStrategy.PORT)
        assert ep1 == ep2

    def test_different_domains_get_different_ports(self):
        ep1 = opensearch_endpoint("domain-a", "us-east-1", OpenSearchEndpointStrategy.PORT)
        ep2 = opensearch_endpoint("domain-b", "us-east-1", OpenSearchEndpointStrategy.PORT)
        assert ep1 != ep2
        assert "4510" in ep1
        assert "4511" in ep2

    def test_port_exhaustion(self):
        """Allocating more than 50 domains should raise RuntimeError."""
        for i in range(50):
            opensearch_endpoint(f"domain-{i}", "us-east-1", OpenSearchEndpointStrategy.PORT)
        with pytest.raises(RuntimeError, match="port range exhausted"):
            opensearch_endpoint("domain-50", "us-east-1", OpenSearchEndpointStrategy.PORT)


class TestParseOpenSearchUrl:
    """Test parsing incoming OpenSearch request URLs."""

    def test_path_style_parsing(self):
        result = parse_opensearch_url("/opensearch/us-east-1/my-domain", "localhost:4566")
        assert result is not None
        assert result["region"] == "us-east-1"
        assert result["domain_name"] == "my-domain"

    def test_domain_host_parsing(self):
        result = parse_opensearch_url(
            "/",
            "my-domain.us-east-1.opensearch.localhost.localstack.cloud:4566",
        )
        assert result is not None
        assert result["region"] == "us-east-1"
        assert result["domain_name"] == "my-domain"

    def test_unrecognized_returns_none(self):
        result = parse_opensearch_url("/some/path", "example.com")
        assert result is None

    def test_path_style_with_subpath(self):
        result = parse_opensearch_url(
            "/opensearch/us-west-2/search-domain/_search", "localhost:4566"
        )
        assert result is not None
        assert result["domain_name"] == "search-domain"
        assert result["region"] == "us-west-2"
