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
        assert ep == ("http://my-domain.us-east-1.opensearch.localhost.robotocore.cloud:4566")

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
            "my-domain.us-east-1.opensearch.localhost.robotocore.cloud:4566",
        )
        assert result is not None
        assert result["region"] == "us-east-1"
        assert result["domain_name"] == "my-domain"

    def test_domain_host_localstack_alias(self):
        """localstack.cloud must be accepted as a backward-compat alias."""
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

    def test_empty_path_returns_none(self):
        result = parse_opensearch_url("/", "localhost:4566")
        assert result is None

    def test_path_missing_domain_name_returns_none(self):
        result = parse_opensearch_url("/opensearch/us-east-1", "localhost:4566")
        assert result is None

    def test_path_missing_region_returns_none(self):
        result = parse_opensearch_url("/opensearch/", "localhost:4566")
        assert result is None

    def test_domain_host_different_region(self):
        result = parse_opensearch_url(
            "/_cluster/health",
            "analytics.ap-southeast-1.opensearch.localhost.localstack.cloud:4566",
        )
        assert result is not None
        assert result["region"] == "ap-southeast-1"
        assert result["domain_name"] == "analytics"

    def test_domain_host_without_port(self):
        result = parse_opensearch_url(
            "/",
            "my-domain.us-east-1.opensearch.localhost.localstack.cloud",
        )
        assert result is not None
        assert result["domain_name"] == "my-domain"

    def test_plain_localhost_returns_none(self):
        result = parse_opensearch_url("/", "localhost:4566")
        assert result is None


class TestOpenSearchStrategyEnvVarEdgeCases:
    """Test edge cases in strategy env var handling."""

    def test_uppercase_strategy_is_accepted(self, monkeypatch):
        monkeypatch.setenv("OPENSEARCH_ENDPOINT_STRATEGY", "PATH")
        assert get_opensearch_endpoint_strategy() == OpenSearchEndpointStrategy.PATH

    def test_mixed_case_strategy_is_accepted(self, monkeypatch):
        monkeypatch.setenv("OPENSEARCH_ENDPOINT_STRATEGY", "Port")
        assert get_opensearch_endpoint_strategy() == OpenSearchEndpointStrategy.PORT

    def test_whitespace_padded_strategy_is_accepted(self, monkeypatch):
        monkeypatch.setenv("OPENSEARCH_ENDPOINT_STRATEGY", "  domain  ")
        assert get_opensearch_endpoint_strategy() == OpenSearchEndpointStrategy.DOMAIN

    def test_empty_string_falls_back_to_domain(self, monkeypatch):
        monkeypatch.setenv("OPENSEARCH_ENDPOINT_STRATEGY", "")
        assert get_opensearch_endpoint_strategy() == OpenSearchEndpointStrategy.DOMAIN


class TestOpenSearchCustomGatewayPort:
    """Test that GATEWAY_PORT env var affects endpoint generation."""

    def test_domain_strategy_uses_custom_port(self, monkeypatch):
        monkeypatch.setenv("GATEWAY_PORT", "8888")
        ep = opensearch_endpoint("d", "us-east-1", OpenSearchEndpointStrategy.DOMAIN)
        assert ":8888" in ep

    def test_path_strategy_uses_custom_port(self, monkeypatch):
        monkeypatch.setenv("GATEWAY_PORT", "9090")
        ep = opensearch_endpoint("d", "us-east-1", OpenSearchEndpointStrategy.PATH)
        assert ":9090/" in ep


class TestOpenSearchStrategyFromEnvForEndpointGen:
    """Test that opensearch_endpoint reads from env when no explicit strategy."""

    def test_reads_domain_from_env(self, monkeypatch):
        monkeypatch.delenv("OPENSEARCH_ENDPOINT_STRATEGY", raising=False)
        monkeypatch.delenv("GATEWAY_PORT", raising=False)
        ep = opensearch_endpoint("d", "us-east-1")
        assert ".opensearch.localhost.robotocore.cloud" in ep

    def test_reads_path_from_env(self, monkeypatch):
        monkeypatch.setenv("OPENSEARCH_ENDPOINT_STRATEGY", "path")
        monkeypatch.delenv("GATEWAY_PORT", raising=False)
        ep = opensearch_endpoint("d", "us-east-1")
        assert "/opensearch/us-east-1/d" in ep

    def test_reads_port_from_env(self, monkeypatch):
        reset_port_allocations()
        monkeypatch.setenv("OPENSEARCH_ENDPOINT_STRATEGY", "port")
        monkeypatch.delenv("GATEWAY_PORT", raising=False)
        ep = opensearch_endpoint("d", "us-east-1")
        assert "localhost:4510" in ep


class TestOpenSearchPortStrategyAdvanced:
    """Advanced tests for port allocation behavior."""

    def setup_method(self):
        reset_port_allocations()

    def test_ports_are_sequential(self):
        ports = []
        for i in range(5):
            ep = opensearch_endpoint(f"d-{i}", "us-east-1", OpenSearchEndpointStrategy.PORT)
            port = int(ep.rsplit(":", 1)[-1])
            ports.append(port)
        assert ports == [4510, 4511, 4512, 4513, 4514]

    def test_same_domain_different_region_same_port(self):
        """Port allocation is by domain name, not region."""
        ep1 = opensearch_endpoint("shared", "us-east-1", OpenSearchEndpointStrategy.PORT)
        ep2 = opensearch_endpoint("shared", "eu-west-1", OpenSearchEndpointStrategy.PORT)
        assert ep1 == ep2

    def test_reset_clears_all_allocations(self):
        opensearch_endpoint("first", "us-east-1", OpenSearchEndpointStrategy.PORT)
        reset_port_allocations()
        ep = opensearch_endpoint("second", "us-east-1", OpenSearchEndpointStrategy.PORT)
        assert "4510" in ep  # Starts from beginning after reset


class TestOpenSearchRoundTrip:
    """Test that generated endpoints can be parsed back correctly."""

    def test_domain_strategy_roundtrip(self):
        ep = opensearch_endpoint("my-domain", "us-east-1", OpenSearchEndpointStrategy.DOMAIN)
        # The domain strategy URL host is: my-domain.us-east-1.opensearch.localhost.localstack.cloud
        from urllib.parse import urlparse

        parsed = urlparse(ep)
        result = parse_opensearch_url("/", parsed.netloc)
        assert result is not None
        assert result["region"] == "us-east-1"
        assert result["domain_name"] == "my-domain"

    def test_path_strategy_roundtrip(self):
        ep = opensearch_endpoint("search-logs", "eu-west-1", OpenSearchEndpointStrategy.PATH)
        from urllib.parse import urlparse

        parsed = urlparse(ep)
        result = parse_opensearch_url(parsed.path, parsed.netloc)
        assert result is not None
        assert result["region"] == "eu-west-1"
        assert result["domain_name"] == "search-logs"
