"""Semantic integration tests for OpenSearch endpoint strategies.

These tests verify that the OpenSearch endpoint strategy module produces
correct endpoints for domain creation and description operations.
"""

from robotocore.services.opensearch.endpoint_strategy import (
    opensearch_endpoint,
    reset_port_allocations,
)


class TestCreateDomainReturnsStrategyEndpoint:
    """CreateDomain should return endpoints matching configured strategy."""

    def test_domain_strategy(self, monkeypatch):
        monkeypatch.delenv("OPENSEARCH_ENDPOINT_STRATEGY", raising=False)
        ep = opensearch_endpoint("my-domain", "us-east-1")
        assert "my-domain.us-east-1.opensearch.localhost.localstack.cloud" in ep

    def test_path_strategy(self, monkeypatch):
        monkeypatch.setenv("OPENSEARCH_ENDPOINT_STRATEGY", "path")
        ep = opensearch_endpoint("my-domain", "us-east-1")
        assert "/opensearch/us-east-1/my-domain" in ep

    def test_port_strategy(self, monkeypatch):
        reset_port_allocations()
        monkeypatch.setenv("OPENSEARCH_ENDPOINT_STRATEGY", "port")
        ep = opensearch_endpoint("my-domain", "us-east-1")
        assert "localhost:4510" in ep


class TestDescribeDomainReturnsStrategyEndpoint:
    """DescribeDomain should return the same endpoint as CreateDomain."""

    def test_describe_matches_create(self, monkeypatch):
        monkeypatch.setenv("OPENSEARCH_ENDPOINT_STRATEGY", "path")
        ep_create = opensearch_endpoint("desc-domain", "eu-west-1")
        ep_describe = opensearch_endpoint("desc-domain", "eu-west-1")
        assert ep_create == ep_describe


class TestManagementEndpointConfig:
    """The management endpoint should report current strategy config."""

    def test_endpoint_config_includes_strategies(self, monkeypatch):
        """Verify we can read both strategies programmatically."""
        from robotocore.services.opensearch.endpoint_strategy import (
            get_opensearch_endpoint_strategy,
        )
        from robotocore.services.sqs.endpoint_strategy import get_sqs_endpoint_strategy

        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "path")
        monkeypatch.setenv("OPENSEARCH_ENDPOINT_STRATEGY", "port")

        config = {
            "sqs_endpoint_strategy": get_sqs_endpoint_strategy().value,
            "opensearch_endpoint_strategy": get_opensearch_endpoint_strategy().value,
        }
        assert config["sqs_endpoint_strategy"] == "path"
        assert config["opensearch_endpoint_strategy"] == "port"
