"""OpenSearch compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def opensearch():
    return make_client("opensearch")


class TestOpenSearchOperations:
    def test_create_domain(self, opensearch):
        response = opensearch.create_domain(
            DomainName="test-domain",
            EngineVersion="OpenSearch_2.5",
            ClusterConfig={
                "InstanceType": "t3.small.search",
                "InstanceCount": 1,
            },
        )
        status = response["DomainStatus"]
        assert status["DomainName"] == "test-domain"
        assert status["EngineVersion"] == "OpenSearch_2.5"

        # Cleanup
        opensearch.delete_domain(DomainName="test-domain")

    def test_describe_domain(self, opensearch):
        opensearch.create_domain(
            DomainName="describe-domain",
            EngineVersion="OpenSearch_2.5",
        )
        response = opensearch.describe_domain(DomainName="describe-domain")
        assert response["DomainStatus"]["DomainName"] == "describe-domain"

        # Cleanup
        opensearch.delete_domain(DomainName="describe-domain")

    def test_list_domain_names(self, opensearch):
        opensearch.create_domain(
            DomainName="list-domain",
            EngineVersion="OpenSearch_2.5",
        )
        response = opensearch.list_domain_names()
        domain_names = [d["DomainName"] for d in response["DomainNames"]]
        assert "list-domain" in domain_names

        # Cleanup
        opensearch.delete_domain(DomainName="list-domain")

    def test_delete_domain(self, opensearch):
        opensearch.create_domain(
            DomainName="delete-domain",
            EngineVersion="OpenSearch_2.5",
        )
        response = opensearch.delete_domain(DomainName="delete-domain")
        assert response["DomainStatus"]["DomainName"] == "delete-domain"

    def test_add_and_list_tags(self, opensearch):
        create_response = opensearch.create_domain(
            DomainName="tags-domain",
            EngineVersion="OpenSearch_2.5",
        )
        arn = create_response["DomainStatus"]["ARN"]

        opensearch.add_tags(
            ARN=arn,
            TagList=[{"Key": "env", "Value": "test"}],
        )
        response = opensearch.list_tags(ARN=arn)
        tag_keys = [t["Key"] for t in response["TagList"]]
        assert "env" in tag_keys

        # Cleanup
        opensearch.delete_domain(DomainName="tags-domain")
