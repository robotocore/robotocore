"""Elasticsearch Service compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def es():
    return make_client("es")


def _uid():
    return uuid.uuid4().hex[:8]


class TestElasticsearchOperations:
    def test_create_domain(self, es):
        name = f"es-{_uid()}"
        response = es.create_elasticsearch_domain(
            DomainName=name,
            ElasticsearchVersion="7.10",
        )
        assert response["DomainStatus"]["DomainName"] == name
        es.delete_elasticsearch_domain(DomainName=name)

    def test_describe_domain(self, es):
        name = f"es-{_uid()}"
        es.create_elasticsearch_domain(
            DomainName=name,
            ElasticsearchVersion="7.10",
        )
        response = es.describe_elasticsearch_domain(DomainName=name)
        assert response["DomainStatus"]["DomainName"] == name
        es.delete_elasticsearch_domain(DomainName=name)

    def test_list_domain_names(self, es):
        name = f"es-{_uid()}"
        es.create_elasticsearch_domain(
            DomainName=name,
            ElasticsearchVersion="7.10",
        )
        response = es.list_domain_names()
        names = [d["DomainName"] for d in response["DomainNames"]]
        assert name in names
        es.delete_elasticsearch_domain(DomainName=name)

    def test_delete_domain(self, es):
        name = f"es-{_uid()}"
        es.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        response = es.delete_elasticsearch_domain(DomainName=name)
        assert response["DomainStatus"]["DomainName"] == name

    def test_describe_domain_config(self, es):
        name = f"es-{_uid()}"
        es.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        response = es.describe_elasticsearch_domain_config(DomainName=name)
        assert "DomainConfig" in response
        assert "ElasticsearchVersion" in response["DomainConfig"]
        es.delete_elasticsearch_domain(DomainName=name)

    def test_add_tags(self, es):
        name = f"es-{_uid()}"
        create = es.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        arn = create["DomainStatus"]["ARN"]
        es.add_tags(
            ARN=arn,
            TagList=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "dev"}],
        )
        tags = es.list_tags(ARN=arn)["TagList"]
        tag_map = {t["Key"]: t["Value"] for t in tags}
        assert tag_map["env"] == "test"
        assert tag_map["team"] == "dev"
        es.delete_elasticsearch_domain(DomainName=name)

    def test_remove_tags(self, es):
        name = f"es-{_uid()}"
        create = es.create_elasticsearch_domain(DomainName=name, ElasticsearchVersion="7.10")
        arn = create["DomainStatus"]["ARN"]
        es.add_tags(ARN=arn, TagList=[{"Key": "temp", "Value": "yes"}])
        es.remove_tags(ARN=arn, TagKeys=["temp"])
        tags = es.list_tags(ARN=arn)["TagList"]
        keys = [t["Key"] for t in tags]
        assert "temp" not in keys
        es.delete_elasticsearch_domain(DomainName=name)

    def test_create_domain_with_cluster_config(self, es):
        name = f"es-{_uid()}"
        response = es.create_elasticsearch_domain(
            DomainName=name,
            ElasticsearchVersion="7.10",
            ElasticsearchClusterConfig={
                "InstanceType": "t3.small.elasticsearch",
                "InstanceCount": 1,
            },
        )
        assert response["DomainStatus"]["DomainName"] == name
        es.delete_elasticsearch_domain(DomainName=name)

    def test_list_domain_names_empty(self, es):
        response = es.list_domain_names()
        assert "DomainNames" in response
