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
