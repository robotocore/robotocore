"""Route 53 Domains compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def route53domains():
    return make_client("route53domains", region_name="us-east-1")


class TestRoute53DomainsOperations:
    def test_list_domains(self, route53domains):
        """ListDomains returns a list of domains."""
        response = route53domains.list_domains()
        assert "Domains" in response
        assert isinstance(response["Domains"], list)

    def test_list_operations(self, route53domains):
        """ListOperations returns a list of operations."""
        response = route53domains.list_operations()
        assert "Operations" in response
        assert isinstance(response["Operations"], list)

    def test_list_domains_status_code(self, route53domains):
        """ListDomains returns HTTP 200."""
        response = route53domains.list_domains()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_operations_status_code(self, route53domains):
        """ListOperations returns HTTP 200."""
        response = route53domains.list_operations()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
