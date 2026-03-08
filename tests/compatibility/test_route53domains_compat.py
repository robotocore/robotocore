"""Route 53 Domains compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client

CONTACT = {
    "FirstName": "Test",
    "LastName": "User",
    "ContactType": "PERSON",
    "AddressLine1": "123 Main St",
    "City": "Seattle",
    "State": "WA",
    "CountryCode": "US",
    "ZipCode": "98101",
    "PhoneNumber": "+1.2065551234",
    "Email": "test@example.com",
}


def _unique_domain():
    return f"{uuid.uuid4().hex[:12]}.com"


def _register(client, domain_name):
    return client.register_domain(
        DomainName=domain_name,
        DurationInYears=2,
        AdminContact=CONTACT,
        RegistrantContact=CONTACT,
        TechContact=CONTACT,
    )


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

    def test_register_domain(self, route53domains):
        """RegisterDomain registers a domain and returns an operation ID."""
        domain = _unique_domain()
        resp = _register(route53domains, domain)
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_domain(self, route53domains):
        """DeleteDomain deletes a registered domain."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.delete_domain(DomainName=domain)
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_domain_detail(self, route53domains):
        """GetDomainDetail returns details for a registered domain."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.get_domain_detail(DomainName=domain)
        assert "DomainName" in resp
        assert resp["DomainName"] == domain

    def test_update_domain_nameservers(self, route53domains):
        """UpdateDomainNameservers updates nameservers for a domain."""
        domain = _unique_domain()
        _register(route53domains, domain)
        resp = route53domains.update_domain_nameservers(
            DomainName=domain,
            Nameservers=[
                {"Name": "ns1.example.com"},
                {"Name": "ns2.example.com"},
            ],
        )
        assert "OperationId" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_operation_detail(self, route53domains):
        """GetOperationDetail returns details for an operation."""
        domain = _unique_domain()
        reg = _register(route53domains, domain)
        op_id = reg["OperationId"]
        resp = route53domains.get_operation_detail(OperationId=op_id)
        assert "OperationId" in resp
        assert resp["OperationId"] == op_id
