"""Compatibility tests for VPC Lattice service."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def vpclattice_client():
    return make_client("vpc-lattice")


@pytest.fixture
def service_name():
    return f"test-svc-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def service_network_name():
    return f"test-net-{uuid.uuid4().hex[:8]}"


class TestVpcLatticeService:
    """Tests for VPC Lattice service operations."""

    def test_create_service(self, vpclattice_client, service_name):
        resp = vpclattice_client.create_service(name=service_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["name"] == service_name
        assert "id" in resp
        assert "arn" in resp
        assert resp["status"] == "ACTIVE"
        assert "dnsEntry" in resp
        assert "domainName" in resp["dnsEntry"]

    def test_create_service_returns_arn_with_correct_format(self, vpclattice_client, service_name):
        resp = vpclattice_client.create_service(name=service_name)
        arn = resp["arn"]
        assert arn.startswith("arn:aws:vpc-lattice:")
        assert ":service/" in arn

    def test_list_services(self, vpclattice_client, service_name):
        vpclattice_client.create_service(name=service_name)
        resp = vpclattice_client.list_services()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "items" in resp
        names = [s["name"] for s in resp["items"]]
        assert service_name in names

    def test_get_service(self, vpclattice_client, service_name):
        create_resp = vpclattice_client.create_service(name=service_name)
        svc_id = create_resp["id"]

        get_resp = vpclattice_client.get_service(serviceIdentifier=svc_id)
        assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert get_resp["id"] == svc_id
        assert get_resp["name"] == service_name
        assert get_resp["status"] == "ACTIVE"
        assert get_resp["arn"] == create_resp["arn"]

    def test_get_service_has_dns_entry(self, vpclattice_client, service_name):
        create_resp = vpclattice_client.create_service(name=service_name)
        svc_id = create_resp["id"]

        get_resp = vpclattice_client.get_service(serviceIdentifier=svc_id)
        assert "dnsEntry" in get_resp
        assert "domainName" in get_resp["dnsEntry"]


class TestVpcLatticeServiceNetwork:
    """Tests for VPC Lattice service network operations."""

    def test_create_service_network(self, vpclattice_client, service_network_name):
        resp = vpclattice_client.create_service_network(name=service_network_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["name"] == service_network_name
        assert "id" in resp
        assert "arn" in resp

    def test_create_service_network_arn_format(self, vpclattice_client, service_network_name):
        resp = vpclattice_client.create_service_network(name=service_network_name)
        arn = resp["arn"]
        assert arn.startswith("arn:aws:vpc-lattice:")
        assert ":servicenetwork/" in arn

    def test_list_service_networks(self, vpclattice_client, service_network_name):
        vpclattice_client.create_service_network(name=service_network_name)
        resp = vpclattice_client.list_service_networks()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "items" in resp
        names = [n["name"] for n in resp["items"]]
        assert service_network_name in names
