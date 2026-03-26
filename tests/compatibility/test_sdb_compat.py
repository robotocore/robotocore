"""Compatibility tests for Amazon SimpleDB (sdb)."""

import uuid

import boto3
import pytest


@pytest.fixture
def sdb():
    return boto3.client(
        "sdb",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture
def domain(sdb):
    """Create a domain for testing and delete it after."""
    name = "compat-test-domain"
    sdb.create_domain(DomainName=name)
    yield name
    try:
        sdb.delete_domain(DomainName=name)
    except Exception:  # noqa: BLE001 - best-effort cleanup
        pass


class TestSdbDomainMetadata:
    def test_domain_metadata_empty(self, sdb, domain):
        """DomainMetadata returns stats for an empty domain."""
        resp = sdb.domain_metadata(DomainName=domain)
        assert "ItemCount" in resp
        assert resp["ItemCount"] == 0
        assert "AttributeNameCount" in resp
        assert resp["AttributeNameCount"] == 0

    def test_domain_metadata_with_items(self, sdb, domain):
        """DomainMetadata reflects items after puts."""
        sdb.put_attributes(
            DomainName=domain,
            ItemName="item1",
            Attributes=[{"Name": "color", "Value": "red"}],
        )
        resp = sdb.domain_metadata(DomainName=domain)
        assert resp["ItemCount"] == 1
        assert resp["AttributeNameCount"] >= 1


class TestSdbDeleteAttributes:
    def test_delete_all_attributes(self, sdb, domain):
        """DeleteAttributes removes all attributes when none specified."""
        sdb.put_attributes(
            DomainName=domain,
            ItemName="item1",
            Attributes=[{"Name": "color", "Value": "blue"}],
        )
        sdb.delete_attributes(DomainName=domain, ItemName="item1")
        resp = sdb.get_attributes(DomainName=domain, ItemName="item1")
        assert resp.get("Attributes", []) == []

    def test_delete_specific_attribute(self, sdb, domain):
        """DeleteAttributes with Attributes removes only specified attributes."""
        sdb.put_attributes(
            DomainName=domain,
            ItemName="item1",
            Attributes=[
                {"Name": "color", "Value": "blue"},
                {"Name": "size", "Value": "large"},
            ],
        )
        sdb.delete_attributes(
            DomainName=domain,
            ItemName="item1",
            Attributes=[{"Name": "color", "Value": "blue"}],
        )
        resp = sdb.get_attributes(DomainName=domain, ItemName="item1")
        attrs = {a["Name"]: a["Value"] for a in resp.get("Attributes", [])}
        assert "color" not in attrs
        assert attrs.get("size") == "large"


class TestSdbBatchPutAttributes:
    def test_batch_put_multiple_items(self, sdb, domain):
        """BatchPutAttributes stores attributes for multiple items."""
        items = [
            {"Name": "alpha", "Attributes": [{"Name": "val", "Value": "1"}]},
            {"Name": "beta", "Attributes": [{"Name": "val", "Value": "2"}]},
        ]
        sdb.batch_put_attributes(DomainName=domain, Items=items)
        alpha = sdb.get_attributes(DomainName=domain, ItemName="alpha")
        beta = sdb.get_attributes(DomainName=domain, ItemName="beta")
        alpha_vals = {a["Name"]: a["Value"] for a in alpha.get("Attributes", [])}
        beta_vals = {a["Name"]: a["Value"] for a in beta.get("Attributes", [])}
        assert alpha_vals.get("val") == "1"
        assert beta_vals.get("val") == "2"


class TestSdbBatchDeleteAttributes:
    def test_batch_delete_multiple_items(self, sdb, domain):
        """BatchDeleteAttributes removes attributes from multiple items."""
        items = [
            {"Name": "x", "Attributes": [{"Name": "k", "Value": "v"}]},
            {"Name": "y", "Attributes": [{"Name": "k", "Value": "v"}]},
        ]
        sdb.batch_put_attributes(DomainName=domain, Items=items)
        sdb.batch_delete_attributes(DomainName=domain, Items=items)
        x = sdb.get_attributes(DomainName=domain, ItemName="x")
        assert x.get("Attributes", []) == []


class TestSdbSelect:
    def test_select_all_from_domain(self, sdb, domain):
        """Select * FROM domain returns all items."""
        items = [
            {"Name": "p", "Attributes": [{"Name": "n", "Value": "1"}]},
            {"Name": "q", "Attributes": [{"Name": "n", "Value": "2"}]},
        ]
        sdb.batch_put_attributes(DomainName=domain, Items=items)
        resp = sdb.select(SelectExpression=f"select * from `{domain}`")
        assert "Items" in resp
        item_names = {i["Name"] for i in resp["Items"]}
        assert "p" in item_names
        assert "q" in item_names
        assert len(item_names) == 2


class TestSdbCreateDomain:
    """Tests for CreateDomain operation."""

    def test_create_domain(self, sdb):
        """CreateDomain succeeds and the domain appears in ListDomains."""
        name = f"test-create-{uuid.uuid4().hex[:8]}"
        sdb.create_domain(DomainName=name)
        try:
            resp = sdb.list_domains()
            assert "DomainNames" in resp
            assert name in resp["DomainNames"]
        finally:
            sdb.delete_domain(DomainName=name)


class TestSdbDeleteDomain:
    """Tests for DeleteDomain operation."""

    def test_delete_domain(self, sdb):
        """DeleteDomain removes the domain from the list."""
        name = f"test-del-{uuid.uuid4().hex[:8]}"
        sdb.create_domain(DomainName=name)
        sdb.delete_domain(DomainName=name)
        resp = sdb.list_domains()
        assert name not in resp.get("DomainNames", [])


class TestSdbListDomains:
    """Tests for ListDomains operation."""

    def test_list_domains(self, sdb):
        """ListDomains returns 200 and DomainNames key when domains exist."""
        name = f"test-list-{uuid.uuid4().hex[:8]}"
        sdb.create_domain(DomainName=name)
        try:
            resp = sdb.list_domains()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "DomainNames" in resp
            assert name in resp["DomainNames"]
        finally:
            sdb.delete_domain(DomainName=name)
