"""AWS RAM (Resource Access Manager) compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def ram():
    return make_client("ram")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def resource_share(ram):
    """Create a resource share and delete it after the test."""
    name = _unique("compat-share")
    resp = ram.create_resource_share(name=name, allowExternalPrincipals=True)
    share = resp["resourceShare"]
    yield share
    try:
        ram.delete_resource_share(resourceShareArn=share["resourceShareArn"])
    except Exception:
        pass


class TestRAMResourceShareLifecycle:
    def test_get_resource_shares_empty(self, ram):
        """get_resource_shares returns a list (possibly with prior shares)."""
        resp = ram.get_resource_shares(resourceOwner="SELF")
        assert "resourceShares" in resp
        assert isinstance(resp["resourceShares"], list)

    def test_create_resource_share(self, ram):
        name = _unique("create-test")
        resp = ram.create_resource_share(name=name, allowExternalPrincipals=True)
        share = resp["resourceShare"]
        assert share["name"] == name
        assert share["allowExternalPrincipals"] is True
        assert share["status"] == "ACTIVE"
        assert "resourceShareArn" in share
        assert "owningAccountId" in share
        assert "creationTime" in share
        assert "lastUpdatedTime" in share
        # cleanup
        ram.delete_resource_share(resourceShareArn=share["resourceShareArn"])

    def test_create_resource_share_appears_in_list(self, ram, resource_share):
        resp = ram.get_resource_shares(resourceOwner="SELF")
        arns = [s["resourceShareArn"] for s in resp["resourceShares"]]
        assert resource_share["resourceShareArn"] in arns

    def test_update_resource_share_name(self, ram, resource_share):
        new_name = _unique("updated")
        resp = ram.update_resource_share(
            resourceShareArn=resource_share["resourceShareArn"],
            name=new_name,
        )
        updated = resp["resourceShare"]
        assert updated["name"] == new_name
        assert updated["resourceShareArn"] == resource_share["resourceShareArn"]

    def test_update_resource_share_allow_external(self, ram, resource_share):
        resp = ram.update_resource_share(
            resourceShareArn=resource_share["resourceShareArn"],
            allowExternalPrincipals=False,
        )
        updated = resp["resourceShare"]
        assert updated["allowExternalPrincipals"] is False

    def test_delete_resource_share(self, ram):
        name = _unique("delete-test")
        resp = ram.create_resource_share(name=name, allowExternalPrincipals=True)
        arn = resp["resourceShare"]["resourceShareArn"]

        del_resp = ram.delete_resource_share(resourceShareArn=arn)
        assert del_resp["returnValue"] is True

    def test_create_multiple_resource_shares(self, ram):
        name1 = _unique("multi1")
        name2 = _unique("multi2")
        resp1 = ram.create_resource_share(name=name1, allowExternalPrincipals=True)
        resp2 = ram.create_resource_share(name=name2, allowExternalPrincipals=False)
        arn1 = resp1["resourceShare"]["resourceShareArn"]
        arn2 = resp2["resourceShare"]["resourceShareArn"]

        resp = ram.get_resource_shares(resourceOwner="SELF")
        arns = [s["resourceShareArn"] for s in resp["resourceShares"]]
        assert arn1 in arns
        assert arn2 in arns

        # cleanup
        ram.delete_resource_share(resourceShareArn=arn1)
        ram.delete_resource_share(resourceShareArn=arn2)
