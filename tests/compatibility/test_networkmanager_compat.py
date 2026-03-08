"""Network Manager compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def nm():
    return make_client("networkmanager")


@pytest.fixture
def global_network(nm):
    """Create a global network for use in tests, return its metadata."""
    desc = f"test-gn-{uuid.uuid4().hex[:8]}"
    resp = nm.create_global_network(
        Description=desc,
        Tags=[{"Key": "env", "Value": "compat-test"}],
    )
    gn = resp["GlobalNetwork"]
    yield gn


class TestNetworkManagerGlobalNetworks:
    def test_create_global_network(self, nm):
        desc = f"test-{uuid.uuid4().hex[:8]}"
        resp = nm.create_global_network(Description=desc)
        gn = resp["GlobalNetwork"]
        assert "GlobalNetworkId" in gn
        assert "GlobalNetworkArn" in gn
        assert gn["Description"] == desc

    def test_create_global_network_with_tags(self, nm):
        resp = nm.create_global_network(
            Description="tagged-network",
            Tags=[
                {"Key": "team", "Value": "platform"},
                {"Key": "project", "Value": "robotocore"},
            ],
        )
        gn = resp["GlobalNetwork"]
        assert "GlobalNetworkId" in gn
        tags = {t["Key"]: t["Value"] for t in gn.get("Tags", [])}
        assert tags.get("team") == "platform"
        assert tags.get("project") == "robotocore"

    def test_describe_global_networks(self, nm, global_network):
        resp = nm.describe_global_networks()
        gn_ids = [gn["GlobalNetworkId"] for gn in resp["GlobalNetworks"]]
        assert global_network["GlobalNetworkId"] in gn_ids

    def test_describe_global_networks_by_id(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.describe_global_networks(GlobalNetworkIds=[gn_id])
        assert len(resp["GlobalNetworks"]) >= 1
        returned_ids = [gn["GlobalNetworkId"] for gn in resp["GlobalNetworks"]]
        assert gn_id in returned_ids

    def test_describe_global_networks_returns_fields(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.describe_global_networks(GlobalNetworkIds=[gn_id])
        gn = resp["GlobalNetworks"][0]
        assert "GlobalNetworkId" in gn
        assert "GlobalNetworkArn" in gn
        assert "State" in gn


class TestNetworkManagerSites:
    def test_create_site(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.create_site(
            GlobalNetworkId=gn_id,
            Description=f"site-{uuid.uuid4().hex[:8]}",
        )
        site = resp["Site"]
        assert "SiteId" in site
        assert "SiteArn" in site
        assert site["GlobalNetworkId"] == gn_id

    def test_create_site_with_location(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.create_site(
            GlobalNetworkId=gn_id,
            Description="site-with-location",
            Location={
                "Address": "123 Main St",
                "Latitude": "37.7749",
                "Longitude": "-122.4194",
            },
        )
        site = resp["Site"]
        assert "SiteId" in site

    def test_get_sites(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        nm.create_site(GlobalNetworkId=gn_id, Description="list-site-1")
        nm.create_site(GlobalNetworkId=gn_id, Description="list-site-2")
        resp = nm.get_sites(GlobalNetworkId=gn_id)
        assert len(resp["Sites"]) >= 2

    def test_get_sites_empty(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.get_sites(GlobalNetworkId=gn_id)
        assert "Sites" in resp
        assert isinstance(resp["Sites"], list)


class TestNetworkManagerCoreNetworks:
    def test_create_core_network(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.create_core_network(GlobalNetworkId=gn_id)
        cn = resp["CoreNetwork"]
        assert "CoreNetworkId" in cn
        assert "CoreNetworkArn" in cn
        assert cn["GlobalNetworkId"] == gn_id
        # cleanup
        nm.delete_core_network(CoreNetworkId=cn["CoreNetworkId"])

    def test_get_core_network(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.create_core_network(GlobalNetworkId=gn_id)
        cn_id = resp["CoreNetwork"]["CoreNetworkId"]
        try:
            result = nm.get_core_network(CoreNetworkId=cn_id)
            cn = result["CoreNetwork"]
            assert cn["CoreNetworkId"] == cn_id
            assert "CoreNetworkArn" in cn
            assert "GlobalNetworkId" in cn
        finally:
            nm.delete_core_network(CoreNetworkId=cn_id)

    def test_delete_core_network(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.create_core_network(GlobalNetworkId=gn_id)
        cn_id = resp["CoreNetwork"]["CoreNetworkId"]
        del_resp = nm.delete_core_network(CoreNetworkId=cn_id)
        assert "CoreNetwork" in del_resp


class TestNetworkmanagerAutoCoverage:
    """Auto-generated coverage tests for networkmanager."""

    @pytest.fixture
    def client(self):
        return make_client("networkmanager")

    def test_list_core_networks(self, client):
        """ListCoreNetworks returns a response."""
        resp = client.list_core_networks()
        assert "CoreNetworks" in resp
