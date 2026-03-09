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

    def test_delete_site(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.create_site(GlobalNetworkId=gn_id, Description=f"del-site-{uuid.uuid4().hex[:8]}")
        site_id = resp["Site"]["SiteId"]
        del_resp = nm.delete_site(GlobalNetworkId=gn_id, SiteId=site_id)
        assert "Site" in del_resp
        assert del_resp["Site"]["SiteId"] == site_id

    def test_delete_site_removes_from_list(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.create_site(GlobalNetworkId=gn_id, Description=f"del-list-{uuid.uuid4().hex[:8]}")
        site_id = resp["Site"]["SiteId"]
        nm.delete_site(GlobalNetworkId=gn_id, SiteId=site_id)
        sites_resp = nm.get_sites(GlobalNetworkId=gn_id)
        site_ids = [s["SiteId"] for s in sites_resp["Sites"]]
        assert site_id not in site_ids

    def test_create_site_with_tags(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.create_site(
            GlobalNetworkId=gn_id,
            Description=f"tagged-site-{uuid.uuid4().hex[:8]}",
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "infra"}],
        )
        site = resp["Site"]
        assert "SiteId" in site
        tags = {t["Key"]: t["Value"] for t in site.get("Tags", [])}
        assert tags.get("env") == "test"
        assert tags.get("team") == "infra"


class TestNetworkManagerDevices:
    def test_create_device(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        desc = f"dev-{uuid.uuid4().hex[:8]}"
        resp = nm.create_device(GlobalNetworkId=gn_id, Description=desc)
        device = resp["Device"]
        assert "DeviceId" in device
        assert "DeviceArn" in device
        assert device["GlobalNetworkId"] == gn_id
        assert device["Description"] == desc

    def test_create_device_with_tags(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.create_device(
            GlobalNetworkId=gn_id,
            Description=f"tagged-dev-{uuid.uuid4().hex[:8]}",
            Tags=[{"Key": "role", "Value": "router"}],
        )
        device = resp["Device"]
        assert "DeviceId" in device
        tags = {t["Key"]: t["Value"] for t in device.get("Tags", [])}
        assert tags.get("role") == "router"

    def test_create_device_with_site(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        site = nm.create_site(GlobalNetworkId=gn_id, Description="dev-site")["Site"]
        resp = nm.create_device(
            GlobalNetworkId=gn_id,
            Description=f"dev-with-site-{uuid.uuid4().hex[:8]}",
            SiteId=site["SiteId"],
        )
        device = resp["Device"]
        assert "DeviceId" in device
        assert device.get("SiteId") == site["SiteId"]

    def test_get_devices_returns_created(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        d1 = nm.create_device(GlobalNetworkId=gn_id, Description="list-dev-1")["Device"]
        d2 = nm.create_device(GlobalNetworkId=gn_id, Description="list-dev-2")["Device"]
        resp = nm.get_devices(GlobalNetworkId=gn_id)
        dev_ids = [d["DeviceId"] for d in resp["Devices"]]
        assert d1["DeviceId"] in dev_ids
        assert d2["DeviceId"] in dev_ids

    def test_delete_device(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        desc = f"del-dev-{uuid.uuid4().hex[:8]}"
        device = nm.create_device(GlobalNetworkId=gn_id, Description=desc)["Device"]
        del_resp = nm.delete_device(GlobalNetworkId=gn_id, DeviceId=device["DeviceId"])
        assert "Device" in del_resp
        assert del_resp["Device"]["DeviceId"] == device["DeviceId"]

    def test_delete_device_removes_from_list(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        desc = f"del-list-dev-{uuid.uuid4().hex[:8]}"
        device = nm.create_device(GlobalNetworkId=gn_id, Description=desc)["Device"]
        dev_id = device["DeviceId"]
        nm.delete_device(GlobalNetworkId=gn_id, DeviceId=dev_id)
        resp = nm.get_devices(GlobalNetworkId=gn_id)
        dev_ids = [d["DeviceId"] for d in resp["Devices"]]
        assert dev_id not in dev_ids


class TestNetworkManagerLinks:
    @pytest.fixture
    def site_in_network(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        desc = f"link-site-{uuid.uuid4().hex[:8]}"
        resp = nm.create_site(GlobalNetworkId=gn_id, Description=desc)
        return resp["Site"]

    def test_create_link(self, nm, global_network, site_in_network):
        gn_id = global_network["GlobalNetworkId"]
        desc = f"link-{uuid.uuid4().hex[:8]}"
        resp = nm.create_link(
            GlobalNetworkId=gn_id,
            SiteId=site_in_network["SiteId"],
            Bandwidth={"UploadSpeed": 10, "DownloadSpeed": 50},
            Description=desc,
        )
        link = resp["Link"]
        assert "LinkId" in link
        assert "LinkArn" in link
        assert link["GlobalNetworkId"] == gn_id
        assert link["SiteId"] == site_in_network["SiteId"]
        assert link["Description"] == desc

    def test_create_link_with_tags(self, nm, global_network, site_in_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.create_link(
            GlobalNetworkId=gn_id,
            SiteId=site_in_network["SiteId"],
            Bandwidth={"UploadSpeed": 100, "DownloadSpeed": 200},
            Description=f"tagged-link-{uuid.uuid4().hex[:8]}",
            Tags=[{"Key": "type", "Value": "fiber"}],
        )
        link = resp["Link"]
        assert "LinkId" in link
        tags = {t["Key"]: t["Value"] for t in link.get("Tags", [])}
        assert tags.get("type") == "fiber"

    def test_create_link_bandwidth(self, nm, global_network, site_in_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.create_link(
            GlobalNetworkId=gn_id,
            SiteId=site_in_network["SiteId"],
            Bandwidth={"UploadSpeed": 25, "DownloadSpeed": 100},
            Description=f"bw-link-{uuid.uuid4().hex[:8]}",
        )
        link = resp["Link"]
        assert link["Bandwidth"]["UploadSpeed"] == 25
        assert link["Bandwidth"]["DownloadSpeed"] == 100

    def test_get_links_returns_created(self, nm, global_network, site_in_network):
        gn_id = global_network["GlobalNetworkId"]
        l1 = nm.create_link(
            GlobalNetworkId=gn_id,
            SiteId=site_in_network["SiteId"],
            Bandwidth={"UploadSpeed": 10, "DownloadSpeed": 50},
            Description="list-link-1",
        )["Link"]
        l2 = nm.create_link(
            GlobalNetworkId=gn_id,
            SiteId=site_in_network["SiteId"],
            Bandwidth={"UploadSpeed": 10, "DownloadSpeed": 50},
            Description="list-link-2",
        )["Link"]
        resp = nm.get_links(GlobalNetworkId=gn_id)
        link_ids = [ln["LinkId"] for ln in resp["Links"]]
        assert l1["LinkId"] in link_ids
        assert l2["LinkId"] in link_ids

    def test_delete_link(self, nm, global_network, site_in_network):
        gn_id = global_network["GlobalNetworkId"]
        link = nm.create_link(
            GlobalNetworkId=gn_id,
            SiteId=site_in_network["SiteId"],
            Bandwidth={"UploadSpeed": 10, "DownloadSpeed": 50},
            Description=f"del-link-{uuid.uuid4().hex[:8]}",
        )["Link"]
        del_resp = nm.delete_link(GlobalNetworkId=gn_id, LinkId=link["LinkId"])
        assert "Link" in del_resp
        assert del_resp["Link"]["LinkId"] == link["LinkId"]

    def test_delete_link_removes_from_list(self, nm, global_network, site_in_network):
        gn_id = global_network["GlobalNetworkId"]
        link = nm.create_link(
            GlobalNetworkId=gn_id,
            SiteId=site_in_network["SiteId"],
            Bandwidth={"UploadSpeed": 10, "DownloadSpeed": 50},
            Description=f"del-list-link-{uuid.uuid4().hex[:8]}",
        )["Link"]
        link_id = link["LinkId"]
        nm.delete_link(GlobalNetworkId=gn_id, LinkId=link_id)
        resp = nm.get_links(GlobalNetworkId=gn_id)
        link_ids = [ln["LinkId"] for ln in resp["Links"]]
        assert link_id not in link_ids


class TestNetworkManagerTagging:
    def test_tag_resource(self, nm):
        desc = f"tag-test-{uuid.uuid4().hex[:8]}"
        gn = nm.create_global_network(Description=desc)["GlobalNetwork"]
        gn_arn = gn["GlobalNetworkArn"]
        resp = nm.tag_resource(
            ResourceArn=gn_arn,
            Tags=[{"Key": "app", "Value": "robotocore"}, {"Key": "env", "Value": "test"}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify tags were applied
        tags_resp = nm.list_tags_for_resource(ResourceArn=gn_arn)
        tag_map = {t["Key"]: t["Value"] for t in tags_resp["TagList"]}
        assert tag_map["app"] == "robotocore"
        assert tag_map["env"] == "test"

    def test_untag_resource(self, nm):
        gn = nm.create_global_network(
            Description=f"untag-test-{uuid.uuid4().hex[:8]}",
            Tags=[{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}],
        )["GlobalNetwork"]
        gn_arn = gn["GlobalNetworkArn"]
        resp = nm.untag_resource(ResourceArn=gn_arn, TagKeys=["a"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify tag was removed
        tags_resp = nm.list_tags_for_resource(ResourceArn=gn_arn)
        tag_map = {t["Key"]: t["Value"] for t in tags_resp["TagList"]}
        assert "a" not in tag_map
        assert tag_map.get("b") == "2"

    def test_tag_resource_overwrites_existing(self, nm):
        gn = nm.create_global_network(
            Description=f"tag-overwrite-{uuid.uuid4().hex[:8]}",
            Tags=[{"Key": "env", "Value": "old"}],
        )["GlobalNetwork"]
        gn_arn = gn["GlobalNetworkArn"]
        nm.tag_resource(ResourceArn=gn_arn, Tags=[{"Key": "env", "Value": "new"}])
        tags_resp = nm.list_tags_for_resource(ResourceArn=gn_arn)
        tag_map = {t["Key"]: t["Value"] for t in tags_resp["TagList"]}
        assert tag_map["env"] == "new"


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

    def test_get_devices(self, client):
        """GetDevices returns a response with Devices key."""
        desc = f"test-gn-{uuid.uuid4().hex[:8]}"
        gn_resp = client.create_global_network(Description=desc)
        gn_id = gn_resp["GlobalNetwork"]["GlobalNetworkId"]
        resp = client.get_devices(GlobalNetworkId=gn_id)
        assert "Devices" in resp
        assert isinstance(resp["Devices"], list)

    def test_get_links(self, client):
        """GetLinks returns a response with Links key."""
        desc = f"test-gn-{uuid.uuid4().hex[:8]}"
        gn_resp = client.create_global_network(Description=desc)
        gn_id = gn_resp["GlobalNetwork"]["GlobalNetworkId"]
        resp = client.get_links(GlobalNetworkId=gn_id)
        assert "Links" in resp
        assert isinstance(resp["Links"], list)

    def test_list_tags_for_resource(self, client):
        """ListTagsForResource returns tags on a global network."""
        desc = f"test-gn-{uuid.uuid4().hex[:8]}"
        gn_resp = client.create_global_network(
            Description=desc,
            Tags=[{"Key": "env", "Value": "test"}],
        )
        gn_arn = gn_resp["GlobalNetwork"]["GlobalNetworkArn"]
        resp = client.list_tags_for_resource(ResourceArn=gn_arn)
        assert "TagList" in resp
        tag_map = {t["Key"]: t["Value"] for t in resp["TagList"]}
        assert tag_map.get("env") == "test"
