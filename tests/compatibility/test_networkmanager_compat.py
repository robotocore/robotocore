"""Network Manager compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

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
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.describe_global_networks(GlobalNetworkIds=[gn_id])
        gn_ids = [gn["GlobalNetworkId"] for gn in resp["GlobalNetworks"]]
        assert gn_id in gn_ids

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


class TestNetworkManagerListOperations:
    """Tests for NetworkManager list operations."""

    def test_list_attachments(self, nm):
        """ListAttachments returns empty list."""
        resp = nm.list_attachments()
        assert "Attachments" in resp
        assert isinstance(resp["Attachments"], list)

    def test_list_connect_peers(self, nm):
        """ListConnectPeers returns empty list."""
        resp = nm.list_connect_peers()
        assert "ConnectPeers" in resp
        assert isinstance(resp["ConnectPeers"], list)

    def test_list_peerings(self, nm):
        """ListPeerings returns empty list."""
        resp = nm.list_peerings()
        assert "Peerings" in resp
        assert isinstance(resp["Peerings"], list)

    def test_list_organization_service_access_status(self, nm):
        """ListOrganizationServiceAccessStatus returns OrganizationStatus."""
        resp = nm.list_organization_service_access_status()
        assert "OrganizationStatus" in resp

    def test_list_core_network_policy_versions(self, nm, global_network):
        """ListCoreNetworkPolicyVersions returns list for a real core network."""
        gn_id = global_network["GlobalNetworkId"]
        cn = nm.create_core_network(GlobalNetworkId=gn_id)["CoreNetwork"]
        cn_id = cn["CoreNetworkId"]
        try:
            resp = nm.list_core_network_policy_versions(CoreNetworkId=cn_id)
            assert "CoreNetworkPolicyVersions" in resp
            assert isinstance(resp["CoreNetworkPolicyVersions"], list)
        finally:
            nm.delete_core_network(CoreNetworkId=cn_id)

    def test_list_core_network_prefix_list_associations(self, nm, global_network):
        """ListCoreNetworkPrefixListAssociations returns list."""
        gn_id = global_network["GlobalNetworkId"]
        cn = nm.create_core_network(GlobalNetworkId=gn_id)["CoreNetwork"]
        cn_id = cn["CoreNetworkId"]
        try:
            resp = nm.list_core_network_prefix_list_associations(CoreNetworkId=cn_id)
            assert "PrefixListAssociations" in resp
            assert isinstance(resp["PrefixListAssociations"], list)
        finally:
            nm.delete_core_network(CoreNetworkId=cn_id)


class TestNetworkManagerGetWithGlobalNetwork:
    """Tests for Get operations that take a GlobalNetworkId and return lists."""

    def test_get_connections(self, nm, global_network):
        """GetConnections returns empty Connections list."""
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.get_connections(GlobalNetworkId=gn_id)
        assert "Connections" in resp
        assert isinstance(resp["Connections"], list)

    def test_get_connect_peer_associations(self, nm, global_network):
        """GetConnectPeerAssociations returns empty list."""
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.get_connect_peer_associations(GlobalNetworkId=gn_id)
        assert "ConnectPeerAssociations" in resp
        assert isinstance(resp["ConnectPeerAssociations"], list)

    def test_get_customer_gateway_associations(self, nm, global_network):
        """GetCustomerGatewayAssociations returns empty list."""
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.get_customer_gateway_associations(GlobalNetworkId=gn_id)
        assert "CustomerGatewayAssociations" in resp
        assert isinstance(resp["CustomerGatewayAssociations"], list)

    def test_get_link_associations(self, nm, global_network):
        """GetLinkAssociations returns empty list."""
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.get_link_associations(GlobalNetworkId=gn_id)
        assert "LinkAssociations" in resp
        assert isinstance(resp["LinkAssociations"], list)

    def test_get_network_resource_counts(self, nm, global_network):
        """GetNetworkResourceCounts returns list."""
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.get_network_resource_counts(GlobalNetworkId=gn_id)
        assert "NetworkResourceCounts" in resp
        assert isinstance(resp["NetworkResourceCounts"], list)

    def test_get_network_resources(self, nm, global_network):
        """GetNetworkResources returns list."""
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.get_network_resources(GlobalNetworkId=gn_id)
        assert "NetworkResources" in resp
        assert isinstance(resp["NetworkResources"], list)

    def test_get_network_resource_relationships(self, nm, global_network):
        """GetNetworkResourceRelationships returns list."""
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.get_network_resource_relationships(GlobalNetworkId=gn_id)
        assert "Relationships" in resp
        assert isinstance(resp["Relationships"], list)

    def test_get_network_telemetry(self, nm, global_network):
        """GetNetworkTelemetry returns list."""
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.get_network_telemetry(GlobalNetworkId=gn_id)
        assert "NetworkTelemetry" in resp
        assert isinstance(resp["NetworkTelemetry"], list)

    def test_get_transit_gateway_connect_peer_associations(self, nm, global_network):
        """GetTransitGatewayConnectPeerAssociations returns empty list."""
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.get_transit_gateway_connect_peer_associations(GlobalNetworkId=gn_id)
        assert "TransitGatewayConnectPeerAssociations" in resp
        assert isinstance(resp["TransitGatewayConnectPeerAssociations"], list)

    def test_get_transit_gateway_registrations(self, nm, global_network):
        """GetTransitGatewayRegistrations returns empty list."""
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.get_transit_gateway_registrations(GlobalNetworkId=gn_id)
        assert "TransitGatewayRegistrations" in resp
        assert isinstance(resp["TransitGatewayRegistrations"], list)

    def test_get_resource_policy(self, nm, global_network):
        """GetResourcePolicy returns a response (possibly empty)."""
        gn_arn = global_network["GlobalNetworkArn"]
        resp = nm.get_resource_policy(ResourceArn=gn_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestNetworkManagerGetWithCoreNetwork:
    """Tests for Get operations that need a core network."""

    @pytest.fixture
    def core_network(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        cn = nm.create_core_network(GlobalNetworkId=gn_id)["CoreNetwork"]
        yield cn
        nm.delete_core_network(CoreNetworkId=cn["CoreNetworkId"])

    def test_get_core_network_change_events(self, nm, core_network):
        """GetCoreNetworkChangeEvents returns list."""
        cn_id = core_network["CoreNetworkId"]
        resp = nm.get_core_network_change_events(CoreNetworkId=cn_id, PolicyVersionId=1)
        assert "CoreNetworkChangeEvents" in resp
        assert isinstance(resp["CoreNetworkChangeEvents"], list)

    def test_get_core_network_change_set(self, nm, core_network):
        """GetCoreNetworkChangeSet returns list."""
        cn_id = core_network["CoreNetworkId"]
        resp = nm.get_core_network_change_set(CoreNetworkId=cn_id, PolicyVersionId=1)
        assert "CoreNetworkChanges" in resp
        assert isinstance(resp["CoreNetworkChanges"], list)

    def test_get_network_routes(self, nm, global_network, core_network):
        """GetNetworkRoutes returns route data."""
        gn_id = global_network["GlobalNetworkId"]
        cn_id = core_network["CoreNetworkId"]
        resp = nm.get_network_routes(
            GlobalNetworkId=gn_id,
            RouteTableIdentifier={
                "CoreNetworkSegmentEdge": {
                    "CoreNetworkId": cn_id,
                    "SegmentName": "seg1",
                    "EdgeLocation": "us-east-1",
                }
            },
        )
        assert "NetworkRoutes" in resp
        assert isinstance(resp["NetworkRoutes"], list)
        assert "RouteTableArn" in resp


class TestNetworkManagerGetWithFakeIds:
    """Tests for Get operations that return NotFoundException on fake IDs."""

    def test_get_connect_attachment_not_found(self, nm):
        """GetConnectAttachment with fake ID returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            nm.get_connect_attachment(AttachmentId="attachment-fake123456")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_connect_peer_not_found(self, nm):
        """GetConnectPeer with fake ID returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            nm.get_connect_peer(ConnectPeerId="cp-fake123456")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_direct_connect_gateway_attachment_not_found(self, nm):
        """GetDirectConnectGatewayAttachment with fake ID returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            nm.get_direct_connect_gateway_attachment(AttachmentId="attachment-fake789")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_site_to_site_vpn_attachment_not_found(self, nm):
        """GetSiteToSiteVpnAttachment with fake ID returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            nm.get_site_to_site_vpn_attachment(AttachmentId="attachment-fakevpn")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_transit_gateway_peering_not_found(self, nm):
        """GetTransitGatewayPeering with fake ID returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            nm.get_transit_gateway_peering(PeeringId="peering-fake123")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_transit_gateway_route_table_attachment_not_found(self, nm):
        """GetTransitGatewayRouteTableAttachment with fake ID."""
        with pytest.raises(ClientError) as exc:
            nm.get_transit_gateway_route_table_attachment(AttachmentId="attachment-fakert")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_vpc_attachment_not_found(self, nm):
        """GetVpcAttachment with fake ID returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            nm.get_vpc_attachment(AttachmentId="attachment-fakevpc")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_route_analysis_not_found(self, nm, global_network):
        """GetRouteAnalysis with fake ID returns NotFoundException."""
        gn_id = global_network["GlobalNetworkId"]
        with pytest.raises(ClientError) as exc:
            nm.get_route_analysis(GlobalNetworkId=gn_id, RouteAnalysisId="ra-fake123")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestNetworkManagerCoreNetworkPolicyAndRouting:
    """Tests for GetCoreNetworkPolicy, ListAttachmentRoutingPolicyAssociations,
    and ListCoreNetworkRoutingInformation."""

    @pytest.fixture
    def core_network(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        cn = nm.create_core_network(GlobalNetworkId=gn_id)["CoreNetwork"]
        yield cn
        nm.delete_core_network(CoreNetworkId=cn["CoreNetworkId"])

    def test_get_core_network_policy_not_found(self, nm, core_network):
        """GetCoreNetworkPolicy raises NotFoundException when no policy exists."""
        cn_id = core_network["CoreNetworkId"]
        with pytest.raises(ClientError) as exc:
            nm.get_core_network_policy(CoreNetworkId=cn_id)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_list_attachment_routing_policy_associations(self, nm, core_network):
        """ListAttachmentRoutingPolicyAssociations returns associations list."""
        cn_id = core_network["CoreNetworkId"]
        resp = nm.list_attachment_routing_policy_associations(
            CoreNetworkId=cn_id, AttachmentId="attachment-fake123"
        )
        assert "AttachmentRoutingPolicyAssociations" in resp
        assert isinstance(resp["AttachmentRoutingPolicyAssociations"], list)

    def test_list_core_network_routing_information(self, nm, core_network):
        """ListCoreNetworkRoutingInformation returns routing info list."""
        cn_id = core_network["CoreNetworkId"]
        resp = nm.list_core_network_routing_information(
            CoreNetworkId=cn_id, SegmentName="seg1", EdgeLocation="us-east-1"
        )
        assert "CoreNetworkRoutingInformation" in resp
        assert isinstance(resp["CoreNetworkRoutingInformation"], list)


class TestNetworkManagerConnectionOps:
    """Tests for Connection CRUD operations."""

    def test_create_connection(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        d1 = nm.create_device(GlobalNetworkId=gn_id, Description="conn-d1")["Device"]
        d2 = nm.create_device(GlobalNetworkId=gn_id, Description="conn-d2")["Device"]
        resp = nm.create_connection(
            GlobalNetworkId=gn_id,
            DeviceId=d1["DeviceId"],
            ConnectedDeviceId=d2["DeviceId"],
            Description="test-connection",
        )
        conn = resp["Connection"]
        assert "ConnectionId" in conn
        assert "ConnectionArn" in conn
        assert conn["GlobalNetworkId"] == gn_id
        assert conn["DeviceId"] == d1["DeviceId"]
        assert conn["ConnectedDeviceId"] == d2["DeviceId"]

    def test_update_connection(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        d1 = nm.create_device(GlobalNetworkId=gn_id, Description="upd-conn-d1")["Device"]
        d2 = nm.create_device(GlobalNetworkId=gn_id, Description="upd-conn-d2")["Device"]
        conn = nm.create_connection(
            GlobalNetworkId=gn_id,
            DeviceId=d1["DeviceId"],
            ConnectedDeviceId=d2["DeviceId"],
        )["Connection"]
        resp = nm.update_connection(
            GlobalNetworkId=gn_id,
            ConnectionId=conn["ConnectionId"],
            Description="updated-conn",
        )
        assert resp["Connection"]["Description"] == "updated-conn"
        assert resp["Connection"]["ConnectionId"] == conn["ConnectionId"]

    def test_delete_connection(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        d1 = nm.create_device(GlobalNetworkId=gn_id, Description="del-conn-d1")["Device"]
        d2 = nm.create_device(GlobalNetworkId=gn_id, Description="del-conn-d2")["Device"]
        conn = nm.create_connection(
            GlobalNetworkId=gn_id,
            DeviceId=d1["DeviceId"],
            ConnectedDeviceId=d2["DeviceId"],
        )["Connection"]
        resp = nm.delete_connection(GlobalNetworkId=gn_id, ConnectionId=conn["ConnectionId"])
        assert "Connection" in resp
        assert resp["Connection"]["ConnectionId"] == conn["ConnectionId"]


class TestNetworkManagerUpdateOps:
    """Tests for Update operations on global network, device, site, link."""

    def test_update_global_network(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.update_global_network(GlobalNetworkId=gn_id, Description="updated-gn")
        assert resp["GlobalNetwork"]["Description"] == "updated-gn"
        assert resp["GlobalNetwork"]["GlobalNetworkId"] == gn_id

    def test_delete_global_network(self, nm):
        gn = nm.create_global_network(Description="to-delete")["GlobalNetwork"]
        resp = nm.delete_global_network(GlobalNetworkId=gn["GlobalNetworkId"])
        assert "GlobalNetwork" in resp
        assert resp["GlobalNetwork"]["GlobalNetworkId"] == gn["GlobalNetworkId"]

    def test_update_device(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        dev = nm.create_device(GlobalNetworkId=gn_id, Description="orig")["Device"]
        resp = nm.update_device(
            GlobalNetworkId=gn_id, DeviceId=dev["DeviceId"], Description="updated-dev"
        )
        assert resp["Device"]["Description"] == "updated-dev"
        assert resp["Device"]["DeviceId"] == dev["DeviceId"]

    def test_update_site(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        site = nm.create_site(GlobalNetworkId=gn_id, Description="orig-site")["Site"]
        resp = nm.update_site(
            GlobalNetworkId=gn_id, SiteId=site["SiteId"], Description="updated-site"
        )
        assert resp["Site"]["Description"] == "updated-site"
        assert resp["Site"]["SiteId"] == site["SiteId"]

    def test_update_link(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        site = nm.create_site(GlobalNetworkId=gn_id, Description="link-site")["Site"]
        link = nm.create_link(
            GlobalNetworkId=gn_id,
            SiteId=site["SiteId"],
            Bandwidth={"UploadSpeed": 10, "DownloadSpeed": 50},
            Description="orig-link",
        )["Link"]
        resp = nm.update_link(
            GlobalNetworkId=gn_id, LinkId=link["LinkId"], Description="updated-link"
        )
        assert resp["Link"]["Description"] == "updated-link"
        assert resp["Link"]["LinkId"] == link["LinkId"]


class TestNetworkManagerLinkAssociationOps:
    """Tests for AssociateLink and DisassociateLink."""

    def test_associate_and_disassociate_link(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        site = nm.create_site(GlobalNetworkId=gn_id, Description="assoc-site")["Site"]
        dev = nm.create_device(GlobalNetworkId=gn_id, Description="assoc-dev")["Device"]
        link = nm.create_link(
            GlobalNetworkId=gn_id,
            SiteId=site["SiteId"],
            Bandwidth={"UploadSpeed": 10, "DownloadSpeed": 50},
        )["Link"]

        resp = nm.associate_link(
            GlobalNetworkId=gn_id, DeviceId=dev["DeviceId"], LinkId=link["LinkId"]
        )
        assoc = resp["LinkAssociation"]
        assert assoc["GlobalNetworkId"] == gn_id
        assert assoc["DeviceId"] == dev["DeviceId"]
        assert assoc["LinkId"] == link["LinkId"]

        resp2 = nm.disassociate_link(
            GlobalNetworkId=gn_id, DeviceId=dev["DeviceId"], LinkId=link["LinkId"]
        )
        assert "LinkAssociation" in resp2


class TestNetworkManagerResourcePolicyOps:
    """Tests for PutResourcePolicy and DeleteResourcePolicy."""

    def test_put_and_delete_resource_policy(self, nm, global_network):
        import json

        gn_arn = global_network["GlobalNetworkArn"]
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "networkmanager:GetCoreNetwork",
                        "Resource": "*",
                    }
                ],
            }
        )
        resp = nm.put_resource_policy(ResourceArn=gn_arn, PolicyDocument=policy)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        resp2 = nm.delete_resource_policy(ResourceArn=gn_arn)
        assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestNetworkManagerCoreNetworkPolicyOps:
    """Tests for PutCoreNetworkPolicy, UpdateCoreNetwork, DeleteCoreNetworkPolicyVersion."""

    @pytest.fixture
    def core_network(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        cn = nm.create_core_network(GlobalNetworkId=gn_id)["CoreNetwork"]
        yield cn
        nm.delete_core_network(CoreNetworkId=cn["CoreNetworkId"])

    def test_put_core_network_policy(self, nm, core_network):
        import json

        cn_id = core_network["CoreNetworkId"]
        policy = json.dumps(
            {
                "version": "2021.12",
                "core-network-configuration": {
                    "asn-ranges": ["64512-65534"],
                    "edge-locations": [{"location": "us-east-1"}],
                },
                "segments": [{"name": "prod", "edge-locations": ["us-east-1"]}],
            }
        )
        resp = nm.put_core_network_policy(CoreNetworkId=cn_id, PolicyDocument=policy)
        assert "CoreNetworkPolicy" in resp
        assert resp["CoreNetworkPolicy"]["CoreNetworkId"] == cn_id

    def test_update_core_network(self, nm, core_network):
        cn_id = core_network["CoreNetworkId"]
        resp = nm.update_core_network(CoreNetworkId=cn_id, Description="updated-cn")
        assert "CoreNetwork" in resp
        assert resp["CoreNetwork"]["CoreNetworkId"] == cn_id

    def test_delete_core_network_policy_version(self, nm, core_network):
        import json

        cn_id = core_network["CoreNetworkId"]
        policy = json.dumps(
            {
                "version": "2021.12",
                "core-network-configuration": {
                    "asn-ranges": ["64512-65534"],
                    "edge-locations": [{"location": "us-east-1"}],
                },
                "segments": [{"name": "prod", "edge-locations": ["us-east-1"]}],
            }
        )
        nm.put_core_network_policy(CoreNetworkId=cn_id, PolicyDocument=policy)
        resp = nm.delete_core_network_policy_version(CoreNetworkId=cn_id, PolicyVersionId=1)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_execute_core_network_change_set_not_found(self, nm, core_network):
        """ExecuteCoreNetworkChangeSet raises NotFoundException for missing policy."""
        cn_id = core_network["CoreNetworkId"]
        with pytest.raises(ClientError) as exc:
            nm.execute_core_network_change_set(CoreNetworkId=cn_id, PolicyVersionId=999)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestNetworkManagerAttachmentOps:
    """Tests for VPC attachment create/accept/reject/delete and connect attachment."""

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    @pytest.fixture
    def core_network_with_vpc(self, nm, global_network, ec2_client):
        gn_id = global_network["GlobalNetworkId"]
        cn = nm.create_core_network(GlobalNetworkId=gn_id)["CoreNetwork"]
        vpc = ec2_client.create_vpc(CidrBlock="10.50.0.0/16")["Vpc"]
        subnet = ec2_client.create_subnet(VpcId=vpc["VpcId"], CidrBlock="10.50.1.0/24")["Subnet"]
        yield {
            "cn": cn,
            "vpc": vpc,
            "subnet": subnet,
            "gn_id": gn_id,
        }
        nm.delete_core_network(CoreNetworkId=cn["CoreNetworkId"])

    def test_create_vpc_attachment(self, nm, core_network_with_vpc):
        ctx = core_network_with_vpc
        cn_id = ctx["cn"]["CoreNetworkId"]
        vpc_arn = f"arn:aws:ec2:us-east-1:123456789012:vpc/{ctx['vpc']['VpcId']}"
        subnet_arn = f"arn:aws:ec2:us-east-1:123456789012:subnet/{ctx['subnet']['SubnetId']}"
        resp = nm.create_vpc_attachment(
            CoreNetworkId=cn_id, VpcArn=vpc_arn, SubnetArns=[subnet_arn]
        )
        att = resp["VpcAttachment"]["Attachment"]
        assert "AttachmentId" in att
        assert att["CoreNetworkId"] == cn_id

    def test_accept_attachment(self, nm, core_network_with_vpc):
        ctx = core_network_with_vpc
        cn_id = ctx["cn"]["CoreNetworkId"]
        vpc_arn = f"arn:aws:ec2:us-east-1:123456789012:vpc/{ctx['vpc']['VpcId']}"
        subnet_arn = f"arn:aws:ec2:us-east-1:123456789012:subnet/{ctx['subnet']['SubnetId']}"
        vpc_att = nm.create_vpc_attachment(
            CoreNetworkId=cn_id, VpcArn=vpc_arn, SubnetArns=[subnet_arn]
        )
        att_id = vpc_att["VpcAttachment"]["Attachment"]["AttachmentId"]
        resp = nm.accept_attachment(AttachmentId=att_id)
        assert "Attachment" in resp

    def test_reject_attachment(self, nm, core_network_with_vpc, ec2_client):
        ctx = core_network_with_vpc
        cn_id = ctx["cn"]["CoreNetworkId"]
        vpc2 = ec2_client.create_vpc(CidrBlock="10.51.0.0/16")["Vpc"]
        sub2 = ec2_client.create_subnet(VpcId=vpc2["VpcId"], CidrBlock="10.51.1.0/24")["Subnet"]
        vpc_arn = f"arn:aws:ec2:us-east-1:123456789012:vpc/{vpc2['VpcId']}"
        subnet_arn = f"arn:aws:ec2:us-east-1:123456789012:subnet/{sub2['SubnetId']}"
        vpc_att = nm.create_vpc_attachment(
            CoreNetworkId=cn_id, VpcArn=vpc_arn, SubnetArns=[subnet_arn]
        )
        att_id = vpc_att["VpcAttachment"]["Attachment"]["AttachmentId"]
        resp = nm.reject_attachment(AttachmentId=att_id)
        assert "Attachment" in resp

    def test_delete_attachment(self, nm, core_network_with_vpc):
        ctx = core_network_with_vpc
        cn_id = ctx["cn"]["CoreNetworkId"]
        vpc_arn = f"arn:aws:ec2:us-east-1:123456789012:vpc/{ctx['vpc']['VpcId']}"
        subnet_arn = f"arn:aws:ec2:us-east-1:123456789012:subnet/{ctx['subnet']['SubnetId']}"
        vpc_att = nm.create_vpc_attachment(
            CoreNetworkId=cn_id, VpcArn=vpc_arn, SubnetArns=[subnet_arn]
        )
        att_id = vpc_att["VpcAttachment"]["Attachment"]["AttachmentId"]
        resp = nm.delete_attachment(AttachmentId=att_id)
        assert "Attachment" in resp

    def test_update_vpc_attachment(self, nm, core_network_with_vpc):
        ctx = core_network_with_vpc
        cn_id = ctx["cn"]["CoreNetworkId"]
        vpc_arn = f"arn:aws:ec2:us-east-1:123456789012:vpc/{ctx['vpc']['VpcId']}"
        subnet_arn = f"arn:aws:ec2:us-east-1:123456789012:subnet/{ctx['subnet']['SubnetId']}"
        vpc_att = nm.create_vpc_attachment(
            CoreNetworkId=cn_id, VpcArn=vpc_arn, SubnetArns=[subnet_arn]
        )
        att_id = vpc_att["VpcAttachment"]["Attachment"]["AttachmentId"]
        resp = nm.update_vpc_attachment(AttachmentId=att_id, AddSubnetArns=[], RemoveSubnetArns=[])
        assert "VpcAttachment" in resp

    def test_create_connect_attachment(self, nm, core_network_with_vpc):
        ctx = core_network_with_vpc
        cn_id = ctx["cn"]["CoreNetworkId"]
        vpc_arn = f"arn:aws:ec2:us-east-1:123456789012:vpc/{ctx['vpc']['VpcId']}"
        subnet_arn = f"arn:aws:ec2:us-east-1:123456789012:subnet/{ctx['subnet']['SubnetId']}"
        vpc_att = nm.create_vpc_attachment(
            CoreNetworkId=cn_id, VpcArn=vpc_arn, SubnetArns=[subnet_arn]
        )
        transport_att_id = vpc_att["VpcAttachment"]["Attachment"]["AttachmentId"]
        resp = nm.create_connect_attachment(
            CoreNetworkId=cn_id,
            TransportAttachmentId=transport_att_id,
            EdgeLocation="us-east-1",
            Options={"Protocol": "GRE"},
        )
        ca = resp["ConnectAttachment"]
        assert "Attachment" in ca
        assert ca["Attachment"]["CoreNetworkId"] == cn_id


class TestNetworkManagerConnectPeerOps:
    """Tests for CreateConnectPeer, DeleteConnectPeer, Associate/Disassociate."""

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    @pytest.fixture
    def connect_attachment(self, nm, global_network, ec2_client):
        gn_id = global_network["GlobalNetworkId"]
        cn = nm.create_core_network(GlobalNetworkId=gn_id)["CoreNetwork"]
        cn_id = cn["CoreNetworkId"]
        vpc = ec2_client.create_vpc(CidrBlock="10.60.0.0/16")["Vpc"]
        sub = ec2_client.create_subnet(VpcId=vpc["VpcId"], CidrBlock="10.60.1.0/24")["Subnet"]
        vpc_att = nm.create_vpc_attachment(
            CoreNetworkId=cn_id,
            VpcArn=f"arn:aws:ec2:us-east-1:123456789012:vpc/{vpc['VpcId']}",
            SubnetArns=[f"arn:aws:ec2:us-east-1:123456789012:subnet/{sub['SubnetId']}"],
        )
        transport_id = vpc_att["VpcAttachment"]["Attachment"]["AttachmentId"]
        ca = nm.create_connect_attachment(
            CoreNetworkId=cn_id,
            TransportAttachmentId=transport_id,
            EdgeLocation="us-east-1",
            Options={"Protocol": "GRE"},
        )
        ca_att_id = ca["ConnectAttachment"]["Attachment"]["AttachmentId"]
        yield {"ca_att_id": ca_att_id, "gn_id": gn_id, "cn_id": cn_id}
        nm.delete_core_network(CoreNetworkId=cn_id)

    def test_create_connect_peer(self, nm, connect_attachment):
        resp = nm.create_connect_peer(
            ConnectAttachmentId=connect_attachment["ca_att_id"],
            PeerAddress="10.0.0.1",
        )
        assert "ConnectPeer" in resp
        assert "ConnectPeerId" in resp["ConnectPeer"]

    def test_delete_connect_peer(self, nm, connect_attachment):
        cp = nm.create_connect_peer(
            ConnectAttachmentId=connect_attachment["ca_att_id"],
            PeerAddress="10.0.0.2",
        )["ConnectPeer"]
        resp = nm.delete_connect_peer(ConnectPeerId=cp["ConnectPeerId"])
        assert "ConnectPeer" in resp
        assert resp["ConnectPeer"]["ConnectPeerId"] == cp["ConnectPeerId"]

    def test_associate_and_disassociate_connect_peer(self, nm, connect_attachment):
        gn_id = connect_attachment["gn_id"]
        cp = nm.create_connect_peer(
            ConnectAttachmentId=connect_attachment["ca_att_id"],
            PeerAddress="10.0.0.3",
        )["ConnectPeer"]
        dev = nm.create_device(GlobalNetworkId=gn_id, Description="cp-dev")["Device"]

        resp = nm.associate_connect_peer(
            GlobalNetworkId=gn_id,
            ConnectPeerId=cp["ConnectPeerId"],
            DeviceId=dev["DeviceId"],
        )
        assert "ConnectPeerAssociation" in resp

        resp2 = nm.disassociate_connect_peer(
            GlobalNetworkId=gn_id, ConnectPeerId=cp["ConnectPeerId"]
        )
        assert "ConnectPeerAssociation" in resp2

    def test_delete_connect_peer_not_found(self, nm):
        """DeleteConnectPeer with fake ID returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            nm.delete_connect_peer(ConnectPeerId="cp-nonexistent")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestNetworkManagerTransitGatewayOps:
    """Tests for RegisterTransitGateway, CreateTransitGatewayPeering, DeletePeering."""

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    def test_register_transit_gateway(self, nm, global_network, ec2_client):
        gn_id = global_network["GlobalNetworkId"]
        tgw = ec2_client.create_transit_gateway(Description="test-tgw")["TransitGateway"]
        resp = nm.register_transit_gateway(
            GlobalNetworkId=gn_id, TransitGatewayArn=tgw["TransitGatewayArn"]
        )
        assert "TransitGatewayRegistration" in resp
        reg = resp["TransitGatewayRegistration"]
        assert reg["GlobalNetworkId"] == gn_id

    def test_create_transit_gateway_peering(self, nm, global_network, ec2_client):
        gn_id = global_network["GlobalNetworkId"]
        cn = nm.create_core_network(GlobalNetworkId=gn_id)["CoreNetwork"]
        cn_id = cn["CoreNetworkId"]
        tgw = ec2_client.create_transit_gateway(Description="peer-tgw")["TransitGateway"]
        try:
            resp = nm.create_transit_gateway_peering(
                CoreNetworkId=cn_id,
                TransitGatewayArn=tgw["TransitGatewayArn"],
            )
            assert "TransitGatewayPeering" in resp
            peering = resp["TransitGatewayPeering"]
            assert "Peering" in peering
            peering_id = peering["Peering"]["PeeringId"]

            # DeletePeering
            del_resp = nm.delete_peering(PeeringId=peering_id)
            assert "Peering" in del_resp
        finally:
            nm.delete_core_network(CoreNetworkId=cn_id)

    def test_create_transit_gateway_route_table_attachment(self, nm, global_network, ec2_client):
        gn_id = global_network["GlobalNetworkId"]
        cn = nm.create_core_network(GlobalNetworkId=gn_id)["CoreNetwork"]
        cn_id = cn["CoreNetworkId"]
        tgw = ec2_client.create_transit_gateway(Description="rta-tgw")["TransitGateway"]
        try:
            peering = nm.create_transit_gateway_peering(
                CoreNetworkId=cn_id,
                TransitGatewayArn=tgw["TransitGatewayArn"],
            )
            peering_id = peering["TransitGatewayPeering"]["Peering"]["PeeringId"]
            rt_arn = "arn:aws:ec2:us-east-1:123456789012:transit-gateway-route-table/tgw-rtb-fake"
            resp = nm.create_transit_gateway_route_table_attachment(
                PeeringId=peering_id, TransitGatewayRouteTableArn=rt_arn
            )
            assert "TransitGatewayRouteTableAttachment" in resp
        finally:
            nm.delete_core_network(CoreNetworkId=cn_id)

    def test_delete_peering_not_found(self, nm):
        """DeletePeering with fake ID returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            nm.delete_peering(PeeringId="peering-nonexistent")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_associate_transit_gateway_connect_peer(self, nm, global_network, ec2_client):
        gn_id = global_network["GlobalNetworkId"]
        dev = nm.create_device(GlobalNetworkId=gn_id, Description="tgwcp-dev")["Device"]
        tgw_cp_arn = "arn:aws:ec2:us-east-1:123456789012:transit-gateway-connect-peer/tgw-cp-fake"
        resp = nm.associate_transit_gateway_connect_peer(
            GlobalNetworkId=gn_id,
            TransitGatewayConnectPeerArn=tgw_cp_arn,
            DeviceId=dev["DeviceId"],
        )
        assert "TransitGatewayConnectPeerAssociation" in resp

    def test_deregister_transit_gateway_not_found(self, nm, global_network):
        """DeregisterTransitGateway with unregistered ARN returns NotFoundException."""
        gn_id = global_network["GlobalNetworkId"]
        fake_arn = "arn:aws:ec2:us-east-1:123456789012:transit-gateway/tgw-nonexistent"
        with pytest.raises(ClientError) as exc:
            nm.deregister_transit_gateway(GlobalNetworkId=gn_id, TransitGatewayArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_disassociate_transit_gateway_connect_peer_not_found(self, nm, global_network):
        """DisassociateTransitGatewayConnectPeer with fake ARN returns NotFoundException."""
        gn_id = global_network["GlobalNetworkId"]
        fake_arn = (
            "arn:aws:ec2:us-east-1:123456789012:transit-gateway-connect-peer/tgw-cp-nonexistent"
        )
        with pytest.raises(ClientError) as exc:
            nm.disassociate_transit_gateway_connect_peer(
                GlobalNetworkId=gn_id, TransitGatewayConnectPeerArn=fake_arn
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestNetworkManagerCustomerGatewayOps:
    """Tests for AssociateCustomerGateway and DisassociateCustomerGateway."""

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    def test_associate_customer_gateway(self, nm, global_network, ec2_client):
        gn_id = global_network["GlobalNetworkId"]
        dev = nm.create_device(GlobalNetworkId=gn_id, Description="cgw-dev")["Device"]
        cgw = ec2_client.create_customer_gateway(Type="ipsec.1", BgpAsn=65000, IpAddress="9.8.7.6")[
            "CustomerGateway"
        ]
        cgw_arn = f"arn:aws:ec2:us-east-1:123456789012:customer-gateway/{cgw['CustomerGatewayId']}"
        resp = nm.associate_customer_gateway(
            GlobalNetworkId=gn_id,
            CustomerGatewayArn=cgw_arn,
            DeviceId=dev["DeviceId"],
        )
        assert "CustomerGatewayAssociation" in resp

    def test_disassociate_customer_gateway_not_found(self, nm, global_network):
        """DisassociateCustomerGateway with unassociated ARN returns NotFoundException."""
        gn_id = global_network["GlobalNetworkId"]
        fake_arn = "arn:aws:ec2:us-east-1:123456789012:customer-gateway/cgw-nonexistent"
        with pytest.raises(ClientError) as exc:
            nm.disassociate_customer_gateway(GlobalNetworkId=gn_id, CustomerGatewayArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestNetworkManagerSpecialAttachmentOps:
    """Tests for SiteToSiteVpn, DirectConnectGateway, and PrefixList attachments."""

    @pytest.fixture
    def core_network(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        cn = nm.create_core_network(GlobalNetworkId=gn_id)["CoreNetwork"]
        yield cn
        nm.delete_core_network(CoreNetworkId=cn["CoreNetworkId"])

    def test_create_site_to_site_vpn_attachment(self, nm, core_network):
        cn_id = core_network["CoreNetworkId"]
        vpn_arn = "arn:aws:ec2:us-east-1:123456789012:vpn-connection/vpn-fake"
        resp = nm.create_site_to_site_vpn_attachment(CoreNetworkId=cn_id, VpnConnectionArn=vpn_arn)
        assert "SiteToSiteVpnAttachment" in resp

    def test_create_direct_connect_gateway_attachment(self, nm, core_network):
        cn_id = core_network["CoreNetworkId"]
        dx_arn = "arn:aws:directconnect::123456789012:dx-gateway/fake"
        resp = nm.create_direct_connect_gateway_attachment(
            CoreNetworkId=cn_id,
            DirectConnectGatewayArn=dx_arn,
            EdgeLocations=["us-east-1"],
        )
        assert "DirectConnectGatewayAttachment" in resp

    def test_create_core_network_prefix_list_association(self, nm, core_network):
        cn_id = core_network["CoreNetworkId"]
        resp = nm.create_core_network_prefix_list_association(
            CoreNetworkId=cn_id,
            PrefixListArn="arn:aws:ec2:us-east-1:123456789012:prefix-list/pl-fake",
            PrefixListAlias="test-alias",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_direct_connect_gateway_attachment_not_found(self, nm):
        """UpdateDirectConnectGatewayAttachment with fake ID returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            nm.update_direct_connect_gateway_attachment(
                AttachmentId="attachment-nonexistent",
                EdgeLocations=["us-east-1"],
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestNetworkManagerRoutingPolicyAndMisc:
    """Tests for routing policy labels, route analysis, and misc operations."""

    @pytest.fixture
    def core_network(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        cn = nm.create_core_network(GlobalNetworkId=gn_id)["CoreNetwork"]
        yield cn
        nm.delete_core_network(CoreNetworkId=cn["CoreNetworkId"])

    def test_put_attachment_routing_policy_label_not_found(self, nm, core_network):
        """PutAttachmentRoutingPolicyLabel with fake attachment returns NotFoundException."""
        cn_id = core_network["CoreNetworkId"]
        with pytest.raises(ClientError) as exc:
            nm.put_attachment_routing_policy_label(
                CoreNetworkId=cn_id,
                AttachmentId="attachment-nonexistent",
                RoutingPolicyLabel="test-label",
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_remove_attachment_routing_policy_label(self, nm, core_network):
        cn_id = core_network["CoreNetworkId"]
        resp = nm.remove_attachment_routing_policy_label(
            CoreNetworkId=cn_id, AttachmentId="attachment-fake"
        )
        assert "AttachmentId" in resp

    def test_start_route_analysis(self, nm, global_network):
        gn_id = global_network["GlobalNetworkId"]
        resp = nm.start_route_analysis(
            GlobalNetworkId=gn_id,
            Source={
                "TransitGatewayAttachmentArn": (
                    "arn:aws:ec2:us-east-1:123456789012:transit-gateway-attachment/tgw-attach-fake"
                ),
                "IpAddress": "10.0.0.1",
            },
            Destination={
                "TransitGatewayAttachmentArn": (
                    "arn:aws:ec2:us-east-1:123456789012:transit-gateway-attachment/tgw-attach-fake2"
                ),
                "IpAddress": "10.0.1.1",
            },
        )
        assert "RouteAnalysis" in resp
        ra = resp["RouteAnalysis"]
        assert "RouteAnalysisId" in ra
        assert ra["GlobalNetworkId"] == gn_id

    def test_start_organization_service_access_update(self, nm):
        resp = nm.start_organization_service_access_update(Action="ENABLE")
        assert "OrganizationStatus" in resp

    def test_update_network_resource_metadata_not_found(self, nm, global_network):
        """UpdateNetworkResourceMetadata with fake resource returns NotFoundException."""
        gn_id = global_network["GlobalNetworkId"]
        with pytest.raises(ClientError) as exc:
            nm.update_network_resource_metadata(
                GlobalNetworkId=gn_id,
                ResourceArn="arn:aws:ec2:us-east-1:123456789012:vpc/vpc-nonexistent",
                Metadata={"key": "val"},
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_accept_attachment_not_found(self, nm):
        """AcceptAttachment with fake ID returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            nm.accept_attachment(AttachmentId="attachment-nonexistent")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_reject_attachment_not_found(self, nm):
        """RejectAttachment with fake ID returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            nm.reject_attachment(AttachmentId="attachment-nonexistent")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_delete_attachment_not_found(self, nm):
        """DeleteAttachment with fake ID returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            nm.delete_attachment(AttachmentId="attachment-nonexistent")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"
