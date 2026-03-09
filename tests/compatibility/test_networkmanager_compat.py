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
