"""MediaConnect compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def mediaconnect():
    return make_client("mediaconnect")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestMediaConnectFlows:
    @pytest.fixture
    def flow(self, mediaconnect):
        name = _unique("flow")
        resp = mediaconnect.create_flow(
            Name=name,
            Source={
                "Name": "source-1",
                "Protocol": "zixi-push",
                "WhitelistCidr": "0.0.0.0/0",
            },
        )
        flow_arn = resp["Flow"]["FlowArn"]
        yield {"name": name, "arn": flow_arn, "flow": resp["Flow"]}
        try:
            mediaconnect.delete_flow(FlowArn=flow_arn)
        except Exception:
            pass

    def test_list_flows(self, mediaconnect):
        resp = mediaconnect.list_flows()
        assert "Flows" in resp
        assert isinstance(resp["Flows"], list)

    def test_create_flow(self, mediaconnect):
        name = _unique("flow")
        resp = mediaconnect.create_flow(
            Name=name,
            Source={
                "Name": "source-1",
                "Protocol": "zixi-push",
                "WhitelistCidr": "0.0.0.0/0",
            },
        )
        flow = resp["Flow"]
        assert flow["Name"] == name
        assert "FlowArn" in flow
        mediaconnect.delete_flow(FlowArn=flow["FlowArn"])

    def test_describe_flow(self, mediaconnect, flow):
        resp = mediaconnect.describe_flow(FlowArn=flow["arn"])
        assert resp["Flow"]["Name"] == flow["name"]
        assert resp["Flow"]["FlowArn"] == flow["arn"]

    def test_delete_flow(self, mediaconnect):
        name = _unique("flow")
        resp = mediaconnect.create_flow(
            Name=name,
            Source={
                "Name": "source-1",
                "Protocol": "zixi-push",
                "WhitelistCidr": "0.0.0.0/0",
            },
        )
        flow_arn = resp["Flow"]["FlowArn"]
        mediaconnect.delete_flow(FlowArn=flow_arn)
        # Verify flow is gone from list
        flows = mediaconnect.list_flows()["Flows"]
        arns = [f["FlowArn"] for f in flows]
        assert flow_arn not in arns

    def test_create_flow_appears_in_list(self, mediaconnect, flow):
        flows = mediaconnect.list_flows()["Flows"]
        arns = [f["FlowArn"] for f in flows]
        assert flow["arn"] in arns

    def test_start_flow(self, mediaconnect, flow):
        resp = mediaconnect.start_flow(FlowArn=flow["arn"])
        assert resp["FlowArn"] == flow["arn"]
        assert resp["Status"] == "STARTING"

    def test_stop_flow(self, mediaconnect, flow):
        mediaconnect.start_flow(FlowArn=flow["arn"])
        resp = mediaconnect.stop_flow(FlowArn=flow["arn"])
        assert resp["FlowArn"] == flow["arn"]
        assert resp["Status"] == "STOPPING"

    def test_create_flow_has_source(self, mediaconnect, flow):
        resp = mediaconnect.describe_flow(FlowArn=flow["arn"])
        source = resp["Flow"]["Source"]
        assert "SourceArn" in source
        assert source["Name"] == "source-1"


class TestMediaConnectTags:
    @pytest.fixture
    def flow(self, mediaconnect):
        name = _unique("flow")
        resp = mediaconnect.create_flow(
            Name=name,
            Source={
                "Name": "source-1",
                "Protocol": "zixi-push",
                "WhitelistCidr": "0.0.0.0/0",
            },
        )
        flow_arn = resp["Flow"]["FlowArn"]
        yield {"name": name, "arn": flow_arn}
        try:
            mediaconnect.delete_flow(FlowArn=flow_arn)
        except Exception:
            pass

    def test_tag_resource(self, mediaconnect, flow):
        mediaconnect.tag_resource(ResourceArn=flow["arn"], Tags={"env": "test"})
        resp = mediaconnect.list_tags_for_resource(ResourceArn=flow["arn"])
        assert "Tags" in resp
        assert resp["Tags"]["env"] == "test"

    def test_tag_resource_overwrites(self, mediaconnect, flow):
        mediaconnect.tag_resource(ResourceArn=flow["arn"], Tags={"env": "dev"})
        mediaconnect.tag_resource(ResourceArn=flow["arn"], Tags={"env": "prod"})
        resp = mediaconnect.list_tags_for_resource(ResourceArn=flow["arn"])
        assert resp["Tags"]["env"] == "prod"

    def test_tag_resource_multiple(self, mediaconnect, flow):
        mediaconnect.tag_resource(ResourceArn=flow["arn"], Tags={"env": "test", "team": "backend"})
        resp = mediaconnect.list_tags_for_resource(ResourceArn=flow["arn"])
        assert resp["Tags"]["env"] == "test"
        assert resp["Tags"]["team"] == "backend"


class TestMediaConnectOutputs:
    @pytest.fixture
    def flow(self, mediaconnect):
        name = _unique("flow")
        resp = mediaconnect.create_flow(
            Name=name,
            Source={
                "Name": "source-1",
                "Protocol": "zixi-push",
                "WhitelistCidr": "0.0.0.0/0",
            },
        )
        flow_arn = resp["Flow"]["FlowArn"]
        yield {"name": name, "arn": flow_arn}
        try:
            mediaconnect.delete_flow(FlowArn=flow_arn)
        except Exception:
            pass

    def test_add_flow_outputs(self, mediaconnect, flow):
        resp = mediaconnect.add_flow_outputs(
            FlowArn=flow["arn"],
            Outputs=[{"Protocol": "zixi-push", "Name": "out1"}],
        )
        assert "Outputs" in resp
        assert len(resp["Outputs"]) == 1
        assert resp["Outputs"][0]["Name"] == "out1"

    def test_add_flow_outputs_multiple(self, mediaconnect, flow):
        resp = mediaconnect.add_flow_outputs(
            FlowArn=flow["arn"],
            Outputs=[
                {"Protocol": "zixi-push", "Name": "out1"},
                {"Protocol": "zixi-push", "Name": "out2"},
            ],
        )
        assert len(resp["Outputs"]) == 2
        names = {o["Name"] for o in resp["Outputs"]}
        assert "out1" in names
        assert "out2" in names


class TestMediaConnectSources:
    @pytest.fixture
    def flow(self, mediaconnect):
        name = _unique("flow")
        resp = mediaconnect.create_flow(
            Name=name,
            Source={
                "Name": "source-1",
                "Protocol": "zixi-push",
                "WhitelistCidr": "0.0.0.0/0",
            },
        )
        flow_arn = resp["Flow"]["FlowArn"]
        source_arn = resp["Flow"]["Source"]["SourceArn"]
        yield {"name": name, "arn": flow_arn, "source_arn": source_arn}
        try:
            mediaconnect.delete_flow(FlowArn=flow_arn)
        except Exception:
            pass

    def test_add_flow_sources(self, mediaconnect, flow):
        resp = mediaconnect.add_flow_sources(
            FlowArn=flow["arn"],
            Sources=[
                {
                    "Name": "src2",
                    "Protocol": "zixi-push",
                    "WhitelistCidr": "0.0.0.0/0",
                }
            ],
        )
        assert "Sources" in resp
        assert len(resp["Sources"]) == 1
        assert resp["Sources"][0]["Name"] == "src2"
        assert "SourceArn" in resp["Sources"][0]

    def test_update_flow_source(self, mediaconnect, flow):
        resp = mediaconnect.update_flow_source(
            FlowArn=flow["arn"],
            SourceArn=flow["source_arn"],
            Description="updated source",
        )
        assert "Source" in resp
        assert resp["Source"]["Description"] == "updated source"


class TestMediaConnectEntitlements:
    @pytest.fixture
    def flow(self, mediaconnect):
        name = _unique("flow")
        resp = mediaconnect.create_flow(
            Name=name,
            Source={
                "Name": "source-1",
                "Protocol": "zixi-push",
                "WhitelistCidr": "0.0.0.0/0",
            },
        )
        flow_arn = resp["Flow"]["FlowArn"]
        yield {"name": name, "arn": flow_arn}
        try:
            mediaconnect.delete_flow(FlowArn=flow_arn)
        except Exception:
            pass

    def test_grant_flow_entitlements(self, mediaconnect, flow):
        resp = mediaconnect.grant_flow_entitlements(
            FlowArn=flow["arn"],
            Entitlements=[
                {
                    "Subscribers": ["arn:aws:iam::123456789012:root"],
                    "Name": "ent1",
                }
            ],
        )
        assert "Entitlements" in resp
        assert len(resp["Entitlements"]) == 1
        ent = resp["Entitlements"][0]
        assert ent["Name"] == "ent1"
        assert "EntitlementArn" in ent

    def test_revoke_flow_entitlement(self, mediaconnect, flow):
        ent_resp = mediaconnect.grant_flow_entitlements(
            FlowArn=flow["arn"],
            Entitlements=[
                {
                    "Subscribers": ["arn:aws:iam::123456789012:root"],
                    "Name": "ent-revoke",
                }
            ],
        )
        ent_arn = ent_resp["Entitlements"][0]["EntitlementArn"]
        resp = mediaconnect.revoke_flow_entitlement(FlowArn=flow["arn"], EntitlementArn=ent_arn)
        assert resp["EntitlementArn"] == ent_arn
        assert resp["FlowArn"] == flow["arn"]

    def test_update_flow_entitlement(self, mediaconnect, flow):
        ent_resp = mediaconnect.grant_flow_entitlements(
            FlowArn=flow["arn"],
            Entitlements=[
                {
                    "Subscribers": ["arn:aws:iam::123456789012:root"],
                    "Name": "ent-update",
                }
            ],
        )
        ent_arn = ent_resp["Entitlements"][0]["EntitlementArn"]
        resp = mediaconnect.update_flow_entitlement(
            FlowArn=flow["arn"],
            EntitlementArn=ent_arn,
            Description="updated entitlement",
        )
        assert "Entitlement" in resp
        assert resp["FlowArn"] == flow["arn"]
        assert resp["Entitlement"]["Description"] == "updated entitlement"


class TestMediaConnectVpcInterfaces:
    @pytest.fixture
    def flow(self, mediaconnect):
        name = _unique("flow")
        resp = mediaconnect.create_flow(
            Name=name,
            Source={
                "Name": "source-1",
                "Protocol": "zixi-push",
                "WhitelistCidr": "0.0.0.0/0",
            },
        )
        flow_arn = resp["Flow"]["FlowArn"]
        yield {"name": name, "arn": flow_arn}
        try:
            mediaconnect.delete_flow(FlowArn=flow_arn)
        except Exception:
            pass

    def test_add_flow_vpc_interfaces(self, mediaconnect, flow):
        # AddFlowVpcInterfaces returns empty response body but succeeds (200)
        resp = mediaconnect.add_flow_vpc_interfaces(
            FlowArn=flow["arn"],
            VpcInterfaces=[
                {
                    "Name": "vpc1",
                    "RoleArn": "arn:aws:iam::123456789012:role/test",
                    "SecurityGroupIds": ["sg-123"],
                    "SubnetId": "subnet-123",
                }
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_remove_flow_vpc_interface(self, mediaconnect, flow):
        mediaconnect.add_flow_vpc_interfaces(
            FlowArn=flow["arn"],
            VpcInterfaces=[
                {
                    "Name": "vpc-remove",
                    "RoleArn": "arn:aws:iam::123456789012:role/test",
                    "SecurityGroupIds": ["sg-123"],
                    "SubnetId": "subnet-123",
                }
            ],
        )
        resp = mediaconnect.remove_flow_vpc_interface(
            FlowArn=flow["arn"], VpcInterfaceName="vpc-remove"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestMediaConnectBridges:
    """Tests for MediaConnect Bridge CRUD operations."""

    @pytest.fixture
    def bridge(self, mediaconnect):
        name = _unique("bridge")
        resp = mediaconnect.create_bridge(
            Name=name,
            PlacementArn="arn:aws:mediaconnect:us-east-1:123456789012:bridge:test",
            Sources=[
                {
                    "NetworkSource": {
                        "Name": "src1",
                        "Protocol": "zixi-push",
                        "MulticastIp": "239.0.0.1",
                        "Port": 5000,
                        "NetworkName": "net1",
                    }
                }
            ],
        )
        bridge = resp["Bridge"]
        yield bridge
        try:
            mediaconnect.delete_bridge(BridgeArn=bridge["BridgeArn"])
        except Exception:
            pass

    def test_create_bridge(self, mediaconnect):
        """CreateBridge creates a bridge and returns its details."""
        name = _unique("bridge")
        resp = mediaconnect.create_bridge(
            Name=name,
            PlacementArn="arn:aws:mediaconnect:us-east-1:123456789012:bridge:test",
            Sources=[
                {
                    "NetworkSource": {
                        "Name": "src1",
                        "Protocol": "zixi-push",
                        "MulticastIp": "239.0.0.1",
                        "Port": 5000,
                        "NetworkName": "net1",
                    }
                }
            ],
        )
        bridge = resp["Bridge"]
        assert bridge["Name"] == name
        assert "BridgeArn" in bridge
        assert bridge["BridgeState"] == "ACTIVE"
        assert len(bridge["Sources"]) == 1
        mediaconnect.delete_bridge(BridgeArn=bridge["BridgeArn"])

    def test_describe_bridge(self, mediaconnect, bridge):
        """DescribeBridge returns the bridge details."""
        resp = mediaconnect.describe_bridge(BridgeArn=bridge["BridgeArn"])
        assert resp["Bridge"]["Name"] == bridge["Name"]
        assert resp["Bridge"]["BridgeArn"] == bridge["BridgeArn"]
        assert resp["Bridge"]["BridgeState"] == "ACTIVE"

    def test_delete_bridge(self, mediaconnect):
        """DeleteBridge removes the bridge."""
        name = _unique("bridge")
        resp = mediaconnect.create_bridge(
            Name=name,
            PlacementArn="arn:aws:mediaconnect:us-east-1:123456789012:bridge:test",
            Sources=[
                {
                    "NetworkSource": {
                        "Name": "src1",
                        "Protocol": "zixi-push",
                        "MulticastIp": "239.0.0.1",
                        "Port": 5000,
                        "NetworkName": "net1",
                    }
                }
            ],
        )
        bridge_arn = resp["Bridge"]["BridgeArn"]
        mediaconnect.delete_bridge(BridgeArn=bridge_arn)
        bridges = mediaconnect.list_bridges()["Bridges"]
        arns = [b["BridgeArn"] for b in bridges]
        assert bridge_arn not in arns

    def test_bridge_appears_in_list(self, mediaconnect, bridge):
        """Created bridge appears in ListBridges."""
        bridges = mediaconnect.list_bridges()["Bridges"]
        arns = [b["BridgeArn"] for b in bridges]
        assert bridge["BridgeArn"] in arns

    def test_add_bridge_outputs(self, mediaconnect, bridge):
        """AddBridgeOutputs adds outputs to an existing bridge."""
        resp = mediaconnect.add_bridge_outputs(
            BridgeArn=bridge["BridgeArn"],
            Outputs=[
                {
                    "NetworkOutput": {
                        "IpAddress": "10.0.0.1",
                        "Name": "out1",
                        "NetworkName": "net1",
                        "Port": 6000,
                        "Protocol": "zixi-push",
                        "Ttl": 64,
                    }
                }
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Outputs" in resp

    def test_add_bridge_sources(self, mediaconnect, bridge):
        """AddBridgeSources adds sources to an existing bridge."""
        resp = mediaconnect.add_bridge_sources(
            BridgeArn=bridge["BridgeArn"],
            Sources=[
                {
                    "NetworkSource": {
                        "Name": "src2",
                        "Protocol": "zixi-push",
                        "MulticastIp": "239.0.0.2",
                        "Port": 5001,
                        "NetworkName": "net1",
                    }
                }
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Sources" in resp

    def test_update_bridge(self, mediaconnect, bridge):
        """UpdateBridge modifies bridge properties."""
        resp = mediaconnect.update_bridge(BridgeArn=bridge["BridgeArn"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Bridge" in resp
        assert resp["Bridge"]["BridgeArn"] == bridge["BridgeArn"]


class TestMediaConnectGateways:
    """Tests for MediaConnect Gateway CRUD operations."""

    @pytest.fixture
    def gateway(self, mediaconnect):
        name = _unique("gw")
        resp = mediaconnect.create_gateway(
            Name=name,
            EgressCidrBlocks=["10.0.0.0/16"],
            Networks=[{"Name": "net1", "CidrBlock": "10.0.0.0/24"}],
        )
        gw = resp["Gateway"]
        yield gw
        try:
            mediaconnect.delete_gateway(GatewayArn=gw["GatewayArn"])
        except Exception:
            pass

    def test_create_gateway(self, mediaconnect):
        """CreateGateway creates a gateway and returns its details."""
        name = _unique("gw")
        resp = mediaconnect.create_gateway(
            Name=name,
            EgressCidrBlocks=["10.0.0.0/16"],
            Networks=[{"Name": "net1", "CidrBlock": "10.0.0.0/24"}],
        )
        gw = resp["Gateway"]
        assert gw["Name"] == name
        assert "GatewayArn" in gw
        assert gw["GatewayState"] == "ACTIVE"
        assert gw["EgressCidrBlocks"] == ["10.0.0.0/16"]
        assert len(gw["Networks"]) == 1
        mediaconnect.delete_gateway(GatewayArn=gw["GatewayArn"])

    def test_describe_gateway(self, mediaconnect, gateway):
        """DescribeGateway returns the gateway details."""
        resp = mediaconnect.describe_gateway(GatewayArn=gateway["GatewayArn"])
        assert resp["Gateway"]["Name"] == gateway["Name"]
        assert resp["Gateway"]["GatewayArn"] == gateway["GatewayArn"]
        assert resp["Gateway"]["GatewayState"] == "ACTIVE"

    def test_delete_gateway(self, mediaconnect):
        """DeleteGateway removes the gateway."""
        name = _unique("gw")
        resp = mediaconnect.create_gateway(
            Name=name,
            EgressCidrBlocks=["10.0.0.0/16"],
            Networks=[{"Name": "net1", "CidrBlock": "10.0.0.0/24"}],
        )
        gw_arn = resp["Gateway"]["GatewayArn"]
        mediaconnect.delete_gateway(GatewayArn=gw_arn)
        gws = mediaconnect.list_gateways()["Gateways"]
        arns = [g["GatewayArn"] for g in gws]
        assert gw_arn not in arns

    def test_gateway_appears_in_list(self, mediaconnect, gateway):
        """Created gateway appears in ListGateways."""
        gws = mediaconnect.list_gateways()["Gateways"]
        arns = [g["GatewayArn"] for g in gws]
        assert gateway["GatewayArn"] in arns


class TestMediaConnectUpdateFlow:
    """Tests for UpdateFlow operation."""

    @pytest.fixture
    def flow(self, mediaconnect):
        name = _unique("flow")
        resp = mediaconnect.create_flow(
            Name=name,
            Source={
                "Name": "source-1",
                "Protocol": "zixi-push",
                "WhitelistCidr": "0.0.0.0/0",
            },
        )
        flow_arn = resp["Flow"]["FlowArn"]
        yield {"name": name, "arn": flow_arn}
        try:
            mediaconnect.delete_flow(FlowArn=flow_arn)
        except Exception:
            pass

    def test_update_flow(self, mediaconnect, flow):
        """UpdateFlow modifies flow properties."""
        resp = mediaconnect.update_flow(FlowArn=flow["arn"])
        assert "Flow" in resp
        assert resp["Flow"]["FlowArn"] == flow["arn"]
        assert resp["Flow"]["Status"] == "UPDATING"


class TestMediaConnectListOperations:
    """Tests for various List operations."""

    @pytest.fixture
    def client(self):
        return make_client("mediaconnect")

    def test_list_bridges(self, client):
        """ListBridges returns empty bridges list."""
        resp = client.list_bridges()
        assert "Bridges" in resp
        assert isinstance(resp["Bridges"], list)

    def test_list_entitlements(self, client):
        """ListEntitlements returns entitlements list."""
        resp = client.list_entitlements()
        assert "Entitlements" in resp
        assert isinstance(resp["Entitlements"], list)

    def test_list_gateway_instances(self, client):
        """ListGatewayInstances returns instances list."""
        resp = client.list_gateway_instances()
        assert "Instances" in resp
        assert isinstance(resp["Instances"], list)

    def test_list_gateways(self, client):
        """ListGateways returns gateways list."""
        resp = client.list_gateways()
        assert "Gateways" in resp
        assert isinstance(resp["Gateways"], list)

    def test_list_offerings(self, client):
        """ListOfferings returns offerings with expected structure."""
        resp = client.list_offerings()
        assert "Offerings" in resp
        assert isinstance(resp["Offerings"], list)
        # Moto provides default offerings
        if resp["Offerings"]:
            offering = resp["Offerings"][0]
            assert "OfferingArn" in offering
            assert "CurrencyCode" in offering
            assert "PricePerUnit" in offering

    def test_list_reservations(self, client):
        """ListReservations returns reservations list."""
        resp = client.list_reservations()
        assert "Reservations" in resp
        assert isinstance(resp["Reservations"], list)

    def test_list_router_inputs(self, client):
        """ListRouterInputs returns router inputs list."""
        resp = client.list_router_inputs()
        assert "RouterInputs" in resp
        assert isinstance(resp["RouterInputs"], list)

    def test_list_router_network_interfaces(self, client):
        """ListRouterNetworkInterfaces returns network interfaces list."""
        resp = client.list_router_network_interfaces()
        assert "RouterNetworkInterfaces" in resp
        assert isinstance(resp["RouterNetworkInterfaces"], list)

    def test_list_router_outputs(self, client):
        """ListRouterOutputs returns router outputs list."""
        resp = client.list_router_outputs()
        assert "RouterOutputs" in resp
        assert isinstance(resp["RouterOutputs"], list)
