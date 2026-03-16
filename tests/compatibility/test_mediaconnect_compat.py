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
            pass  # best-effort cleanup

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
            pass  # best-effort cleanup

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

    def test_untag_resource(self, mediaconnect, flow):
        """UntagResource removes specified tags from a resource."""
        mediaconnect.tag_resource(ResourceArn=flow["arn"], Tags={"k1": "v1", "k2": "v2"})
        mediaconnect.untag_resource(ResourceArn=flow["arn"], TagKeys=["k1"])
        resp = mediaconnect.list_tags_for_resource(ResourceArn=flow["arn"])
        assert "k1" not in resp["Tags"]
        assert resp["Tags"]["k2"] == "v2"


class TestMediaConnectGlobalTags:
    """Tests for global tag operations."""

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
            pass  # best-effort cleanup

    def test_tag_global_resource(self, mediaconnect, flow):
        """TagGlobalResource adds tags visible via ListTagsForGlobalResource."""
        mediaconnect.tag_global_resource(ResourceArn=flow["arn"], Tags={"gk": "gv"})
        resp = mediaconnect.list_tags_for_global_resource(ResourceArn=flow["arn"])
        assert "Tags" in resp
        assert resp["Tags"]["gk"] == "gv"

    def test_list_tags_for_global_resource(self, mediaconnect, flow):
        """ListTagsForGlobalResource returns tags for a resource."""
        mediaconnect.tag_global_resource(ResourceArn=flow["arn"], Tags={"a": "1", "b": "2"})
        resp = mediaconnect.list_tags_for_global_resource(ResourceArn=flow["arn"])
        assert resp["Tags"]["a"] == "1"
        assert resp["Tags"]["b"] == "2"

    def test_untag_global_resource(self, mediaconnect, flow):
        """UntagGlobalResource removes specified tags."""
        mediaconnect.tag_global_resource(ResourceArn=flow["arn"], Tags={"x": "1", "y": "2"})
        mediaconnect.untag_global_resource(ResourceArn=flow["arn"], TagKeys=["x"])
        resp = mediaconnect.list_tags_for_global_resource(ResourceArn=flow["arn"])
        assert "x" not in resp["Tags"]
        assert resp["Tags"]["y"] == "2"

    def test_list_tags_for_global_resource_untagged(self, mediaconnect):
        """ListTagsForGlobalResource raises NotFoundException for real but untagged flow."""
        flow = mediaconnect.create_flow(
            Name=_unique("flow"),
            Source={
                "Name": "src",
                "Protocol": "zixi-push",
                "WhitelistCidr": "0.0.0.0/0",
            },
        )
        flow_arn = flow["Flow"]["FlowArn"]
        with pytest.raises(mediaconnect.exceptions.NotFoundException):
            mediaconnect.list_tags_for_global_resource(ResourceArn=flow_arn)
        mediaconnect.delete_flow(FlowArn=flow_arn)


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
            pass  # best-effort cleanup

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

    def test_remove_flow_output(self, mediaconnect, flow):
        """RemoveFlowOutput removes an output from a flow."""
        mediaconnect.add_flow_outputs(
            FlowArn=flow["arn"],
            Outputs=[{"Protocol": "zixi-push", "Name": "out-rm"}],
        )
        resp = mediaconnect.remove_flow_output(FlowArn=flow["arn"], OutputArn="out-rm")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_remove_flow_output_confirms_removal(self, mediaconnect, flow):
        """RemoveFlowOutput output no longer appears in DescribeFlow."""
        mediaconnect.add_flow_outputs(
            FlowArn=flow["arn"],
            Outputs=[{"Protocol": "zixi-push", "Name": "out-check-rm"}],
        )
        desc_before = mediaconnect.describe_flow(FlowArn=flow["arn"])
        names_before = {o["Name"] for o in desc_before["Flow"].get("Outputs", [])}
        assert "out-check-rm" in names_before

        mediaconnect.remove_flow_output(FlowArn=flow["arn"], OutputArn="out-check-rm")
        desc_after = mediaconnect.describe_flow(FlowArn=flow["arn"])
        names_after = {o["Name"] for o in desc_after["Flow"].get("Outputs", [])}
        assert "out-check-rm" not in names_after


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
            pass  # best-effort cleanup

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

    def test_remove_flow_source(self, mediaconnect, flow):
        """RemoveFlowSource removes an added source from a flow."""
        add_resp = mediaconnect.add_flow_sources(
            FlowArn=flow["arn"],
            Sources=[
                {
                    "Name": "src-to-remove",
                    "Protocol": "zixi-push",
                    "WhitelistCidr": "0.0.0.0/0",
                }
            ],
        )
        new_src_arn = add_resp["Sources"][0]["SourceArn"]
        resp = mediaconnect.remove_flow_source(FlowArn=flow["arn"], SourceArn=new_src_arn)
        assert resp["FlowArn"] == flow["arn"]
        assert resp["SourceArn"] == new_src_arn


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
            pass  # best-effort cleanup

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
            pass  # best-effort cleanup

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
            pass  # best-effort cleanup

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

    def test_remove_bridge_output(self, mediaconnect, bridge):
        """RemoveBridgeOutput removes an output from a bridge."""
        mediaconnect.add_bridge_outputs(
            BridgeArn=bridge["BridgeArn"],
            Outputs=[
                {
                    "NetworkOutput": {
                        "IpAddress": "10.0.0.1",
                        "Name": "out-rm",
                        "NetworkName": "net1",
                        "Port": 6000,
                        "Protocol": "zixi-push",
                        "Ttl": 64,
                    }
                }
            ],
        )
        resp = mediaconnect.remove_bridge_output(BridgeArn=bridge["BridgeArn"], OutputName="out-rm")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_remove_bridge_source(self, mediaconnect, bridge):
        """RemoveBridgeSource removes a source from a bridge."""
        mediaconnect.add_bridge_sources(
            BridgeArn=bridge["BridgeArn"],
            Sources=[
                {
                    "NetworkSource": {
                        "Name": "src-rm",
                        "Protocol": "zixi-push",
                        "MulticastIp": "239.0.0.3",
                        "Port": 5002,
                        "NetworkName": "net1",
                    }
                }
            ],
        )
        resp = mediaconnect.remove_bridge_source(BridgeArn=bridge["BridgeArn"], SourceName="src-rm")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_bridge_output(self, mediaconnect, bridge):
        """UpdateBridgeOutput updates an output on a bridge."""
        mediaconnect.add_bridge_outputs(
            BridgeArn=bridge["BridgeArn"],
            Outputs=[
                {
                    "NetworkOutput": {
                        "IpAddress": "10.0.0.2",
                        "Name": "out-upd",
                        "NetworkName": "net1",
                        "Port": 6001,
                        "Protocol": "zixi-push",
                        "Ttl": 64,
                    }
                }
            ],
        )
        resp = mediaconnect.update_bridge_output(
            BridgeArn=bridge["BridgeArn"], OutputName="out-upd"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_bridge_source(self, mediaconnect, bridge):
        """UpdateBridgeSource updates a source on a bridge."""
        resp = mediaconnect.update_bridge_source(BridgeArn=bridge["BridgeArn"], SourceName="src1")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_bridge_state(self, mediaconnect, bridge):
        """UpdateBridgeState changes the bridge state."""
        resp = mediaconnect.update_bridge_state(
            BridgeArn=bridge["BridgeArn"], DesiredState="ACTIVE"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


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
            pass  # best-effort cleanup

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


class TestMediaConnectGatewayInstances:
    """Tests for gateway instance operations."""

    def test_describe_gateway_instance_not_found(self, mediaconnect):
        """DescribeGatewayInstance raises NotFoundException for fake ARN."""
        fake_arn = "arn:aws:mediaconnect:us-east-1:123456789012:gateway-instance:fake"
        with pytest.raises(mediaconnect.exceptions.NotFoundException):
            mediaconnect.describe_gateway_instance(GatewayInstanceArn=fake_arn)

    def test_deregister_gateway_instance_not_found(self, mediaconnect):
        """DeregisterGatewayInstance raises NotFoundException for fake ARN."""
        fake_arn = "arn:aws:mediaconnect:us-east-1:123456789012:gateway-instance:fake"
        with pytest.raises(mediaconnect.exceptions.NotFoundException):
            mediaconnect.deregister_gateway_instance(GatewayInstanceArn=fake_arn, Force=True)

    def test_update_gateway_instance_not_found(self, mediaconnect):
        """UpdateGatewayInstance raises NotFoundException for fake ARN."""
        fake_arn = "arn:aws:mediaconnect:us-east-1:123456789012:gateway-instance:fake"
        with pytest.raises(mediaconnect.exceptions.NotFoundException):
            mediaconnect.update_gateway_instance(GatewayInstanceArn=fake_arn)


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
            pass  # best-effort cleanup

    def test_update_flow(self, mediaconnect, flow):
        """UpdateFlow modifies flow properties."""
        resp = mediaconnect.update_flow(FlowArn=flow["arn"])
        assert "Flow" in resp
        assert resp["Flow"]["FlowArn"] == flow["arn"]
        assert resp["Flow"]["Status"] == "UPDATING"

    def test_describe_flow_source_metadata(self, mediaconnect, flow):
        """DescribeFlowSourceMetadata returns metadata for a flow."""
        resp = mediaconnect.describe_flow_source_metadata(FlowArn=flow["arn"])
        assert resp["FlowArn"] == flow["arn"]
        assert "Messages" in resp
        assert isinstance(resp["Messages"], list)

    def test_describe_flow_source_thumbnail(self, mediaconnect, flow):
        """DescribeFlowSourceThumbnail returns 200 for a valid flow."""
        resp = mediaconnect.describe_flow_source_thumbnail(FlowArn=flow["arn"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


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


class TestMediaConnectMediaStreams:
    """Tests for MediaStream operations."""

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
            pass  # best-effort cleanup

    def test_remove_flow_media_stream_not_found(self, mediaconnect, flow):
        """RemoveFlowMediaStream raises NotFoundException for nonexistent stream."""
        with pytest.raises(mediaconnect.exceptions.NotFoundException):
            mediaconnect.remove_flow_media_stream(
                FlowArn=flow["arn"], MediaStreamName="nonexistent"
            )


class TestMediaConnectOfferings:
    """Tests for offering and reservation operations."""

    def test_describe_offering_not_found(self, mediaconnect):
        """DescribeOffering raises NotFoundException for fake ARN."""
        fake_arn = "arn:aws:mediaconnect:us-east-1:123456789012:offering:fake"
        with pytest.raises(mediaconnect.exceptions.NotFoundException):
            mediaconnect.describe_offering(OfferingArn=fake_arn)

    def test_describe_offering(self, mediaconnect):
        """DescribeOffering returns offering details for a valid offering."""
        offerings = mediaconnect.list_offerings()["Offerings"]
        if not offerings:
            pytest.skip("No offerings available")
        resp = mediaconnect.describe_offering(OfferingArn=offerings[0]["OfferingArn"])
        offering = resp["Offering"]
        assert "OfferingArn" in offering
        assert "CurrencyCode" in offering
        assert "PricePerUnit" in offering
        assert "Duration" in offering

    def test_describe_reservation_not_found(self, mediaconnect):
        """DescribeReservation raises NotFoundException for fake ARN."""
        fake_arn = "arn:aws:mediaconnect:us-east-1:123456789012:reservation:fake"
        with pytest.raises(mediaconnect.exceptions.NotFoundException):
            mediaconnect.describe_reservation(ReservationArn=fake_arn)

    def test_purchase_offering(self, mediaconnect):
        """PurchaseOffering creates a reservation from an offering."""
        offerings = mediaconnect.list_offerings()["Offerings"]
        if not offerings:
            pytest.skip("No offerings available")
        name = _unique("res")
        resp = mediaconnect.purchase_offering(
            OfferingArn=offerings[0]["OfferingArn"],
            ReservationName=name,
            Start="2026-01-01T00:00:00Z",
        )
        reservation = resp["Reservation"]
        assert reservation["ReservationName"] == name
        assert "ReservationArn" in reservation
        assert reservation["ReservationState"] == "ACTIVE"
        assert "CurrencyCode" in reservation

    def test_describe_reservation_after_purchase(self, mediaconnect):
        """DescribeReservation returns details after PurchaseOffering."""
        offerings = mediaconnect.list_offerings()["Offerings"]
        if not offerings:
            pytest.skip("No offerings available")
        name = _unique("res")
        purchase = mediaconnect.purchase_offering(
            OfferingArn=offerings[0]["OfferingArn"],
            ReservationName=name,
            Start="2026-01-01T00:00:00Z",
        )
        res_arn = purchase["Reservation"]["ReservationArn"]
        resp = mediaconnect.describe_reservation(ReservationArn=res_arn)
        assert resp["Reservation"]["ReservationArn"] == res_arn
        assert resp["Reservation"]["ReservationName"] == name
        assert resp["Reservation"]["ReservationState"] == "ACTIVE"

    def test_purchase_offering_appears_in_list(self, mediaconnect):
        """Purchased reservation appears in ListReservations."""
        offerings = mediaconnect.list_offerings()["Offerings"]
        if not offerings:
            pytest.skip("No offerings available")
        name = _unique("res")
        purchase = mediaconnect.purchase_offering(
            OfferingArn=offerings[0]["OfferingArn"],
            ReservationName=name,
            Start="2026-01-01T00:00:00Z",
        )
        res_arn = purchase["Reservation"]["ReservationArn"]
        reservations = mediaconnect.list_reservations()["Reservations"]
        arns = [r["ReservationArn"] for r in reservations]
        assert res_arn in arns


class TestMediaConnectRouterNetworkInterfaces:
    """Tests for router network interface CRUD operations."""

    @pytest.fixture
    def rni(self, mediaconnect):
        name = _unique("rni")
        resp = mediaconnect.create_router_network_interface(
            Name=name,
            Configuration={"Vpc": {"SubnetId": "subnet-123", "SecurityGroupIds": ["sg-123"]}},
        )
        rni = resp["RouterNetworkInterface"]
        yield {"name": name, "arn": rni["Arn"], "state": rni["State"]}
        try:
            mediaconnect.delete_router_network_interface(Arn=rni["Arn"])
        except Exception:
            pass  # best-effort cleanup

    def test_create_router_network_interface(self, mediaconnect):
        """CreateRouterNetworkInterface creates and returns a network interface."""
        name = _unique("rni")
        resp = mediaconnect.create_router_network_interface(
            Name=name,
            Configuration={"Vpc": {"SubnetId": "subnet-abc", "SecurityGroupIds": ["sg-abc"]}},
        )
        rni = resp["RouterNetworkInterface"]
        assert rni["Name"] == name
        assert "Arn" in rni
        assert rni["State"] == "ACTIVE"
        mediaconnect.delete_router_network_interface(Arn=rni["Arn"])

    def test_get_router_network_interface(self, mediaconnect, rni):
        """GetRouterNetworkInterface returns the interface details."""
        resp = mediaconnect.get_router_network_interface(Arn=rni["arn"])
        iface = resp["RouterNetworkInterface"]
        assert iface["Arn"] == rni["arn"]
        assert iface["Name"] == rni["name"]

    def test_update_router_network_interface(self, mediaconnect, rni):
        """UpdateRouterNetworkInterface succeeds on a valid interface."""
        resp = mediaconnect.update_router_network_interface(Arn=rni["arn"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_router_network_interface(self, mediaconnect):
        """DeleteRouterNetworkInterface removes the interface."""
        name = _unique("rni")
        resp = mediaconnect.create_router_network_interface(
            Name=name,
            Configuration={"Vpc": {"SubnetId": "subnet-del", "SecurityGroupIds": ["sg-del"]}},
        )
        rni_arn = resp["RouterNetworkInterface"]["Arn"]
        del_resp = mediaconnect.delete_router_network_interface(Arn=rni_arn)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_batch_get_router_network_interface(self, mediaconnect, rni):
        """BatchGetRouterNetworkInterface returns interfaces by ARN."""
        resp = mediaconnect.batch_get_router_network_interface(Arns=[rni["arn"]])
        assert "RouterNetworkInterfaces" in resp
        assert isinstance(resp["RouterNetworkInterfaces"], list)

    def test_batch_get_router_network_interface_empty(self, mediaconnect):
        """BatchGetRouterNetworkInterface with fake ARN returns empty or error."""
        fake = "arn:aws:mediaconnect:us-east-1:123456789012:router-ni:fake"
        resp = mediaconnect.batch_get_router_network_interface(Arns=[fake])
        assert "RouterNetworkInterfaces" in resp


class TestMediaConnectRouterInputs:
    """Tests for router input CRUD and lifecycle operations."""

    @pytest.fixture
    def rni(self, mediaconnect):
        name = _unique("rni")
        resp = mediaconnect.create_router_network_interface(
            Name=name,
            Configuration={"Vpc": {"SubnetId": "subnet-ri", "SecurityGroupIds": ["sg-ri"]}},
        )
        rni = resp["RouterNetworkInterface"]
        yield rni["Arn"]
        try:
            mediaconnect.delete_router_network_interface(Arn=rni["Arn"])
        except Exception:
            pass  # best-effort cleanup

    @pytest.fixture
    def router_input(self, mediaconnect, rni):
        name = _unique("ri")
        resp = mediaconnect.create_router_input(
            Name=name,
            Configuration={
                "Standard": {
                    "NetworkInterfaceArn": rni,
                    "ProtocolConfiguration": {
                        "SrtCaller": {
                            "SourceAddress": "10.0.0.1",
                            "SourcePort": 5000,
                            "MinimumLatencyMilliseconds": 100,
                        }
                    },
                    "Protocol": "srt-caller",
                }
            },
            MaximumBitrate=1000000,
            RoutingScope="SESSION",
            Tier="STANDARD",
        )
        ri = resp["RouterInput"]
        yield {"name": name, "arn": ri["Arn"]}
        try:
            mediaconnect.delete_router_input(Arn=ri["Arn"])
        except Exception:
            pass  # best-effort cleanup

    def test_create_router_input(self, mediaconnect, rni):
        """CreateRouterInput creates and returns a router input."""
        name = _unique("ri")
        resp = mediaconnect.create_router_input(
            Name=name,
            Configuration={
                "Standard": {
                    "NetworkInterfaceArn": rni,
                    "ProtocolConfiguration": {
                        "SrtCaller": {
                            "SourceAddress": "10.0.0.1",
                            "SourcePort": 5000,
                            "MinimumLatencyMilliseconds": 100,
                        }
                    },
                    "Protocol": "srt-caller",
                }
            },
            MaximumBitrate=1000000,
            RoutingScope="SESSION",
            Tier="STANDARD",
        )
        ri = resp["RouterInput"]
        assert ri["Name"] == name
        assert "Arn" in ri
        assert ri["State"] == "ACTIVE"
        mediaconnect.delete_router_input(Arn=ri["Arn"])

    def test_get_router_input(self, mediaconnect, router_input):
        """GetRouterInput returns the router input details."""
        resp = mediaconnect.get_router_input(Arn=router_input["arn"])
        assert "RouterInput" in resp
        ri = resp["RouterInput"]
        assert ri["Arn"] == router_input["arn"]

    def test_start_router_input(self, mediaconnect, router_input):
        """StartRouterInput starts a router input."""
        resp = mediaconnect.start_router_input(Arn=router_input["arn"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_router_input(self, mediaconnect, router_input):
        """StopRouterInput stops a router input."""
        mediaconnect.start_router_input(Arn=router_input["arn"])
        resp = mediaconnect.stop_router_input(Arn=router_input["arn"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_restart_router_input(self, mediaconnect, router_input):
        """RestartRouterInput restarts a router input."""
        resp = mediaconnect.restart_router_input(Arn=router_input["arn"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_router_input(self, mediaconnect, router_input):
        """UpdateRouterInput modifies a router input."""
        resp = mediaconnect.update_router_input(Arn=router_input["arn"], MaximumBitrate=2000000)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_router_input_source_metadata(self, mediaconnect, router_input):
        """GetRouterInputSourceMetadata returns metadata for a router input."""
        resp = mediaconnect.get_router_input_source_metadata(Arn=router_input["arn"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_router_input_thumbnail(self, mediaconnect, router_input):
        """GetRouterInputThumbnail returns 200 for a valid router input."""
        resp = mediaconnect.get_router_input_thumbnail(Arn=router_input["arn"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_router_input(self, mediaconnect, rni):
        """DeleteRouterInput removes the router input."""
        name = _unique("ri")
        resp = mediaconnect.create_router_input(
            Name=name,
            Configuration={
                "Standard": {
                    "NetworkInterfaceArn": rni,
                    "ProtocolConfiguration": {
                        "SrtCaller": {
                            "SourceAddress": "10.0.0.1",
                            "SourcePort": 5000,
                            "MinimumLatencyMilliseconds": 100,
                        }
                    },
                    "Protocol": "srt-caller",
                }
            },
            MaximumBitrate=1000000,
            RoutingScope="SESSION",
            Tier="STANDARD",
        )
        ri_arn = resp["RouterInput"]["Arn"]
        del_resp = mediaconnect.delete_router_input(Arn=ri_arn)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_batch_get_router_input(self, mediaconnect, router_input):
        """BatchGetRouterInput returns inputs by ARN."""
        resp = mediaconnect.batch_get_router_input(Arns=[router_input["arn"]])
        assert "RouterInputs" in resp
        assert isinstance(resp["RouterInputs"], list)


class TestMediaConnectRouterOutputs:
    """Tests for router output CRUD and lifecycle operations."""

    @pytest.fixture
    def rni(self, mediaconnect):
        name = _unique("rni")
        resp = mediaconnect.create_router_network_interface(
            Name=name,
            Configuration={"Vpc": {"SubnetId": "subnet-ro", "SecurityGroupIds": ["sg-ro"]}},
        )
        rni = resp["RouterNetworkInterface"]
        yield rni["Arn"]
        try:
            mediaconnect.delete_router_network_interface(Arn=rni["Arn"])
        except Exception:
            pass  # best-effort cleanup

    @pytest.fixture
    def router_output(self, mediaconnect, rni):
        name = _unique("ro")
        resp = mediaconnect.create_router_output(
            Name=name,
            Configuration={
                "Standard": {
                    "NetworkInterfaceArn": rni,
                    "ProtocolConfiguration": {
                        "SrtCaller": {
                            "DestinationAddress": "10.0.0.1",
                            "DestinationPort": 5001,
                            "MinimumLatencyMilliseconds": 100,
                        }
                    },
                    "Protocol": "srt-caller",
                }
            },
            MaximumBitrate=1000000,
            RoutingScope="SESSION",
            Tier="STANDARD",
        )
        ro = resp["RouterOutput"]
        yield {"name": name, "arn": ro["Arn"]}
        try:
            mediaconnect.delete_router_output(Arn=ro["Arn"])
        except Exception:
            pass  # best-effort cleanup

    def test_create_router_output(self, mediaconnect, rni):
        """CreateRouterOutput creates and returns a router output."""
        name = _unique("ro")
        resp = mediaconnect.create_router_output(
            Name=name,
            Configuration={
                "Standard": {
                    "NetworkInterfaceArn": rni,
                    "ProtocolConfiguration": {
                        "SrtCaller": {
                            "DestinationAddress": "10.0.0.1",
                            "DestinationPort": 5001,
                            "MinimumLatencyMilliseconds": 100,
                        }
                    },
                    "Protocol": "srt-caller",
                }
            },
            MaximumBitrate=1000000,
            RoutingScope="SESSION",
            Tier="STANDARD",
        )
        ro = resp["RouterOutput"]
        assert ro["Name"] == name
        assert "Arn" in ro
        assert ro["State"] == "ACTIVE"
        mediaconnect.delete_router_output(Arn=ro["Arn"])

    def test_get_router_output(self, mediaconnect, router_output):
        """GetRouterOutput returns the router output details."""
        resp = mediaconnect.get_router_output(Arn=router_output["arn"])
        assert "RouterOutput" in resp
        ro = resp["RouterOutput"]
        assert ro["Arn"] == router_output["arn"]

    def test_start_router_output(self, mediaconnect, router_output):
        """StartRouterOutput starts a router output."""
        resp = mediaconnect.start_router_output(Arn=router_output["arn"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_router_output(self, mediaconnect, router_output):
        """StopRouterOutput stops a router output."""
        mediaconnect.start_router_output(Arn=router_output["arn"])
        resp = mediaconnect.stop_router_output(Arn=router_output["arn"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_restart_router_output(self, mediaconnect, router_output):
        """RestartRouterOutput restarts a router output."""
        resp = mediaconnect.restart_router_output(Arn=router_output["arn"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_router_output(self, mediaconnect, router_output):
        """UpdateRouterOutput modifies a router output."""
        resp = mediaconnect.update_router_output(Arn=router_output["arn"], MaximumBitrate=2000000)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_router_output(self, mediaconnect, rni):
        """DeleteRouterOutput removes the router output."""
        name = _unique("ro")
        resp = mediaconnect.create_router_output(
            Name=name,
            Configuration={
                "Standard": {
                    "NetworkInterfaceArn": rni,
                    "ProtocolConfiguration": {
                        "SrtCaller": {
                            "DestinationAddress": "10.0.0.1",
                            "DestinationPort": 5001,
                            "MinimumLatencyMilliseconds": 100,
                        }
                    },
                    "Protocol": "srt-caller",
                }
            },
            MaximumBitrate=1000000,
            RoutingScope="SESSION",
            Tier="STANDARD",
        )
        ro_arn = resp["RouterOutput"]["Arn"]
        del_resp = mediaconnect.delete_router_output(Arn=ro_arn)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_batch_get_router_output(self, mediaconnect, router_output):
        """BatchGetRouterOutput returns outputs by ARN."""
        resp = mediaconnect.batch_get_router_output(Arns=[router_output["arn"]])
        assert "RouterOutputs" in resp
        assert isinstance(resp["RouterOutputs"], list)


class TestMediaConnectTakeRouterInput:
    """Tests for TakeRouterInput operation."""

    @pytest.fixture
    def rni(self, mediaconnect):
        name = _unique("rni")
        resp = mediaconnect.create_router_network_interface(
            Name=name,
            Configuration={"Vpc": {"SubnetId": "subnet-take", "SecurityGroupIds": ["sg-take"]}},
        )
        rni = resp["RouterNetworkInterface"]
        yield rni["Arn"]
        try:
            mediaconnect.delete_router_network_interface(Arn=rni["Arn"])
        except Exception:
            pass  # best-effort cleanup

    def test_take_router_input(self, mediaconnect, rni):
        """TakeRouterInput connects a router output to a router input."""
        ri_resp = mediaconnect.create_router_input(
            Name=_unique("ri"),
            Configuration={
                "Standard": {
                    "NetworkInterfaceArn": rni,
                    "ProtocolConfiguration": {
                        "SrtCaller": {
                            "SourceAddress": "10.0.0.1",
                            "SourcePort": 5000,
                            "MinimumLatencyMilliseconds": 100,
                        }
                    },
                    "Protocol": "srt-caller",
                }
            },
            MaximumBitrate=1000000,
            RoutingScope="SESSION",
            Tier="STANDARD",
        )
        ri_arn = ri_resp["RouterInput"]["Arn"]

        ro_resp = mediaconnect.create_router_output(
            Name=_unique("ro"),
            Configuration={
                "Standard": {
                    "NetworkInterfaceArn": rni,
                    "ProtocolConfiguration": {
                        "SrtCaller": {
                            "DestinationAddress": "10.0.0.1",
                            "DestinationPort": 5002,
                            "MinimumLatencyMilliseconds": 100,
                        }
                    },
                    "Protocol": "srt-caller",
                }
            },
            MaximumBitrate=1000000,
            RoutingScope="SESSION",
            Tier="STANDARD",
        )
        ro_arn = ro_resp["RouterOutput"]["Arn"]

        resp = mediaconnect.take_router_input(RouterOutputArn=ro_arn, RouterInputArn=ri_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Cleanup
        mediaconnect.delete_router_input(Arn=ri_arn)
        mediaconnect.delete_router_output(Arn=ro_arn)
