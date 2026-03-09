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
