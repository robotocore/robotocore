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
