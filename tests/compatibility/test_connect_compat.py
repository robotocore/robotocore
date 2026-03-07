"""Amazon Connect compatibility tests."""

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def connect():
    return make_client("connect")


def _create_instance(client):
    """Helper to create a Connect instance."""
    resp = client.create_instance(
        IdentityManagementType="CONNECT_MANAGED",
        InboundCallsEnabled=True,
        OutboundCallsEnabled=True,
    )
    return resp["Id"], resp["Arn"]


def _list_all_instance_ids(client):
    """Paginate through all instances and return their IDs."""
    ids = []
    paginator = client.get_paginator("list_instances")
    for page in paginator.paginate():
        for inst in page.get("InstanceSummaryList", []):
            ids.append(inst["Id"])
    return ids


class TestConnectInstances:
    def test_create_instance(self, connect):
        instance_id, arn = _create_instance(connect)
        assert instance_id is not None
        assert "arn:aws:connect:" in arn
        assert instance_id in arn

    def test_create_instance_returns_id_and_arn(self, connect):
        resp = connect.create_instance(
            IdentityManagementType="CONNECT_MANAGED",
            InboundCallsEnabled=True,
            OutboundCallsEnabled=False,
        )
        assert "Id" in resp
        assert "Arn" in resp
        assert len(resp["Id"]) > 0

    def test_list_instances(self, connect):
        _create_instance(connect)
        resp = connect.list_instances()
        assert "InstanceSummaryList" in resp
        assert len(resp["InstanceSummaryList"]) >= 1

    def test_list_instances_has_summary_fields(self, connect):
        """Verify list_instances returns summaries with expected fields."""
        resp = connect.list_instances()
        assert len(resp["InstanceSummaryList"]) >= 1
        summary = resp["InstanceSummaryList"][0]
        assert "Id" in summary
        assert "Arn" in summary
        assert "IdentityManagementType" in summary

    def test_describe_instance(self, connect):
        instance_id, arn = _create_instance(connect)
        resp = connect.describe_instance(InstanceId=instance_id)
        assert "Instance" in resp
        instance = resp["Instance"]
        assert instance["Id"] == instance_id
        assert instance["Arn"] == arn

    def test_describe_instance_fields(self, connect):
        instance_id, _ = _create_instance(connect)
        resp = connect.describe_instance(InstanceId=instance_id)
        instance = resp["Instance"]
        assert instance["IdentityManagementType"] == "CONNECT_MANAGED"
        assert instance["InboundCallsEnabled"] is True
        assert instance["OutboundCallsEnabled"] is True

    def test_describe_instance_matches_create(self, connect):
        """Describe returns the same ID and ARN that create returned."""
        instance_id, arn = _create_instance(connect)
        resp = connect.describe_instance(InstanceId=instance_id)
        assert resp["Instance"]["Id"] == instance_id
        assert resp["Instance"]["Arn"] == arn

    def test_delete_instance(self, connect):
        instance_id, _ = _create_instance(connect)
        connect.delete_instance(InstanceId=instance_id)
        with pytest.raises(ClientError):
            connect.describe_instance(InstanceId=instance_id)

    def test_delete_instance_then_describe_fails(self, connect):
        instance_id, _ = _create_instance(connect)
        connect.delete_instance(InstanceId=instance_id)
        with pytest.raises(ClientError):
            connect.describe_instance(InstanceId=instance_id)

    def test_create_multiple_instances_unique_ids(self, connect):
        id1, _ = _create_instance(connect)
        id2, _ = _create_instance(connect)
        assert id1 != id2
        # Both should be describable
        resp1 = connect.describe_instance(InstanceId=id1)
        resp2 = connect.describe_instance(InstanceId=id2)
        assert resp1["Instance"]["Id"] == id1
        assert resp2["Instance"]["Id"] == id2

    def test_delete_one_of_multiple_instances(self, connect):
        id1, _ = _create_instance(connect)
        id2, _ = _create_instance(connect)
        connect.delete_instance(InstanceId=id1)
        # Verify deleted instance is gone
        with pytest.raises(ClientError):
            connect.describe_instance(InstanceId=id1)
        # Verify the other instance still exists
        resp = connect.describe_instance(InstanceId=id2)
        assert resp["Instance"]["Id"] == id2
