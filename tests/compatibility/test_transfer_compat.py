"""AWS Transfer Family compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def transfer():
    return make_client("transfer")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestTransferServerOperations:
    """Tests for Transfer Family server CRUD operations."""

    def test_create_server_returns_server_id(self, transfer):
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        assert "ServerId" in resp
        server_id = resp["ServerId"]
        assert len(server_id) > 0
        # cleanup
        transfer.delete_server(ServerId=server_id)

    def test_describe_server(self, transfer):
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            server = desc["Server"]
            assert server["ServerId"] == server_id
            assert server["State"] == "ONLINE"
            assert server["IdentityProviderType"] == "SERVICE_MANAGED"
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_create_server_with_endpoint_type(self, transfer):
        resp = transfer.create_server(
            IdentityProviderType="SERVICE_MANAGED",
            EndpointType="PUBLIC",
        )
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["EndpointType"] == "PUBLIC"
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_create_server_with_protocols(self, transfer):
        resp = transfer.create_server(
            IdentityProviderType="SERVICE_MANAGED",
            Protocols=["SFTP"],
        )
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            assert "SFTP" in desc["Server"]["Protocols"]
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_create_server_with_tags(self, transfer):
        tags = [
            {"Key": "env", "Value": "test"},
            {"Key": "project", "Value": _unique("proj")},
        ]
        resp = transfer.create_server(
            IdentityProviderType="SERVICE_MANAGED",
            Tags=tags,
        )
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            returned_tags = desc["Server"]["Tags"]
            tag_map = {t["Key"]: t["Value"] for t in returned_tags}
            assert tag_map["env"] == "test"
            assert tag_map["project"] == tags[1]["Value"]
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_delete_server(self, transfer):
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        # delete should succeed without error
        transfer.delete_server(ServerId=server_id)

    def test_describe_server_after_delete_raises(self, transfer):
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        transfer.delete_server(ServerId=server_id)
        with pytest.raises(ClientError):
            transfer.describe_server(ServerId=server_id)

    def test_describe_server_has_expected_keys(self, transfer):
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            server = desc["Server"]
            # Verify core keys are present
            assert "Arn" in server
            assert "ServerId" in server
            assert "State" in server
        finally:
            transfer.delete_server(ServerId=server_id)


class TestTransferAutoCoverage:
    """Auto-generated coverage tests for transfer."""

    @pytest.fixture
    def client(self):
        return make_client("transfer")

    def test_list_servers(self, client):
        """ListServers returns a response."""
        resp = client.list_servers()
        assert "Servers" in resp
