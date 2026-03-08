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


class TestTransferUserOperations:
    """Tests for Transfer Family user CRUD operations."""

    def test_create_user(self, transfer):
        """CreateUser returns ServerId and UserName."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            user_resp = transfer.create_user(
                ServerId=server_id,
                UserName="testuser",
                Role="arn:aws:iam::123456789012:role/transfer-role",
            )
            assert user_resp["ServerId"] == server_id
            assert user_resp["UserName"] == "testuser"
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="testuser")
            except ClientError:
                pass
            transfer.delete_server(ServerId=server_id)

    def test_describe_user(self, transfer):
        """DescribeUser returns user details including Arn and Role."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="descuser",
                Role="arn:aws:iam::123456789012:role/transfer-role",
                HomeDirectory="/home/descuser",
            )
            desc = transfer.describe_user(ServerId=server_id, UserName="descuser")
            user = desc["User"]
            assert user["UserName"] == "descuser"
            assert "Arn" in user
            assert user["Role"] == "arn:aws:iam::123456789012:role/transfer-role"
            assert desc["ServerId"] == server_id
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="descuser")
            except ClientError:
                pass
            transfer.delete_server(ServerId=server_id)

    def test_delete_user(self, transfer):
        """DeleteUser removes the user; subsequent describe raises error."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="deluser",
                Role="arn:aws:iam::123456789012:role/transfer-role",
            )
            transfer.delete_user(ServerId=server_id, UserName="deluser")
            with pytest.raises(ClientError):
                transfer.describe_user(ServerId=server_id, UserName="deluser")
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_describe_nonexistent_server_raises(self, transfer):
        """DescribeServer with a nonexistent server ID raises an error."""
        with pytest.raises(ClientError):
            transfer.describe_server(ServerId="s-000000000000000000")

    def test_list_servers_contains_created(self, transfer):
        """ListServers includes a newly created server."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            listed = transfer.list_servers()
            server_ids = [s["ServerId"] for s in listed["Servers"]]
            assert server_id in server_ids
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
