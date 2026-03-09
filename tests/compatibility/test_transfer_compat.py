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


class TestTransferSshPublicKeyOperations:
    """Tests for Transfer Family SSH public key operations."""

    def test_import_ssh_public_key(self, transfer):
        """ImportSshPublicKey returns ServerId, UserName, and SshPublicKeyId."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="sshuser",
                Role="arn:aws:iam::123456789012:role/transfer-role",
            )
            key_resp = transfer.import_ssh_public_key(
                ServerId=server_id,
                UserName="sshuser",
                SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC test@test",
            )
            assert key_resp["ServerId"] == server_id
            assert key_resp["UserName"] == "sshuser"
            assert len(key_resp["SshPublicKeyId"]) > 0
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="sshuser")
            except ClientError:
                pass
            transfer.delete_server(ServerId=server_id)

    def test_import_ssh_key_visible_in_describe_user(self, transfer):
        """Imported SSH key appears in DescribeUser response."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="sshuser2",
                Role="arn:aws:iam::123456789012:role/transfer-role",
            )
            transfer.import_ssh_public_key(
                ServerId=server_id,
                UserName="sshuser2",
                SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC test@test",
            )
            desc = transfer.describe_user(ServerId=server_id, UserName="sshuser2")
            keys = desc["User"]["SshPublicKeys"]
            assert len(keys) >= 1
            assert keys[0]["SshPublicKeyBody"] == (
                "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC test@test"
            )
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="sshuser2")
            except ClientError:
                pass
            transfer.delete_server(ServerId=server_id)

    def test_delete_ssh_public_key(self, transfer):
        """DeleteSshPublicKey removes the key from the user."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="sshuser3",
                Role="arn:aws:iam::123456789012:role/transfer-role",
            )
            key_resp = transfer.import_ssh_public_key(
                ServerId=server_id,
                UserName="sshuser3",
                SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC test@test",
            )
            key_id = key_resp["SshPublicKeyId"]
            transfer.delete_ssh_public_key(
                ServerId=server_id, UserName="sshuser3", SshPublicKeyId=key_id
            )
            desc = transfer.describe_user(ServerId=server_id, UserName="sshuser3")
            keys = desc["User"].get("SshPublicKeys", [])
            assert len(keys) == 0
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="sshuser3")
            except ClientError:
                pass
            transfer.delete_server(ServerId=server_id)

    def test_import_multiple_ssh_keys(self, transfer):
        """Multiple SSH keys can be imported for one user."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="multikey",
                Role="arn:aws:iam::123456789012:role/transfer-role",
            )
            transfer.import_ssh_public_key(
                ServerId=server_id,
                UserName="multikey",
                SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC key1@test",
            )
            transfer.import_ssh_public_key(
                ServerId=server_id,
                UserName="multikey",
                SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQD key2@test",
            )
            desc = transfer.describe_user(ServerId=server_id, UserName="multikey")
            keys = desc["User"]["SshPublicKeys"]
            assert len(keys) >= 2
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="multikey")
            except ClientError:
                pass
            transfer.delete_server(ServerId=server_id)


class TestTransferAdditionalServerOps:
    """Additional server operation tests."""

    def test_list_servers_returns_server_details(self, transfer):
        """ListServers entries contain Arn, ServerId, State."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            listed = transfer.list_servers()
            servers = listed["Servers"]
            match = [s for s in servers if s["ServerId"] == server_id]
            assert len(match) == 1
            assert "Arn" in match[0]
            assert "State" in match[0]
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_list_servers_empty_initially(self, transfer):
        """ListServers returns Servers key even when empty."""
        resp = transfer.list_servers()
        assert "Servers" in resp

    def test_describe_nonexistent_user_raises(self, transfer):
        """DescribeUser with nonexistent username raises error."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            with pytest.raises(ClientError) as exc_info:
                transfer.describe_user(ServerId=server_id, UserName="nouser")
            assert exc_info.value.response["Error"]["Code"] == "UserNotFound"
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_create_user_with_home_directory(self, transfer):
        """CreateUser with HomeDirectory is reflected in DescribeUser."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="homedir",
                Role="arn:aws:iam::123456789012:role/transfer-role",
                HomeDirectory="/home/homedir",
            )
            desc = transfer.describe_user(ServerId=server_id, UserName="homedir")
            assert desc["User"]["HomeDirectory"] == "/home/homedir"
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="homedir")
            except ClientError:
                pass
            transfer.delete_server(ServerId=server_id)

    def test_create_multiple_users_on_server(self, transfer):
        """Multiple users can be created on the same server."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="user1",
                Role="arn:aws:iam::123456789012:role/transfer-role",
            )
            transfer.create_user(
                ServerId=server_id,
                UserName="user2",
                Role="arn:aws:iam::123456789012:role/transfer-role",
            )
            # Both users should be describable
            u1 = transfer.describe_user(ServerId=server_id, UserName="user1")
            u2 = transfer.describe_user(ServerId=server_id, UserName="user2")
            assert u1["User"]["UserName"] == "user1"
            assert u2["User"]["UserName"] == "user2"
        finally:
            for name in ["user1", "user2"]:
                try:
                    transfer.delete_user(ServerId=server_id, UserName=name)
                except ClientError:
                    pass
            transfer.delete_server(ServerId=server_id)

    def test_describe_user_has_arn(self, transfer):
        """DescribeUser response includes Arn."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="arnuser",
                Role="arn:aws:iam::123456789012:role/transfer-role",
            )
            desc = transfer.describe_user(ServerId=server_id, UserName="arnuser")
            assert "Arn" in desc["User"]
            assert "transfer" in desc["User"]["Arn"]
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="arnuser")
            except ClientError:
                pass
            transfer.delete_server(ServerId=server_id)

    def test_create_server_returns_arn_via_describe(self, transfer):
        """Created server's ARN contains the server ID."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            arn = desc["Server"]["Arn"]
            assert server_id in arn
            assert arn.startswith("arn:aws:transfer:")
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_create_multiple_servers(self, transfer):
        """Multiple servers can coexist and be listed."""
        ids = []
        try:
            for _ in range(3):
                resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
                ids.append(resp["ServerId"])
            listed = transfer.list_servers()
            listed_ids = [s["ServerId"] for s in listed["Servers"]]
            for sid in ids:
                assert sid in listed_ids
        finally:
            for sid in ids:
                try:
                    transfer.delete_server(ServerId=sid)
                except ClientError:
                    pass
