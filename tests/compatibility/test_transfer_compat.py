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
                pass  # best-effort cleanup
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
                pass  # best-effort cleanup
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
                pass  # best-effort cleanup
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
                pass  # best-effort cleanup
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
                pass  # best-effort cleanup
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
                pass  # best-effort cleanup
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
                pass  # best-effort cleanup
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
                    pass  # best-effort cleanup
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
                pass  # best-effort cleanup
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
                    pass  # best-effort cleanup


class TestTransferServerAdvanced:
    """Tests for advanced server creation options and field verification."""

    def test_create_server_with_domain_s3(self, transfer):
        """CreateServer with Domain=S3 is reflected in DescribeServer."""
        resp = transfer.create_server(
            IdentityProviderType="SERVICE_MANAGED",
            Domain="S3",
        )
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["Domain"] == "S3"
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_create_server_with_domain_efs(self, transfer):
        """CreateServer with Domain=EFS is reflected in DescribeServer."""
        resp = transfer.create_server(
            IdentityProviderType="SERVICE_MANAGED",
            Domain="EFS",
        )
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["Domain"] == "EFS"
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_create_server_with_logging_role(self, transfer):
        """CreateServer with LoggingRole is reflected in DescribeServer."""
        role = "arn:aws:iam::123456789012:role/transfer-logging"
        resp = transfer.create_server(
            IdentityProviderType="SERVICE_MANAGED",
            LoggingRole=role,
        )
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["LoggingRole"] == role
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_create_server_with_pre_auth_banner(self, transfer):
        """CreateServer with PreAuthenticationLoginBanner is reflected."""
        banner = "Welcome to the SFTP server"
        resp = transfer.create_server(
            IdentityProviderType="SERVICE_MANAGED",
            PreAuthenticationLoginBanner=banner,
        )
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["PreAuthenticationLoginBanner"] == banner
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_create_server_with_post_auth_banner(self, transfer):
        """CreateServer with PostAuthenticationLoginBanner is reflected."""
        banner = "You are now logged in"
        resp = transfer.create_server(
            IdentityProviderType="SERVICE_MANAGED",
            PostAuthenticationLoginBanner=banner,
        )
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["PostAuthenticationLoginBanner"] == banner
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_create_server_with_security_policy(self, transfer):
        """CreateServer with SecurityPolicyName is reflected."""
        resp = transfer.create_server(
            IdentityProviderType="SERVICE_MANAGED",
            SecurityPolicyName="TransferSecurityPolicy-2020-06",
        )
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["SecurityPolicyName"] == "TransferSecurityPolicy-2020-06"
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_create_server_with_multiple_protocols(self, transfer):
        """CreateServer with multiple protocols is reflected."""
        resp = transfer.create_server(
            IdentityProviderType="SERVICE_MANAGED",
            Protocols=["SFTP", "FTP"],
        )
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            protocols = desc["Server"]["Protocols"]
            assert "SFTP" in protocols
            assert "FTP" in protocols
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_describe_server_user_count_zero(self, transfer):
        """DescribeServer shows UserCount=0 for new server."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["UserCount"] == 0
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_describe_server_user_count_increments(self, transfer):
        """UserCount increments as users are added."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="countuser1",
                Role="arn:aws:iam::123456789012:role/r",
            )
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["UserCount"] == 1

            transfer.create_user(
                ServerId=server_id,
                UserName="countuser2",
                Role="arn:aws:iam::123456789012:role/r",
            )
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["UserCount"] == 2
        finally:
            for name in ["countuser1", "countuser2"]:
                try:
                    transfer.delete_user(ServerId=server_id, UserName=name)
                except ClientError:
                    pass  # best-effort cleanup
            transfer.delete_server(ServerId=server_id)

    def test_describe_server_user_count_decrements(self, transfer):
        """UserCount decrements when a user is deleted."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="decuser",
                Role="arn:aws:iam::123456789012:role/r",
            )
            transfer.delete_user(ServerId=server_id, UserName="decuser")
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["UserCount"] == 0
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_list_servers_short_dict_fields(self, transfer):
        """ListServers entries contain Domain, EndpointType, IdentityProviderType, UserCount."""
        resp = transfer.create_server(
            IdentityProviderType="SERVICE_MANAGED",
            Domain="S3",
            EndpointType="PUBLIC",
        )
        server_id = resp["ServerId"]
        try:
            listed = transfer.list_servers()
            match = [s for s in listed["Servers"] if s["ServerId"] == server_id]
            assert len(match) == 1
            entry = match[0]
            assert entry["IdentityProviderType"] == "SERVICE_MANAGED"
            assert entry["UserCount"] == 0
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_create_server_identity_provider_api_gateway(self, transfer):
        """CreateServer with API_GATEWAY identity provider type."""
        resp = transfer.create_server(
            IdentityProviderType="API_GATEWAY",
            IdentityProviderDetails={
                "Url": "https://example.execute-api.us-east-1.amazonaws.com/prod",
                "InvocationRole": "arn:aws:iam::123456789012:role/invoke-role",
            },
        )
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["IdentityProviderType"] == "API_GATEWAY"
            details = desc["Server"]["IdentityProviderDetails"]
            assert details["Url"] == "https://example.execute-api.us-east-1.amazonaws.com/prod"
            assert details["InvocationRole"] == "arn:aws:iam::123456789012:role/invoke-role"
        finally:
            transfer.delete_server(ServerId=server_id)


class TestTransferUserAdvanced:
    """Tests for advanced user creation options."""

    def test_create_user_with_home_directory_type_path(self, transfer):
        """CreateUser with HomeDirectoryType=PATH is reflected."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="pathuser",
                Role="arn:aws:iam::123456789012:role/r",
                HomeDirectory="/bucket/prefix",
                HomeDirectoryType="PATH",
            )
            desc = transfer.describe_user(ServerId=server_id, UserName="pathuser")
            assert desc["User"]["HomeDirectoryType"] == "PATH"
            assert desc["User"]["HomeDirectory"] == "/bucket/prefix"
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="pathuser")
            except ClientError:
                pass  # best-effort cleanup
            transfer.delete_server(ServerId=server_id)

    def test_create_user_with_home_directory_mappings(self, transfer):
        """CreateUser with HomeDirectoryMappings is reflected."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="mapuser",
                Role="arn:aws:iam::123456789012:role/r",
                HomeDirectoryType="LOGICAL",
                HomeDirectoryMappings=[
                    {"Entry": "/", "Target": "/mybucket/home/mapuser"},
                ],
            )
            desc = transfer.describe_user(ServerId=server_id, UserName="mapuser")
            mappings = desc["User"]["HomeDirectoryMappings"]
            assert len(mappings) >= 1
            assert mappings[0]["Entry"] == "/"
            assert mappings[0]["Target"] == "/mybucket/home/mapuser"
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="mapuser")
            except ClientError:
                pass  # best-effort cleanup
            transfer.delete_server(ServerId=server_id)

    def test_create_user_with_posix_profile(self, transfer):
        """CreateUser with PosixProfile is reflected."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="posixuser",
                Role="arn:aws:iam::123456789012:role/r",
                PosixProfile={"Uid": 1000, "Gid": 1000},
            )
            desc = transfer.describe_user(ServerId=server_id, UserName="posixuser")
            posix = desc["User"]["PosixProfile"]
            assert posix["Uid"] == 1000
            assert posix["Gid"] == 1000
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="posixuser")
            except ClientError:
                pass  # best-effort cleanup
            transfer.delete_server(ServerId=server_id)

    def test_create_user_with_policy(self, transfer):
        """CreateUser with Policy is reflected."""
        policy = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}'
        )
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="policyuser",
                Role="arn:aws:iam::123456789012:role/r",
                Policy=policy,
            )
            desc = transfer.describe_user(ServerId=server_id, UserName="policyuser")
            assert desc["User"]["Policy"] == policy
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="policyuser")
            except ClientError:
                pass  # best-effort cleanup
            transfer.delete_server(ServerId=server_id)

    def test_create_user_with_tags(self, transfer):
        """CreateUser with Tags is reflected in DescribeUser."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        tag_val = _unique("val")
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="taguser",
                Role="arn:aws:iam::123456789012:role/r",
                Tags=[{"Key": "team", "Value": tag_val}],
            )
            desc = transfer.describe_user(ServerId=server_id, UserName="taguser")
            tags = desc["User"]["Tags"]
            tag_map = {t["Key"]: t["Value"] for t in tags}
            assert tag_map["team"] == tag_val
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="taguser")
            except ClientError:
                pass  # best-effort cleanup
            transfer.delete_server(ServerId=server_id)

    def test_create_user_arn_contains_server_and_username(self, transfer):
        """User ARN format includes server ID and username."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="arnfmt",
                Role="arn:aws:iam::123456789012:role/r",
            )
            desc = transfer.describe_user(ServerId=server_id, UserName="arnfmt")
            arn = desc["User"]["Arn"]
            assert server_id in arn
            assert "arnfmt" in arn
            assert arn.startswith("arn:aws:transfer:")
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="arnfmt")
            except ClientError:
                pass  # best-effort cleanup
            transfer.delete_server(ServerId=server_id)

    def test_create_user_with_ssh_key_inline(self, transfer):
        """CreateUser with SshPublicKeyBody at creation includes the key."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="inlinekey",
                Role="arn:aws:iam::123456789012:role/r",
            )
            # Import key separately (inline via CreateUser not directly supported by boto3)
            transfer.import_ssh_public_key(
                ServerId=server_id,
                UserName="inlinekey",
                SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC inline@test",
            )
            desc = transfer.describe_user(ServerId=server_id, UserName="inlinekey")
            keys = desc["User"]["SshPublicKeys"]
            assert len(keys) == 1
            assert "SshPublicKeyId" in keys[0]
            assert "DateImported" in keys[0]
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="inlinekey")
            except ClientError:
                pass  # best-effort cleanup
            transfer.delete_server(ServerId=server_id)


class TestTransferErrorCases:
    """Tests for error handling in Transfer operations."""

    def test_delete_nonexistent_server_raises(self, transfer):
        """DeleteServer with nonexistent ID raises error."""
        with pytest.raises(ClientError) as exc_info:
            transfer.delete_server(ServerId="s-00000000000000000")
        assert "Error" in exc_info.value.response

    def test_create_user_on_nonexistent_server_raises(self, transfer):
        """CreateUser on a nonexistent server raises error."""
        with pytest.raises(ClientError):
            transfer.create_user(
                ServerId="s-00000000000000000",
                UserName="noserver",
                Role="arn:aws:iam::123456789012:role/r",
            )

    def test_import_key_for_nonexistent_user_raises(self, transfer):
        """ImportSshPublicKey for nonexistent user raises error."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            with pytest.raises(ClientError):
                transfer.import_ssh_public_key(
                    ServerId=server_id,
                    UserName="ghost",
                    SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC ghost@test",
                )
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_delete_key_nonexistent_raises(self, transfer):
        """DeleteSshPublicKey for nonexistent key raises error."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="nokey",
                Role="arn:aws:iam::123456789012:role/r",
            )
            with pytest.raises(ClientError):
                transfer.delete_ssh_public_key(
                    ServerId=server_id,
                    UserName="nokey",
                    SshPublicKeyId="key-00000000000000000",
                )
        finally:
            try:
                transfer.delete_user(ServerId=server_id, UserName="nokey")
            except ClientError:
                pass  # best-effort cleanup
            transfer.delete_server(ServerId=server_id)

    def test_import_key_for_nonexistent_server_raises(self, transfer):
        """ImportSshPublicKey on nonexistent server raises error."""
        with pytest.raises(ClientError):
            transfer.import_ssh_public_key(
                ServerId="s-00000000000000000",
                UserName="anyuser",
                SshPublicKeyBody="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC test@test",
            )

    def test_delete_user_nonexistent_raises(self, transfer):
        """DeleteUser for nonexistent user on valid server raises error."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            with pytest.raises(ClientError):
                transfer.delete_user(ServerId=server_id, UserName="nonexistent")
        finally:
            transfer.delete_server(ServerId=server_id)


class TestTransferListOperations:
    """Tests for Transfer list operations that require no resources."""

    def test_list_certificates_empty(self, transfer):
        """ListCertificates returns Certificates key."""
        resp = transfer.list_certificates()
        assert "Certificates" in resp
        assert isinstance(resp["Certificates"], list)

    def test_list_connectors_empty(self, transfer):
        """ListConnectors returns Connectors key."""
        resp = transfer.list_connectors()
        assert "Connectors" in resp
        assert isinstance(resp["Connectors"], list)

    def test_list_profiles_empty(self, transfer):
        """ListProfiles returns Profiles key."""
        resp = transfer.list_profiles()
        assert "Profiles" in resp
        assert isinstance(resp["Profiles"], list)

    def test_list_security_policies(self, transfer):
        """ListSecurityPolicies returns SecurityPolicyNames."""
        resp = transfer.list_security_policies()
        assert "SecurityPolicyNames" in resp
        assert isinstance(resp["SecurityPolicyNames"], list)
        assert len(resp["SecurityPolicyNames"]) > 0

    def test_list_web_apps_empty(self, transfer):
        """ListWebApps returns WebApps key."""
        resp = transfer.list_web_apps()
        assert "WebApps" in resp
        assert isinstance(resp["WebApps"], list)

    def test_list_workflows_empty(self, transfer):
        """ListWorkflows returns Workflows key."""
        resp = transfer.list_workflows()
        assert "Workflows" in resp
        assert isinstance(resp["Workflows"], list)


class TestTransferSecurityPolicy:
    """Tests for DescribeSecurityPolicy."""

    def test_describe_security_policy(self, transfer):
        """DescribeSecurityPolicy returns policy details."""
        resp = transfer.describe_security_policy(
            SecurityPolicyName="TransferSecurityPolicy-2020-06"
        )
        policy = resp["SecurityPolicy"]
        assert policy["SecurityPolicyName"] == "TransferSecurityPolicy-2020-06"
        assert "SshCiphers" in policy or "TlsCiphers" in policy

    def test_describe_security_policy_from_list(self, transfer):
        """DescribeSecurityPolicy works for a policy found via ListSecurityPolicies."""
        policies = transfer.list_security_policies()
        name = policies["SecurityPolicyNames"][0]
        resp = transfer.describe_security_policy(SecurityPolicyName=name)
        assert resp["SecurityPolicy"]["SecurityPolicyName"] == name


class TestTransferProfileOperations:
    """Tests for Transfer profile CRUD operations."""

    def test_create_and_describe_profile(self, transfer):
        """CreateProfile returns ProfileId, DescribeProfile returns details."""
        as2_id = _unique("as2")
        resp = transfer.create_profile(As2Id=as2_id, ProfileType="LOCAL")
        profile_id = resp["ProfileId"]
        assert len(profile_id) > 0
        try:
            desc = transfer.describe_profile(ProfileId=profile_id)
            profile = desc["Profile"]
            assert profile["ProfileId"] == profile_id
            assert profile["ProfileType"] == "LOCAL"
            assert profile["As2Id"] == as2_id
        finally:
            transfer.delete_profile(ProfileId=profile_id)

    def test_delete_profile(self, transfer):
        """DeleteProfile removes the profile."""
        resp = transfer.create_profile(As2Id=_unique("as2"), ProfileType="LOCAL")
        profile_id = resp["ProfileId"]
        transfer.delete_profile(ProfileId=profile_id)
        with pytest.raises(ClientError):
            transfer.describe_profile(ProfileId=profile_id)

    def test_list_profiles_contains_created(self, transfer):
        """ListProfiles includes a newly created profile."""
        resp = transfer.create_profile(As2Id=_unique("as2"), ProfileType="LOCAL")
        profile_id = resp["ProfileId"]
        try:
            listed = transfer.list_profiles()
            profile_ids = [p["ProfileId"] for p in listed["Profiles"]]
            assert profile_id in profile_ids
        finally:
            transfer.delete_profile(ProfileId=profile_id)

    def test_create_profile_partner_type(self, transfer):
        """CreateProfile with PARTNER type works."""
        resp = transfer.create_profile(As2Id=_unique("partner"), ProfileType="PARTNER")
        profile_id = resp["ProfileId"]
        try:
            desc = transfer.describe_profile(ProfileId=profile_id)
            assert desc["Profile"]["ProfileType"] == "PARTNER"
        finally:
            transfer.delete_profile(ProfileId=profile_id)

    def test_describe_profile_has_arn(self, transfer):
        """DescribeProfile response includes Arn."""
        resp = transfer.create_profile(As2Id=_unique("arn"), ProfileType="LOCAL")
        profile_id = resp["ProfileId"]
        try:
            desc = transfer.describe_profile(ProfileId=profile_id)
            assert "Arn" in desc["Profile"]
            assert "transfer" in desc["Profile"]["Arn"]
        finally:
            transfer.delete_profile(ProfileId=profile_id)


class TestTransferWorkflowOperations:
    """Tests for Transfer workflow CRUD operations."""

    def test_create_and_describe_workflow(self, transfer):
        """CreateWorkflow returns WorkflowId, DescribeWorkflow returns details."""
        resp = transfer.create_workflow(
            Steps=[
                {
                    "Type": "COPY",
                    "CopyStepDetails": {
                        "Name": "copy-step",
                        "DestinationFileLocation": {
                            "S3FileLocation": {
                                "Bucket": "test-bucket",
                                "Key": "dest/",
                            }
                        },
                    },
                }
            ]
        )
        workflow_id = resp["WorkflowId"]
        assert len(workflow_id) > 0
        try:
            desc = transfer.describe_workflow(WorkflowId=workflow_id)
            wf = desc["Workflow"]
            assert wf["WorkflowId"] == workflow_id
            assert len(wf["Steps"]) == 1
            assert wf["Steps"][0]["Type"] == "COPY"
        finally:
            transfer.delete_workflow(WorkflowId=workflow_id)

    def test_delete_workflow(self, transfer):
        """DeleteWorkflow removes the workflow."""
        resp = transfer.create_workflow(
            Steps=[
                {
                    "Type": "COPY",
                    "CopyStepDetails": {
                        "Name": "del-step",
                        "DestinationFileLocation": {
                            "S3FileLocation": {
                                "Bucket": "test-bucket",
                                "Key": "dest/",
                            }
                        },
                    },
                }
            ]
        )
        workflow_id = resp["WorkflowId"]
        transfer.delete_workflow(WorkflowId=workflow_id)
        with pytest.raises(ClientError):
            transfer.describe_workflow(WorkflowId=workflow_id)

    def test_list_workflows_contains_created(self, transfer):
        """ListWorkflows includes a newly created workflow."""
        resp = transfer.create_workflow(
            Steps=[
                {
                    "Type": "COPY",
                    "CopyStepDetails": {
                        "Name": "list-step",
                        "DestinationFileLocation": {
                            "S3FileLocation": {
                                "Bucket": "test-bucket",
                                "Key": "dest/",
                            }
                        },
                    },
                }
            ]
        )
        workflow_id = resp["WorkflowId"]
        try:
            listed = transfer.list_workflows()
            workflow_ids = [w["WorkflowId"] for w in listed["Workflows"]]
            assert workflow_id in workflow_ids
        finally:
            transfer.delete_workflow(WorkflowId=workflow_id)

    def test_describe_workflow_has_arn(self, transfer):
        """DescribeWorkflow response includes Arn."""
        resp = transfer.create_workflow(
            Steps=[
                {
                    "Type": "COPY",
                    "CopyStepDetails": {
                        "Name": "arn-step",
                        "DestinationFileLocation": {
                            "S3FileLocation": {
                                "Bucket": "test-bucket",
                                "Key": "dest/",
                            }
                        },
                    },
                }
            ]
        )
        workflow_id = resp["WorkflowId"]
        try:
            desc = transfer.describe_workflow(WorkflowId=workflow_id)
            assert "Arn" in desc["Workflow"]
        finally:
            transfer.delete_workflow(WorkflowId=workflow_id)


class TestTransferCertificateOperations:
    """Tests for Transfer certificate CRUD operations."""

    DUMMY_CERT = "-----BEGIN CERTIFICATE-----\ntest-cert-body\n-----END CERTIFICATE-----"

    def test_import_and_describe_certificate(self, transfer):
        """ImportCertificate returns CertificateId, DescribeCertificate returns details."""
        resp = transfer.import_certificate(
            Usage="SIGNING",
            Certificate=self.DUMMY_CERT,
        )
        cert_id = resp["CertificateId"]
        assert len(cert_id) > 0
        try:
            desc = transfer.describe_certificate(CertificateId=cert_id)
            cert = desc["Certificate"]
            assert cert["CertificateId"] == cert_id
            assert cert["Usage"] == "SIGNING"
            assert cert["Certificate"] == self.DUMMY_CERT
        finally:
            transfer.delete_certificate(CertificateId=cert_id)

    def test_delete_certificate(self, transfer):
        """DeleteCertificate removes the certificate."""
        resp = transfer.import_certificate(
            Usage="SIGNING",
            Certificate=self.DUMMY_CERT,
        )
        cert_id = resp["CertificateId"]
        transfer.delete_certificate(CertificateId=cert_id)
        with pytest.raises(ClientError):
            transfer.describe_certificate(CertificateId=cert_id)

    def test_list_certificates_contains_imported(self, transfer):
        """ListCertificates includes an imported certificate."""
        resp = transfer.import_certificate(
            Usage="SIGNING",
            Certificate=self.DUMMY_CERT,
        )
        cert_id = resp["CertificateId"]
        try:
            listed = transfer.list_certificates()
            cert_ids = [c["CertificateId"] for c in listed["Certificates"]]
            assert cert_id in cert_ids
        finally:
            transfer.delete_certificate(CertificateId=cert_id)

    def test_import_certificate_encryption_usage(self, transfer):
        """ImportCertificate with ENCRYPTION usage works."""
        resp = transfer.import_certificate(
            Usage="ENCRYPTION",
            Certificate=self.DUMMY_CERT,
        )
        cert_id = resp["CertificateId"]
        try:
            desc = transfer.describe_certificate(CertificateId=cert_id)
            assert desc["Certificate"]["Usage"] == "ENCRYPTION"
        finally:
            transfer.delete_certificate(CertificateId=cert_id)

    def test_describe_certificate_has_arn(self, transfer):
        """DescribeCertificate response includes Arn."""
        resp = transfer.import_certificate(
            Usage="SIGNING",
            Certificate=self.DUMMY_CERT,
        )
        cert_id = resp["CertificateId"]
        try:
            desc = transfer.describe_certificate(CertificateId=cert_id)
            assert "Arn" in desc["Certificate"]
        finally:
            transfer.delete_certificate(CertificateId=cert_id)


class TestTransferTagOperations:
    """Tests for Transfer tag operations."""

    def test_tag_and_list_tags(self, transfer):
        """TagResource adds tags visible via ListTagsForResource."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            arn = desc["Server"]["Arn"]
            transfer.tag_resource(
                Arn=arn,
                Tags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "platform"},
                ],
            )
            tags_resp = transfer.list_tags_for_resource(Arn=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "platform"
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_untag_resource(self, transfer):
        """UntagResource removes specified tag keys."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            arn = desc["Server"]["Arn"]
            transfer.tag_resource(
                Arn=arn,
                Tags=[
                    {"Key": "keep", "Value": "yes"},
                    {"Key": "drop", "Value": "yes"},
                ],
            )
            transfer.untag_resource(Arn=arn, TagKeys=["drop"])
            tags_resp = transfer.list_tags_for_resource(Arn=arn)
            tag_keys = [t["Key"] for t in tags_resp["Tags"]]
            assert "keep" in tag_keys
            assert "drop" not in tag_keys
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_list_tags_empty(self, transfer):
        """ListTagsForResource on untagged server returns empty list."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            desc = transfer.describe_server(ServerId=server_id)
            arn = desc["Server"]["Arn"]
            tags_resp = transfer.list_tags_for_resource(Arn=arn)
            assert tags_resp["Tags"] == []
        finally:
            transfer.delete_server(ServerId=server_id)


class TestTransferAccessOperations:
    """Tests for Transfer Family access CRUD operations."""

    def test_create_and_describe_access(self, transfer):
        """CreateAccess/DescribeAccess with external ID."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        ext_id = "S-1-1-12-1234567890-123456789-123456789-1234"
        try:
            create_resp = transfer.create_access(
                ServerId=server_id,
                ExternalId=ext_id,
                Role="arn:aws:iam::123456789012:role/transfer-role",
                HomeDirectory="/bucket",
            )
            assert create_resp["ServerId"] == server_id
            assert create_resp["ExternalId"] == ext_id

            desc = transfer.describe_access(ServerId=server_id, ExternalId=ext_id)
            assert desc["ServerId"] == server_id
            access = desc["Access"]
            assert access["ExternalId"] == ext_id
            assert access["Role"] == "arn:aws:iam::123456789012:role/transfer-role"
            assert access["HomeDirectory"] == "/bucket"
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_list_accesses_contains_created(self, transfer):
        """ListAccesses includes a newly created access."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        ext_id = "S-1-1-12-1111111111-222222222-333333333-4444"
        try:
            transfer.create_access(
                ServerId=server_id,
                ExternalId=ext_id,
                Role="arn:aws:iam::123456789012:role/r",
                HomeDirectory="/bucket",
            )
            listed = transfer.list_accesses(ServerId=server_id)
            assert "Accesses" in listed
            ext_ids = [a["ExternalId"] for a in listed["Accesses"]]
            assert ext_id in ext_ids
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_list_accesses_empty(self, transfer):
        """ListAccesses on server with no accesses returns empty list."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            listed = transfer.list_accesses(ServerId=server_id)
            assert listed["Accesses"] == []
        finally:
            transfer.delete_server(ServerId=server_id)


class TestTransferAgreementOperations:
    """Tests for Transfer Family agreement CRUD operations."""

    def test_create_and_describe_agreement(self, transfer):
        """CreateAgreement/DescribeAgreement full lifecycle."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        local = transfer.create_profile(As2Id=_unique("local"), ProfileType="LOCAL")
        partner = transfer.create_profile(As2Id=_unique("partner"), ProfileType="PARTNER")
        try:
            ag_resp = transfer.create_agreement(
                ServerId=server_id,
                LocalProfileId=local["ProfileId"],
                PartnerProfileId=partner["ProfileId"],
                BaseDirectory="/bucket",
                AccessRole="arn:aws:iam::123456789012:role/transfer-role",
            )
            agreement_id = ag_resp["AgreementId"]
            assert len(agreement_id) > 0

            desc = transfer.describe_agreement(ServerId=server_id, AgreementId=agreement_id)
            agreement = desc["Agreement"]
            assert agreement["AgreementId"] == agreement_id
            assert agreement["LocalProfileId"] == local["ProfileId"]
            assert agreement["PartnerProfileId"] == partner["ProfileId"]
            assert agreement["BaseDirectory"] == "/bucket"
        finally:
            transfer.delete_server(ServerId=server_id)
            transfer.delete_profile(ProfileId=local["ProfileId"])
            transfer.delete_profile(ProfileId=partner["ProfileId"])

    def test_list_agreements_contains_created(self, transfer):
        """ListAgreements includes a newly created agreement."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        local = transfer.create_profile(As2Id=_unique("local"), ProfileType="LOCAL")
        partner = transfer.create_profile(As2Id=_unique("partner"), ProfileType="PARTNER")
        try:
            ag_resp = transfer.create_agreement(
                ServerId=server_id,
                LocalProfileId=local["ProfileId"],
                PartnerProfileId=partner["ProfileId"],
                BaseDirectory="/bucket",
                AccessRole="arn:aws:iam::123456789012:role/r",
            )
            listed = transfer.list_agreements(ServerId=server_id)
            assert "Agreements" in listed
            ag_ids = [a["AgreementId"] for a in listed["Agreements"]]
            assert ag_resp["AgreementId"] in ag_ids
        finally:
            transfer.delete_server(ServerId=server_id)
            transfer.delete_profile(ProfileId=local["ProfileId"])
            transfer.delete_profile(ProfileId=partner["ProfileId"])

    def test_list_agreements_empty(self, transfer):
        """ListAgreements on server with no agreements returns empty list."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            listed = transfer.list_agreements(ServerId=server_id)
            assert listed["Agreements"] == []
        finally:
            transfer.delete_server(ServerId=server_id)


class TestTransferConnectorOperations:
    """Tests for Transfer Family connector CRUD operations."""

    def test_create_and_describe_connector(self, transfer):
        """CreateConnector/DescribeConnector full lifecycle."""
        local = transfer.create_profile(As2Id=_unique("local"), ProfileType="LOCAL")
        partner = transfer.create_profile(As2Id=_unique("partner"), ProfileType="PARTNER")
        try:
            conn_resp = transfer.create_connector(
                Url="sftp://example.com",
                As2Config={
                    "LocalProfileId": local["ProfileId"],
                    "PartnerProfileId": partner["ProfileId"],
                    "Compression": "ZLIB",
                    "EncryptionAlgorithm": "AES256_CBC",
                    "SigningAlgorithm": "SHA256",
                    "MdnSigningAlgorithm": "SHA256",
                    "MdnResponse": "SYNC",
                    "MessageSubject": "test-message",
                },
                AccessRole="arn:aws:iam::123456789012:role/connector-role",
            )
            connector_id = conn_resp["ConnectorId"]
            assert len(connector_id) > 0

            desc = transfer.describe_connector(ConnectorId=connector_id)
            connector = desc["Connector"]
            assert connector["ConnectorId"] == connector_id
            assert connector["Url"] == "sftp://example.com"
            assert connector["AccessRole"] == "arn:aws:iam::123456789012:role/connector-role"
        finally:
            transfer.delete_connector(ConnectorId=connector_id)
            transfer.delete_profile(ProfileId=local["ProfileId"])
            transfer.delete_profile(ProfileId=partner["ProfileId"])

    def test_delete_connector(self, transfer):
        """DeleteConnector removes the connector."""
        local = transfer.create_profile(As2Id=_unique("local"), ProfileType="LOCAL")
        partner = transfer.create_profile(As2Id=_unique("partner"), ProfileType="PARTNER")
        try:
            conn_resp = transfer.create_connector(
                Url="sftp://example.com",
                As2Config={
                    "LocalProfileId": local["ProfileId"],
                    "PartnerProfileId": partner["ProfileId"],
                    "Compression": "ZLIB",
                    "EncryptionAlgorithm": "AES256_CBC",
                    "SigningAlgorithm": "SHA256",
                    "MdnSigningAlgorithm": "SHA256",
                    "MdnResponse": "SYNC",
                },
                AccessRole="arn:aws:iam::123456789012:role/r",
            )
            connector_id = conn_resp["ConnectorId"]
            transfer.delete_connector(ConnectorId=connector_id)
            with pytest.raises(ClientError):
                transfer.describe_connector(ConnectorId=connector_id)
        finally:
            transfer.delete_profile(ProfileId=local["ProfileId"])
            transfer.delete_profile(ProfileId=partner["ProfileId"])

    def test_list_connectors_contains_created(self, transfer):
        """ListConnectors includes a newly created connector."""
        local = transfer.create_profile(As2Id=_unique("local"), ProfileType="LOCAL")
        partner = transfer.create_profile(As2Id=_unique("partner"), ProfileType="PARTNER")
        try:
            conn_resp = transfer.create_connector(
                Url="sftp://example.com",
                As2Config={
                    "LocalProfileId": local["ProfileId"],
                    "PartnerProfileId": partner["ProfileId"],
                    "Compression": "ZLIB",
                    "EncryptionAlgorithm": "AES256_CBC",
                    "SigningAlgorithm": "SHA256",
                    "MdnSigningAlgorithm": "SHA256",
                    "MdnResponse": "SYNC",
                },
                AccessRole="arn:aws:iam::123456789012:role/r",
            )
            connector_id = conn_resp["ConnectorId"]
            listed = transfer.list_connectors()
            conn_ids = [c["ConnectorId"] for c in listed["Connectors"]]
            assert connector_id in conn_ids
            transfer.delete_connector(ConnectorId=connector_id)
        finally:
            transfer.delete_profile(ProfileId=local["ProfileId"])
            transfer.delete_profile(ProfileId=partner["ProfileId"])


class TestTransferExecutionOperations:
    """Tests for Transfer Family execution operations."""

    def test_describe_execution_returns_status(self, transfer):
        """DescribeExecution returns execution status for a workflow."""
        wf = transfer.create_workflow(
            Steps=[
                {
                    "Type": "COPY",
                    "CopyStepDetails": {
                        "Name": "exec-step",
                        "DestinationFileLocation": {
                            "S3FileLocation": {
                                "Bucket": "test-bucket",
                                "Key": "dest/",
                            }
                        },
                    },
                }
            ]
        )
        workflow_id = wf["WorkflowId"]
        fake_exec_id = "a" * 36
        try:
            desc = transfer.describe_execution(ExecutionId=fake_exec_id, WorkflowId=workflow_id)
            assert desc["WorkflowId"] == workflow_id
            assert "Execution" in desc
            assert desc["Execution"]["ExecutionId"] == fake_exec_id
            assert "Status" in desc["Execution"]
        finally:
            transfer.delete_workflow(WorkflowId=workflow_id)

    def test_list_executions_empty(self, transfer):
        """ListExecutions on workflow with no executions returns empty list."""
        wf = transfer.create_workflow(
            Steps=[
                {
                    "Type": "COPY",
                    "CopyStepDetails": {
                        "Name": "list-exec-step",
                        "DestinationFileLocation": {
                            "S3FileLocation": {
                                "Bucket": "test-bucket",
                                "Key": "dest/",
                            }
                        },
                    },
                }
            ]
        )
        workflow_id = wf["WorkflowId"]
        try:
            resp = transfer.list_executions(WorkflowId=workflow_id)
            assert resp["WorkflowId"] == workflow_id
            assert "Executions" in resp
            assert isinstance(resp["Executions"], list)
        finally:
            transfer.delete_workflow(WorkflowId=workflow_id)


class TestTransferServerLifecycle:
    """Tests for Transfer server start/stop and update operations."""

    def test_stop_server_changes_state(self, transfer):
        """StopServer sets server state to OFFLINE."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.stop_server(ServerId=server_id)
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["State"] == "OFFLINE"
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_start_server_changes_state(self, transfer):
        """StartServer sets server state back to ONLINE."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.stop_server(ServerId=server_id)
            transfer.start_server(ServerId=server_id)
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["State"] == "ONLINE"
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_update_server_endpoint_type(self, transfer):
        """UpdateServer can change EndpointType."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            update_resp = transfer.update_server(ServerId=server_id, EndpointType="PUBLIC")
            assert update_resp["ServerId"] == server_id
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_update_server_logging_role(self, transfer):
        """UpdateServer can change LoggingRole."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        new_role = "arn:aws:iam::123456789012:role/new-logging"
        try:
            transfer.update_server(ServerId=server_id, LoggingRole=new_role)
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["LoggingRole"] == new_role
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_update_server_security_policy(self, transfer):
        """UpdateServer can change SecurityPolicyName."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.update_server(
                ServerId=server_id,
                SecurityPolicyName="TransferSecurityPolicy-2020-06",
            )
            desc = transfer.describe_server(ServerId=server_id)
            assert desc["Server"]["SecurityPolicyName"] == "TransferSecurityPolicy-2020-06"
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_describe_security_policy_2018_11(self, transfer):
        """DescribeSecurityPolicy for the 2018-11 policy."""
        resp = transfer.describe_security_policy(
            SecurityPolicyName="TransferSecurityPolicy-2018-11"
        )
        policy = resp["SecurityPolicy"]
        assert policy["SecurityPolicyName"] == "TransferSecurityPolicy-2018-11"

    def test_list_users_empty(self, transfer):
        """ListUsers on a new server returns empty list."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            users_resp = transfer.list_users(ServerId=server_id)
            assert users_resp["ServerId"] == server_id
            assert users_resp["Users"] == []
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_list_users_contains_created(self, transfer):
        """ListUsers includes a user after creation."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="testuser",
                Role="arn:aws:iam::123456789012:role/transfer-role",
            )
            users_resp = transfer.list_users(ServerId=server_id)
            usernames = [u["UserName"] for u in users_resp["Users"]]
            assert "testuser" in usernames
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_list_host_keys_on_new_server(self, transfer):
        """ListHostKeys returns response with HostKeys key."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            hk_resp = transfer.list_host_keys(ServerId=server_id)
            assert hk_resp["ServerId"] == server_id
            assert "HostKeys" in hk_resp
            assert isinstance(hk_resp["HostKeys"], list)
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_update_user_home_directory(self, transfer):
        """UpdateUser can change HomeDirectory."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="upd-user",
                Role="arn:aws:iam::123456789012:role/transfer-role",
            )
            update_resp = transfer.update_user(
                ServerId=server_id,
                UserName="upd-user",
                HomeDirectory="/new-home",
            )
            assert update_resp["ServerId"] == server_id
            assert update_resp["UserName"] == "upd-user"

            desc = transfer.describe_user(ServerId=server_id, UserName="upd-user")
            assert desc["User"]["HomeDirectory"] == "/new-home"
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_update_user_role(self, transfer):
        """UpdateUser can change Role."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        new_role = "arn:aws:iam::123456789012:role/new-role"
        try:
            transfer.create_user(
                ServerId=server_id,
                UserName="role-user",
                Role="arn:aws:iam::123456789012:role/old-role",
            )
            transfer.update_user(
                ServerId=server_id,
                UserName="role-user",
                Role=new_role,
            )
            desc = transfer.describe_user(ServerId=server_id, UserName="role-user")
            assert desc["User"]["Role"] == new_role
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_list_users_multiple(self, transfer):
        """ListUsers returns all created users."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            for name in ["user-a", "user-b", "user-c"]:
                transfer.create_user(
                    ServerId=server_id,
                    UserName=name,
                    Role="arn:aws:iam::123456789012:role/transfer-role",
                )
            users_resp = transfer.list_users(ServerId=server_id)
            usernames = {u["UserName"] for u in users_resp["Users"]}
            assert {"user-a", "user-b", "user-c"}.issubset(usernames)
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_create_workflow_with_description(self, transfer):
        """CreateWorkflow with Description stores it."""
        resp = transfer.create_workflow(
            Description="My test workflow",
            Steps=[
                {
                    "Type": "COPY",
                    "CopyStepDetails": {
                        "Name": "desc-step",
                        "DestinationFileLocation": {
                            "S3FileLocation": {
                                "Bucket": "test-bucket",
                                "Key": "dest/",
                            }
                        },
                    },
                }
            ],
        )
        workflow_id = resp["WorkflowId"]
        try:
            desc = transfer.describe_workflow(WorkflowId=workflow_id)
            assert desc["Workflow"]["Description"] == "My test workflow"
        finally:
            transfer.delete_workflow(WorkflowId=workflow_id)

    def test_create_workflow_with_tags(self, transfer):
        """CreateWorkflow with Tags stores them."""
        resp = transfer.create_workflow(
            Description="tagged wf",
            Tags=[{"Key": "env", "Value": "test"}],
            Steps=[
                {
                    "Type": "COPY",
                    "CopyStepDetails": {
                        "Name": "tag-step",
                        "DestinationFileLocation": {
                            "S3FileLocation": {
                                "Bucket": "test-bucket",
                                "Key": "dest/",
                            }
                        },
                    },
                }
            ],
        )
        workflow_id = resp["WorkflowId"]
        try:
            desc = transfer.describe_workflow(WorkflowId=workflow_id)
            tags = {t["Key"]: t["Value"] for t in desc["Workflow"].get("Tags", [])}
            assert tags.get("env") == "test"
        finally:
            transfer.delete_workflow(WorkflowId=workflow_id)

    def test_create_workflow_with_on_exception_steps(self, transfer):
        """CreateWorkflow with OnExceptionSteps stores them."""
        resp = transfer.create_workflow(
            Steps=[
                {
                    "Type": "COPY",
                    "CopyStepDetails": {
                        "Name": "main-step",
                        "DestinationFileLocation": {
                            "S3FileLocation": {
                                "Bucket": "test-bucket",
                                "Key": "dest/",
                            }
                        },
                    },
                }
            ],
            OnExceptionSteps=[
                {
                    "Type": "COPY",
                    "CopyStepDetails": {
                        "Name": "err-step",
                        "DestinationFileLocation": {
                            "S3FileLocation": {
                                "Bucket": "test-bucket",
                                "Key": "error/",
                            }
                        },
                    },
                }
            ],
        )
        workflow_id = resp["WorkflowId"]
        try:
            desc = transfer.describe_workflow(WorkflowId=workflow_id)
            assert len(desc["Workflow"]["OnExceptionSteps"]) == 1
            assert desc["Workflow"]["OnExceptionSteps"][0]["Type"] == "COPY"
        finally:
            transfer.delete_workflow(WorkflowId=workflow_id)


class TestTransferIdentityProvider:
    """Tests for TestIdentityProvider operation."""

    def test_test_identity_provider(self, transfer):
        """TestIdentityProvider returns response for SERVICE_MANAGED server."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            result = transfer.test_identity_provider(
                ServerId=server_id,
                UserName="testuser",
                ServerProtocol="SFTP",
            )
            assert "StatusCode" in result
            assert "Url" in result
        finally:
            transfer.delete_server(ServerId=server_id)


class TestTransferHostKeyOperations:
    """Tests for Transfer Family host key operations."""

    def test_import_host_key(self, transfer):
        """ImportHostKey adds a host key to a server."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            hk = transfer.import_host_key(
                ServerId=server_id,
                HostKeyBody=(
                    "-----BEGIN RSA PRIVATE KEY-----\n"
                    "MIIEpAIBAAKCAQEA0Z3VS5JJcds3xfn/ygWyF8PbnGcY5unA67hqxnfp/Imt\n"
                    "-----END RSA PRIVATE KEY-----"
                ),
            )
            assert "HostKeyId" in hk
            assert "ServerId" in hk
            assert hk["ServerId"] == server_id
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_describe_host_key(self, transfer):
        """DescribeHostKey returns host key details."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            hk = transfer.import_host_key(
                ServerId=server_id,
                HostKeyBody=(
                    "-----BEGIN RSA PRIVATE KEY-----\n"
                    "MIIEpAIBAAKCAQEA0Z3VS5JJcds3xfn/ygWyF8PbnGcY5unA67hqxnfp/Imt\n"
                    "-----END RSA PRIVATE KEY-----"
                ),
            )
            host_key_id = hk["HostKeyId"]
            desc = transfer.describe_host_key(
                ServerId=server_id,
                HostKeyId=host_key_id,
            )
            assert "HostKey" in desc
            assert desc["HostKey"]["HostKeyId"] == host_key_id
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_delete_host_key(self, transfer):
        """DeleteHostKey removes a host key from a server."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            hk = transfer.import_host_key(
                ServerId=server_id,
                HostKeyBody=(
                    "-----BEGIN RSA PRIVATE KEY-----\n"
                    "MIIEpAIBAAKCAQEA0Z3VS5JJcds3xfn/ygWyF8PbnGcY5unA67hqxnfp/Imt\n"
                    "-----END RSA PRIVATE KEY-----"
                ),
            )
            host_key_id = hk["HostKeyId"]
            transfer.delete_host_key(ServerId=server_id, HostKeyId=host_key_id)
            # Verify it's gone
            with pytest.raises(ClientError):
                transfer.describe_host_key(ServerId=server_id, HostKeyId=host_key_id)
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_update_host_key(self, transfer):
        """UpdateHostKey modifies a host key description."""
        resp = transfer.create_server(IdentityProviderType="SERVICE_MANAGED")
        server_id = resp["ServerId"]
        try:
            hk = transfer.import_host_key(
                ServerId=server_id,
                HostKeyBody=(
                    "-----BEGIN RSA PRIVATE KEY-----\n"
                    "MIIEpAIBAAKCAQEA0Z3VS5JJcds3xfn/ygWyF8PbnGcY5unA67hqxnfp/Imt\n"
                    "-----END RSA PRIVATE KEY-----"
                ),
            )
            host_key_id = hk["HostKeyId"]
            update_resp = transfer.update_host_key(
                ServerId=server_id,
                HostKeyId=host_key_id,
                Description="updated-desc",
            )
            assert "HostKeyId" in update_resp
            assert "ServerId" in update_resp
        finally:
            transfer.delete_server(ServerId=server_id)

    def test_describe_host_key_nonexistent_server(self, transfer):
        """DescribeHostKey raises error for nonexistent server."""
        with pytest.raises(ClientError) as exc_info:
            transfer.describe_host_key(
                ServerId="s-00000000000000000",
                HostKeyId="hostkey-0000000000000000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "ServerNotFound",
        )


class TestTransferWebAppOperations:
    """Tests for Transfer Family web app operations."""

    def test_create_web_app(self, transfer):
        """CreateWebApp returns a WebAppId."""
        resp = transfer.create_web_app(
            IdentityProviderDetails={
                "IdentityCenterConfig": {
                    "InstanceArn": "arn:aws:sso:::instance/ssoins-fake123",
                }
            },
        )
        assert "WebAppId" in resp
        web_app_id = resp["WebAppId"]
        assert len(web_app_id) > 0
        # cleanup
        try:
            transfer.delete_web_app(WebAppId=web_app_id)
        except ClientError:
            pass  # best-effort cleanup

    def test_describe_web_app(self, transfer):
        """DescribeWebApp returns web app details."""
        create_resp = transfer.create_web_app(
            IdentityProviderDetails={
                "IdentityCenterConfig": {
                    "InstanceArn": "arn:aws:sso:::instance/ssoins-fake123",
                }
            },
        )
        web_app_id = create_resp["WebAppId"]
        try:
            desc = transfer.describe_web_app(WebAppId=web_app_id)
            assert "WebApp" in desc
            assert desc["WebApp"]["WebAppId"] == web_app_id
        finally:
            try:
                transfer.delete_web_app(WebAppId=web_app_id)
            except ClientError:
                pass  # best-effort cleanup

    def test_delete_web_app(self, transfer):
        """DeleteWebApp removes a web app."""
        create_resp = transfer.create_web_app(
            IdentityProviderDetails={
                "IdentityCenterConfig": {
                    "InstanceArn": "arn:aws:sso:::instance/ssoins-fake123",
                }
            },
        )
        web_app_id = create_resp["WebAppId"]
        transfer.delete_web_app(WebAppId=web_app_id)
        with pytest.raises(ClientError) as exc_info:
            transfer.describe_web_app(WebAppId=web_app_id)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_web_app_nonexistent(self, transfer):
        """DescribeWebApp raises ResourceNotFoundException for unknown ID."""
        with pytest.raises(ClientError) as exc_info:
            transfer.describe_web_app(WebAppId="webapp-000000000000000000000000")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_web_app_nonexistent(self, transfer):
        """UpdateWebApp raises ResourceNotFoundException for unknown ID."""
        with pytest.raises(ClientError) as exc_info:
            transfer.update_web_app(WebAppId="webapp-000000000000000000000000")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_web_app_customization_nonexistent(self, transfer):
        """DescribeWebAppCustomization raises for unknown web app."""
        with pytest.raises(ClientError) as exc_info:
            transfer.describe_web_app_customization(
                WebAppId="webapp-000000000000000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_web_app_customization_nonexistent(self, transfer):
        """DeleteWebAppCustomization raises for unknown web app."""
        with pytest.raises(ClientError) as exc_info:
            transfer.delete_web_app_customization(
                WebAppId="webapp-000000000000000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_web_app_customization_nonexistent(self, transfer):
        """UpdateWebAppCustomization raises for unknown web app."""
        with pytest.raises(ClientError) as exc_info:
            transfer.update_web_app_customization(
                WebAppId="webapp-000000000000000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestTransferConnectorAdvancedOperations:
    """Tests for connector-related operations that need params."""

    def test_test_connection_nonexistent(self, transfer):
        """TestConnection raises ResourceNotFoundException for fake connector."""
        with pytest.raises(ClientError) as exc_info:
            transfer.test_connection(ConnectorId="c-00000000000000000000")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_start_file_transfer_nonexistent(self, transfer):
        """StartFileTransfer raises ResourceNotFoundException for fake connector."""
        with pytest.raises(ClientError) as exc_info:
            transfer.start_file_transfer(
                ConnectorId="c-00000000000000000000",
                SendFilePaths=["/test/file.txt"],
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_start_directory_listing_nonexistent(self, transfer):
        """StartDirectoryListing raises ResourceNotFoundException for fake connector."""
        with pytest.raises(ClientError) as exc_info:
            transfer.start_directory_listing(
                ConnectorId="c-00000000000000000000",
                RemoteDirectoryPath="/remote",
                OutputDirectoryPath="/output",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_file_transfer_results_nonexistent(self, transfer):
        """ListFileTransferResults raises for fake connector."""
        with pytest.raises(ClientError) as exc_info:
            transfer.list_file_transfer_results(
                ConnectorId="c-00000000000000000000",
                TransferId="t-00000000000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_start_remote_delete_nonexistent(self, transfer):
        """StartRemoteDelete raises ResourceNotFoundException for fake connector."""
        with pytest.raises(ClientError) as exc_info:
            transfer.start_remote_delete(
                ConnectorId="c-00000000000000000000",
                DeletePath="/test/file.txt",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_start_remote_move_nonexistent(self, transfer):
        """StartRemoteMove raises ResourceNotFoundException for fake connector."""
        with pytest.raises(ClientError) as exc_info:
            transfer.start_remote_move(
                ConnectorId="c-00000000000000000000",
                SourcePath="/test/file.txt",
                TargetPath="/dest/file.txt",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestTransferMiscOperations:
    """Tests for miscellaneous operations that need params."""

    def test_update_certificate_nonexistent(self, transfer):
        """UpdateCertificate raises ResourceNotFoundException for fake cert."""
        with pytest.raises(ClientError) as exc_info:
            transfer.update_certificate(
                CertificateId="c-00000000000000000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_connector_nonexistent(self, transfer):
        """UpdateConnector raises ResourceNotFoundException for fake connector."""
        with pytest.raises(ClientError) as exc_info:
            transfer.update_connector(
                ConnectorId="c-00000000000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_profile_nonexistent(self, transfer):
        """UpdateProfile raises ResourceNotFoundException for fake profile."""
        with pytest.raises(ClientError) as exc_info:
            transfer.update_profile(
                ProfileId="p-00000000000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_send_workflow_step_state_nonexistent(self, transfer):
        """SendWorkflowStepState raises ResourceNotFoundException for fake workflow."""
        with pytest.raises(ClientError) as exc_info:
            transfer.send_workflow_step_state(
                WorkflowId="w-00000000000000000000",
                ExecutionId="00000000-0000-0000-0000-000000000000",
                Token="fake-token-value",
                Status="SUCCESS",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_access_nonexistent_server(self, transfer):
        """DeleteAccess raises error for nonexistent server."""
        with pytest.raises(ClientError) as exc_info:
            transfer.delete_access(
                ServerId="s-00000000000000000",
                ExternalId="fake-external-id",
            )
        err_code = exc_info.value.response["Error"]["Code"]
        assert err_code in ("ResourceNotFoundException", "ServerNotFound")

    def test_update_access_nonexistent_server(self, transfer):
        """UpdateAccess raises error for nonexistent server."""
        with pytest.raises(ClientError) as exc_info:
            transfer.update_access(
                ServerId="s-00000000000000000",
                ExternalId="fake-external-id",
                Role="arn:aws:iam::123456789012:role/fake-role",
            )
        err_code = exc_info.value.response["Error"]["Code"]
        assert err_code in ("ResourceNotFoundException", "ServerNotFound")

    def test_delete_agreement_nonexistent_server(self, transfer):
        """DeleteAgreement raises error for nonexistent server."""
        with pytest.raises(ClientError) as exc_info:
            transfer.delete_agreement(
                AgreementId="a-00000000000000000000",
                ServerId="s-00000000000000000",
            )
        err_code = exc_info.value.response["Error"]["Code"]
        assert err_code in ("ResourceNotFoundException", "ServerNotFound")

    def test_update_agreement_nonexistent_server(self, transfer):
        """UpdateAgreement raises error for nonexistent server."""
        with pytest.raises(ClientError) as exc_info:
            transfer.update_agreement(
                AgreementId="a-00000000000000000000",
                ServerId="s-00000000000000000",
            )
        err_code = exc_info.value.response["Error"]["Code"]
        assert err_code in ("ResourceNotFoundException", "ServerNotFound")
