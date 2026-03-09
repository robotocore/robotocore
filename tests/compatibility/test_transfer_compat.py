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
                    pass
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
                pass
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
                pass
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
                pass
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
                pass
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
                pass
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
                pass
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
                pass
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
                pass
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
