"""Compatibility tests for AWS Directory Service (DS)."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def ds():
    return make_client("ds")


@pytest.fixture
def ec2():
    return make_client("ec2")


@pytest.fixture
def directory(ds, ec2):
    """Create a SimpleAD directory with VPC infrastructure, clean up after test."""
    vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    subnet1 = ec2.create_subnet(
        VpcId=vpc_id, CidrBlock="10.0.1.0/24", AvailabilityZone="us-east-1a"
    )
    subnet2 = ec2.create_subnet(
        VpcId=vpc_id, CidrBlock="10.0.2.0/24", AvailabilityZone="us-east-1b"
    )
    sid1 = subnet1["Subnet"]["SubnetId"]
    sid2 = subnet2["Subnet"]["SubnetId"]

    resp = ds.create_directory(
        Name="corp.example.com",
        Password="P@ssw0rd!",
        Size="Small",
        VpcSettings={"VpcId": vpc_id, "SubnetIds": [sid1, sid2]},
    )
    dir_id = resp["DirectoryId"]

    yield dir_id

    # Cleanup: delete directory, subnets, VPC
    try:
        ds.delete_directory(DirectoryId=dir_id)
    except ClientError:
        pass  # best-effort cleanup
    for sid in [sid1, sid2]:
        try:
            ec2.delete_subnet(SubnetId=sid)
        except ClientError:
            pass  # best-effort cleanup
    try:
        ec2.delete_vpc(VpcId=vpc_id)
    except ClientError:
        pass  # best-effort cleanup


class TestDSDescribeOperations:
    """Test describe operations against an empty state."""

    def test_describe_directories_empty(self, ds):
        """describe_directories with no filter returns a list (possibly empty)."""
        resp = ds.describe_directories()
        assert "DirectoryDescriptions" in resp
        assert isinstance(resp["DirectoryDescriptions"], list)

    def test_describe_directories_invalid_id(self, ds):
        """describe_directories with a bogus ID raises ValidationException."""
        with pytest.raises(ClientError) as exc_info:
            ds.describe_directories(DirectoryIds=["d-bogus123"])
        assert exc_info.value.response["Error"]["Code"] in (
            "ValidationException",
            "EntityDoesNotExistException",
        )


class TestDSDirectoryOperations:
    """Test create, describe, and delete directory lifecycle."""

    def test_create_directory(self, directory, ds):
        """create_directory returns a DirectoryId and directory is describable."""
        assert directory.startswith("d-")

        resp = ds.describe_directories(DirectoryIds=[directory])
        descriptions = resp["DirectoryDescriptions"]
        assert len(descriptions) == 1
        d = descriptions[0]
        assert d["DirectoryId"] == directory
        assert d["Name"] == "corp.example.com"
        assert d["Size"] == "Small"
        assert d["Type"] == "SimpleAD"

    def test_delete_directory(self, ds, ec2):
        """delete_directory removes the directory so it cannot be described."""
        vpc = ec2.create_vpc(CidrBlock="10.1.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.1.1.0/24", AvailabilityZone="us-east-1a")
        s2 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.1.2.0/24", AvailabilityZone="us-east-1b")
        sid1, sid2 = s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]

        resp = ds.create_directory(
            Name="del.example.com",
            Password="P@ssw0rd!",
            Size="Small",
            VpcSettings={"VpcId": vpc_id, "SubnetIds": [sid1, sid2]},
        )
        dir_id = resp["DirectoryId"]

        # Delete it
        del_resp = ds.delete_directory(DirectoryId=dir_id)
        assert del_resp["DirectoryId"] == dir_id

        # Should no longer be found
        with pytest.raises(ClientError) as exc_info:
            ds.describe_directories(DirectoryIds=[dir_id])
        assert exc_info.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDSTags:
    """Test tagging operations on directories."""

    def test_add_and_list_tags(self, ds, directory):
        """add_tags_to_resource adds tags that are visible via list_tags_for_resource."""
        ds.add_tags_to_resource(
            ResourceId=directory,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )

        resp = ds.list_tags_for_resource(ResourceId=directory)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "platform"

    def test_remove_tags(self, ds, directory):
        """remove_tags_from_resource removes specified tag keys."""
        ds.add_tags_to_resource(
            ResourceId=directory,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
                {"Key": "cost-center", "Value": "123"},
            ],
        )

        ds.remove_tags_from_resource(ResourceId=directory, TagKeys=["team", "cost-center"])

        resp = ds.list_tags_for_resource(ResourceId=directory)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert "env" in tags
        assert "team" not in tags
        assert "cost-center" not in tags

    def test_list_tags_empty(self, ds, directory):
        """list_tags_for_resource on a directory with no tags returns empty list."""
        resp = ds.list_tags_for_resource(ResourceId=directory)
        assert resp["Tags"] == []


class TestDsAutoCoverage:
    """Auto-generated coverage tests for ds."""

    @pytest.fixture
    def client(self):
        return make_client("ds")

    def test_describe_trusts(self, client):
        """DescribeTrusts returns a response."""
        resp = client.describe_trusts()
        assert "Trusts" in resp

    def test_get_directory_limits(self, client):
        """GetDirectoryLimits returns a response."""
        resp = client.get_directory_limits()
        assert "DirectoryLimits" in resp

    def test_list_log_subscriptions(self, client):
        """ListLogSubscriptions returns a response."""
        resp = client.list_log_subscriptions()
        assert "LogSubscriptions" in resp


class TestDsLDAPSSettings:
    """Test LDAPS enable/disable operations."""

    def test_enable_ldaps_unsupported_directory_type(self, ds, directory):
        """EnableLDAPS on a SimpleAD directory raises UnsupportedOperationException."""
        with pytest.raises(ClientError) as exc_info:
            ds.enable_ldaps(DirectoryId=directory, Type="Client")
        assert exc_info.value.response["Error"]["Code"] == "UnsupportedOperationException"

    def test_disable_ldaps_unsupported_directory_type(self, ds, directory):
        """DisableLDAPS on a SimpleAD directory raises UnsupportedOperationException."""
        with pytest.raises(ClientError) as exc_info:
            ds.disable_ldaps(DirectoryId=directory, Type="Client")
        assert exc_info.value.response["Error"]["Code"] == "UnsupportedOperationException"

    def test_describe_ldaps_settings(self, ds, directory):
        """DescribeLDAPSSettings on a SimpleAD directory raises UnsupportedOperationException."""
        with pytest.raises(ClientError) as exc_info:
            ds.describe_ldaps_settings(DirectoryId=directory, Type="Client")
        assert exc_info.value.response["Error"]["Code"] == "UnsupportedOperationException"


class TestDsSettings:
    """Test DescribeSettings and UpdateSettings operations."""

    def test_describe_settings_simple_ad_rejected(self, ds, directory):
        """DescribeSettings on SimpleAD raises InvalidParameterException."""
        with pytest.raises(ClientError) as exc_info:
            ds.describe_settings(DirectoryId=directory)
        assert exc_info.value.response["Error"]["Code"] == "InvalidParameterException"

    def test_describe_settings_microsoft_ad(self, ds, ec2):
        """DescribeSettings on MicrosoftAD returns directory settings."""
        vpc = ec2.create_vpc(CidrBlock="10.70.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.70.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.70.2.0/24", AvailabilityZone="us-east-1b"
        )
        sid1, sid2 = s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]
        resp = ds.create_microsoft_ad(
            Name="settings.example.com",
            Password="P@ssw0rd!",
            VpcSettings={"VpcId": vpc_id, "SubnetIds": [sid1, sid2]},
        )
        dir_id = resp["DirectoryId"]
        try:
            settings_resp = ds.describe_settings(DirectoryId=dir_id)
            assert settings_resp["DirectoryId"] == dir_id
            assert "SettingEntries" in settings_resp
            assert isinstance(settings_resp["SettingEntries"], list)
            assert len(settings_resp["SettingEntries"]) > 0
            # Check structure of a setting entry
            entry = settings_resp["SettingEntries"][0]
            assert "Name" in entry
            assert "Type" in entry
            assert "AppliedValue" in entry
        finally:
            ds.delete_directory(DirectoryId=dir_id)
            for sid in [sid1, sid2]:
                try:
                    ec2.delete_subnet(SubnetId=sid)
                except ClientError:
                    pass  # best-effort cleanup
            try:
                ec2.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass  # best-effort cleanup

    def test_update_settings_microsoft_ad(self, ds, ec2):
        """UpdateSettings on MicrosoftAD returns the directory ID."""
        vpc = ec2.create_vpc(CidrBlock="10.71.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.71.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.71.2.0/24", AvailabilityZone="us-east-1b"
        )
        sid1, sid2 = s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]
        resp = ds.create_microsoft_ad(
            Name="updsettings.example.com",
            Password="P@ssw0rd!",
            VpcSettings={"VpcId": vpc_id, "SubnetIds": [sid1, sid2]},
        )
        dir_id = resp["DirectoryId"]
        try:
            upd = ds.update_settings(
                DirectoryId=dir_id,
                Settings=[{"Name": "TLS_1_0", "Value": "Disable"}],
            )
            assert upd["DirectoryId"] == dir_id
        finally:
            ds.delete_directory(DirectoryId=dir_id)
            for sid in [sid1, sid2]:
                try:
                    ec2.delete_subnet(SubnetId=sid)
                except ClientError:
                    pass  # best-effort cleanup
            try:
                ec2.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass  # best-effort cleanup


class TestDsMicrosoftAD:
    """Test Microsoft AD directory operations."""

    def test_create_microsoft_ad(self, ds, ec2):
        """CreateMicrosoftAD returns a DirectoryId and creates a MicrosoftAD type directory."""
        vpc = ec2.create_vpc(CidrBlock="10.72.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.72.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.72.2.0/24", AvailabilityZone="us-east-1b"
        )
        sid1, sid2 = s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]
        resp = ds.create_microsoft_ad(
            Name="msad.example.com",
            Password="P@ssw0rd!",
            VpcSettings={"VpcId": vpc_id, "SubnetIds": [sid1, sid2]},
        )
        dir_id = resp["DirectoryId"]
        assert dir_id.startswith("d-")
        try:
            desc = ds.describe_directories(DirectoryIds=[dir_id])
            d = desc["DirectoryDescriptions"][0]
            assert d["DirectoryId"] == dir_id
            assert d["Type"] == "MicrosoftAD"
            assert d["Name"] == "msad.example.com"
        finally:
            ds.delete_directory(DirectoryId=dir_id)
            for sid in [sid1, sid2]:
                try:
                    ec2.delete_subnet(SubnetId=sid)
                except ClientError:
                    pass  # best-effort cleanup
            try:
                ec2.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass  # best-effort cleanup


class TestDsSso:
    """Test SSO enable/disable operations."""

    def test_disable_sso(self, ds, directory):
        """DisableSso on a directory succeeds."""
        resp = ds.disable_sso(DirectoryId=directory)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_enable_sso_without_alias_raises(self, ds, directory):
        """EnableSso without an alias raises ClientException."""
        with pytest.raises(ClientError) as exc_info:
            ds.enable_sso(DirectoryId=directory)
        assert exc_info.value.response["Error"]["Code"] == "ClientException"

    def test_disable_sso_nonexistent_raises(self, ds):
        """DisableSso with a nonexistent directory raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc_info:
            ds.disable_sso(DirectoryId="d-0000000000")
        err = exc_info.value.response["Error"]["Code"]
        assert err in ("EntityDoesNotExistException", "ValidationException")


class TestDsLogSubscription:
    """Test log subscription operations."""

    def test_delete_log_subscription_nonexistent(self, ds, directory):
        """DeleteLogSubscription for a directory with no subscription raises error."""
        with pytest.raises(ClientError) as exc_info:
            ds.delete_log_subscription(DirectoryId=directory)
        assert exc_info.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDsDeleteTrust:
    """Test trust deletion with nonexistent trust."""

    def test_delete_trust_nonexistent(self, ds):
        """DeleteTrust with a nonexistent trust ID raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc_info:
            ds.delete_trust(TrustId="t-0000000000")
        assert exc_info.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDsAlias:
    """Test CreateAlias operations."""

    def test_create_alias(self, ds, directory):
        """CreateAlias sets an alias on a directory and it appears in describe."""
        alias_name = f"alias{uuid.uuid4().hex[:8]}"
        resp = ds.create_alias(DirectoryId=directory, Alias=alias_name)
        assert resp["DirectoryId"] == directory
        assert resp["Alias"] == alias_name

        # Verify alias appears in describe
        desc = ds.describe_directories(DirectoryIds=[directory])
        d = desc["DirectoryDescriptions"][0]
        assert d["Alias"] == alias_name
        assert d["AccessUrl"] == f"{alias_name}.awsapps.com"

    def test_create_alias_nonexistent_directory(self, ds):
        """CreateAlias on a nonexistent directory raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc_info:
            ds.create_alias(DirectoryId="d-0000000000", Alias="bogusalias")
        assert exc_info.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDsTrustLifecycle:
    """Test trust create, describe, and delete lifecycle."""

    def test_create_and_describe_trust(self, ds, directory):
        """CreateTrust creates a trust and DescribeTrusts returns its details."""
        resp = ds.create_trust(
            DirectoryId=directory,
            RemoteDomainName="remote.example.com",
            TrustPassword="TrustP@ss1!",
            TrustDirection="One-Way: Outgoing",
        )
        trust_id = resp["TrustId"]
        assert trust_id.startswith("t-")

        try:
            # Describe with directory filter
            trusts = ds.describe_trusts(DirectoryId=directory)
            assert len(trusts["Trusts"]) >= 1
            trust = next(t for t in trusts["Trusts"] if t["TrustId"] == trust_id)
            assert trust["DirectoryId"] == directory
            assert trust["RemoteDomainName"] == "remote.example.com"
            assert trust["TrustDirection"] == "One-Way: Outgoing"
            assert trust["TrustState"] == "Creating"
        finally:
            ds.delete_trust(TrustId=trust_id)

    def test_describe_trusts_by_trust_id(self, ds, directory):
        """DescribeTrusts can filter by specific TrustIds."""
        resp = ds.create_trust(
            DirectoryId=directory,
            RemoteDomainName="byid.example.com",
            TrustPassword="TrustP@ss2!",
            TrustDirection="Two-Way",
        )
        trust_id = resp["TrustId"]

        try:
            trusts = ds.describe_trusts(DirectoryId=directory, TrustIds=[trust_id])
            assert len(trusts["Trusts"]) == 1
            assert trusts["Trusts"][0]["TrustId"] == trust_id
            assert trusts["Trusts"][0]["TrustDirection"] == "Two-Way"
        finally:
            ds.delete_trust(TrustId=trust_id)

    def test_delete_trust(self, ds, directory):
        """DeleteTrust removes the trust so it no longer appears in DescribeTrusts."""
        resp = ds.create_trust(
            DirectoryId=directory,
            RemoteDomainName="deltrust.example.com",
            TrustPassword="TrustP@ss3!",
            TrustDirection="One-Way: Outgoing",
        )
        trust_id = resp["TrustId"]

        del_resp = ds.delete_trust(TrustId=trust_id)
        assert del_resp["TrustId"] == trust_id

        # Trust should be gone or in Deleted state
        trusts = ds.describe_trusts(DirectoryId=directory)
        active = [
            t
            for t in trusts["Trusts"]
            if t["TrustId"] == trust_id and t.get("TrustState") not in ("Deleted", "Deleting")
        ]
        assert len(active) == 0

    def test_create_trust_nonexistent_directory(self, ds):
        """CreateTrust on a nonexistent directory raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc_info:
            ds.create_trust(
                DirectoryId="d-0000000000",
                RemoteDomainName="x.example.com",
                TrustPassword="TrustP@ss!",
                TrustDirection="One-Way: Outgoing",
            )
        assert exc_info.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDsLogSubscriptionLifecycle:
    """Test log subscription create, list, and delete lifecycle."""

    def test_create_and_list_log_subscription(self, ds, directory):
        """CreateLogSubscription creates a subscription visible via ListLogSubscriptions."""
        ds.create_log_subscription(
            DirectoryId=directory,
            LogGroupName="/aws/ds/test-log-group",
        )

        try:
            resp = ds.list_log_subscriptions(DirectoryId=directory)
            assert len(resp["LogSubscriptions"]) == 1
            sub = resp["LogSubscriptions"][0]
            assert sub["DirectoryId"] == directory
            assert sub["LogGroupName"] == "/aws/ds/test-log-group"
        finally:
            ds.delete_log_subscription(DirectoryId=directory)

    def test_delete_log_subscription(self, ds, directory):
        """DeleteLogSubscription removes the subscription."""
        ds.create_log_subscription(
            DirectoryId=directory,
            LogGroupName="/aws/ds/del-log-group",
        )

        del_resp = ds.delete_log_subscription(DirectoryId=directory)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Should be gone
        resp = ds.list_log_subscriptions(DirectoryId=directory)
        assert len(resp["LogSubscriptions"]) == 0

    def test_create_duplicate_log_subscription_raises(self, ds, directory):
        """Creating a second log subscription raises EntityAlreadyExistsException."""
        ds.create_log_subscription(
            DirectoryId=directory,
            LogGroupName="/aws/ds/dup-group",
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                ds.create_log_subscription(
                    DirectoryId=directory,
                    LogGroupName="/aws/ds/dup-group-2",
                )
            assert exc_info.value.response["Error"]["Code"] == "EntityAlreadyExistsException"
        finally:
            ds.delete_log_subscription(DirectoryId=directory)

    def test_create_log_subscription_nonexistent_directory(self, ds):
        """CreateLogSubscription on nonexistent directory raises error."""
        with pytest.raises(ClientError) as exc_info:
            ds.create_log_subscription(
                DirectoryId="d-0000000000",
                LogGroupName="/aws/ds/bogus",
            )
        assert exc_info.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDsSsoWithAlias:
    """Test EnableSso after creating an alias."""

    def test_enable_sso_with_alias(self, ds, directory):
        """EnableSso succeeds after an alias is set on the directory."""
        alias_name = f"ssoalias{uuid.uuid4().hex[:8]}"
        ds.create_alias(DirectoryId=directory, Alias=alias_name)

        resp = ds.enable_sso(DirectoryId=directory)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestDsConnectDirectory:
    """Test AD Connector directory lifecycle."""

    def test_create_connect_directory(self, ds, ec2):
        """connect_directory creates an ADConnector type directory."""
        vpc = ec2.create_vpc(CidrBlock="10.83.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.83.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.83.2.0/24", AvailabilityZone="us-east-1b"
        )
        sid1, sid2 = s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]
        resp = ds.connect_directory(
            Name="conn.example.com",
            Password="P@ssw0rd!",
            Size="Small",
            ConnectSettings={
                "VpcId": vpc_id,
                "SubnetIds": [sid1, sid2],
                "CustomerDnsIps": ["10.0.0.1"],
                "CustomerUserName": "admin",
            },
        )
        dir_id = resp["DirectoryId"]
        assert dir_id.startswith("d-")
        try:
            desc = ds.describe_directories(DirectoryIds=[dir_id])
            d = desc["DirectoryDescriptions"][0]
            assert d["DirectoryId"] == dir_id
            assert d["Type"] == "ADConnector"
            assert d["Name"] == "conn.example.com"
            assert d["Size"] == "Small"
        finally:
            ds.delete_directory(DirectoryId=dir_id)
            for sid in [sid1, sid2]:
                try:
                    ec2.delete_subnet(SubnetId=sid)
                except ClientError:
                    pass  # best-effort cleanup
            try:
                ec2.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass  # best-effort cleanup

    def test_delete_connect_directory(self, ds, ec2):
        """delete_directory removes an ADConnector directory."""
        vpc = ec2.create_vpc(CidrBlock="10.84.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.84.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.84.2.0/24", AvailabilityZone="us-east-1b"
        )
        sid1, sid2 = s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]
        resp = ds.connect_directory(
            Name="delconn.example.com",
            Password="P@ssw0rd!",
            Size="Small",
            ConnectSettings={
                "VpcId": vpc_id,
                "SubnetIds": [sid1, sid2],
                "CustomerDnsIps": ["10.0.0.1"],
                "CustomerUserName": "admin",
            },
        )
        dir_id = resp["DirectoryId"]
        del_resp = ds.delete_directory(DirectoryId=dir_id)
        assert del_resp["DirectoryId"] == dir_id

        with pytest.raises(ClientError) as exc_info:
            ds.describe_directories(DirectoryIds=[dir_id])
        assert exc_info.value.response["Error"]["Code"] == "EntityDoesNotExistException"

        for sid in [sid1, sid2]:
            try:
                ec2.delete_subnet(SubnetId=sid)
            except ClientError:
                pass  # best-effort cleanup
        try:
            ec2.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass  # best-effort cleanup


class TestDsTagErrors:
    """Test tag operation error handling."""

    def test_add_tags_nonexistent_directory(self, ds):
        """add_tags_to_resource on nonexistent directory raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc_info:
            ds.add_tags_to_resource(
                ResourceId="d-0000000000",
                Tags=[{"Key": "a", "Value": "b"}],
            )
        assert exc_info.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_list_tags_nonexistent_directory(self, ds):
        """list_tags_for_resource on nonexistent directory raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc_info:
            ds.list_tags_for_resource(ResourceId="d-0000000000")
        assert exc_info.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_remove_tags_nonexistent_directory(self, ds):
        """remove_tags_from_resource on nonexistent directory raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc_info:
            ds.remove_tags_from_resource(ResourceId="d-0000000000", TagKeys=["a"])
        assert exc_info.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDsTagOverwrite:
    """Test tag overwrite behavior."""

    def test_overwrite_tag_value(self, ds, directory):
        """add_tags_to_resource overwrites existing tag values."""
        ds.add_tags_to_resource(
            ResourceId=directory,
            Tags=[{"Key": "env", "Value": "dev"}],
        )
        ds.add_tags_to_resource(
            ResourceId=directory,
            Tags=[{"Key": "env", "Value": "prod"}],
        )
        resp = ds.list_tags_for_resource(ResourceId=directory)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["env"] == "prod"

    def test_add_multiple_tags_incremental(self, ds, directory):
        """Multiple add_tags_to_resource calls accumulate tags."""
        ds.add_tags_to_resource(
            ResourceId=directory,
            Tags=[{"Key": "key1", "Value": "val1"}],
        )
        ds.add_tags_to_resource(
            ResourceId=directory,
            Tags=[{"Key": "key2", "Value": "val2"}],
        )
        resp = ds.list_tags_for_resource(ResourceId=directory)
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags["key1"] == "val1"
        assert tags["key2"] == "val2"


class TestDsDeleteErrors:
    """Test delete operation errors."""

    def test_delete_nonexistent_directory(self, ds):
        """delete_directory on nonexistent directory raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc_info:
            ds.delete_directory(DirectoryId="d-0000000000")
        assert exc_info.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDsTagPagination:
    """Test tag listing with pagination."""

    def test_list_tags_with_limit(self, ds, directory):
        """ListTagsForResource with Limit returns paginated results."""
        ds.add_tags_to_resource(
            ResourceId=directory,
            Tags=[{"Key": f"k{i}", "Value": f"v{i}"} for i in range(5)],
        )
        resp = ds.list_tags_for_resource(ResourceId=directory, Limit=2)
        assert len(resp["Tags"]) == 2
        assert "NextToken" in resp

    def test_list_tags_paginate_all(self, ds, directory):
        """ListTagsForResource paginates through all tags."""
        ds.add_tags_to_resource(
            ResourceId=directory,
            Tags=[{"Key": f"page{i}", "Value": f"val{i}"} for i in range(5)],
        )
        all_tags = []
        resp = ds.list_tags_for_resource(ResourceId=directory, Limit=2)
        all_tags.extend(resp["Tags"])
        while "NextToken" in resp:
            resp = ds.list_tags_for_resource(
                ResourceId=directory, Limit=2, NextToken=resp["NextToken"]
            )
            all_tags.extend(resp["Tags"])
        tag_keys = {t["Key"] for t in all_tags}
        for i in range(5):
            assert f"page{i}" in tag_keys


class TestDsGetDirectoryLimitsDetail:
    """Test GetDirectoryLimits returns detailed limit fields."""

    def test_get_directory_limits_fields(self, ds):
        """GetDirectoryLimits response contains expected limit fields."""
        resp = ds.get_directory_limits()
        limits = resp["DirectoryLimits"]
        # Should have cloud-only and connected directory limits
        assert "CloudOnlyDirectoriesLimit" in limits
        assert "CloudOnlyDirectoriesCurrentCount" in limits
        assert "ConnectedDirectoriesLimit" in limits
        assert "ConnectedDirectoriesCurrentCount" in limits


class TestDsDescribeWithDirectory:
    """Tests for describe operations that require a directory."""

    def test_describe_event_topics_empty(self, ds):
        """DescribeEventTopics with no filter returns EventTopics list."""
        resp = ds.describe_event_topics()
        assert "EventTopics" in resp
        assert isinstance(resp["EventTopics"], list)

    def test_describe_event_topics_for_directory(self, ds, directory):
        """DescribeEventTopics filtered by DirectoryId returns list."""
        resp = ds.describe_event_topics(DirectoryId=directory)
        assert "EventTopics" in resp
        assert isinstance(resp["EventTopics"], list)

    def test_describe_snapshots_empty(self, ds):
        """DescribeSnapshots with no filter returns Snapshots list."""
        resp = ds.describe_snapshots()
        assert "Snapshots" in resp
        assert isinstance(resp["Snapshots"], list)

    def test_describe_snapshots_for_directory(self, ds, directory):
        """DescribeSnapshots filtered by DirectoryId returns list."""
        resp = ds.describe_snapshots(DirectoryId=directory)
        assert "Snapshots" in resp
        assert isinstance(resp["Snapshots"], list)

    def test_describe_conditional_forwarders_nonexistent(self, ds):
        """DescribeConditionalForwarders for nonexistent dir raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc:
            ds.describe_conditional_forwarders(DirectoryId="d-0000000000")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_describe_conditional_forwarders_for_directory(self, ds, directory):
        """DescribeConditionalForwarders for a real directory returns list."""
        resp = ds.describe_conditional_forwarders(DirectoryId=directory)
        assert "ConditionalForwarders" in resp
        assert isinstance(resp["ConditionalForwarders"], list)

    def test_describe_domain_controllers_nonexistent(self, ds):
        """DescribeDomainControllers for nonexistent dir raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc:
            ds.describe_domain_controllers(DirectoryId="d-0000000000")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_describe_regions_nonexistent(self, ds):
        """DescribeRegions for nonexistent dir raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc:
            ds.describe_regions(DirectoryId="d-0000000000")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_describe_shared_directories_nonexistent(self, ds):
        """DescribeSharedDirectories for nonexistent dir raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc:
            ds.describe_shared_directories(OwnerDirectoryId="d-0000000000")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_describe_shared_directories_for_directory(self, ds, directory):
        """DescribeSharedDirectories for a real directory returns list."""
        resp = ds.describe_shared_directories(OwnerDirectoryId=directory)
        assert "SharedDirectories" in resp
        assert isinstance(resp["SharedDirectories"], list)

    def test_describe_update_directory_nonexistent(self, ds):
        """DescribeUpdateDirectory for nonexistent dir raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc:
            ds.describe_update_directory(DirectoryId="d-0000000000", UpdateType="OS")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_get_snapshot_limits_nonexistent(self, ds):
        """GetSnapshotLimits for nonexistent dir raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc:
            ds.get_snapshot_limits(DirectoryId="d-0000000000")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_get_snapshot_limits_for_directory(self, ds, directory):
        """GetSnapshotLimits for a real directory returns SnapshotLimits."""
        resp = ds.get_snapshot_limits(DirectoryId=directory)
        assert "SnapshotLimits" in resp
        limits = resp["SnapshotLimits"]
        assert "ManualSnapshotsLimit" in limits
        assert "ManualSnapshotsCurrentCount" in limits

    def test_describe_client_authentication_settings_nonexistent(self, ds):
        """DescribeClientAuthenticationSettings for nonexistent dir raises error."""
        with pytest.raises(ClientError) as exc:
            ds.describe_client_authentication_settings(DirectoryId="d-0000000000")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_describe_certificate_nonexistent(self, ds):
        """DescribeCertificate for nonexistent dir raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc:
            ds.describe_certificate(DirectoryId="d-0000000000", CertificateId="c-0000000000")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_describe_ad_assessment_nonexistent(self, ds):
        """DescribeADAssessment for nonexistent assessment raises error."""
        with pytest.raises(ClientError) as exc:
            ds.describe_ad_assessment(AssessmentId="a-0000000000")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_describe_ca_enrollment_policy_nonexistent(self, ds):
        """DescribeCAEnrollmentPolicy for nonexistent dir raises error."""
        with pytest.raises(ClientError) as exc:
            ds.describe_ca_enrollment_policy(DirectoryId="d-0000000000")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDsIpRoutes:
    """Tests for ListIpRoutes operation."""

    def test_list_ip_routes_nonexistent(self, ds):
        """ListIpRoutes for nonexistent directory raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc:
            ds.list_ip_routes(DirectoryId="d-0000000000")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_list_ip_routes_empty(self, ds, directory):
        """ListIpRoutes for a real directory returns an empty IpRoutesInfo list."""
        resp = ds.list_ip_routes(DirectoryId=directory)
        assert "IpRoutesInfo" in resp
        assert isinstance(resp["IpRoutesInfo"], list)


class TestDsSchemaExtensions:
    """Tests for ListSchemaExtensions operation."""

    def test_list_schema_extensions_nonexistent(self, ds):
        """ListSchemaExtensions for nonexistent directory raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc:
            ds.list_schema_extensions(DirectoryId="d-0000000000")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_list_schema_extensions_empty(self, ds, directory):
        """ListSchemaExtensions for a real directory returns empty SchemaExtensionsInfo."""
        resp = ds.list_schema_extensions(DirectoryId=directory)
        assert "SchemaExtensionsInfo" in resp
        assert isinstance(resp["SchemaExtensionsInfo"], list)


class TestDsSnapshotOps:
    """Tests for DS snapshot operations."""

    def test_create_and_delete_snapshot(self, ds, directory):
        """CreateSnapshot creates a snapshot, DeleteSnapshot removes it."""
        create_resp = ds.create_snapshot(DirectoryId=directory, Name="test-snap")
        assert "SnapshotId" in create_resp
        snap_id = create_resp["SnapshotId"]

        del_resp = ds.delete_snapshot(SnapshotId=snap_id)
        assert "SnapshotId" in del_resp
        assert del_resp["SnapshotId"] == snap_id

    def test_delete_snapshot_nonexistent(self, ds):
        """DeleteSnapshot with fake ID raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc:
            ds.delete_snapshot(SnapshotId="s-0000000000")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDsConditionalForwarderOps:
    """Tests for DS conditional forwarder operations."""

    def test_create_and_delete_conditional_forwarder(self, ds, directory):
        """Create and delete a conditional forwarder."""
        create_resp = ds.create_conditional_forwarder(
            DirectoryId=directory,
            RemoteDomainName="remote.example.com",
            DnsIpAddrs=["10.0.0.1", "10.0.0.2"],
        )
        assert create_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        del_resp = ds.delete_conditional_forwarder(
            DirectoryId=directory,
            RemoteDomainName="remote.example.com",
        )
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_conditional_forwarder(self, ds, directory):
        """UpdateConditionalForwarder updates DNS IPs."""
        ds.create_conditional_forwarder(
            DirectoryId=directory,
            RemoteDomainName="upd.example.com",
            DnsIpAddrs=["10.0.0.1"],
        )
        try:
            upd_resp = ds.update_conditional_forwarder(
                DirectoryId=directory,
                RemoteDomainName="upd.example.com",
                DnsIpAddrs=["10.0.0.3", "10.0.0.4"],
            )
            assert upd_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            ds.delete_conditional_forwarder(
                DirectoryId=directory,
                RemoteDomainName="upd.example.com",
            )

    def test_delete_conditional_forwarder_nonexistent(self, ds, directory):
        """DeleteConditionalForwarder for unknown domain raises error."""
        with pytest.raises(ClientError) as exc:
            ds.delete_conditional_forwarder(
                DirectoryId=directory,
                RemoteDomainName="nonexistent.example.com",
            )
        err = exc.value.response["Error"]["Code"]
        assert err in ("EntityDoesNotExistException", "ClientException")


class TestDsEventTopicOps:
    """Tests for DS event topic operations."""

    def test_register_and_deregister_event_topic(self, ds, directory):
        """RegisterEventTopic and DeregisterEventTopic lifecycle."""
        reg_resp = ds.register_event_topic(
            DirectoryId=directory,
            TopicName="ds-test-topic",
        )
        assert reg_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        dereg_resp = ds.deregister_event_topic(
            DirectoryId=directory,
            TopicName="ds-test-topic",
        )
        assert dereg_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestDsCertificateOps:
    """Tests for DS certificate operations."""

    def test_list_certificates(self, ds, directory):
        """ListCertificates returns a list for the directory."""
        resp = ds.list_certificates(DirectoryId=directory)
        assert "CertificatesInfo" in resp
        assert isinstance(resp["CertificatesInfo"], list)

    def test_list_certificates_nonexistent(self, ds):
        """ListCertificates for nonexistent directory raises error."""
        with pytest.raises(ClientError) as exc:
            ds.list_certificates(DirectoryId="d-0000000000")
        err = exc.value.response["Error"]["Code"]
        assert err in ("EntityDoesNotExistException", "DirectoryDoesNotExistException")


class TestDsIpRouteOps:
    """Tests for DS IP route add/remove operations."""

    def test_add_and_remove_ip_routes(self, ds, directory):
        """AddIpRoutes adds routes, RemoveIpRoutes removes them."""
        add_resp = ds.add_ip_routes(
            DirectoryId=directory,
            IpRoutes=[
                {"CidrIp": "203.0.113.0/24", "Description": "Test route"},
            ],
        )
        assert add_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        remove_resp = ds.remove_ip_routes(
            DirectoryId=directory,
            CidrIps=["203.0.113.0/24"],
        )
        assert remove_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_add_ip_routes_nonexistent_dir(self, ds):
        """AddIpRoutes for nonexistent directory raises error."""
        with pytest.raises(ClientError) as exc:
            ds.add_ip_routes(
                DirectoryId="d-0000000000",
                IpRoutes=[{"CidrIp": "203.0.113.0/24"}],
            )
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDsRadiusOps:
    """Tests for DS RADIUS operations."""

    def test_disable_radius_nonexistent(self, ds):
        """DisableRadius for nonexistent directory raises error."""
        with pytest.raises(ClientError) as exc:
            ds.disable_radius(DirectoryId="d-0000000000")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_enable_radius(self, ds, directory):
        """EnableRadius sets RADIUS settings for the directory."""
        resp = ds.enable_radius(
            DirectoryId=directory,
            RadiusSettings={
                "RadiusServers": ["10.0.0.100"],
                "RadiusPort": 1812,
                "RadiusTimeout": 10,
                "RadiusRetries": 3,
                "SharedSecret": "s3cr3tP@ss",
                "AuthenticationProtocol": "PAP",
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_radius(self, ds, directory):
        """UpdateRadius updates RADIUS settings."""
        # First enable
        ds.enable_radius(
            DirectoryId=directory,
            RadiusSettings={
                "RadiusServers": ["10.0.0.100"],
                "RadiusPort": 1812,
                "RadiusTimeout": 10,
                "RadiusRetries": 3,
                "SharedSecret": "s3cr3tP@ss",
                "AuthenticationProtocol": "PAP",
            },
        )
        upd_resp = ds.update_radius(
            DirectoryId=directory,
            RadiusSettings={
                "RadiusServers": ["10.0.0.200"],
                "RadiusPort": 1812,
                "RadiusTimeout": 15,
                "RadiusRetries": 5,
                "SharedSecret": "n3ws3cr3tP@ss",
                "AuthenticationProtocol": "PAP",
            },
        )
        assert upd_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disable_radius(self, ds, directory):
        """DisableRadius after EnableRadius succeeds."""
        ds.enable_radius(
            DirectoryId=directory,
            RadiusSettings={
                "RadiusServers": ["10.0.0.100"],
                "RadiusPort": 1812,
                "RadiusTimeout": 10,
                "RadiusRetries": 3,
                "SharedSecret": "s3cr3tP@ss",
                "AuthenticationProtocol": "PAP",
            },
        )
        resp = ds.disable_radius(DirectoryId=directory)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestDsClientAuthOps:
    """Tests for DS client authentication operations."""

    def test_enable_client_authentication_nonexistent(self, ds):
        """EnableClientAuthentication for nonexistent dir raises error."""
        with pytest.raises(ClientError) as exc:
            ds.enable_client_authentication(
                DirectoryId="d-0000000000",
                Type="SmartCard",
            )
        assert exc.value.response["Error"]["Code"] in (
            "EntityDoesNotExistException",
            "DirectoryDoesNotExistException",
        )

    def test_disable_client_authentication_nonexistent(self, ds):
        """DisableClientAuthentication for nonexistent dir raises error."""
        with pytest.raises(ClientError) as exc:
            ds.disable_client_authentication(
                DirectoryId="d-0000000000",
                Type="SmartCard",
            )
        assert exc.value.response["Error"]["Code"] in (
            "EntityDoesNotExistException",
            "DirectoryDoesNotExistException",
        )


class TestDsComputerOps:
    """Tests for DS computer operations."""

    def test_create_computer(self, ds, directory):
        """CreateComputer creates a computer account."""
        resp = ds.create_computer(
            DirectoryId=directory,
            ComputerName="TESTPC01",
            Password="C0mput3rP@ss!",
        )
        assert "Computer" in resp
        assert resp["Computer"]["ComputerName"] == "TESTPC01"

    def test_create_computer_nonexistent_dir(self, ds):
        """CreateComputer for nonexistent directory raises error."""
        with pytest.raises(ClientError) as exc:
            ds.create_computer(
                DirectoryId="d-0000000000",
                ComputerName="TESTPC02",
                Password="C0mput3rP@ss!",
            )
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDsRestoreFromSnapshot:
    """Tests for RestoreFromSnapshot operation."""

    def test_restore_from_nonexistent_snapshot(self, ds):
        """RestoreFromSnapshot with fake snapshot ID raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc:
            ds.restore_from_snapshot(SnapshotId="s-0000000000")
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_restore_from_snapshot(self, ds, directory):
        """RestoreFromSnapshot with a real snapshot succeeds."""
        snap = ds.create_snapshot(DirectoryId=directory, Name="restore-test")
        snap_id = snap["SnapshotId"]
        try:
            resp = ds.restore_from_snapshot(SnapshotId=snap_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            ds.delete_snapshot(SnapshotId=snap_id)


class TestDsCertificateRegisterDeregister:
    """Tests for RegisterCertificate and DeregisterCertificate operations."""

    def test_register_certificate_nonexistent_dir(self, ds):
        """RegisterCertificate for nonexistent directory raises error."""
        with pytest.raises(ClientError) as exc:
            ds.register_certificate(
                DirectoryId="d-0000000000",
                CertificateData="-----BEGIN CERTIFICATE-----\nMIIBx\n-----END CERTIFICATE-----",
            )
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_deregister_certificate_nonexistent_dir(self, ds):
        """DeregisterCertificate for nonexistent directory raises error."""
        with pytest.raises(ClientError) as exc:
            ds.deregister_certificate(
                DirectoryId="d-0000000000",
                CertificateId="c-0000000000",
            )
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDsResetPassword:
    """Tests for ResetUserPassword operation."""

    def test_reset_user_password(self, ds, directory):
        """ResetUserPassword for a valid directory returns 200."""
        resp = ds.reset_user_password(
            DirectoryId=directory,
            UserName="Administrator",
            NewPassword="N3wP@ssw0rd!",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_reset_user_password_nonexistent_dir(self, ds):
        """ResetUserPassword for nonexistent directory raises error."""
        with pytest.raises(ClientError) as exc:
            ds.reset_user_password(
                DirectoryId="d-0000000000",
                UserName="Administrator",
                NewPassword="N3wP@ssw0rd!",
            )
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"


class TestDsAdditionalOps:
    """Tests for additional DS operations."""

    def test_describe_ldaps_settings_nonexistent(self, ds):
        """DescribeLDAPSSettings with fake DirectoryId raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc:
            ds.describe_ldaps_settings(DirectoryId="d-0000000000")
        assert exc.value.response["Error"]["Code"] in (
            "EntityDoesNotExistException",
            "UnsupportedOperationException",
        )

    def test_create_microsoft_ad_lifecycle(self, ds, ec2):
        """CreateMicrosoftAD with VPC infrastructure creates and describes successfully."""
        vpc = ec2.create_vpc(CidrBlock="10.90.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.90.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.90.2.0/24", AvailabilityZone="us-east-1b"
        )
        sid1, sid2 = s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]
        resp = ds.create_microsoft_ad(
            Name="newmsad.example.com",
            Password="P@ssw0rd!",
            VpcSettings={"VpcId": vpc_id, "SubnetIds": [sid1, sid2]},
        )
        dir_id = resp["DirectoryId"]
        assert dir_id.startswith("d-")
        try:
            desc = ds.describe_directories(DirectoryIds=[dir_id])
            assert len(desc["DirectoryDescriptions"]) == 1
            assert desc["DirectoryDescriptions"][0]["Type"] == "MicrosoftAD"
        finally:
            ds.delete_directory(DirectoryId=dir_id)
            for sid in [sid1, sid2]:
                try:
                    ec2.delete_subnet(SubnetId=sid)
                except ClientError:
                    pass  # best-effort cleanup
            try:
                ec2.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass  # best-effort cleanup


class TestDSUpdateOps:
    """Tests for directory update operations."""

    def test_describe_update_directory(self, ds, directory):
        """DescribeUpdateDirectory lists directory updates."""
        resp = ds.describe_update_directory(
            DirectoryId=directory,
            UpdateType="OS",
        )
        assert "UpdateActivities" in resp


class TestDsSchemaExtensionOps:
    """Tests for schema extension start and cancel operations."""

    def test_start_schema_extension_nonexistent(self, ds):
        """StartSchemaExtension with fake directory raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc:
            ds.start_schema_extension(
                DirectoryId="d-0000000000",
                CreateSnapshotBeforeSchemaExtension=True,
                LdifContent="dn: CN=test,DC=example,DC=com\nchangetype: add\nobjectClass: top",
                Description="Test extension",
            )
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"

    def test_cancel_schema_extension_nonexistent(self, ds):
        """CancelSchemaExtension with fake directory raises EntityDoesNotExistException."""
        with pytest.raises(ClientError) as exc:
            ds.cancel_schema_extension(
                DirectoryId="d-0000000000",
                SchemaExtensionId="e-0000000000",
            )
        assert exc.value.response["Error"]["Code"] == "EntityDoesNotExistException"


@pytest.fixture
def msad_directory(ds, ec2):
    """Create a MicrosoftAD directory with VPC infrastructure, clean up after test."""
    vpc = ec2.create_vpc(CidrBlock="10.90.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    subnet1 = ec2.create_subnet(
        VpcId=vpc_id, CidrBlock="10.90.1.0/24", AvailabilityZone="us-east-1a"
    )
    subnet2 = ec2.create_subnet(
        VpcId=vpc_id, CidrBlock="10.90.2.0/24", AvailabilityZone="us-east-1b"
    )
    sid1 = subnet1["Subnet"]["SubnetId"]
    sid2 = subnet2["Subnet"]["SubnetId"]

    resp = ds.create_microsoft_ad(
        Name="msadfix.example.com",
        Password="P@ssw0rd!",
        VpcSettings={"VpcId": vpc_id, "SubnetIds": [sid1, sid2]},
    )
    dir_id = resp["DirectoryId"]

    yield dir_id

    # Cleanup
    try:
        ds.delete_directory(DirectoryId=dir_id)
    except ClientError:
        pass  # best-effort cleanup
    for sid in [sid1, sid2]:
        try:
            ec2.delete_subnet(SubnetId=sid)
        except ClientError:
            pass  # best-effort cleanup
    try:
        ec2.delete_vpc(VpcId=vpc_id)
    except ClientError:
        pass  # best-effort cleanup


class TestDsLDAPSMicrosoftAD:
    """Test LDAPS operations on MicrosoftAD directories."""

    def test_enable_ldaps_msad(self, ds, msad_directory):
        """EnableLDAPS on MicrosoftAD succeeds."""
        resp = ds.enable_ldaps(DirectoryId=msad_directory, Type="Client")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disable_ldaps_msad(self, ds, msad_directory):
        """DisableLDAPS on MicrosoftAD succeeds."""
        resp = ds.disable_ldaps(DirectoryId=msad_directory, Type="Client")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_ldaps_settings_msad(self, ds, msad_directory):
        """DescribeLDAPSSettings on MicrosoftAD returns settings info."""
        resp = ds.describe_ldaps_settings(DirectoryId=msad_directory, Type="Client")
        assert "LDAPSSettingsInfo" in resp
