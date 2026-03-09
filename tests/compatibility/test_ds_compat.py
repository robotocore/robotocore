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
        pass
    for sid in [sid1, sid2]:
        try:
            ec2.delete_subnet(SubnetId=sid)
        except ClientError:
            pass
    try:
        ec2.delete_vpc(VpcId=vpc_id)
    except ClientError:
        pass


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
                    pass
            try:
                ec2.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass

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
                    pass
            try:
                ec2.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass


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
                    pass
            try:
                ec2.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass


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
                    pass
            try:
                ec2.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass

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
                pass
        try:
            ec2.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass


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
